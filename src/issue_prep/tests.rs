use super::{
    artifacts::{
        ARTIFACT_ORDER, CANDIDATE_FILE, INPUT_FILE, MANIFEST_FILE, PREPARE_FILES, REFINE_FILES,
        REVIEW_FILES,
    },
    contract::{IssueDraft, ModelReview, ReviewVerdict},
    *,
};
use crate::model::ModelBlock;
use platonic_core::ModelUsage;
use serde_json::json;
use std::{fs, path::Path};

const INPUT: &str = "The command is unclear and needs a bounded issue.";

fn config() -> PipelineConfig {
    PipelineConfig {
        provider_kind: "open_ai".into(),
        provider_base_url: "https://example.invalid/v1".into(),
        model: "test-model".into(),
        max_output_tokens: 1_024,
    }
}

fn options(root: &Path, run_dir: &Path) -> IssuePrepOptions {
    options_with_input(root, run_dir, INPUT)
}

fn options_with_input(root: &Path, run_dir: &Path, input: &str) -> IssuePrepOptions {
    IssuePrepOptions {
        workspace_root: root.to_path_buf(),
        config_path: None,
        run_dir: run_dir.to_path_buf(),
        input: input.into(),
    }
}

fn response(text: &str) -> ModelResponse {
    ModelResponse {
        content: vec![ModelBlock::Text { text: text.into() }],
        stop: ModelStop::EndTurn,
        usage: ModelUsage {
            input_tokens: 10,
            output_tokens: 20,
        },
    }
}

fn issue(open_questions: &[&str]) -> IssueDraft {
    IssueDraft {
        title: "Add fixed issue preparation".into(),
        problem: "Issue requests are not normalized before implementation.".into(),
        current_behavior: "Agents interpret free-form issue text directly.".into(),
        expected_behavior: "A fixed preparation pipeline emits one bounded candidate.".into(),
        target_repo_surface: "plato-agent issue-prep CLI".into(),
        scope: vec!["Run fixed preparation stages.".into()],
        non_goals: vec!["No configurable workflow graph.".into()],
        acceptance_criteria: vec!["A candidate contains every required section.".into()],
        proof: vec!["Focused pipeline tests pass.".into()],
        open_questions: open_questions
            .iter()
            .map(|question| (*question).into())
            .collect(),
    }
}

fn issue_json(open_questions: &[&str]) -> String {
    serde_json::to_string(&issue(open_questions)).unwrap()
}

fn review_json(verdict: ReviewVerdict, findings: &[&str]) -> String {
    serde_json::to_string(&ModelReview {
        verdict,
        findings: findings.iter().map(|finding| (*finding).into()).collect(),
    })
    .unwrap()
}

fn candidate_responses() -> [ModelResponse; 3] {
    [
        response(&issue_json(&["Choose the exact command name."])),
        response(&issue_json(&[])),
        response(&review_json(ReviewVerdict::Candidate, &[])),
    ]
}

fn read_validation(run_dir: &Path, name: &str) -> ValidationRecord {
    serde_json::from_slice(&fs::read(run_dir.join(name)).unwrap()).unwrap()
}

#[test]
fn fixed_pipeline_writes_ordered_artifacts_and_identifies_validation_kind() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");
    let responses = candidate_responses();
    let mut calls = 0;

    let outcome = run_issue_prep_with(options(root.path(), &run_dir), config(), |request| {
        assert!(request.tools.is_empty());
        let prompt = request.messages[0]
            .content
            .iter()
            .find_map(|block| match block {
                ModelBlock::Text { text } => Some(text),
                _ => None,
            })
            .unwrap();
        assert!(prompt.starts_with("# Stage:"));
        let response = responses[calls].clone();
        calls += 1;
        Ok(response)
    })
    .unwrap();

    assert_eq!(calls, 3);
    assert!(matches!(
        outcome,
        IssuePrepOutcome::Candidate { ref markdown }
            if markdown.contains("## Acceptance Criteria")
                && markdown.contains("## Proof")
    ));
    for name in ARTIFACT_ORDER {
        assert!(run_dir.join(name).is_file(), "missing {name}");
    }
    assert_eq!(REVIEW_FILES.prompt, "30-review.prompt.md");
    assert_eq!(REVIEW_FILES.result, "31-review.result.json");
    assert_eq!(REVIEW_FILES.validation, "32-review.validation.json");
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(run_dir.join(MANIFEST_FILE)).unwrap()).unwrap();
    assert_eq!(manifest.as_object().unwrap().len(), 7);
    assert_eq!(fs::read_to_string(run_dir.join(INPUT_FILE)).unwrap(), INPUT);
    assert!(
        fs::read_to_string(run_dir.join(PREPARE_FILES.prompt))
            .unwrap()
            .contains(INPUT)
    );

    assert_eq!(
        read_validation(&run_dir, PREPARE_FILES.validation).validation_kind,
        ValidationKind::Structural
    );
    assert_eq!(
        read_validation(&run_dir, REFINE_FILES.validation).validation_kind,
        ValidationKind::Structural
    );
    assert_eq!(
        read_validation(&run_dir, REVIEW_FILES.validation).validation_kind,
        ValidationKind::ModelReview
    );
}

#[test]
fn malformed_prepare_result_blocks_before_refinement() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");

    let outcome = run_issue_prep_with(options(root.path(), &run_dir), config(), |_| {
        Ok(response("not json"))
    })
    .unwrap();

    assert!(matches!(
        outcome,
        IssuePrepOutcome::Blocked { ref stage, .. } if stage == "prepare"
    ));
    assert!(run_dir.join(PREPARE_FILES.validation).is_file());
    assert!(!run_dir.join(REFINE_FILES.prompt).exists());
}

#[test]
fn missing_required_field_blocks_structurally() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");
    let mut invalid = serde_json::to_value(issue(&[])).unwrap();
    invalid.as_object_mut().unwrap().remove("proof");

    let outcome = run_issue_prep_with(options(root.path(), &run_dir), config(), |_| {
        Ok(response(&invalid.to_string()))
    })
    .unwrap();

    assert!(matches!(
        outcome,
        IssuePrepOutcome::Blocked { ref stage, ref reasons }
            if stage == "prepare"
                && reasons.iter().any(|reason| reason.contains("missing field `proof`"))
    ));
    assert_eq!(
        read_validation(&run_dir, PREPARE_FILES.validation).validation_kind,
        ValidationKind::Structural
    );
}

#[test]
fn empty_required_list_blocks_structurally() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");
    let mut invalid = serde_json::to_value(issue(&[])).unwrap();
    invalid["scope"] = json!([]);

    let outcome = run_issue_prep_with(options(root.path(), &run_dir), config(), |_| {
        Ok(response(&invalid.to_string()))
    })
    .unwrap();

    assert!(matches!(
        outcome,
        IssuePrepOutcome::Blocked { ref stage, ref reasons }
            if stage == "prepare"
                && reasons == &["scope must contain at least one item"]
    ));
    let validation = read_validation(&run_dir, PREPARE_FILES.validation);
    assert_eq!(validation.validation_kind, ValidationKind::Structural);
    assert_eq!(validation.status, ValidationStatus::Blocked);
}

#[test]
fn unresolved_refinement_questions_block_before_model_review() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");
    let responses = [
        response(&issue_json(&[])),
        response(&issue_json(&["Which command owns this behavior?"])),
    ];
    let mut calls = 0;

    let outcome = run_issue_prep_with(options(root.path(), &run_dir), config(), |_| {
        let response = responses[calls].clone();
        calls += 1;
        Ok(response)
    })
    .unwrap();

    assert_eq!(calls, 2);
    assert!(matches!(
        outcome,
        IssuePrepOutcome::Blocked { ref stage, ref reasons }
            if stage == "refine"
                && reasons == &["open_questions must be empty after refinement"]
    ));
    assert!(!run_dir.join(REVIEW_FILES.prompt).exists());
}

#[test]
fn failed_run_is_preserved_and_retry_uses_a_new_directory() {
    let root = tempfile::tempdir().unwrap();
    let failed_dir = root.path().join("failed");
    let mut calls = 0;

    let error = run_issue_prep_with(options(root.path(), &failed_dir), config(), |_| {
        calls += 1;
        if calls == 1 {
            Ok(response(&issue_json(&[])))
        } else {
            Err(AppError::Provider("offline".into()))
        }
    })
    .unwrap_err();

    assert!(matches!(error, AppError::Provider(message) if message == "offline"));
    assert!(failed_dir.join(PREPARE_FILES.validation).is_file());
    assert!(failed_dir.join(REFINE_FILES.prompt).is_file());
    assert!(!failed_dir.join(REFINE_FILES.result).exists());

    let retry_dir = root.path().join("retry");
    let responses = candidate_responses();
    let mut retry_calls = 0;
    let outcome = run_issue_prep_with(options(root.path(), &retry_dir), config(), |_| {
        let response = responses[retry_calls].clone();
        retry_calls += 1;
        Ok(response)
    })
    .unwrap();

    assert_eq!(retry_calls, 3);
    assert!(matches!(outcome, IssuePrepOutcome::Candidate { .. }));
    assert!(failed_dir.join(REFINE_FILES.prompt).is_file());
}

#[test]
fn existing_or_partially_populated_directory_is_rejected_without_model_calls() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");
    fs::create_dir(&run_dir).unwrap();
    fs::write(run_dir.join("partial"), "evidence").unwrap();

    let error = run_issue_prep_with(options(root.path(), &run_dir), config(), |_| {
        panic!("existing directories must fail before provider work")
    })
    .unwrap_err();

    assert!(matches!(
        error,
        AppError::Config(message) if message.contains("requires a new run directory")
    ));
    assert_eq!(
        fs::read_to_string(run_dir.join("partial")).unwrap(),
        "evidence"
    );
}

#[test]
fn model_review_findings_block_candidate_generation() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");
    let responses = [
        response(&issue_json(&[])),
        response(&issue_json(&[])),
        response(&review_json(
            ReviewVerdict::Blocked,
            &["Proof does not cover interruption."],
        )),
    ];
    let mut calls = 0;

    let outcome = run_issue_prep_with(options(root.path(), &run_dir), config(), |_| {
        let response = responses[calls].clone();
        calls += 1;
        Ok(response)
    })
    .unwrap();

    assert!(matches!(
        outcome,
        IssuePrepOutcome::Blocked { ref stage, ref reasons }
            if stage == "review" && reasons == &["Proof does not cover interruption."]
    ));
    assert!(!run_dir.join(CANDIDATE_FILE).exists());
    let validation = read_validation(&run_dir, REVIEW_FILES.validation);
    assert_eq!(validation.validation_kind, ValidationKind::ModelReview);
    assert_eq!(validation.status, ValidationStatus::Blocked);
}

#[test]
fn vague_but_structurally_valid_issue_is_not_recorded_as_semantic_proof() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");
    let vague = IssueDraft {
        title: "TBD".into(),
        problem: "TBD".into(),
        current_behavior: "TBD".into(),
        expected_behavior: "TBD".into(),
        target_repo_surface: "TBD".into(),
        scope: vec!["TBD".into()],
        non_goals: vec!["TBD".into()],
        acceptance_criteria: vec!["TBD".into()],
        proof: vec!["TBD".into()],
        open_questions: vec![],
    };
    let vague = serde_json::to_string(&vague).unwrap();
    let responses = [
        response(&vague),
        response(&vague),
        response(&review_json(ReviewVerdict::Candidate, &[])),
    ];
    let mut calls = 0;

    let outcome = run_issue_prep_with(options(root.path(), &run_dir), config(), |_| {
        let response = responses[calls].clone();
        calls += 1;
        Ok(response)
    })
    .unwrap();

    assert!(matches!(outcome, IssuePrepOutcome::Candidate { .. }));
    assert_eq!(
        read_validation(&run_dir, PREPARE_FILES.validation).validation_kind,
        ValidationKind::Structural
    );
    assert_eq!(
        read_validation(&run_dir, REVIEW_FILES.validation).validation_kind,
        ValidationKind::ModelReview
    );
    assert!(
        fs::read_to_string(run_dir.join(REVIEW_FILES.prompt))
            .unwrap()
            .contains("not independent semantic proof")
    );
}

#[test]
fn github_url_remains_ordinary_unmodified_input() {
    let root = tempfile::tempdir().unwrap();
    let run_dir = root.path().join("run");
    let input = "Clarify https://github.com/referential-ai/plato-agent/issues/245.";
    let responses = candidate_responses();
    let mut calls = 0;

    run_issue_prep_with(
        options_with_input(root.path(), &run_dir, input),
        config(),
        |_| {
            let response = responses[calls].clone();
            calls += 1;
            Ok(response)
        },
    )
    .unwrap();

    assert_eq!(fs::read_to_string(run_dir.join(INPUT_FILE)).unwrap(), input);
    let prompt = fs::read_to_string(run_dir.join(PREPARE_FILES.prompt)).unwrap();
    assert!(prompt.contains(input));
    assert_eq!(prompt.matches("\"issue_markdown\"").count(), 1);
}
