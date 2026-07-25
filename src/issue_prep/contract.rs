use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IssueDraft {
    pub(super) title: String,
    pub(super) problem: String,
    pub(super) current_behavior: String,
    pub(super) expected_behavior: String,
    pub(super) target_repo_surface: String,
    pub(super) scope: Vec<String>,
    pub(super) non_goals: Vec<String>,
    pub(super) acceptance_criteria: Vec<String>,
    pub(super) proof: Vec<String>,
    pub(super) open_questions: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ModelReview {
    pub(super) verdict: ReviewVerdict,
    pub(super) findings: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ReviewVerdict {
    Candidate,
    Blocked,
}

pub(super) fn validate_prepared(issue: &IssueDraft) -> Vec<String> {
    validate_issue(issue, false)
}

pub(super) fn validate_refined(issue: &IssueDraft) -> Vec<String> {
    validate_issue(issue, true)
}

pub(super) fn validate_review(review: &ModelReview) -> Vec<String> {
    match review.verdict {
        ReviewVerdict::Candidate if review.findings.is_empty() => vec![],
        ReviewVerdict::Candidate => {
            vec!["candidate verdict must not include unresolved findings".into()]
        }
        ReviewVerdict::Blocked if review.findings.is_empty() => {
            vec!["blocked verdict must include at least one finding".into()]
        }
        ReviewVerdict::Blocked => review.findings.clone(),
    }
}

pub(super) fn render_candidate(issue: &IssueDraft) -> String {
    let mut output = format!(
        "# {}\n\n## Problem\n\n{}\n\n## Current Behavior\n\n{}\n\n## Expected Behavior\n\n{}\n\n## Target Repo / Surface\n\n{}\n\n## Scope\n\n",
        issue.title.trim(),
        issue.problem.trim(),
        issue.current_behavior.trim(),
        issue.expected_behavior.trim(),
        issue.target_repo_surface.trim(),
    );
    push_list(&mut output, &issue.scope, false);
    output.push_str("\n## Non-goals\n\n");
    push_list(&mut output, &issue.non_goals, false);
    output.push_str("\n## Acceptance Criteria\n\n");
    push_list(&mut output, &issue.acceptance_criteria, true);
    output.push_str("\n## Proof\n\n");
    push_list(&mut output, &issue.proof, false);
    output
}

fn validate_issue(issue: &IssueDraft, require_no_open_questions: bool) -> Vec<String> {
    let mut errors = Vec::new();
    require_text(&mut errors, "title", &issue.title);
    require_text(&mut errors, "problem", &issue.problem);
    require_text(&mut errors, "current_behavior", &issue.current_behavior);
    require_text(&mut errors, "expected_behavior", &issue.expected_behavior);
    require_text(
        &mut errors,
        "target_repo_surface",
        &issue.target_repo_surface,
    );
    require_list(&mut errors, "scope", &issue.scope);
    require_list(&mut errors, "non_goals", &issue.non_goals);
    require_list(
        &mut errors,
        "acceptance_criteria",
        &issue.acceptance_criteria,
    );
    require_list(&mut errors, "proof", &issue.proof);
    if require_no_open_questions && !issue.open_questions.is_empty() {
        errors.push("open_questions must be empty after refinement".into());
    }
    errors
}

fn require_text(errors: &mut Vec<String>, field: &str, value: &str) {
    if value.trim().is_empty() {
        errors.push(format!("{field} must not be empty"));
    }
}

fn require_list(errors: &mut Vec<String>, field: &str, values: &[String]) {
    if values.is_empty() {
        errors.push(format!("{field} must contain at least one item"));
    } else if values.iter().any(|value| value.trim().is_empty()) {
        errors.push(format!("{field} must not contain empty items"));
    }
}

fn push_list(output: &mut String, items: &[String], checklist: bool) {
    for item in items {
        output.push_str(if checklist { "- [ ] " } else { "- " });
        output.push_str(item.trim());
        output.push('\n');
    }
}
