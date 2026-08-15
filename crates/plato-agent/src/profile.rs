use crate::{AppError, AppResult};
use platonic_client::client::DaemonClient;
use platonic_protocol::{
    ProfileContent, ProfileCreateParams, ProfileOpenDecision, ProfileOpenResult, ProfileSummary,
    ThreadRepositoryRequest,
};
use std::{
    io::{BufRead, Write},
    path::Path,
};

/// Selects or creates one workspace profile, opens its durable home, and returns the home thread.
pub fn select_profile_home(
    client: &mut DaemonClient,
    workspace_root: &Path,
    requested: Option<&str>,
    config_path: Option<&Path>,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> AppResult<String> {
    let workspace_id = client
        .workspace_id()
        .ok_or_else(|| AppError::DaemonProtocol("profile selection requires hello".into()))?
        .to_owned();
    let profiles = client
        .profile_list(Some(workspace_id.clone()), Some(100))?
        .profiles;
    let profile = match select_profile(profiles, requested, input, output)? {
        Some(profile) => profile,
        None => create_profile(
            client,
            workspace_root,
            &workspace_id,
            config_path,
            input,
            output,
        )?,
    };
    writeln!(output, "Profile: {} ({})", profile.display_name, profile.id)?;
    open_home(client, profile, input, output)
}

fn select_profile(
    profiles: Vec<ProfileSummary>,
    requested: Option<&str>,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> AppResult<Option<ProfileSummary>> {
    if let Some(requested) = requested {
        return profiles
            .into_iter()
            .find(|profile| profile.display_name == requested || profile.id.as_str() == requested)
            .map(Some)
            .ok_or_else(|| AppError::Config(format!("profile not found: {requested}")));
    }
    match profiles.as_slice() {
        [] => Ok(None),
        [profile] => Ok(Some(profile.clone())),
        _ => {
            writeln!(output, "Profiles:")?;
            for (index, profile) in profiles.iter().enumerate() {
                writeln!(output, "  {}. {}", index + 1, profile.display_name)?;
            }
            write!(output, "Profile name or number: ")?;
            output.flush()?;
            let mut answer = String::new();
            input.read_line(&mut answer)?;
            let answer = answer.trim();
            if let Ok(index) = answer.parse::<usize>()
                && let Some(profile) = index.checked_sub(1).and_then(|index| profiles.get(index))
            {
                return Ok(Some(profile.clone()));
            }
            profiles
                .into_iter()
                .find(|profile| profile.display_name == answer)
                .map(Some)
                .ok_or_else(|| AppError::Config(format!("profile not found: {answer}")))
        }
    }
}

fn create_profile(
    client: &mut DaemonClient,
    workspace_root: &Path,
    workspace_id: &str,
    config_path: Option<&Path>,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> AppResult<ProfileSummary> {
    let default_name = workspace_root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("default");
    write!(output, "Profile name [{default_name}]: ")?;
    output.flush()?;
    let mut answer = String::new();
    input.read_line(&mut answer)?;
    let display_name = match answer.trim() {
        "" => default_name.to_owned(),
        name => name.to_owned(),
    };
    let config_path = config_path.map(|path| {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            workspace_root.join(path)
        };
        path.to_string_lossy().into_owned()
    });
    let readiness = client.daemon_status(None, config_path.clone())?;
    if !readiness.model.key_present {
        return Err(AppError::Config(
            "provider key is unavailable for profile creation".into(),
        ));
    }
    Ok(client
        .profile_create(ProfileCreateParams {
            workspace_id: workspace_id.into(),
            display_name,
            model: None,
            reasoning_effort: Default::default(),
            approval_policy: Default::default(),
            toolset: None,
            content: ProfileContent::default(),
            config_path,
        })?
        .status
        .profile)
}

fn open_home(
    client: &mut DaemonClient,
    profile: ProfileSummary,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> AppResult<String> {
    let result = match client.profile_open_resolve(profile.id.clone())? {
        opened @ ProfileOpenResult::Opened { .. } => opened,
        ProfileOpenResult::NoHome { .. } => client.profile_open_start(
            profile.id.clone(),
            format!("plato-profile-home-{}", profile.id),
            vec![ThreadRepositoryRequest {
                repo: ".".into(),
                branch: None,
            }],
            ".".into(),
            ".".into(),
        )?,
        result => result,
    };
    let result = match result {
        ProfileOpenResult::ApprovalRequired {
            home_reservation_id,
            thread_id,
            effect,
            reason,
            ..
        } => {
            writeln!(output, "profile.open {thread_id} ({effect:?}): {reason}")?;
            write!(output, "Approve profile home? [y/N/c] ")?;
            output.flush()?;
            let mut answer = String::new();
            input.read_line(&mut answer)?;
            let decision = match answer.trim().to_ascii_lowercase().as_str() {
                "y" | "yes" => ProfileOpenDecision::Grant,
                "c" | "cancel" => ProfileOpenDecision::Cancel,
                _ => ProfileOpenDecision::Deny {
                    reason: "profile home approval denied".into(),
                },
            };
            client.profile_open_decide(home_reservation_id, decision)?
        }
        result => result,
    };
    match result {
        ProfileOpenResult::Opened {
            thread, created, ..
        } => {
            writeln!(
                output,
                "Home: {} ({})",
                thread.authority.thread_id,
                if created { "created" } else { "reused" }
            )?;
            Ok(thread.authority.thread_id)
        }
        ProfileOpenResult::Denied { reason, .. } => {
            Err(AppError::Config(format!("profile home denied: {reason}")))
        }
        ProfileOpenResult::Canceled { .. } => Err(AppError::Config("profile home canceled".into())),
        ProfileOpenResult::NoHome { .. } | ProfileOpenResult::ApprovalRequired { .. } => Err(
            AppError::DaemonProtocol("profile home remained unresolved".into()),
        ),
    }
}
