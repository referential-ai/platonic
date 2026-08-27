use super::{prepare::PreparedProfileContext, session::PLATONIC_MEMORY_SEPARATOR};
use crate::{
    AppResult,
    model::{ModelRequest, system_prompt},
    tools::PLATONIC_MEMORY_FILENAME,
};
use platonic_core::{ContextFragment, ContextLane, ContextPack};

#[cfg(test)]
pub(super) fn context_pack(
    request: &ModelRequest,
    token_budget: u32,
    platonic_memory: Option<&str>,
) -> AppResult<ContextPack> {
    context_pack_with_profile_and_interruption(request, token_budget, None, platonic_memory, None)
}

pub(super) fn context_pack_with_profile_and_interruption(
    request: &ModelRequest,
    token_budget: u32,
    profile_context: Option<&PreparedProfileContext>,
    platonic_memory: Option<&str>,
    voice_interruption: Option<&str>,
) -> AppResult<ContextPack> {
    let messages = serde_json::to_string(&request.messages)?;
    let tools = serde_json::to_string(&request.tools)?;
    let mut system_contract = system_prompt().to_string();
    if profile_context.is_some() || platonic_memory.is_some() || voice_interruption.is_some() {
        system_contract.push_str(PLATONIC_MEMORY_SEPARATOR);
    }
    // Keep the fragment sum equal to the estimate of the concatenated provider system text.
    let system_context_tokens = estimate_tokens(&request.system);
    let system_contract_tokens = estimate_tokens(&system_contract);
    let mut fragments = vec![ContextFragment {
        lane: ContextLane::SystemContract,
        source: "system_prompt".into(),
        content: system_contract.clone(),
        estimated_tokens: system_contract_tokens,
    }];
    let mut accounted_system_tokens = system_contract_tokens;
    let mut through_system = system_contract;
    if let Some(profile) = profile_context {
        through_system.push_str(&profile.content);
        if platonic_memory.is_some() || voice_interruption.is_some() {
            through_system.push_str(PLATONIC_MEMORY_SEPARATOR);
        }
        let through_profile_tokens = estimate_tokens(&through_system);
        fragments.push(ContextFragment {
            lane: ContextLane::RetrievedContext,
            source: profile.source(),
            content: profile.content.clone(),
            estimated_tokens: through_profile_tokens.saturating_sub(accounted_system_tokens),
        });
        accounted_system_tokens = through_profile_tokens;
    }
    if let Some(content) = platonic_memory {
        through_system.push_str(content);
        if voice_interruption.is_some() {
            through_system.push_str(PLATONIC_MEMORY_SEPARATOR);
        }
        let through_memory_tokens = estimate_tokens(&through_system);
        fragments.push(ContextFragment {
            lane: ContextLane::RetrievedContext,
            source: PLATONIC_MEMORY_FILENAME.into(),
            content: content.into(),
            estimated_tokens: through_memory_tokens.saturating_sub(accounted_system_tokens),
        });
        accounted_system_tokens = through_memory_tokens;
    }
    if let Some(content) = voice_interruption {
        fragments.push(ContextFragment {
            lane: ContextLane::CurrentTask,
            source: "voice.interruption".into(),
            content: content.into(),
            estimated_tokens: system_context_tokens.saturating_sub(accounted_system_tokens),
        });
    }
    fragments.extend([
        ContextFragment {
            lane: ContextLane::RecentTurns,
            source: "model.messages".into(),
            estimated_tokens: estimate_tokens(&messages),
            content: messages,
        },
        ContextFragment {
            lane: ContextLane::ToolSchemas,
            source: "model.tools".into(),
            estimated_tokens: estimate_tokens(&tools),
            content: tools,
        },
    ]);
    Ok(ContextPack {
        token_budget,
        fragments,
    })
}
pub(crate) fn estimate_tokens(content: &str) -> u32 {
    let estimate = (content.chars().count() / 4).saturating_add(1);
    estimate.try_into().unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::super::session::{
        estimated_context_tokens, hydrated_messages, provider_system_context,
        provider_system_context_with_profile_and_interruption, session_messages_from,
    };
    use super::*;
    use crate::{config::Config, ledger::SessionTurn, tool_catalog::tool_specs};
    use platonic_core::ProfileId;

    #[test]
    fn profile_memory_and_interruption_fragments_sum_to_exact_provider_system_context() {
        let profile = PreparedProfileContext {
            profile_id: ProfileId::new("profile-context-accounting").unwrap(),
            revision: 2,
            content_hash: "hash".into(),
            content: "profile content".into(),
            truncated: false,
        };
        let memory = "workspace memory";
        let interruption = "interruption context";
        let system = provider_system_context_with_profile_and_interruption(
            Some(&profile.content),
            Some(memory),
            Some(interruption),
        );
        let request = ModelRequest {
            model: Config::default().provider.model,
            system: system.clone(),
            max_output_tokens: 1,
            reasoning_effort: None,
            messages: vec![],
            tools: vec![],
        };
        let context = context_pack_with_profile_and_interruption(
            &request,
            u32::MAX,
            Some(&profile),
            Some(memory),
            Some(interruption),
        )
        .unwrap();
        let system_tokens = context.fragments[..4]
            .iter()
            .map(|fragment| fragment.estimated_tokens)
            .sum::<u32>();
        assert_eq!(system_tokens, estimate_tokens(&system));
        assert_eq!(context.fragments[1].content, profile.content);
        assert_eq!(context.fragments[2].content, memory);
        assert_eq!(context.fragments[3].content, interruption);
    }

    #[test]
    fn platonic_memory_budget_can_drop_oldest_turn_without_trimming_memory() {
        let mut config = Config::default();
        let tools = tool_specs(&config.tools.enabled, false);
        let turns = vec![SessionTurn {
            question: "old question ".repeat(40),
            final_answer: "old answer ".repeat(40),
        }];
        let question = "current question";
        let memory = "workspace memory ".repeat(30);
        let system_context = provider_system_context(Some(&memory));
        let all_messages = session_messages_from(&turns, question, false);
        let without_memory =
            estimated_context_tokens(system_prompt(), &all_messages, &tools).unwrap();
        let with_memory = estimated_context_tokens(&system_context, &all_messages, &tools).unwrap();
        assert!(with_memory > without_memory);
        config.limits.token_budget = without_memory;

        let hydration =
            hydrated_messages(&turns, question, &config, &tools, &system_context).unwrap();

        assert_eq!(hydration.dropped_turns, 1);
        assert_eq!(hydration.estimated_tokens_before, with_memory);
        assert!(hydration.estimated_tokens_after <= config.limits.token_budget);
        let request = ModelRequest {
            model: config.provider.model,
            system: system_context,
            max_output_tokens: config.limits.max_output_tokens,
            reasoning_effort: None,
            messages: hydration.retained_messages,
            tools,
        };
        let context = context_pack(&request, config.limits.token_budget, Some(&memory)).unwrap();
        context.validate_budget().unwrap();
        assert_eq!(context.estimated_tokens(), hydration.estimated_tokens_after);
        assert_eq!(
            context
                .fragments
                .iter()
                .find(|fragment| fragment.lane == ContextLane::RetrievedContext)
                .unwrap()
                .content,
            memory
        );
    }
}
