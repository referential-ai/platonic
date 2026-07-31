use crate::{AppError, AppResult};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    thread,
    time::{Duration, Instant},
};

pub(super) const DISCORD_MESSAGE_LIMIT: usize = 2_000;
pub(super) const PRESENTATION_TIMEOUT: Duration = Duration::from_millis(1_500);
const PRODUCT_MESSAGE_RETRY_LIMIT: Duration = Duration::from_secs(30);
const TERMINAL_REACTION_WAIT_LIMIT: Duration = Duration::from_secs(2);

pub(super) struct DiscordRestClient {
    pub(super) agent: ureq::Agent,
    presentation_agent: ureq::Agent,
    pub(super) api_base: String,
    token: String,
    rate_limits: Cell<DiscordRateLimits>,
}

impl DiscordRestClient {
    pub(super) fn new(api_base: &str, token: String) -> Self {
        Self {
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(35))
                .build(),
            presentation_agent: ureq::AgentBuilder::new()
                .timeout(PRESENTATION_TIMEOUT)
                .build(),
            api_base: api_base.trim_end_matches('/').into(),
            token,
            rate_limits: Cell::new(DiscordRateLimits::default()),
        }
    }

    pub(super) fn application_id(&self) -> AppResult<u64> {
        let response = self
            .request(
                self.agent
                    .get(&format!("{}/oauth2/applications/@me", self.api_base)),
            )
            .call()
            .map_err(|error| discord_http_error("application lookup", error))?;
        let response: DiscordApplication = response.into_json().map_err(|_| {
            AppError::Provider("discord application lookup returned invalid JSON".into())
        })?;
        response.id.parse().map_err(|_| {
            AppError::Provider("discord application lookup returned an invalid id".into())
        })
    }

    pub(super) fn gateway_url(&self) -> AppResult<String> {
        let response = self
            .request(self.agent.get(&format!("{}/gateway/bot", self.api_base)))
            .call()
            .map_err(|error| discord_http_error("gateway discovery", error))?;
        let response: GatewayBotResponse = response.into_json().map_err(|_| {
            AppError::Provider("discord gateway discovery returned invalid JSON".into())
        })?;
        if response.url.is_empty() {
            return Err(AppError::Provider(
                "discord gateway discovery returned an empty URL".into(),
            ));
        }
        Ok(response.url)
    }

    pub(super) fn send_message(&self, channel_id: u64, text: &str) -> AppResult<()> {
        for content in discord_chunks(text) {
            let mut retry_available = true;
            loop {
                self.require_product_allowed()?;
                match self
                    .request(
                        self.agent
                            .post(&format!("{}/channels/{channel_id}/messages", self.api_base)),
                    )
                    .send_json(CreateMessage {
                        content: content.clone(),
                        allowed_mentions: AllowedMentions { parse: Vec::new() },
                    }) {
                    Ok(_) => break,
                    Err(error) => {
                        let error =
                            self.discord_http_error("message send", RestClass::Product, error);
                        if retry_available
                            && let Some(delay) = product_message_retry_delay(error.rate_limit)
                        {
                            retry_available = false;
                            thread::sleep(delay);
                            continue;
                        }
                        return Err(error.app_error);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn trigger_typing(&self, channel_id: u64) -> AppResult<()> {
        if !self.presentation_allowed("typing") {
            return Ok(());
        }
        self.request(
            self.presentation_agent
                .post(&format!("{}/channels/{channel_id}/typing", self.api_base)),
        )
        .call()
        .map_err(|error| {
            self.discord_http_error("typing", RestClass::Presentation, error)
                .app_error
        })?;
        Ok(())
    }

    pub(super) fn reaction(
        &self,
        channel_id: u64,
        message_id: u64,
        emoji: &str,
        action: ReactionAction,
    ) -> AppResult<()> {
        match self.reaction_attempt(channel_id, message_id, emoji, action)? {
            PresentationAttempt::Sent | PresentationAttempt::Gated => Ok(()),
            PresentationAttempt::RateLimited(_) => Err(AppError::Provider(
                "discord reaction returned HTTP 429".into(),
            )),
        }
    }

    pub(super) fn add_terminal_reaction(
        &self,
        channel_id: u64,
        message_id: u64,
        emoji: &str,
    ) -> AppResult<()> {
        match self.reaction_attempt(channel_id, message_id, emoji, ReactionAction::Add)? {
            PresentationAttempt::Sent | PresentationAttempt::Gated => Ok(()),
            PresentationAttempt::RateLimited(rate_limit) => {
                let Some(wait) = terminal_reaction_wait(rate_limit) else {
                    return Err(AppError::Provider(
                        "discord terminal reaction returned HTTP 429".into(),
                    ));
                };
                eprintln!("discord terminal reaction rate limited; waiting {wait:?}");
                thread::sleep(wait);
                match self.reaction_attempt(channel_id, message_id, emoji, ReactionAction::Add)? {
                    PresentationAttempt::Sent | PresentationAttempt::Gated => Ok(()),
                    PresentationAttempt::RateLimited(_) => Err(AppError::Provider(
                        "discord terminal reaction returned HTTP 429".into(),
                    )),
                }
            }
        }
    }

    fn reaction_attempt(
        &self,
        channel_id: u64,
        message_id: u64,
        emoji: &str,
        action: ReactionAction,
    ) -> AppResult<PresentationAttempt> {
        if !self.presentation_allowed("reaction") {
            return Ok(PresentationAttempt::Gated);
        }
        let emoji = url::form_urlencoded::byte_serialize(emoji.as_bytes()).collect::<String>();
        let url = format!(
            "{}/channels/{channel_id}/messages/{message_id}/reactions/{emoji}/@me",
            self.api_base
        );
        let request = match action {
            ReactionAction::Add => self.presentation_agent.put(&url),
            ReactionAction::Remove => self.presentation_agent.delete(&url),
        };
        match self.request(request).call() {
            Ok(_) => Ok(PresentationAttempt::Sent),
            Err(error) => {
                let error = self.discord_http_error("reaction", RestClass::Presentation, error);
                match error.rate_limit {
                    Some(rate_limit) => Ok(PresentationAttempt::RateLimited(rate_limit)),
                    None => Err(error.app_error),
                }
            }
        }
    }

    fn presentation_allowed(&self, operation: &str) -> bool {
        let allowed = self.rate_limits.get().presentation_allowed(Instant::now());
        if !allowed {
            eprintln!("discord {operation} dropped: rate-limit gate open");
        }
        allowed
    }

    fn require_product_allowed(&self) -> AppResult<()> {
        if self.rate_limits.get().product_allowed(Instant::now()) {
            Ok(())
        } else {
            Err(AppError::Provider(
                "discord REST is globally rate limited".into(),
            ))
        }
    }

    fn discord_http_error(
        &self,
        operation: &str,
        class: RestClass,
        error: ureq::Error,
    ) -> DiscordRestError {
        match error {
            ureq::Error::Status(429, response) => {
                let header_retry_after = response.header("Retry-After").and_then(parse_retry_after);
                let header_global = response.header("X-RateLimit-Global") == Some("true")
                    || response.header("X-RateLimit-Scope") == Some("global");
                let rate_limit = response.into_json::<DiscordRateLimitResponse>().ok();
                let body_retry_after = rate_limit
                    .as_ref()
                    .and_then(|rate_limit| parse_retry_after_number(rate_limit.retry_after));
                let retry_after = [header_retry_after, body_retry_after]
                    .into_iter()
                    .flatten()
                    .max();
                let global =
                    header_global || rate_limit.is_some_and(|rate_limit| rate_limit.global);
                if let Some(retry_after) = retry_after {
                    let mut limits = self.rate_limits.get();
                    limits.record(class, global, retry_after, Instant::now());
                    self.rate_limits.set(limits);
                }
                DiscordRestError {
                    app_error: AppError::Provider(format!("discord {operation} returned HTTP 429")),
                    rate_limit: retry_after.map(|retry_after| DiscordRateLimit {
                        retry_after,
                        global,
                    }),
                }
            }
            error => DiscordRestError {
                app_error: discord_http_error(operation, error),
                rate_limit: None,
            },
        }
    }

    pub(super) fn request(&self, request: ureq::Request) -> ureq::Request {
        request
            .set("Authorization", &format!("Bot {}", self.token))
            .set("User-Agent", "plato-agent/0.1")
    }
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    parse_retry_after_number(value.parse().ok()?)
}

fn parse_retry_after_number(value: f64) -> Option<Duration> {
    Duration::try_from_secs_f64(value).ok()
}

fn product_message_retry_delay(rate_limit: Option<DiscordRateLimit>) -> Option<Duration> {
    rate_limit
        .map(|rate_limit| rate_limit.retry_after)
        .filter(|delay| *delay <= PRODUCT_MESSAGE_RETRY_LIMIT)
}

fn terminal_reaction_wait(rate_limit: DiscordRateLimit) -> Option<Duration> {
    (!rate_limit.global).then(|| rate_limit.retry_after.min(TERMINAL_REACTION_WAIT_LIMIT))
}

#[derive(Clone, Copy)]
pub(super) enum ReactionAction {
    Add,
    Remove,
}

#[derive(Clone, Copy)]
enum RestClass {
    Presentation,
    Product,
}

struct DiscordRestError {
    app_error: AppError,
    rate_limit: Option<DiscordRateLimit>,
}

#[derive(Clone, Copy)]
struct DiscordRateLimit {
    retry_after: Duration,
    global: bool,
}

enum PresentationAttempt {
    Sent,
    Gated,
    RateLimited(DiscordRateLimit),
}

#[derive(Clone, Copy, Default)]
struct DiscordRateLimits {
    presentation_not_before: Option<Instant>,
    global_not_before: Option<Instant>,
}

impl DiscordRateLimits {
    fn presentation_allowed(&self, now: Instant) -> bool {
        self.global_not_before
            .is_none_or(|deadline| now >= deadline)
            && self
                .presentation_not_before
                .is_none_or(|deadline| now >= deadline)
    }

    fn product_allowed(&self, now: Instant) -> bool {
        self.global_not_before
            .is_none_or(|deadline| now >= deadline)
    }

    fn record(&mut self, class: RestClass, global: bool, retry_after: Duration, now: Instant) {
        let Some(deadline) = now.checked_add(retry_after) else {
            return;
        };
        if global {
            self.global_not_before = Some(
                self.global_not_before
                    .map_or(deadline, |current| current.max(deadline)),
            );
        } else if matches!(class, RestClass::Presentation) {
            self.presentation_not_before = Some(
                self.presentation_not_before
                    .map_or(deadline, |current| current.max(deadline)),
            );
        }
    }
}

pub(super) fn discord_http_error(operation: &str, error: ureq::Error) -> AppError {
    match error {
        ureq::Error::Status(status, _) => {
            AppError::Provider(format!("discord {operation} returned HTTP {status}"))
        }
        ureq::Error::Transport(_) => {
            AppError::Provider(format!("discord {operation} transport failed"))
        }
    }
}

fn discord_chunks(text: &str) -> Vec<String> {
    let characters = text.chars().collect::<Vec<_>>();
    characters
        .chunks(DISCORD_MESSAGE_LIMIT)
        .map(|chunk| chunk.iter().collect())
        .collect()
}

#[derive(Deserialize)]
struct GatewayBotResponse {
    url: String,
}

#[derive(Deserialize)]
struct DiscordApplication {
    id: String,
}

#[derive(Serialize)]
pub(super) struct CreateMessage {
    pub(super) content: String,
    pub(super) allowed_mentions: AllowedMentions,
}

#[derive(Deserialize)]
struct DiscordRateLimitResponse {
    retry_after: f64,
    #[serde(default)]
    global: bool,
}

#[derive(Serialize)]
pub(super) struct AllowedMentions {
    pub(super) parse: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discord_gateway::{
        daemon_bridge::{EYES_EMOJI, SUCCESS_EMOJI},
        test_support::*,
    };
    use serde_json::{Value, json};

    #[test]
    fn product_message_retries_once_after_header_or_body_retry_after() {
        for first_response in [
            FakeResponse {
                status: 429,
                body: json!({}),
                headers: vec![("Retry-After", "0.02")],
            },
            FakeResponse {
                status: 429,
                body: json!({"retry_after": 0.02, "global": true}),
                headers: Vec::new(),
            },
        ] {
            let rest = spawn_scripted_rest(vec![
                first_response,
                FakeResponse {
                    status: 200,
                    body: json!({"id": "message_1"}),
                    headers: Vec::new(),
                },
            ]);
            let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

            client.send_message(200, "same chunk").unwrap();

            let requests = rest.handle.join().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].body, requests[1].body);
            assert_eq!(requests[0].body["content"], "same chunk");
            let retry_delay = requests[1]
                .received_at
                .duration_since(requests[0].received_at);
            assert!(retry_delay >= Duration::from_millis(20));
            assert!(retry_delay < Duration::from_secs(1));
        }
    }

    #[test]
    fn product_message_retry_requires_a_usable_bounded_delay() {
        for invalid in ["", "later", "-1", "NaN", "inf"] {
            assert_eq!(parse_retry_after(invalid), None);
        }
        assert_eq!(parse_retry_after("0"), Some(Duration::ZERO));
        assert_eq!(
            product_message_retry_delay(Some(DiscordRateLimit {
                retry_after: PRODUCT_MESSAGE_RETRY_LIMIT,
                global: true,
            })),
            Some(PRODUCT_MESSAGE_RETRY_LIMIT)
        );
        assert_eq!(
            product_message_retry_delay(Some(DiscordRateLimit {
                retry_after: PRODUCT_MESSAGE_RETRY_LIMIT + Duration::from_millis(1),
                global: false,
            })),
            None
        );
        assert_eq!(product_message_retry_delay(None), None);
    }

    #[test]
    fn product_message_does_not_retry_unusable_429_delays() {
        for response in [
            FakeResponse {
                status: 429,
                body: json!({}),
                headers: Vec::new(),
            },
            FakeResponse {
                status: 429,
                body: json!("invalid"),
                headers: vec![("Retry-After", "later")],
            },
            FakeResponse {
                status: 429,
                body: json!({"retry_after": -1.0}),
                headers: Vec::new(),
            },
            FakeResponse {
                status: 429,
                body: json!({}),
                headers: vec![("Retry-After", "NaN")],
            },
            FakeResponse {
                status: 429,
                body: json!({"retry_after": 30.001}),
                headers: Vec::new(),
            },
        ] {
            let rest = spawn_observed_rest(vec![FakeRestAction::Respond(response)]);
            let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

            let result = client.send_message(200, "one attempt");
            let requests = rest.finish();

            assert!(result.is_err());
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].body["content"], "one attempt");
        }
    }

    #[test]
    fn product_message_second_429_stops_and_abandons_later_chunks() {
        let rate_limited = || {
            FakeRestAction::Respond(FakeResponse {
                status: 429,
                body: json!({"retry_after": 0.0, "global": false}),
                headers: Vec::new(),
            })
        };
        let rest = spawn_observed_rest(vec![rate_limited(), rate_limited()]);
        let client = DiscordRestClient::new(&rest.base_url, "secret-token".into());
        let first_chunk = "a".repeat(DISCORD_MESSAGE_LIMIT);
        let message = format!("{first_chunk}later");

        let error = client.send_message(200, &message).unwrap_err();
        let requests = rest.finish();

        assert!(!error.to_string().contains("secret-token"));
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.body["content"] == first_chunk)
        );
    }

    #[test]
    fn product_message_does_not_retry_ambiguous_failures() {
        for action in [
            FakeRestAction::Respond(FakeResponse {
                status: 500,
                body: json!({}),
                headers: Vec::new(),
            }),
            FakeRestAction::Disconnect,
        ] {
            let rest = spawn_observed_rest(vec![action]);
            let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

            let result = client.send_message(200, "one attempt");
            let requests = rest.finish();

            assert!(result.is_err());
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].body["content"], "one attempt");
        }
    }

    #[test]
    fn terminal_reaction_wait_is_capped_at_two_seconds() {
        assert_eq!(
            terminal_reaction_wait(DiscordRateLimit {
                retry_after: Duration::from_millis(250),
                global: false,
            }),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            terminal_reaction_wait(DiscordRateLimit {
                retry_after: Duration::from_secs(30),
                global: false,
            }),
            Some(TERMINAL_REACTION_WAIT_LIMIT)
        );
        assert_eq!(
            terminal_reaction_wait(DiscordRateLimit {
                retry_after: Duration::from_millis(250),
                global: true,
            }),
            None
        );
    }

    #[test]
    fn terminal_reaction_drops_after_one_rate_limited_retry() {
        let rest = spawn_scripted_rest(vec![
            FakeResponse {
                status: 429,
                body: json!({"retry_after": 0.02, "global": false}),
                headers: Vec::new(),
            },
            FakeResponse {
                status: 429,
                body: json!({"retry_after": 0.02, "global": false}),
                headers: Vec::new(),
            },
        ]);
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

        let error = client
            .add_terminal_reaction(200, 300, SUCCESS_EMOJI)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("terminal reaction returned HTTP 429")
        );
        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert_reaction(&requests[0], "PUT", SUCCESS_EMOJI);
        assert_reaction(&requests[1], "PUT", SUCCESS_EMOJI);
    }

    #[test]
    fn terminal_reaction_does_not_wait_or_retry_global_429() {
        let rest = spawn_scripted_rest(vec![FakeResponse {
            status: 429,
            body: json!({"retry_after": 0.02, "global": true}),
            headers: Vec::new(),
        }]);
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

        assert!(
            client
                .add_terminal_reaction(200, 300, SUCCESS_EMOJI)
                .is_err()
        );

        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert_reaction(&requests[0], "PUT", SUCCESS_EMOJI);
    }

    #[test]
    fn terminal_reaction_does_not_retry_429_without_retry_after() {
        let rest = spawn_scripted_rest(vec![FakeResponse {
            status: 429,
            body: json!({}),
            headers: Vec::new(),
        }]);
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

        assert!(
            client
                .add_terminal_reaction(200, 300, SUCCESS_EMOJI)
                .is_err()
        );

        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert_reaction(&requests[0], "PUT", SUCCESS_EMOJI);
    }

    #[test]
    fn scoped_rate_limit_drops_presentation_while_product_messages_flow() {
        let rest = spawn_scripted_rest(vec![
            FakeResponse {
                status: 429,
                body: json!({"retry_after": 1336.57, "global": false}),
                headers: vec![("Retry-After", "2")],
            },
            FakeResponse {
                status: 200,
                body: json!({"id": "message_1"}),
                headers: Vec::new(),
            },
        ]);
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

        client
            .reaction(200, 300, EYES_EMOJI, ReactionAction::Add)
            .unwrap_err();
        client.trigger_typing(200).unwrap();
        client.send_message(200, "still delivered").unwrap();

        let limits = client.rate_limits.get();
        assert!(
            limits.presentation_not_before.unwrap() > Instant::now() + Duration::from_secs(1_300)
        );
        assert!(limits.global_not_before.is_none());
        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].path, "/channels/200/messages");
    }

    #[test]
    fn presentation_endpoints_accept_empty_no_content_responses() {
        let rest = spawn_scripted_rest(
            (0..3)
                .map(|_| FakeResponse {
                    status: 204,
                    body: Value::Null,
                    headers: Vec::new(),
                })
                .collect(),
        );
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

        client
            .reaction(200, 300, EYES_EMOJI, ReactionAction::Add)
            .unwrap();
        client.trigger_typing(200).unwrap();
        client
            .reaction(200, 300, EYES_EMOJI, ReactionAction::Remove)
            .unwrap();

        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].method, "POST");
        assert_eq!(requests[1].path, "/channels/200/typing");
        assert_reaction(&requests[2], "DELETE", EYES_EMOJI);
    }

    #[test]
    fn presentation_timeout_is_bounded_to_one_attempt() {
        let rest = spawn_stalled_rest(Duration::from_secs(3));
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());
        let started = Instant::now();

        let error = client.trigger_typing(200).unwrap_err();
        let elapsed = started.elapsed();

        assert!(error.to_string().contains("typing transport failed"));
        assert!(elapsed >= Duration::from_secs(1));
        assert!(elapsed < Duration::from_millis(2_500));
        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/channels/200/typing");
    }

    #[test]
    fn global_rate_limit_blocks_due_product_message_without_sending() {
        let rest = spawn_scripted_rest(vec![FakeResponse {
            status: 429,
            body: json!({"retry_after": 1336.57, "global": true}),
            headers: vec![("X-RateLimit-Scope", "global")],
        }]);
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

        client
            .reaction(200, 300, EYES_EMOJI, ReactionAction::Add)
            .unwrap_err();
        let error = client.send_message(200, "not sent").unwrap_err();

        assert!(error.to_string().contains("globally rate limited"));
        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
    }

    #[test]
    fn header_only_global_rate_limit_is_honored() {
        let rest = spawn_scripted_rest(vec![FakeResponse {
            status: 429,
            body: json!({}),
            headers: vec![("Retry-After", "90"), ("X-RateLimit-Global", "true")],
        }]);
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

        client.trigger_typing(200).unwrap_err();

        let limits = client.rate_limits.get();
        assert!(limits.global_not_before.unwrap() > Instant::now() + Duration::from_secs(89));
        assert!(client.send_message(200, "not sent").is_err());
        assert_eq!(rest.handle.join().unwrap().len(), 1);
    }

    #[test]
    fn product_global_rate_limit_blocks_the_next_product_message() {
        let rest = spawn_scripted_rest(vec![FakeResponse {
            status: 429,
            body: json!({"retry_after": 90.0, "global": true}),
            headers: Vec::new(),
        }]);
        let client = DiscordRestClient::new(&rest.base_url, "test-token".into());

        assert!(client.send_message(200, "first").is_err());
        let error = client.send_message(200, "second").unwrap_err();

        assert!(error.to_string().contains("globally rate limited"));
        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/channels/200/messages");
    }

    #[test]
    fn rate_limit_deadlines_use_the_full_duration_and_expire_at_the_boundary() {
        let now = Instant::now();
        let mut limits = DiscordRateLimits::default();
        limits.record(
            RestClass::Presentation,
            false,
            Duration::from_secs_f64(1336.57),
            now,
        );
        assert!(!limits.presentation_allowed(now + Duration::from_secs(1_336)));
        assert!(limits.product_allowed(now + Duration::from_secs(1_336)));
        assert!(limits.presentation_allowed(now + Duration::from_secs_f64(1336.57)));

        limits.record(RestClass::Presentation, true, Duration::from_secs(2), now);
        assert!(!limits.product_allowed(now + Duration::from_secs(1)));
        assert!(limits.product_allowed(now + Duration::from_secs(2)));
    }

    #[test]
    fn discord_http_errors_never_include_the_token() {
        let rest = spawn_fake_rest(1, 401, None);
        let client = DiscordRestClient::new(&rest.base_url, "secret-token".into());

        let error = client.send_message(200, "hello").unwrap_err();
        rest.handle.join().unwrap();

        assert!(!error.to_string().contains("secret-token"));
    }
}
