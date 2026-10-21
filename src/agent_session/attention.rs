//! Non-consuming attention classification and readiness over the retained transcript.
//!
//! The caller owns retention, the read cursor, receipts, and availability. Classify
//! only events actually recorded after duplicate/confirmed-own-echo suppression;
//! all recorded context still belongs in `Snapshot`, including MODE and peer text.
//! Neither a wait nor an explicit away/back transition changes these stamps or the cursor.
//!
//! `observed_at` is elapsed monotonic time in ONE worker epoch, recorded at ingest,
//! not when a wait starts. Restore older persisted stamps with `None`; unread
//! relevant restored events are immediately ready. Never compare durations from
//! different worker epochs or assign restored events a fresh debounce window.
//! Persist the classification, or conservatively restore unknown events as
//! `Critical`, rather than silently treating old unread messages as context.
//!
//! Local disconnect, uncertain-delivery/receipt-loss, recovered-receipt and
//! history-gap notices must be stamped `Critical`. Readiness does not choose
//! their transport response: a disconnect still ends a wait with the existing
//! connection error. A snapshot
//! error must likewise become a visible error, never an idle/timeout response.

use crate::protocol::{Message, valid_channel};
use serde::{Deserialize, Serialize};
use std::{fmt, time::Duration};

const QUIET: Duration = Duration::from_secs(2);
const MAX_BURST: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WakeMode {
    All,
    Messages,
    Mentions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Classification {
    Context,
    Message { channel: bool, addressed: bool },
    Critical,
}

impl WakeMode {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Messages => "messages",
            Self::Mentions => "mentions",
        }
    }
}

impl Classification {
    pub(super) fn relevant(self, mode: WakeMode) -> bool {
        match self {
            Self::Critical => true,
            Self::Context => mode == WakeMode::All,
            Self::Message { addressed, .. } => mode != WakeMode::Mentions || addressed,
        }
    }

    fn channel_text(self) -> bool {
        matches!(self, Self::Message { channel: true, .. })
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Identity<'a> {
    pub nick: &'a str,
    pub account: &'a str,
}

/// `reply_to_own` must come from a bounded set of msgids observed with this
/// session's authenticated account AND nickname (including confirmed echoes).
/// A peer's `+reply` value or matching sender nickname alone does not prove that
/// the referenced message is ours. This classification grants no authority.
pub(super) fn classify(
    message: &Message,
    identity: Identity<'_>,
    reply_to_own: bool,
) -> Classification {
    if matches!(message.command.as_str(), "ERROR" | "FAIL")
        || (message.command == "KICK"
            && message
                .params
                .get(1)
                .is_some_and(|nick| same_name(nick, identity.nick)))
    {
        return Classification::Critical;
    }
    if !matches!(message.command.as_str(), "PRIVMSG" | "NOTICE") || message.params.len() < 2 {
        return Classification::Context;
    }

    let target = &message.params[0];
    let body = &message.params[1];
    let addressed = same_name(target, identity.nick)
        || (reply_to_own && message.tags.get("+reply").is_some_and(|id| !id.is_empty()))
        || [identity.nick, identity.account].into_iter().any(|name| {
            !name.is_empty() && (explicit_mention(body, name) || leading_address(body, name))
        })
        || ["all", "here"]
            .into_iter()
            .any(|token| broadcast(body, token));
    Classification::Message {
        channel: valid_channel(target),
        addressed,
    }
}

fn folded(c: char) -> char {
    // Streaming equivalent of protocol::fold, whose String result allocates.
    // The exhaustive ASCII-equivalence regression below checks their agreement.
    match c {
        '[' | '{' => '{',
        ']' | '}' => '}',
        '\\' | '|' => '|',
        '^' | '~' => '~',
        _ => c.to_ascii_lowercase(),
    }
}

fn same_name(left: &str, right: &str) -> bool {
    left.chars().map(folded).eq(right.chars().map(folded))
}

fn explicit_mention(body: &str, token: &str) -> bool {
    let mut previous = None;
    for (start, c) in body.char_indices() {
        if previous.is_none_or(boundary) {
            let mut candidate = body[start..].chars();
            let prefix_matches = candidate.next() == Some('@');
            if prefix_matches
                && token.chars().all(|wanted| {
                    candidate
                        .next()
                        .is_some_and(|actual| folded(actual) == folded(wanted))
                })
                && candidate.next().is_none_or(boundary)
            {
                return true;
            }
        }
        previous = Some(c);
    }
    false
}

/// `@all`/`@here` only as a standalone word: not joined by `/` or other
/// punctuation (as in `@all/@here` lists), quoted, or inside a backtick code
/// span, so documentation and examples do not wake every listener.
fn broadcast(body: &str, token: &str) -> bool {
    let mut code = false;
    let mut previous: Option<char> = None;
    for (start, c) in body.char_indices() {
        if c == '`' {
            code = !code;
        } else if c == '@'
            && !code
            && previous.is_none_or(|p| p.is_whitespace() || p == '(')
            && body[start + 1..]
                .get(..token.len())
                .is_some_and(|word| word.eq_ignore_ascii_case(token))
            && body[start + 1 + token.len()..]
                .chars()
                .next()
                .is_none_or(|next| next.is_whitespace() || ",.:;!?)".contains(next))
        {
            return true;
        }
        previous = Some(c);
    }
    false
}

/// Compatibility for direct/group vocatives on the first line. The colon
/// (or a direct-name comma) must occur within 64 bytes, including indentation.
/// Requiring separators between names avoids treating prose like "assign Sol:"
/// as a dispatch. This has no roster, sender-role or authority dependency.
fn leading_address(body: &str, name: &str) -> bool {
    let mut end = body.len().min(64);
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    let line = body[..end].split('\n').next().unwrap_or("");
    if let Some(comma) = line.find(',').filter(|index| *index < 64)
        && same_name(line[..comma].trim(), name)
    {
        return true;
    }
    let Some(colon) = line.find(':').filter(|index| *index < 64) else {
        return false;
    };
    let mut rest = line[..colon].trim();
    let mut matched = false;
    loop {
        let end = rest
            .find(|c: char| c.is_whitespace() || "/,&".contains(c))
            .unwrap_or(rest.len());
        let item = rest[..end].strip_prefix('@').unwrap_or(&rest[..end]);
        if item.is_empty()
            || !item
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_[]\\`^{}|~".contains(c))
        {
            return false;
        }
        matched |= same_name(item, name);
        rest = rest[end..].trim_start();
        if rest.is_empty() || rest.eq_ignore_ascii_case("especially") {
            return matched;
        }
        if rest.starts_with(['/', ',', '&']) {
            rest = rest[1..].trim_start();
        } else if rest
            .get(..3)
            .is_some_and(|word| word.eq_ignore_ascii_case("and"))
            && rest[3..].starts_with(char::is_whitespace)
        {
            rest = rest[3..].trim_start();
        } else {
            return false;
        }
        if rest.is_empty() {
            return false;
        }
    }
}

fn boundary(c: char) -> bool {
    // IRC nickname punctuation belongs to the identifier, even at its ends.
    // In particular qa must not match qa-x, qa_x, [qa], or qa|bot. @ is only
    // accepted as part of an explicit mention, avoiding mail@qa false positives.
    c.is_whitespace() || (c.is_ascii_punctuation() && !"-_[]\\`^{}|~@".contains(c))
}

/// Compact metadata beside a retained event; no text, msgid or extra event queue.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stamp {
    pub sequence: u64,
    /// Rendered transcript bytes, using the SAME accounting as its byte limit.
    pub bytes: usize,
    pub observed_at: Option<Duration>,
    pub class: Classification,
}

/// All retained stamps in sequence order, including already-read events.
/// A borrowed iterator may map the caller's event fields into copied `Stamp`s.
pub(super) struct Snapshot<I> {
    pub cursor: u64,
    pub sequence: u64,
    pub events: I,
    pub capacity: usize,
    pub byte_capacity: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Readiness {
    Pending,
    Ready,
    TimedOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SnapshotError {
    InvalidLimits,
    InvalidCursor,
    InvalidSequence,
    InvalidTimestamp,
    RetentionExceeded,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "attention snapshot has invalid retention limits",
            Self::InvalidCursor => "attention snapshot cursor exceeds its sequence",
            Self::InvalidSequence => "attention snapshot has inconsistent event sequences",
            Self::InvalidTimestamp => "attention snapshot has inconsistent monotonic timestamps",
            Self::RetentionExceeded => "attention snapshot exceeds transcript retention bounds",
        })
    }
}

impl std::error::Error for SnapshotError {}

/// Evaluate without consuming anything, allocating, or depending on an active
/// waiter. Work is bounded by `capacity + 1` stamps. A read advances only the
/// caller's cursor; recomputing from its remaining events preserves later alerts.
/// `deadline` is the current caller's deadline, not a new burst start time.
pub(super) fn readiness(
    mode: WakeMode,
    snapshot: Snapshot<impl IntoIterator<Item = Stamp>>,
    now: Duration,
    deadline: Duration,
) -> Result<Readiness, SnapshotError> {
    if snapshot.capacity == 0 || snapshot.byte_capacity == 0 {
        return Err(SnapshotError::InvalidLimits);
    }
    if snapshot.cursor > snapshot.sequence {
        return Err(SnapshotError::InvalidCursor);
    }

    let mut retained_bytes = 0usize;
    let mut unread_count = 0usize;
    let mut unread_bytes = 0usize;
    let mut previous_sequence: Option<u64> = None;
    let mut previous_at: Option<Duration> = None;
    let mut gap = false;
    let mut critical = false;
    let mut pending = false;
    let mut restored_relevant = false;
    let mut first_relevant = Duration::ZERO;
    let mut quiet_anchor = Duration::ZERO;

    for (retained_count, stamp) in snapshot.events.into_iter().enumerate() {
        if retained_count == snapshot.capacity
            || stamp.bytes > snapshot.byte_capacity - retained_bytes
        {
            return Err(SnapshotError::RetentionExceeded);
        }
        retained_bytes += stamp.bytes;
        if stamp.sequence == 0
            || stamp.sequence > snapshot.sequence
            || previous_sequence.is_some_and(|prev| prev.checked_add(1) != Some(stamp.sequence))
        {
            return Err(SnapshotError::InvalidSequence);
        }
        previous_sequence = Some(stamp.sequence);
        match stamp.observed_at {
            Some(at) if at > now || previous_at.is_some_and(|prev| at < prev) => {
                return Err(SnapshotError::InvalidTimestamp);
            }
            Some(at) => previous_at = Some(at),
            None if previous_at.is_some() => return Err(SnapshotError::InvalidTimestamp),
            None => {}
        }
        if stamp.sequence <= snapshot.cursor {
            continue;
        }

        if unread_count == 0 && stamp.sequence - snapshot.cursor > 1 {
            gap = true;
        }
        unread_count += 1;
        unread_bytes += stamp.bytes;
        critical |= stamp.class == Classification::Critical;
        let relevant = stamp.class.relevant(mode);
        if relevant {
            restored_relevant |= stamp.observed_at.is_none();
            if !pending {
                first_relevant = stamp.observed_at.unwrap_or_default();
                pending = true;
            }
        }
        if pending && (relevant || stamp.class.channel_text()) {
            quiet_anchor = stamp.observed_at.unwrap_or_default();
        }
    }

    if previous_sequence.is_some_and(|last| last != snapshot.sequence) {
        return Err(SnapshotError::InvalidSequence);
    }
    gap |= unread_count == 0 && snapshot.sequence > snapshot.cursor;
    // ceil(3 * capacity / 4), without multiplication overflow. Only UNREAD
    // retained events count; an old full transcript cannot force busy polling.
    let high_water = unread_count >= snapshot.capacity - snapshot.capacity / 4
        || unread_bytes >= snapshot.byte_capacity - snapshot.byte_capacity / 4;
    if critical || gap || high_water || restored_relevant {
        return Ok(Readiness::Ready);
    }
    if pending
        && (now >= first_relevant.saturating_add(MAX_BURST)
            || now >= quiet_anchor.saturating_add(QUIET)
            || now >= deadline)
    {
        return Ok(Readiness::Ready);
    }
    Ok(if now >= deadline {
        Readiness::TimedOut
    } else {
        Readiness::Pending
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::fold;

    const IDENTITY: Identity<'_> = Identity {
        nick: "w1",
        account: "worker",
    };

    fn message(command: &str, target: &str, body: &str) -> Message {
        Message::new(command, [target, body])
    }

    fn addressed(body: &str, identity: Identity<'_>) -> bool {
        matches!(
            classify(&message("PRIVMSG", "#work", body), identity, false),
            Classification::Message {
                addressed: true,
                ..
            }
        )
    }

    fn stamp(sequence: u64, millis: u64, class: Classification) -> Stamp {
        Stamp {
            sequence,
            bytes: 10,
            observed_at: Some(Duration::from_millis(millis)),
            class,
        }
    }

    fn chat(sequence: u64, millis: u64, body: &str) -> Stamp {
        stamp(
            sequence,
            millis,
            classify(&message("PRIVMSG", "#work", body), IDENTITY, false),
        )
    }

    fn evaluate(
        mode: WakeMode,
        events: &[Stamp],
        cursor: u64,
        now_ms: u64,
        deadline_ms: u64,
    ) -> Result<Readiness, SnapshotError> {
        readiness(
            mode,
            Snapshot {
                cursor,
                sequence: events.last().map_or(cursor, |event| event.sequence),
                events: events.iter().copied(),
                capacity: 512,
                byte_capacity: 512 * 1024,
            },
            Duration::from_millis(now_ms),
            Duration::from_millis(deadline_ms),
        )
    }

    #[test]
    fn token_bounded_nickname_account_and_explicit_mentions() {
        for body in [
            "w1: status?",
            "@W1, hello",
            "worker: status?",
            "@WORKER!",
            "w1, status?",
        ] {
            assert!(addressed(body, IDENTITY), "{body}");
        }
        let identity = Identity {
            nick: "qa",
            account: "sol",
        };
        assert!(!addressed("opaque isolated console SQL", identity));
        assert!(addressed("qa, sol: status?", identity));
        for body in [
            "qa-x", "qa_x", "[qa]", "qa|bot", "xqa", "qa2", "solé", "éqa", "mail@qa", "@qa-more",
            "@@qa", "@sol_2",
        ] {
            assert!(!addressed(body, identity), "{body}");
        }
    }

    #[test]
    fn bounded_leading_group_vocatives_exclude_third_person_prose() {
        let identity = Identity {
            nick: "Sol",
            account: "worker",
        };
        for body in [
            "Sol: please inspect",
            " sol, please inspect",
            "Fable/Sol: inspect",
            "Co-builder, Sol and QA especially: inspect",
            "Fable & Sol: inspect",
            "Fable and worker: inspect",
            "Fable,Sol/QA: inspect",
            "Fable, Sol: inspect",
            "Fable: ask @Sol anywhere",
            "line one\n@Sol please",
        ] {
            assert!(addressed(body, identity), "{body}");
        }
        for body in [
            "assign this to Sol",
            "Sol UI is frozen",
            "say worker",
            "(Sol)",
            "I assigned Sol: inspect",
            "Please ask Sol: inspect",
            "Note: Sol should inspect",
            "Fable: Sol has finished",
            "intro\nSol: inspect",
            "Fable//Sol: inspect",
            "Fable and Sol/: inspect",
            "Fable, Sol's patch: inspect",
            "solé: inspect",
            "mail@Sol: inspect",
            "Team: inspect",
            "all: inspect",
            "@Sol-more: inspect",
        ] {
            assert!(!addressed(body, identity), "{body}");
        }
        // A colon at byte 63 fits; byte 64 is outside the bounded prefix.
        assert!(addressed(
            &format!("{}Sol: inspect", " ".repeat(60)),
            identity
        ));
        assert!(!addressed(
            &format!("{}Sol: inspect", " ".repeat(61)),
            identity
        ));
        assert!(!addressed(
            &format!("{}Sol: inspect", "é".repeat(40)),
            identity
        ));
    }

    #[test]
    fn complete_identifiers_use_existing_rfc1459_fold() {
        let identity = Identity {
            nick: "[QA]\\^",
            account: "build_bot-v2",
        };
        for body in ["{qa}|~: inspect", "@{QA}|~!", "BUILD_BOT-V2, ready?"] {
            assert!(addressed(body, identity), "{body}");
        }
        for body in ["x{qa}|~", "{qa}|~tail", "build_bot-v20", "build_bot"] {
            assert!(!addressed(body, identity), "{body}");
        }
        assert!(!addressed(
            "any text",
            Identity {
                nick: "",
                account: ""
            }
        ));
    }

    #[test]
    fn streaming_name_fold_matches_protocol_fold() {
        for byte in 0..=127u8 {
            let c = char::from(byte);
            assert_eq!(folded(c).to_string(), fold(&c.to_string()));
        }
        for name in ["é", "Σ", "ß", "[MiXeD]\\^_bot-9", "[qa]", "{QA}"] {
            assert_eq!(name.chars().map(folded).collect::<String>(), fold(name));
        }
    }

    #[test]
    fn stamp_serde_roundtrip_keeps_classification_and_requires_explicit_clock_recovery() {
        let original = chat(12, 1234, "@w1 hello");
        let encoded = serde_json::to_string(&original).unwrap();
        let mut restored: Stamp = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.sequence, original.sequence);
        assert_eq!(restored.bytes, original.bytes);
        assert_eq!(restored.class, original.class);
        assert_eq!(restored.observed_at, original.observed_at);
        // Deserialization cannot identify a worker epoch; the restoring owner
        // must explicitly discard the old clock before publishing a snapshot.
        restored.observed_at = None;
        assert_eq!(
            evaluate(WakeMode::Mentions, &[restored], 11, 0, 5000),
            Ok(Readiness::Ready)
        );
        assert!(
            serde_json::from_str::<Stamp>(
                &encoded.replace("\"sequence\"", "\"extra\":true,\"sequence\"")
            )
            .is_err()
        );
    }

    #[test]
    fn only_explicit_bounded_all_and_here_broadcast() {
        for body in ["@all: status", "(@HERE)", "hello @all!"] {
            assert!(addressed(body, IDENTITY), "{body}");
        }
        for body in [
            "all here",
            "@alliance",
            "mail@all",
            "@here_now",
            "@channel",
            "@room",
            // Lists, quotes and code spans are examples, not broadcasts.
            "It wakes for @all/@here or a reply",
            "send `@all` only when needed",
            "run `airc send space '#space' '@here status'`",
            "the literal \"@all\"",
            "a|@all",
        ] {
            assert!(!addressed(body, IDENTITY), "{body}");
        }
    }

    #[test]
    fn direct_messages_and_verified_reply_lookup_are_addressed() {
        for command in ["PRIVMSG", "NOTICE"] {
            assert_eq!(
                classify(&message(command, "W1", "hello"), IDENTITY, false),
                Classification::Message {
                    channel: false,
                    addressed: true
                }
            );
            let mut reply = message(command, "#work", "following up");
            reply.tags.insert("+reply".into(), "own-id".into());
            // A supplied reply tag is not proof of an own msgid.
            assert_eq!(
                classify(&reply, IDENTITY, false),
                Classification::Message {
                    channel: true,
                    addressed: false
                }
            );
            assert_eq!(
                classify(&reply, IDENTITY, true),
                Classification::Message {
                    channel: true,
                    addressed: true
                }
            );
            reply.tags.clear();
            assert_eq!(
                classify(&reply, IDENTITY, true),
                Classification::Message {
                    channel: true,
                    addressed: false
                }
            );
        }
    }

    #[test]
    fn sender_nickname_never_proves_identity_or_privilege() {
        let mut peer = message("PRIVMSG", "#work", "ordinary chat");
        peer.prefix = Some("w1!someone@host".into());
        peer.tags.insert("account".into(), "other-account".into());
        assert_eq!(
            classify(&peer, IDENTITY, false),
            Classification::Message {
                channel: true,
                addressed: false
            }
        );
    }

    #[test]
    fn kick_self_errors_and_explicit_local_critical_notices_bypass() {
        for incoming in [
            message("KICK", "#work", "W1"),
            Message::new("ERROR", ["connection lost"]),
            Message::new("FAIL", ["PRIVMSG", "SEND_FAILED"]),
        ] {
            let class = classify(&incoming, IDENTITY, false);
            assert_eq!(class, Classification::Critical);
            for mode in [WakeMode::All, WakeMode::Messages, WakeMode::Mentions] {
                assert_eq!(
                    evaluate(mode, &[stamp(1, 500, class)], 0, 500, 5000),
                    Ok(Readiness::Ready)
                );
            }
        }
        assert_eq!(
            classify(&message("KICK", "#work", "peer"), IDENTITY, false),
            Classification::Context
        );
        // Local disconnect, uncertain-receipt and GAP notices share this class.
        assert_eq!(
            evaluate(
                WakeMode::Mentions,
                &[stamp(1, 0, Classification::Critical)],
                0,
                0,
                1000
            ),
            Ok(Readiness::Ready)
        );
    }

    #[test]
    fn messages_and_all_offer_distinct_attention_levels() {
        let chat = [chat(1, 0, "peer chatter")];
        assert_eq!(
            evaluate(WakeMode::Mentions, &chat, 0, 2000, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Messages, &chat, 0, 2000, 5000),
            Ok(Readiness::Ready)
        );
        let context = [stamp(1, 0, Classification::Context)];
        assert_eq!(
            evaluate(WakeMode::Messages, &context, 0, 2000, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::All, &context, 0, 1999, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::All, &context, 0, 2000, 5000),
            Ok(Readiness::Ready)
        );
    }

    #[test]
    fn unaddressed_followups_extend_quiet_but_not_maximum_burst() {
        let events = [
            chat(1, 0, "@w1 review:"),
            chat(2, 1200, "part one"),
            chat(3, 2400, "part two"),
        ];
        assert_eq!(
            evaluate(WakeMode::Mentions, &events[..2], 0, 2000, 20000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 4399, 20000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 4400, 20000),
            Ok(Readiness::Ready)
        );
        let mut continuous = vec![chat(1, 0, "@w1 audit")];
        for second in 1..=10 {
            continuous.push(chat(second + 1, second * 1000, "more context"));
        }
        assert_eq!(
            evaluate(WakeMode::Mentions, &continuous[..10], 0, 9999, 20000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &continuous, 0, 10000, 20000),
            Ok(Readiness::Ready)
        );
    }

    #[test]
    fn wait_start_and_renewal_do_not_restart_existing_debounce() {
        let events = [chat(1, 0, "@w1 hello")];
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 500, 1000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 1999, 60000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 2000, 120000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 3000, 120000),
            Ok(Readiness::Ready)
        );
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn pending_relevant_at_caller_timeout_returns_ready() {
        let events = [chat(1, 4200, "@w1 ping")];
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 4999, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 5000, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(
                WakeMode::Mentions,
                &[chat(1, 4200, "hello peer")],
                0,
                5000,
                5000
            ),
            Ok(Readiness::TimedOut)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &[], 0, 5000, 5000),
            Ok(Readiness::TimedOut)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 4200, 4200),
            Ok(Readiness::Ready)
        );
    }

    #[test]
    fn later_direct_message_also_extends_quiet() {
        let events = [
            chat(1, 0, "@w1 task"),
            stamp(
                2,
                1900,
                classify(&message("NOTICE", "w1", "one more detail"), IDENTITY, false),
            ),
        ];
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 2000, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 3900, 5000),
            Ok(Readiness::Ready)
        );
    }

    #[test]
    fn partial_read_keeps_later_mentions_and_their_original_times() {
        let events = [
            chat(1, 0, "@w1 first"),
            chat(2, 1000, "context"),
            chat(3, 1500, "@w1 second"),
        ];
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 1, 3000, 20000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 1, 3500, 20000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 2, 3500, 20000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 3, 3500, 20000),
            Ok(Readiness::Pending)
        );
        // Consuming the only mention leaves its unaddressed continuation as context.
        assert_eq!(
            evaluate(WakeMode::Mentions, &events[..2], 1, 3500, 20000),
            Ok(Readiness::Pending)
        );
    }

    #[test]
    fn working_or_away_catchup_keeps_all_context_until_explicit_read() {
        let mut events: Vec<_> = (1..=12)
            .map(|seq| chat(seq, seq * 100, "peer chatter"))
            .collect();
        events.push(stamp(13, 1300, Classification::Context));
        events.push(stamp(14, 1400, Classification::Context));
        events.push(chat(15, 1500, "@w1 task one"));
        events.push(chat(16, 1600, "@w1 task two"));
        // No active waiter or availability mutation is required to keep timing.
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 4000, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(events.len(), 16);
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 16, 4000, 5000),
            Ok(Readiness::Pending)
        );
    }

    #[test]
    fn moderation_and_voice_neither_wake_nor_delay_mentions() {
        let mut events = vec![chat(1, 0, "@w1 local task")];
        for (offset, modes) in ["+m", "+v w1", "-v w1", "-m"].into_iter().enumerate() {
            let class = classify(&message("MODE", "#work", modes), IDENTITY, false);
            assert_eq!(class, Classification::Context);
            events.push(stamp(offset as u64 + 2, 1900, class));
        }
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 2000, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 1, 2000, 5000),
            Ok(Readiness::Pending)
        );
    }

    #[test]
    fn high_water_counts_unread_events_not_old_retained_transcript() {
        let events: Vec<_> = (1..=512).map(|seq| chat(seq, 0, "chatter")).collect();
        assert_eq!(
            evaluate(WakeMode::Mentions, &events[..383], 0, 0, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events[..384], 0, 0, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 128, 0, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 129, 0, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 511, 0, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 512, 0, 5000),
            Ok(Readiness::Pending)
        );
    }

    #[test]
    fn high_water_counts_unread_bytes_independently() {
        let mut events = [chat(1, 0, "chatter"), chat(2, 0, "chatter")];
        events[0].bytes = 384 * 1024 - 1;
        events[1].bytes = 1;
        assert_eq!(
            evaluate(WakeMode::Mentions, &events[..1], 0, 0, 5000),
            Ok(Readiness::Pending)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 0, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 1, 0, 5000),
            Ok(Readiness::Pending)
        );
    }

    #[test]
    fn evicted_mention_and_completely_evicted_buffer_both_surface_gap() {
        let events: Vec<_> = (89..=600).map(|seq| chat(seq, 0, "chatter")).collect();
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 0, 5000),
            Ok(Readiness::Ready)
        );
        // Gap persists independently of high-water pressure after heavy eviction.
        assert_eq!(
            evaluate(WakeMode::Mentions, &events[511..], 0, 0, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            readiness(
                WakeMode::Mentions,
                Snapshot {
                    cursor: 0,
                    sequence: 1,
                    events: [],
                    capacity: 512,
                    byte_capacity: 512 * 1024
                },
                Duration::ZERO,
                Duration::from_secs(5)
            ),
            Ok(Readiness::Ready)
        );
    }

    #[test]
    fn restored_unread_relevance_is_immediate_and_survives_partial_read() {
        let mut events = [
            chat(1, 0, "@w1 old"),
            chat(2, 0, "@w1 later"),
            chat(3, 100, "live context"),
        ];
        events[0].observed_at = None;
        events[1].observed_at = None;
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 0, 100, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 1, 100, 5000),
            Ok(Readiness::Ready)
        );
        assert_eq!(
            evaluate(WakeMode::Mentions, &events, 2, 100, 5000),
            Ok(Readiness::Pending)
        );
        events[0].class = Classification::Context;
        assert_eq!(
            evaluate(WakeMode::Mentions, &events[..1], 0, 100, 5000),
            Ok(Readiness::Pending)
        );
    }

    #[test]
    fn invalid_snapshots_return_errors_instead_of_idle_or_false_readiness() {
        assert_eq!(
            evaluate(WakeMode::Mentions, &[chat(1, 1, "@w1")], 0, 0, 5000),
            Err(SnapshotError::InvalidTimestamp)
        );
        assert_eq!(
            evaluate(
                WakeMode::Mentions,
                &[chat(1, 2, "@w1"), chat(2, 1, "x")],
                0,
                2,
                5000
            ),
            Err(SnapshotError::InvalidTimestamp)
        );
        let mut reset_clock = [chat(1, 1, "@w1"), chat(2, 2, "x")];
        reset_clock[1].observed_at = None;
        assert_eq!(
            evaluate(WakeMode::Mentions, &reset_clock, 0, 2, 5000),
            Err(SnapshotError::InvalidTimestamp)
        );
        for events in [
            vec![chat(0, 0, "x")],
            vec![chat(1, 0, "x"), chat(1, 0, "x")],
            vec![chat(1, 0, "x"), chat(3, 0, "x")],
        ] {
            assert_eq!(
                evaluate(WakeMode::Mentions, &events, 0, 0, 5000),
                Err(SnapshotError::InvalidSequence)
            );
        }
        assert_eq!(
            evaluate(WakeMode::Mentions, &[chat(1, 0, "x")], 2, 0, 5000),
            Err(SnapshotError::InvalidCursor)
        );
        let mut oversize = chat(1, 0, "@w1");
        oversize.bytes = 512 * 1024 + 1;
        assert_eq!(
            evaluate(WakeMode::Mentions, &[oversize], 0, 0, 5000),
            Err(SnapshotError::RetentionExceeded)
        );
    }

    #[test]
    fn capacity_bounds_iteration_even_if_caller_supplies_unbounded_events() {
        let events = (1..).map(|seq| chat(seq, 0, "x"));
        assert_eq!(
            readiness(
                WakeMode::Mentions,
                Snapshot {
                    cursor: 0,
                    sequence: 100,
                    events,
                    capacity: 8,
                    byte_capacity: 1024
                },
                Duration::ZERO,
                Duration::from_secs(5)
            ),
            Err(SnapshotError::RetentionExceeded)
        );
    }

    #[test]
    fn invalid_limits_or_missing_snapshot_tail_are_visible_errors() {
        for (capacity, byte_capacity) in [(0, 1024), (512, 0)] {
            assert_eq!(
                readiness(
                    WakeMode::Mentions,
                    Snapshot {
                        cursor: 0,
                        sequence: 0,
                        events: [],
                        capacity,
                        byte_capacity
                    },
                    Duration::ZERO,
                    Duration::from_secs(5)
                ),
                Err(SnapshotError::InvalidLimits)
            );
        }
        for sequence in [0, 2] {
            assert_eq!(
                readiness(
                    WakeMode::Mentions,
                    Snapshot {
                        cursor: 0,
                        sequence,
                        events: [chat(1, 0, "x")],
                        capacity: 512,
                        byte_capacity: 512 * 1024
                    },
                    Duration::ZERO,
                    Duration::from_secs(5)
                ),
                Err(SnapshotError::InvalidSequence)
            );
        }
    }

    #[test]
    fn fractional_high_water_rounds_up_without_overflow() {
        let events = [
            chat(1, 0, "x"),
            chat(2, 0, "x"),
            chat(3, 0, "x"),
            chat(4, 0, "x"),
        ];
        for (count, expected) in [(3, Readiness::Pending), (4, Readiness::Ready)] {
            assert_eq!(
                readiness(
                    WakeMode::Mentions,
                    Snapshot {
                        cursor: 0,
                        sequence: count as u64,
                        events: events[..count].iter().copied(),
                        capacity: 5,
                        byte_capacity: usize::MAX
                    },
                    Duration::ZERO,
                    Duration::from_secs(5)
                ),
                Ok(expected)
            );
        }
    }
}
