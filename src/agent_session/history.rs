use super::state::State;
use crate::{
    client::Connection,
    protocol::{Message, fold},
};
use anyhow::{Context, Result, bail, ensure};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

/// One LATEST page per fresh live JOIN; deadline includes time in the queue.
const JOIN_PAGE: usize = 20;
const JOIN_BYTES: usize = 2 * 1024 * 1024;
const JOIN_DEADLINE: Duration = Duration::from_secs(20);
const MAX_QUEUED: usize = 128;
const MAX_QUEUE_BYTES: usize = 16 * 1024;
/// Active plus discarded chathistory batch IDs.
const MAX_BATCHES: usize = 8;

/// A local fact for the event log; critical notices wake every waiter.
pub(super) struct Notice {
    pub critical: bool,
    pub text: String,
}

/// How the session handles one inbound event while a JOIN page is fetched.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Disposition {
    Live,
    /// A message of the active page; `anchor` permits moving the channel's
    /// recovery checkpoint (no newer live message has been seen there).
    Replay {
        anchor: bool,
    },
    /// Correlation traffic, a discarded line, or an uncorrelated reply.
    Consumed,
}

struct Queued {
    channel: String,
    deadline: Instant,
    live: bool,
}

struct Active {
    channel: String,
    deadline: Instant,
    batch: Option<String>,
    messages: usize,
    bytes: usize,
    complete: bool,
    canceled: bool,
    live: bool,
}

/// Event-driven initial history for fresh live JOINs: one correlated request
/// at a time, streamed into the bounded event log while live input continues.
#[derive(Default)]
pub(super) struct JoinHistory {
    queue: VecDeque<Queued>,
    active: Option<Active>,
    /// Late or uncorrelated chathistory batches whose lines are dropped.
    discard: VecDeque<String>,
    /// After a timeout or uncorrelated reply, no further fetch on this connection.
    quarantined: bool,
    /// Per joined channel; removed when the channel is left.
    status: BTreeMap<String, String>,
}

impl JoinHistory {
    pub fn status(&self, key: &str) -> Option<&str> {
        self.status.get(key).map(String::as_str)
    }

    fn set(&mut self, channel: &str, status: impl Into<String>) {
        self.status.insert(fold(channel), status.into());
    }

    pub fn enqueue(&mut self, channel: &str, now: Instant) -> Option<Notice> {
        let key = fold(channel);
        if self
            .active
            .as_ref()
            .is_some_and(|a| fold(&a.channel) == key)
            || self.queue.iter().any(|q| fold(&q.channel) == key)
        {
            return None;
        }
        let bytes = self.queue.iter().map(|q| q.channel.len()).sum::<usize>() + channel.len();
        let refused = if self.quarantined {
            "history setup is stopped for this connection"
        } else if self.queue.len() >= MAX_QUEUED || bytes > MAX_QUEUE_BYTES {
            "the history setup queue is full"
        } else {
            self.queue.push_back(Queued {
                channel: channel.into(),
                deadline: now + JOIN_DEADLINE,
                live: false,
            });
            self.set(channel, "queued");
            return None;
        };
        self.set(channel, "unavailable");
        Some(Notice {
            critical: true,
            text: format!(
                "GAP: initial history for {channel} was not fetched: {refused}; reconnect explicitly to recover it"
            ),
        })
    }

    /// The channel was left or kicked: drop it, keeping an in-flight reply correlated.
    pub fn cancel(&mut self, key: &str) -> Option<Notice> {
        self.status.remove(key);
        self.queue.retain(|q| fold(&q.channel) != key);
        let active = self.active.as_mut().filter(|a| fold(&a.channel) == key)?;
        active.canceled = true;
        Some(Notice {
            critical: false,
            text: format!(
                "{}: initial history canceled; no longer joined",
                active.channel
            ),
        })
    }

    /// The next request to write, if none is in flight.
    pub fn dispatch(&mut self, now: Instant) -> (Option<Message>, Vec<Notice>) {
        let mut notices = Vec::new();
        if self.active.is_some() || self.quarantined {
            return (None, notices);
        }
        while let Some(next) = self.queue.pop_front() {
            if now >= next.deadline {
                self.set(&next.channel, "incomplete: timed out while queued");
                notices.push(Notice {
                    critical: true,
                    text: format!("GAP: initial history for {} timed out while queued; context may be missing", next.channel),
                });
                continue;
            }
            self.set(&next.channel, "loading");
            notices.push(Notice {
                critical: false,
                text: format!("{}: joined; initial bounded history window (up to {JOIN_PAGE} messages) follows; earlier context may exist", next.channel),
            });
            let request = Message::new("CHATHISTORY", ["LATEST", next.channel.as_str(), "*", "20"]);
            self.active = Some(Active {
                channel: next.channel,
                deadline: next.deadline,
                batch: None,
                messages: 0,
                bytes: 0,
                complete: true,
                canceled: false,
                live: next.live,
            });
            return (Some(request), notices);
        }
        (None, notices)
    }

    /// The request could not be queued for writing; nothing was sent.
    pub fn unsent(&mut self) -> Option<Notice> {
        let active = self.active.take()?;
        self.set(&active.channel, "unavailable: request not sent");
        Some(Notice {
            critical: true,
            text: format!(
                "GAP: initial history for {} was not requested; connection queue unavailable",
                active.channel
            ),
        })
    }

    fn remember(&mut self, batch: &str) {
        if self.discard.len() == MAX_BATCHES - 1 {
            self.discard.pop_front();
        }
        self.discard.push_back(batch.into());
    }

    /// Stop fetching on this connection rather than let a late or ambiguous
    /// reply contaminate another channel's page.
    fn quarantine(&mut self, why: &str) -> Notice {
        self.quarantined = true;
        if let Some(active) = self.active.take() {
            if let Some(batch) = &active.batch {
                self.remember(batch);
            }
            self.set(&active.channel, format!("incomplete: {why}"));
        }
        for queued in std::mem::take(&mut self.queue) {
            self.set(&queued.channel, "unavailable: history setup stopped");
        }
        Notice {
            critical: true,
            text: format!(
                "GAP: initial history setup stopped for this connection: {why}; reconnect explicitly to recover"
            ),
        }
    }

    pub fn expire(&mut self, now: Instant) -> Option<Notice> {
        if self.active.as_ref().is_some_and(|a| now >= a.deadline) {
            return Some(self.quarantine("a history reply timed out"));
        }
        None
    }

    fn finish(&mut self, active: Active) -> Option<Notice> {
        if active.canceled {
            return None;
        }
        if active.complete {
            self.set(
                &active.channel,
                format!("complete ({} messages)", active.messages),
            );
            return None;
        }
        self.set(&active.channel, "incomplete");
        Some(Notice {
            critical: true,
            text: format!(
                "GAP: initial history for {} is incomplete (over {JOIN_PAGE} messages or {JOIN_BYTES} bytes, or uncorrelated content); context may be missing",
                active.channel
            ),
        })
    }

    pub fn observe(&mut self, m: &Message) -> (Disposition, Option<Notice>) {
        let tag = m.tags.get("batch");
        let active_batch = self.active.as_ref().and_then(|a| a.batch.clone());
        if m.command == "BATCH" {
            let first = m.params.first().map(String::as_str).unwrap_or("");
            if let Some(id) = first.strip_prefix('+')
                && m.params.get(1).is_some_and(|kind| kind == "chathistory")
            {
                if let Some(active) = &mut self.active
                    && active.batch.is_none()
                    && tag.is_none()
                    && m.params
                        .get(2)
                        .is_some_and(|t| fold(t) == fold(&active.channel))
                {
                    active.batch = Some(id.into());
                    return (Disposition::Consumed, None);
                }
                self.remember(id);
                return (
                    Disposition::Consumed,
                    Some(self.quarantine("an uncorrelated history batch arrived")),
                );
            }
            if let Some(id) = first.strip_prefix('-') {
                if active_batch.as_deref() == Some(id) {
                    let active = self.active.take().expect("active batch");
                    return (Disposition::Consumed, self.finish(active));
                }
                if let Some(n) = self.discard.iter().position(|d| d == id) {
                    self.discard.remove(n);
                    return (Disposition::Consumed, None);
                }
            }
        }
        if let Some(tag) = tag {
            if active_batch.as_ref() == Some(tag) {
                return (self.line(m), None);
            }
            if self.discard.contains(tag) {
                return (Disposition::Consumed, None);
            }
            return (Disposition::Live, None);
        }
        if m.command == "FAIL" && m.params.first().is_some_and(|c| c == "CHATHISTORY") {
            let correlated = self.active.as_ref().is_some_and(|a| {
                a.batch.is_none() && m.params.get(2).is_some_and(|t| fold(t) == fold(&a.channel))
            });
            if !correlated {
                return (
                    Disposition::Consumed,
                    Some(self.quarantine("an uncorrelated history failure arrived")),
                );
            }
            let active = self.active.take().expect("correlated request");
            if active.canceled {
                return (Disposition::Consumed, None);
            }
            self.set(&active.channel, "unavailable");
            return (
                Disposition::Consumed,
                Some(Notice {
                    critical: true,
                    text: format!(
                        "GAP: initial history unavailable for {}: {}",
                        active.channel,
                        m.params.join(" ")
                    ),
                }),
            );
        }
        if matches!(m.command.as_str(), "PRIVMSG" | "NOTICE")
            && let Some(target) = m.params.first().map(|t| fold(t))
        {
            // Live traffic after JOIN is newer than the page; keep its anchor.
            if let Some(active) = self.active.as_mut().filter(|a| fold(&a.channel) == target) {
                active.live = true;
            }
            for queued in self.queue.iter_mut().filter(|q| fold(&q.channel) == target) {
                queued.live = true;
            }
        }
        (Disposition::Live, None)
    }

    fn line(&mut self, m: &Message) -> Disposition {
        let active = self.active.as_mut().expect("active batch");
        if active.canceled || !active.complete {
            return Disposition::Consumed;
        }
        let correlated = matches!(m.command.as_str(), "PRIVMSG" | "NOTICE")
            && m.params.len() == 2
            && fold(&m.params[0]) == fold(&active.channel);
        if correlated {
            active.messages += usize::from(m.tags.contains_key("msgid"));
            active.bytes += m.params[1].len();
        }
        if !correlated || active.messages > JOIN_PAGE || active.bytes > JOIN_BYTES {
            active.complete = false;
            return Disposition::Consumed;
        }
        Disposition::Replay {
            anchor: !active.live,
        }
    }
}

// Recovery is bounded even while a channel remains busy. It cannot prove an
// unlimited complete history; notices explain absent references and truncation.
pub(super) async fn recover(
    connection: &mut Connection,
    state: &mut State,
    channels: &[String],
) -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(20),
        recover_inner(connection, state, channels),
    )
    .await
    .context("history recovery timed out; session was not started, reconnect explicitly")?
}

async fn recover_inner(
    connection: &mut Connection,
    state: &mut State,
    channels: &[String],
) -> Result<()> {
    let anchors = state.checkpoints.clone();
    // Keep recovery deduplication independent of the 512 reply-reference ring:
    // catching up other channels can evict a reference before live is drained.
    // Bounded by configured channels (128) * pages (6) * page size (20) + 512.
    let mut seen = state.remembered_ids();
    let mut live = Vec::new();
    // Startup JOIN responses are already in the connection's bounded queue.
    // Membership evidence is tracked as each live event is buffered, so a
    // KICK stops that channel's recovery at once.
    while let Ok(message) = connection.inbound.try_recv() {
        ensure!(
            live.len() < 512,
            "live events exceeded recovery buffer; reconnect explicitly"
        );
        state.track(&message);
        live.push(message);
    }
    for channel in channels {
        if !state.joined(channel) {
            continue;
        }
        let mut reference = anchors.get(&fold(channel)).map(|id| format!("msgid={id}"));
        let mut latest = reference.is_none();
        if latest {
            state.context(format!("{channel}: initial bounded history window (up to 20 messages); earlier context may exist"));
        }
        for page in 0..6 {
            let m = Message::new(
                "CHATHISTORY",
                [
                    if latest { "LATEST" } else { "AFTER" },
                    channel.as_str(),
                    reference.as_deref().unwrap_or("*"),
                    "20",
                ],
            );
            connection
                .outbound
                .send(vec![m])
                .await
                .context("disconnected during history request")?;
            let mut batch = None;
            let mut count = 0;
            let mut last = None;
            let mut lost = false;
            loop {
                let m = connection
                    .inbound
                    .recv()
                    .await
                    .context("disconnected during history recovery")?;
                // Every reply must name this request's target; an uncorrelated
                // one could belong to nothing this client asked for.
                if m.command == "FAIL" && m.params.first().is_some_and(|v| v == "CHATHISTORY") {
                    if !m.params.get(2).is_some_and(|v| fold(v) == fold(channel)) {
                        bail!("uncorrelated history failure during recovery; reconnect explicitly");
                    }
                    if state.joined(channel) {
                        state.notice(format!(
                            "GAP: history unavailable for {channel}: {}",
                            m.params.join(" ")
                        ));
                    }
                    lost = !latest && m.params.get(1).is_some_and(|v| v == "HISTORY_LOST");
                    break;
                }
                if m.command == "BATCH" && batch.is_none() {
                    if let Some(id) = m.params.first().and_then(|v| v.strip_prefix('+'))
                        && m.params.get(1).is_some_and(|v| v == "chathistory")
                    {
                        if !m.params.get(2).is_some_and(|v| fold(v) == fold(channel)) {
                            bail!(
                                "uncorrelated history batch during recovery; reconnect explicitly"
                            );
                        }
                        batch = Some(id.to_string());
                        continue;
                    }
                } else if m.command == "BATCH"
                    && m.params.first().and_then(|v| v.strip_prefix('-')) == batch.as_deref()
                {
                    break;
                }
                if batch.is_some() && m.tags.get("batch") == batch.as_ref() {
                    ensure!(
                        !matches!(m.command.as_str(), "PRIVMSG" | "NOTICE")
                            || m.params.first().is_some_and(|v| fold(v) == fold(channel)),
                        "history batch contained another target; reconnect explicitly"
                    );
                    // A KICK stops this channel: its remaining replay is dropped.
                    if !state.joined(channel) {
                        continue;
                    }
                    // Multiline fallback lines after the first carry no msgid;
                    // only identified lines count as messages or move the anchor.
                    if let Some(id) = m.tags.get("msgid") {
                        count += 1;
                        ensure!(count <= 20, "server exceeded requested history bound");
                        last = Some(id.clone());
                        seen.insert(id.clone());
                    }
                    state.ingest(&m, true);
                } else {
                    ensure!(
                        live.len() < 512,
                        "live events exceeded recovery buffer; reconnect explicitly"
                    );
                    state.track(&m);
                    live.push(m);
                }
            }
            if !state.joined(channel) {
                state.context(format!(
                    "{channel}: history recovery stopped; no longer joined"
                ));
                break;
            }
            if lost {
                latest = true;
                reference = None;
                continue;
            }
            // Byte-bounded pages can be short; only an empty page ends catch-up.
            if count == 0 || latest {
                break;
            }
            if page == 5 {
                state.notice(format!("GAP: {channel} catch-up capped at 6 history pages; additional history may be missing"));
                break;
            }
            reference = last.map(|id| format!("msgid={id}"));
            ensure!(
                reference.is_some(),
                "history response has no message reference"
            );
        }
    }
    replay_live(state, live, seen);
    Ok(())
}

fn replay_live(state: &mut State, live: Vec<Message>, mut seen: HashSet<String>) {
    for message in live {
        if message
            .tags
            .get("msgid")
            .is_some_and(|id| !seen.insert(id.clone()))
        {
            continue;
        }
        state.ingest(&message, false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientConfig;

    #[test]
    fn live_recovery_does_not_replay_evicted_history_or_regress_channel_anchor() {
        let config: ClientConfig = toml::from_str(
            r##"
            host = "localhost"
            port = 6697
            account = "alice"
            nick = "alice"
            token_file = "unused"
            channels = ["#first", "#other"]
        "##,
        )
        .unwrap();
        let mut state = State::new(&config).unwrap();
        let mut seen = state.remembered_ids();
        let mut buffered = Vec::new();
        for n in 0..600 {
            let mut m = Message::new(
                "PRIVMSG",
                [if n < 2 { "#first" } else { "#other" }, "message"],
            );
            m.prefix = Some("Human!human@airc".into());
            m.tags.insert("account".into(), "human".into());
            m.tags.insert("msgid".into(), format!("id-{n}"));
            if n == 0 {
                buffered.push(m.clone());
            }
            seen.insert(format!("id-{n}"));
            state.ingest(&m, true);
        }
        assert!(state.resolve_reference("@1").is_err());
        while state.has_unread() {
            state.read(200);
        }
        replay_live(&mut state, buffered, seen);
        assert!(
            !state.has_unread(),
            "duplicate must not become new conversation"
        );
        assert_eq!(state.checkpoints["#first"], "id-1");
    }

    fn line(text: &str) -> Message {
        Message::parse(text).unwrap()
    }

    #[test]
    fn join_history_correlates_one_page_at_a_time_and_quarantines_strays() {
        let now = Instant::now();
        let mut history = JoinHistory::default();
        assert!(history.enqueue("#a", now).is_none());
        assert!(history.enqueue("#A", now).is_none(), "deduplicated by fold");
        assert!(history.enqueue("#b", now).is_none());
        let (request, notices) = history.dispatch(now);
        assert_eq!(request.unwrap().params, ["LATEST", "#a", "*", "20"]);
        assert!(notices.iter().all(|n| !n.critical));
        assert!(history.dispatch(now).0.is_none(), "one request in flight");
        // Live traffic newer than the page keeps the live anchor.
        let live = line("@msgid=live :Bob!bob@airc PRIVMSG #a :live");
        assert_eq!(history.observe(&live).0, Disposition::Live);
        let open = line(":test.airc BATCH +h1 chathistory #a");
        assert_eq!(history.observe(&open).0, Disposition::Consumed);
        let replay = line("@batch=h1;msgid=1 :Bob!bob@airc PRIVMSG #a :old");
        assert_eq!(
            history.observe(&replay).0,
            Disposition::Replay { anchor: false }
        );
        // Another target inside the page makes it incomplete, never live.
        let stray = line("@batch=h1;msgid=2 :Bob!bob@airc PRIVMSG #b :elsewhere");
        assert_eq!(history.observe(&stray).0, Disposition::Consumed);
        let (end, notice) = history.observe(&line(":test.airc BATCH -h1"));
        assert_eq!(end, Disposition::Consumed);
        assert!(notice.is_some_and(|n| n.critical));
        assert_eq!(history.status("#a"), Some("incomplete"));
        assert_eq!(history.dispatch(now).0.unwrap().params[1], "#b");
        // A failure naming another channel cannot settle #b: fetching stops.
        let (failure, notice) = history.observe(&line(
            ":test.airc FAIL CHATHISTORY INVALID_TARGET #a :Join first",
        ));
        assert_eq!(failure, Disposition::Consumed);
        assert!(notice.is_some_and(|n| n.critical));
        assert!(history.status("#b").unwrap().starts_with("incomplete"));
        assert!(history.enqueue("#c", now).is_some_and(|n| n.critical));
        assert!(history.dispatch(now).0.is_none());
        // A late reply is discarded rather than recorded as live conversation.
        for late in [
            ":test.airc BATCH +h2 chathistory #b",
            "@batch=h2;msgid=3 :Bob!bob@airc PRIVMSG #b :late",
            ":test.airc BATCH -h2",
        ] {
            assert_eq!(
                history.observe(&line(late)).0,
                Disposition::Consumed,
                "{late}"
            );
        }
    }

    #[test]
    fn join_history_bounds_cancellation_and_timeouts_are_explicit() {
        let now = Instant::now();
        let mut history = JoinHistory::default();
        history.enqueue("#a", now);
        history.dispatch(now);
        history.observe(&line(":test.airc BATCH +h1 chathistory #a"));
        for n in 0..=JOIN_PAGE {
            let m = line(&format!(
                "@batch=h1;msgid=m{n} :Bob!bob@airc PRIVMSG #a :{n}"
            ));
            let expected = if n < JOIN_PAGE {
                Disposition::Replay { anchor: true }
            } else {
                Disposition::Consumed
            };
            assert_eq!(history.observe(&m).0, expected);
        }
        assert!(history.observe(&line(":test.airc BATCH -h1")).1.is_some());
        // A KICK cancels a queued or in-flight page; its reply stays correlated.
        history.enqueue("#b", now);
        history.dispatch(now);
        assert!(history.cancel("#b").is_some());
        assert_eq!(history.status("#b"), None);
        assert_eq!(
            history
                .observe(&line(
                    ":test.airc FAIL CHATHISTORY INVALID_TARGET #b :Join first"
                ))
                .0,
            Disposition::Consumed
        );
        // A request that outlives its deadline stops further setup fetches.
        history.enqueue("#c", now);
        history.dispatch(now);
        let notice = history.expire(now + JOIN_DEADLINE).unwrap();
        assert!(notice.critical && notice.text.contains("timed out"));
        assert_eq!(
            history
                .observe(&line(":test.airc BATCH +h3 chathistory #c"))
                .0,
            Disposition::Consumed
        );
        assert_eq!(
            history
                .observe(&line("@batch=h3;msgid=x :Bob!bob@airc PRIVMSG #c :late"))
                .0,
            Disposition::Consumed
        );
    }
}
