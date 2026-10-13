use super::state::State;
use crate::{
    client::Connection,
    protocol::{Message, fold},
};
use anyhow::{Context, Result, ensure};
use std::{collections::HashSet, time::Duration};

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
    // Configured JOIN responses are already in the connection's bounded queue.
    while let Ok(message) = connection.inbound.try_recv() {
        ensure!(
            live.len() < 512,
            "live events exceeded recovery buffer; reconnect explicitly"
        );
        live.push(message);
    }
    for channel in channels {
        let mut reference = anchors.get(&fold(channel)).map(|id| format!("msgid={id}"));
        let mut latest = reference.is_none();
        if latest {
            state.notice(format!("{channel}: initial bounded history window (up to 20 messages); earlier context may exist"));
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
                .send(m)
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
                if m.command == "FAIL" && m.params.first().is_some_and(|v| v == "CHATHISTORY") {
                    state.notice(format!(
                        "GAP: history unavailable for {channel}: {}",
                        m.params.join(" ")
                    ));
                    lost = !latest && m.params.get(1).is_some_and(|v| v == "HISTORY_LOST");
                    break;
                }
                if m.command == "BATCH" {
                    if m.params.get(1).is_some_and(|v| v == "chathistory")
                        && m.params.get(2).is_some_and(|v| fold(v) == fold(channel))
                        && let Some(id) = m.params.first().and_then(|v| v.strip_prefix('+'))
                    {
                        batch = Some(id.to_string());
                        continue;
                    }
                    if m.params.first().and_then(|v| v.strip_prefix('-')) == batch.as_deref()
                        && batch.is_some()
                    {
                        break;
                    }
                }
                if batch.is_some() && m.tags.get("batch") == batch.as_ref() {
                    count += 1;
                    ensure!(count <= 20, "server exceeded requested history bound");
                    last = m.tags.get("msgid").cloned();
                    if let Some(id) = &last {
                        seen.insert(id.clone());
                    }
                    state.ingest(&m, true);
                } else {
                    ensure!(
                        live.len() < 512,
                        "live events exceeded recovery buffer; reconnect explicitly"
                    );
                    live.push(m);
                }
            }
            if lost {
                latest = true;
                reference = None;
                continue;
            }
            if count < 20 || latest {
                break;
            }
            if page == 5 {
                state.notice(format!("GAP: {channel} catch-up capped at 120 messages; additional history may be missing"));
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
}
