use crate::{config::Config, protocol::Message};
use anyhow::{Result, ensure};
use rusqlite::{Connection, params};

pub struct Store {
    db: Connection,
    retention: usize,
    days: u32,
}

/// History index row: selection never decodes message bodies.
#[derive(Clone)]
pub struct Entry {
    pub seq: i64,
    pub id: String,
    pub time: String,
}

pub struct Artifact {
    pub channel: String,
    pub filename: String,
    pub data: Vec<u8>,
}

/// A live staged artifact: stored for one account and channel, unannounced
/// and never downloadable until a message publishes it.
pub struct Staged {
    pub account: String,
    pub channel: String,
    pub filename: String,
    pub sha256: String,
    pub bytes: u64,
}

/// Owner, channel and publishing message of a retained artifact. Artifacts
/// published before schema 4 have no recorded owner or message.
pub struct Published {
    pub account: Option<String>,
    pub channel: String,
    pub msgid: Option<String>,
}

/// Why staging stored nothing.
#[derive(Debug, PartialEq, Eq)]
pub enum StageRefusal {
    /// The account's live staged count or bytes would exceed its quota.
    Account,
    /// All stored staged bytes would exceed the server-wide cap.
    Server,
}

#[derive(Clone)]
pub struct Ban {
    pub mask: String,
    pub set_by: String,
    pub set_at: i64,
}

pub fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

impl Store {
    pub fn open(config: &Config) -> Result<Self> {
        if let Ok(meta) = std::fs::symlink_metadata(&config.database) {
            ensure!(
                meta.is_file() && !meta.file_type().is_symlink(),
                "database must be a regular file"
            );
        } else {
            crate::config::write_private(&config.database, b"")?;
        }
        let mut db = Connection::open(&config.database)?;
        db.busy_timeout(std::time::Duration::from_secs(2))?;
        let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        // Version 3 adds no columns: it marks rows that may hold multiline text,
        // which older binaries cannot replay, so they refuse the database.
        // Version 4 adds staged artifacts and artifact owner/message columns.
        ensure!(version <= 4, "database schema is newer than this binary");
        db.execute_batch("PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        let tx = db.transaction()?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, channel TEXT NOT NULL, time TEXT NOT NULL, message TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS messages_channel ON messages(channel, seq);
            CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, moderated INTEGER NOT NULL, topic TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS artifacts (id TEXT PRIMARY KEY, channel TEXT NOT NULL, filename TEXT NOT NULL, data BLOB NOT NULL, time TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS artifacts_channel ON artifacts(channel, time);
            CREATE TABLE IF NOT EXISTS bans (channel TEXT NOT NULL REFERENCES channels(name), mask TEXT NOT NULL, set_by TEXT NOT NULL, set_at INTEGER NOT NULL, PRIMARY KEY(channel, mask));
            CREATE TABLE IF NOT EXISTS staged (id TEXT PRIMARY KEY, account TEXT NOT NULL, channel TEXT NOT NULL, filename TEXT NOT NULL, sha256 TEXT NOT NULL, bytes INTEGER NOT NULL, data BLOB NOT NULL, expires TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS staged_account ON staged(account, expires);")?;
        if version < 4 {
            tx.execute_batch(
                "ALTER TABLE artifacts ADD COLUMN account TEXT;
                ALTER TABLE artifacts ADD COLUMN msgid TEXT;",
            )?;
        }
        tx.execute_batch("PRAGMA user_version=4;")?;
        // Expired staged rows never become visible; drop them with their bytes.
        tx.execute("DELETE FROM staged WHERE expires < ?1", [now()])?;
        // Startup honors the same explicit deletion intent as reload, so a
        // crash before or after a reload commit converges; repeating it is
        // harmless. Configured channels are never listed (validated).
        purge(&tx, &config.removed_channels)?;
        for c in &config.channels {
            tx.execute(
                "INSERT OR IGNORE INTO channels(name, moderated) VALUES (?1, ?2)",
                params![crate::protocol::fold(&c.name), c.moderated],
            )?;
        }
        tx.commit()?;
        Ok(Self {
            db,
            retention: config.limits.history_per_channel,
            days: config.limits.history_days,
        })
    }

    pub fn channel(&self, name: &str) -> Result<(bool, String)> {
        channel_row(&self.db, name)
    }

    /// Prepare one reload's catalog change in a single transaction: purge the
    /// explicitly removed channels, add the new ones (existing rows keep their
    /// topic, moderation, bans and history) and load each addition's stored
    /// state through `load`. Any error, `load`'s included, commits nothing, so
    /// a purge never outlives a failed addition.
    pub fn change_channels<T>(
        &mut self,
        removed: &[String],
        added: &[crate::config::Channel],
        ban_limit: usize,
        mut load: impl FnMut(&crate::config::Channel, (bool, String), Vec<Ban>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let tx = self.db.transaction()?;
        purge(&tx, removed)?;
        let mut loaded = Vec::with_capacity(added.len());
        for channel in added {
            let key = crate::protocol::fold(&channel.name);
            tx.execute(
                "INSERT OR IGNORE INTO channels(name, moderated) VALUES (?1, ?2)",
                params![key, channel.moderated],
            )?;
            loaded.push(load(
                channel,
                channel_row(&tx, &key)?,
                bans(&tx, &key, ban_limit)?,
            )?);
        }
        tx.commit()?;
        Ok(loaded)
    }

    /// Fault injection for staging tests only.
    #[cfg(test)]
    pub(crate) fn execute_for_test(&self, sql: &str) -> Result<()> {
        self.db.execute_batch(sql)?;
        Ok(())
    }

    /// Stored messages, artifacts, staged artifacts, bans and channel rows of
    /// one folded channel, for deletion tests only.
    #[cfg(test)]
    pub(crate) fn rows_for_test(&self, channel: &str) -> [i64; 5] {
        let count = |sql: &str| self.db.query_row(sql, [channel], |r| r.get(0)).unwrap();
        [
            count("SELECT COUNT(*) FROM messages WHERE channel=?1"),
            count("SELECT COUNT(*) FROM artifacts WHERE channel=?1"),
            count("SELECT COUNT(*) FROM staged WHERE channel=?1"),
            count("SELECT COUNT(*) FROM bans WHERE channel=?1"),
            count("SELECT COUNT(*) FROM channels WHERE name=?1"),
        ]
    }

    pub fn set_channel(&self, name: &str, moderated: bool, topic: &str) -> Result<()> {
        self.db.execute(
            "UPDATE channels SET moderated=?2, topic=?3 WHERE name=?1",
            params![name, moderated, topic],
        )?;
        Ok(())
    }

    pub fn bans(&self, channel: &str, limit: usize) -> Result<Vec<Ban>> {
        bans(&self.db, channel, limit)
    }

    pub fn add_ban(&self, channel: &str, ban: &Ban) -> Result<()> {
        self.db.execute(
            "INSERT INTO bans(channel,mask,set_by,set_at) VALUES (?1,?2,?3,?4)",
            params![channel, ban.mask, ban.set_by, ban.set_at],
        )?;
        Ok(())
    }

    pub fn remove_ban(&self, channel: &str, mask: &str) -> Result<()> {
        self.db.execute(
            "DELETE FROM bans WHERE channel=?1 AND mask=?2",
            params![channel, mask],
        )?;
        Ok(())
    }

    fn cutoff(&self) -> String {
        (chrono::Utc::now() - chrono::Duration::days(self.days.into()))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    pub fn append(
        &mut self,
        channel: &str,
        message: &mut Message,
        artifact: Option<(&str, &str, &[u8], usize)>,
    ) -> Result<()> {
        self.commit(channel, message, artifact, None)
    }

    /// Append one message and publish its staged artifacts, in order, in the
    /// same transaction. Any failure stores nothing and leaves them staged.
    pub fn publish(
        &mut self,
        channel: &str,
        message: &mut Message,
        staged: &[String],
        keep: usize,
    ) -> Result<()> {
        self.commit(channel, message, None, Some((staged, keep)))
    }

    fn commit(
        &mut self,
        channel: &str,
        message: &mut Message,
        artifact: Option<(&str, &str, &[u8], usize)>,
        staged: Option<(&[String], usize)>,
    ) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let time = now();
        message.tags.insert("msgid".into(), id.clone());
        message.tags.insert("time".into(), time.clone());
        let account = message.tags.get("account").cloned();
        let cutoff = self.cutoff();
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO messages(id,channel,time,message) VALUES (?1,?2,?3,?4)",
            params![id, channel, time, serde_json::to_string(message)?],
        )?;
        tx.execute("DELETE FROM messages WHERE channel=?1 AND (time < ?2 OR seq NOT IN (SELECT seq FROM messages WHERE channel=?1 ORDER BY seq DESC LIMIT ?3))", params![channel,cutoff,self.retention as i64])?;
        let mut keep = None;
        if let Some((artifact, filename, data, limit)) = artifact {
            tx.execute(
                "INSERT INTO artifacts(id,channel,filename,data,time,account,msgid) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![artifact, channel, filename, data, time, account, id],
            )?;
            keep = Some(limit);
        }
        if let Some((staged, limit)) = staged {
            for artifact in staged {
                let moved = tx.execute(
                    "INSERT INTO artifacts(id,channel,filename,data,time,account,msgid) SELECT id,channel,filename,data,?2,account,?3 FROM staged WHERE id=?1 AND channel=?4 AND account=?5 AND expires>=?2",
                    params![artifact, time, id, channel, account],
                )?;
                ensure!(moved == 1, "staged artifact is no longer available");
                tx.execute("DELETE FROM staged WHERE id=?1", [artifact])?;
            }
            keep = Some(limit);
        }
        if let Some(keep) = keep {
            tx.execute("DELETE FROM artifacts WHERE channel=?1 AND (time < ?2 OR id NOT IN (SELECT id FROM artifacts WHERE channel=?1 ORDER BY rowid DESC LIMIT ?3))", params![channel,cutoff,keep as i64])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Delete at most `limit` expired staged rows, bytes included.
    pub fn sweep_staged(&self, limit: usize) -> Result<usize> {
        Ok(self.db.execute(
            "DELETE FROM staged WHERE id IN (SELECT id FROM staged WHERE expires < ?1 LIMIT ?2)",
            params![now(), limit as i64],
        )?)
    }

    /// Store one unannounced artifact for `account` in folded `channel`. Quotas
    /// count live rows per account and every stored row server-wide.
    #[allow(clippy::too_many_arguments)]
    pub fn stage(
        &mut self,
        id: &str,
        account: &str,
        channel: &str,
        filename: &str,
        sha256: &str,
        data: &[u8],
        account_bytes: usize,
    ) -> Result<std::result::Result<(), StageRefusal>> {
        use crate::attachments::{STAGE_TTL_SECONDS, STAGED_PER_ACCOUNT, STAGED_TOTAL_BYTES};
        self.sweep_staged(crate::attachments::SWEEP_LIMIT)?;
        let time = now();
        let expires = (chrono::Utc::now() + chrono::Duration::seconds(STAGE_TTL_SECONDS))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let tx = self.db.transaction()?;
        let (count, bytes): (i64, i64) = tx.query_row(
            "SELECT COUNT(*), COALESCE(SUM(bytes),0) FROM staged WHERE account=?1 AND expires>=?2",
            params![account, time],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if count as usize >= STAGED_PER_ACCOUNT || bytes as usize + data.len() > account_bytes {
            return Ok(Err(StageRefusal::Account));
        }
        let total: i64 = tx.query_row("SELECT COALESCE(SUM(bytes),0) FROM staged", [], |r| {
            r.get(0)
        })?;
        if total as usize + data.len() > STAGED_TOTAL_BYTES {
            return Ok(Err(StageRefusal::Server));
        }
        tx.execute(
            "INSERT INTO staged(id,account,channel,filename,sha256,bytes,data,expires) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![id, account, channel, filename, sha256, data.len() as i64, data, expires],
        )?;
        tx.commit()?;
        Ok(Ok(()))
    }

    /// A live staged row; an expired one reads as absent even before a sweep.
    pub fn staged(&self, id: &str) -> Result<Option<Staged>> {
        use rusqlite::OptionalExtension;
        Ok(self
            .db
            .query_row(
                "SELECT account,channel,filename,sha256,bytes FROM staged WHERE id=?1 AND expires>=?2",
                params![id, now()],
                |r| {
                    Ok(Staged {
                        account: r.get(0)?,
                        channel: r.get(1)?,
                        filename: r.get(2)?,
                        sha256: r.get(3)?,
                        bytes: r.get::<_, i64>(4)? as u64,
                    })
                },
            )
            .optional()?)
    }

    /// Owner and message of a retained artifact, for consumed-ID refusals.
    pub fn published(&self, id: &str) -> Result<Option<Published>> {
        use rusqlite::OptionalExtension;
        Ok(self
            .db
            .query_row(
                "SELECT account,channel,msgid FROM artifacts WHERE id=?1 AND time>=?2",
                params![id, self.cutoff()],
                |r| {
                    Ok(Published {
                        account: r.get(0)?,
                        channel: r.get(1)?,
                        msgid: r.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    /// Retained history index in sequence order, without message bodies.
    pub fn history(&self, channel: &str) -> Result<Vec<Entry>> {
        let mut stmt = self.db.prepare(
            "SELECT seq,id,time FROM messages WHERE channel=?1 AND time>=?2 ORDER BY seq",
        )?;
        let rows = stmt.query_map(params![channel, self.cutoff()], |r| {
            Ok(Entry {
                seq: r.get(0)?,
                id: r.get(1)?,
                time: r.get(2)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Decode only the selected (page-bounded) history messages.
    pub fn messages(&self, entries: &[&Entry]) -> Result<Vec<Message>> {
        let mut stmt = self
            .db
            .prepare("SELECT message FROM messages WHERE seq=?1")?;
        entries
            .iter()
            .map(|e| {
                let message: String = stmt.query_row([e.seq], |r| r.get(0))?;
                Ok(serde_json::from_str(&message)?)
            })
            .collect()
    }

    pub fn artifact(&self, id: &str) -> Result<Option<Artifact>> {
        use rusqlite::OptionalExtension;
        Ok(self
            .db
            .query_row(
                "SELECT channel,filename,data FROM artifacts WHERE id=?1 AND time>=?2",
                params![id, self.cutoff()],
                |r| {
                    Ok(Artifact {
                        channel: r.get(0)?,
                        filename: r.get(1)?,
                        data: r.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn prune(&self) -> Result<()> {
        self.db
            .execute("DELETE FROM messages WHERE time < ?1", [self.cutoff()])?;
        self.db
            .execute("DELETE FROM artifacts WHERE time < ?1", [self.cutoff()])?;
        self.db
            .execute("DELETE FROM staged WHERE expires < ?1", [now()])?;
        Ok(())
    }
}

fn channel_row(db: &Connection, name: &str) -> Result<(bool, String)> {
    Ok(db.query_row(
        "SELECT moderated, topic FROM channels WHERE name=?1",
        [name],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?)
}

fn bans(db: &Connection, channel: &str, limit: usize) -> Result<Vec<Ban>> {
    let mut statement =
        db.prepare("SELECT mask,set_by,set_at FROM bans WHERE channel=?1 ORDER BY mask LIMIT ?2")?;
    let rows = statement.query_map(params![channel, (limit + 1) as i64], |r| {
        Ok(Ban {
            mask: r.get(0)?,
            set_by: r.get(1)?,
            set_at: r.get(2)?,
        })
    })?;
    let bans = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(bans.len() <= limit, "stored channel ban list exceeds limit");
    Ok(bans)
}

/// Delete every stored row of each named channel: messages, published and
/// staged artifacts, bans, topic and moderation. Only explicit deletion
/// intent reaches this; a channel merely absent from the configuration keeps
/// its rows. This is logical deletion, not secure erasure.
fn purge(db: &Connection, names: &[String]) -> Result<()> {
    for name in names {
        let key = crate::protocol::fold(name);
        for table in ["messages", "artifacts", "staged", "bans"] {
            db.execute(&format!("DELETE FROM {table} WHERE channel=?1"), [&key])?;
        }
        db.execute("DELETE FROM channels WHERE name=?1", [&key])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        attachments::{STAGED_PER_ACCOUNT, STAGED_TOTAL_BYTES},
        config::{Limits, token_hash},
    };

    fn open(dir: &std::path::Path) -> Store {
        Store::open(&config(dir)).unwrap()
    }

    fn config(dir: &std::path::Path) -> Config {
        Config {
            server_name: "test.airc".into(),
            listen: Some("127.0.0.1:0".parse().unwrap()),
            listen_unix: None,
            control_unix: None,
            membership_generation: None,
            database: dir.join("state.db"),
            allow_insecure: false,
            allow_private_messages: false,
            removed_channels: Vec::new(),
            tls: None,
            limits: Limits::default(),
            accounts: vec![crate::config::Account {
                name: "alice".into(),
                token_sha256: token_hash("test"),
            }],
            channels: vec![],
            artifacts: None,
        }
    }

    fn id(n: usize) -> String {
        format!("12345678-1234-4234-8234-{n:012}")
    }

    fn message(account: &str) -> Message {
        let mut m = Message::new("PRIVMSG", ["#work", "[Image #1]"]);
        m.tags.insert("account".into(), account.into());
        m
    }

    #[test]
    fn expired_staged_rows_are_not_live_cannot_publish_and_are_swept() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        let stage = |store: &mut Store, n: usize| {
            store
                .stage(&id(n), "alice", "#work", "x.png", "sha", b"x", 1 << 20)
                .unwrap()
        };
        for n in 0..STAGED_PER_ACCOUNT {
            assert_eq!(stage(&mut store, n), Ok(()));
        }
        assert_eq!(stage(&mut store, 99), Err(StageRefusal::Account));
        // Expire one row without sweeping it: it no longer counts or publishes.
        store
            .execute_for_test(&format!(
                "UPDATE staged SET expires='2000-01-01T00:00:00.000Z' WHERE id='{}'",
                id(0)
            ))
            .unwrap();
        assert!(store.staged(&id(0)).unwrap().is_none());
        assert!(store.staged(&id(1)).unwrap().is_some());
        let mut m = message("alice");
        assert!(store.publish("#work", &mut m, &[id(0)], 16).is_err());
        assert!(store.published(&id(0)).unwrap().is_none());
        // A refused publication stored no message and kept the live rows.
        assert!(store.history("#work").unwrap().is_empty());
        let count = |store: &Store| -> i64 {
            store
                .db
                .query_row("SELECT COUNT(*) FROM staged", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(count(&store), STAGED_PER_ACCOUNT as i64);
        // The next stage sweeps the expired row with its bytes and then fits.
        assert_eq!(stage(&mut store, 100), Ok(()));
        assert_eq!(count(&store), STAGED_PER_ACCOUNT as i64);
        let gone: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM staged WHERE id=?1", [id(0)], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(gone, 0);
        // Publishing moves rows atomically and records owner and message.
        let mut m = message("alice");
        store.publish("#work", &mut m, &[id(1)], 16).unwrap();
        let published = store.published(&id(1)).unwrap().unwrap();
        assert_eq!(published.account.as_deref(), Some("alice"));
        assert_eq!(published.msgid.as_deref(), Some(m.tags["msgid"].as_str()));
        assert!(store.staged(&id(1)).unwrap().is_none());
        // Another account's or channel's staged row never publishes.
        let mut m = message("bob");
        assert!(store.publish("#work", &mut m, &[id(2)], 16).is_err());
        let mut m = message("alice");
        assert!(store.publish("#other", &mut m, &[id(2)], 16).is_err());
        assert_eq!(store.history("#work").unwrap().len(), 1);
    }

    #[test]
    fn server_wide_staged_bytes_include_unswept_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        // One recorded row near the cap (its BLOB is small; quotas use `bytes`).
        store
            .execute_for_test(&format!(
                "INSERT INTO staged VALUES ('{}','bob','#work','b.png','sha',{},x'00','2999-01-01T00:00:00.000Z')",
                id(1),
                STAGED_TOTAL_BYTES - 1
            ))
            .unwrap();
        assert_eq!(
            store
                .stage(&id(2), "alice", "#work", "a.png", "sha", b"xy", 1 << 20)
                .unwrap(),
            Err(StageRefusal::Server)
        );
        assert_eq!(
            store
                .stage(&id(3), "alice", "#work", "a.png", "sha", b"x", 1 << 20)
                .unwrap(),
            Ok(())
        );
    }

    #[test]
    fn startup_purges_only_explicitly_removed_channels_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = |channels: &[&str], removed: &[&str]| Config {
            channels: channels
                .iter()
                .map(|name| crate::config::Channel {
                    name: (*name).into(),
                    members: vec!["alice".into()],
                    operators: vec![],
                    moderated: false,
                })
                .collect(),
            removed_channels: removed.iter().map(|name| (*name).into()).collect(),
            ..config(dir.path())
        };
        let mut store = Store::open(&catalog(&["#a", "#b"], &[])).unwrap();
        for (n, channel) in ["#a", "#b"].into_iter().enumerate() {
            store.append(channel, &mut message("alice"), None).unwrap();
            store.set_channel(channel, true, "kept topic").unwrap();
            let ban = Ban {
                mask: "evil!*@*".into(),
                set_by: "alice".into(),
                set_at: 1,
            };
            store.add_ban(channel, &ban).unwrap();
            let staged = store.stage(&id(n), "alice", channel, "x.png", "sha", b"x", 1 << 20);
            assert_eq!(staged.unwrap(), Ok(()));
        }
        let seeded = [1, 0, 1, 1, 1];
        drop(store);
        // Omitting #b is no deletion intent: its rows stay.
        let store = Store::open(&catalog(&["#a"], &[])).unwrap();
        assert_eq!(store.rows_for_test("#b"), seeded);
        drop(store);
        // Listed intent purges at startup; repeating it is harmless and
        // leaves every other channel's rows.
        for _ in 0..2 {
            let store = Store::open(&catalog(&["#a"], &["#b"])).unwrap();
            assert_eq!(store.rows_for_test("#b"), [0; 5]);
            assert_eq!(store.rows_for_test("#a"), seeded);
        }
        // A later channel with the same name starts from fresh defaults.
        let store = Store::open(&catalog(&["#a", "#b"], &[])).unwrap();
        assert_eq!(store.channel("#b").unwrap(), (false, String::new()));
        assert!(store.bans("#b", 10).unwrap().is_empty());
        assert!(store.history("#b").unwrap().is_empty());
        assert_eq!(store.channel("#a").unwrap(), (true, "kept topic".into()));
    }

    #[test]
    fn schema_four_adds_staging_and_artifact_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let version: i64 = store
            .db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 4);
        drop(store);
        // Reopening an existing version-4 database adds nothing twice.
        let store = open(dir.path());
        assert!(store.published(&id(1)).unwrap().is_none());
    }
}
