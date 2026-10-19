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
        ensure!(version <= 3, "database schema is newer than this binary");
        db.execute_batch("PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        let tx = db.transaction()?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, channel TEXT NOT NULL, time TEXT NOT NULL, message TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS messages_channel ON messages(channel, seq);
            CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, moderated INTEGER NOT NULL, topic TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS artifacts (id TEXT PRIMARY KEY, channel TEXT NOT NULL, filename TEXT NOT NULL, data BLOB NOT NULL, time TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS artifacts_channel ON artifacts(channel, time);
            CREATE TABLE IF NOT EXISTS bans (channel TEXT NOT NULL REFERENCES channels(name), mask TEXT NOT NULL, set_by TEXT NOT NULL, set_at INTEGER NOT NULL, PRIMARY KEY(channel, mask));
            PRAGMA user_version=3;")?;
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
        Ok(self.db.query_row(
            "SELECT moderated, topic FROM channels WHERE name=?1",
            [name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?)
    }

    pub fn set_channel(&self, name: &str, moderated: bool, topic: &str) -> Result<()> {
        self.db.execute(
            "UPDATE channels SET moderated=?2, topic=?3 WHERE name=?1",
            params![name, moderated, topic],
        )?;
        Ok(())
    }

    pub fn bans(&self, channel: &str, limit: usize) -> Result<Vec<Ban>> {
        let mut statement = self.db.prepare(
            "SELECT mask,set_by,set_at FROM bans WHERE channel=?1 ORDER BY mask LIMIT ?2",
        )?;
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
        let id = uuid::Uuid::new_v4().to_string();
        let time = now();
        message.tags.insert("msgid".into(), id.clone());
        message.tags.insert("time".into(), time.clone());
        let cutoff = self.cutoff();
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO messages(id,channel,time,message) VALUES (?1,?2,?3,?4)",
            params![id, channel, time, serde_json::to_string(message)?],
        )?;
        tx.execute("DELETE FROM messages WHERE channel=?1 AND (time < ?2 OR seq NOT IN (SELECT seq FROM messages WHERE channel=?1 ORDER BY seq DESC LIMIT ?3))", params![channel,cutoff,self.retention as i64])?;
        if let Some((id, filename, data, keep)) = artifact {
            tx.execute(
                "INSERT INTO artifacts(id,channel,filename,data,time) VALUES (?1,?2,?3,?4,?5)",
                params![id, channel, filename, data, time],
            )?;
            tx.execute("DELETE FROM artifacts WHERE channel=?1 AND (time < ?2 OR id NOT IN (SELECT id FROM artifacts WHERE channel=?1 ORDER BY rowid DESC LIMIT ?3))", params![channel,cutoff,keep as i64])?;
        }
        tx.commit()?;
        Ok(())
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
        Ok(())
    }
}
