//! Per-batch replay protection, separate from the named-session state schema.
//! No message bodies or credentials. A record is retained until explicit human
//! maintenance; capacity exhaustion refuses, never evicts replay protection.

use anyhow::{Context, Result, ensure};
use rustix::fs::{AtFlags, Dir, Mode, OFlags, mkdirat, openat, renameat, unlinkat};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

const MAX_RECORD: u64 = 4096;
const MAX_ENTRIES: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    InFlight,
    Accepted,
    NotSent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub version: u8,
    pub batch: String,
    pub thread: String,
    pub payload_sha256: String,
    pub endpoint: PathBuf,
    pub endpoint_dev: u64,
    pub endpoint_ino: u64,
    pub phase: Phase,
    pub method: String,
    pub rpc_id: u64,
    pub expected_turn: Option<String>,
    pub accepted_turn: Option<String>,
}

impl Attempt {
    fn validate(&self) -> Result<()> {
        ensure!(self.version == 1, "unsupported submission journal version");
        for value in [&self.batch, &self.thread] {
            ensure!(
                uuid::Uuid::parse_str(value)?.hyphenated().to_string() == *value,
                "invalid journal UUID"
            );
        }
        ensure!(
            self.payload_sha256.len() == 64
                && self.payload_sha256.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid journal payload digest"
        );
        ensure!(
            self.endpoint.is_absolute() && self.endpoint.as_os_str().len() < 104,
            "invalid journal endpoint"
        );
        ensure!(
            matches!(self.method.as_str(), "turn/start" | "turn/steer") && self.rpc_id > 0,
            "invalid journal submission identity"
        );
        ensure!(
            (self.method == "turn/steer") == self.expected_turn.is_some(),
            "invalid journal expected turn"
        );
        for turn in [self.expected_turn.as_deref(), self.accepted_turn.as_deref()]
            .into_iter()
            .flatten()
        {
            ensure!(
                !uuid::Uuid::parse_str(turn)?.is_nil(),
                "invalid journal turn ID"
            );
        }
        ensure!(
            (self.phase == Phase::Accepted) == self.accepted_turn.is_some(),
            "invalid journal receipt state"
        );
        if self.phase == Phase::Accepted && self.method == "turn/steer" {
            ensure!(
                self.accepted_turn == self.expected_turn,
                "journal steer receipt names another turn"
            );
        }
        Ok(())
    }
}

pub struct Journal {
    dir: File,
    uid: u32,
    _lock: File,
    max_entries: usize,
}

fn private(file: &File, uid: u32, directory: bool) -> Result<()> {
    let meta = file.metadata()?;
    ensure!(
        meta.uid() == uid
            && meta.mode() & 0o077 == 0
            && if directory {
                meta.is_dir()
            } else {
                meta.is_file() && meta.nlink() == 1
            },
        "submission journal must be owned, private and not a symlink/hardlink"
    );
    Ok(())
}

impl Journal {
    pub fn open(state_dir: &Path) -> Result<Self> {
        ensure!(
            state_dir.is_absolute(),
            "AIRC delivery state directory must be absolute"
        );
        let uid = rustix::process::geteuid().as_raw();
        let parent = OpenOptions::new()
            .read(true)
            .custom_flags((OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC).bits() as i32)
            .open(state_dir)
            .context("cannot open delivery state directory")?;
        private(&parent, uid, true)?;
        match mkdirat(&parent, "codex-app-server", Mode::from_raw_mode(0o700)) {
            Ok(()) => parent.sync_all()?,
            Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        let dir = File::from(openat(
            &parent,
            "codex-app-server",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        private(&dir, uid, true)?;
        let lock = File::from(openat(
            &dir,
            "owner.lock",
            OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )?);
        private(&lock, uid, false)?;
        lock.try_lock()
            .context("another app-server delivery holds this session journal")?;
        Ok(Self {
            dir,
            uid,
            _lock: lock,
            max_entries: MAX_ENTRIES,
        })
    }

    #[cfg(test)]
    pub(super) fn set_test_capacity(&mut self, capacity: usize) {
        self.max_entries = capacity;
    }

    pub fn load(&self, batch: &str) -> Result<Option<Attempt>> {
        ensure!(
            uuid::Uuid::parse_str(batch)?.hyphenated().to_string() == batch,
            "invalid delivery UUID"
        );
        let name = format!("{batch}.json");
        let file = match openat(
            &self.dir,
            &name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        ) {
            Ok(fd) => File::from(fd),
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        private(&file, self.uid, false)?;
        let mut bytes = Vec::new();
        (&file).take(MAX_RECORD + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_RECORD,
            "submission journal record exceeds bound"
        );
        let record: Attempt = serde_json::from_slice(&bytes)
            .context("invalid submission journal; refusing to reset it")?;
        record.validate()?;
        ensure!(
            record.batch == batch,
            "journal record belongs to another batch"
        );
        // A receipt persisted just before a prior sync error is usable only
        // after the present read can establish durable storage again.
        file.sync_all()?;
        self.dir.sync_all()?;
        Ok(Some(record))
    }

    pub fn save(&self, record: &Attempt) -> Result<()> {
        record.validate()?;
        let bytes = serde_json::to_vec(record)?;
        ensure!(
            bytes.len() as u64 <= MAX_RECORD,
            "submission journal record exceeds bound"
        );
        let name = format!("{}.json", record.batch);
        if self.load(&record.batch)?.is_none() {
            let mut entries = 0;
            ensure!(
                self.max_entries > 0,
                "submission journal is full; inspect and retire settled records explicitly"
            );
            for entry in Dir::read_from(&self.dir)? {
                let entry = entry?;
                if ![c".", c"..", c"owner.lock"].contains(&entry.file_name()) {
                    entries += 1;
                }
                ensure!(
                    entries < self.max_entries,
                    "submission journal is full; inspect and retire settled records explicitly"
                );
            }
        }
        let temporary = format!(".{}.tmp", uuid::Uuid::new_v4());
        let result = (|| {
            let mut file = File::from(openat(
                &self.dir,
                &temporary,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            )?);
            file.write_all(&bytes)?;
            file.sync_all()?;
            renameat(&self.dir, &temporary, &self.dir, &name)?;
            self.dir.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = unlinkat(&self.dir, &temporary, AtFlags::empty());
        }
        result
    }
}
