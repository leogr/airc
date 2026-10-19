use super::state::State;
use anyhow::{Context, Result, ensure};
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};
use tokio::net::UnixStream;

/// Retained events, the last read and receipts, including JSON escaping.
const MAX_STATE: u64 = 16 * 1024 * 1024;

pub(super) struct Storage {
    pub dir: PathBuf,
    pub uid: u32,
}

impl Storage {
    pub fn open(root: &Path, name: &str) -> Result<Self> {
        ensure!(
            !name.is_empty()
                && name.len() <= 32
                && name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c)),
            "session names use 1–32 ASCII letters, digits, underscores or hyphens"
        );
        let uid = UnixStream::pair()?.0.peer_cred()?.uid();
        private_dir(root, uid)?;
        let dir = root.join(name);
        private_dir(&dir, uid)?;
        let store = Self { dir, uid };
        ensure!(
            store.socket().as_os_str().len() < 100,
            "session socket path is too long; choose a shorter --state-dir"
        );
        Ok(store)
    }

    pub fn socket(&self) -> PathBuf {
        self.dir.join("client.sock")
    }

    // Never unlink the stable lock file. Its native advisory lock is released
    // by the kernel on exit, so stale PIDs are not used as lifecycle authority.
    pub fn lock(&self) -> Result<File> {
        let path = self.dir.join("owner.lock");
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                check_private(&path, self.uid, false)?;
                OpenOptions::new().read(true).write(true).open(&path)?
            }
            Err(e) => return Err(e.into()),
        };
        file.try_lock().context(
            "session is already running or starting; use status/disconnect before reconnecting",
        )?;
        Ok(file)
    }

    pub fn load(&self) -> Result<Option<State>> {
        let path = self.dir.join("state.json");
        match fs::symlink_metadata(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(_) => {}
        }
        check_private(&path, self.uid, false)?;
        let mut bytes = Vec::new();
        File::open(path)?
            .take(MAX_STATE + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_STATE,
            "session state exceeds its bound"
        );
        let state: State = serde_json::from_slice(&bytes)
            .context("invalid session state; refusing to reset it")?;
        state.validate()?;
        Ok(Some(state))
    }

    pub fn save(&self, state: &State) -> Result<()> {
        let bytes = serde_json::to_vec(state)?;
        ensure!(
            bytes.len() as u64 <= MAX_STATE,
            "session state exceeds its bound"
        );
        let temporary = self.dir.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, self.dir.join("state.json"))?;
            File::open(&self.dir)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(temporary);
        }
        result
    }
}

fn private_dir(path: &Path, uid: u32) -> Result<()> {
    DirBuilder::new().recursive(true).mode(0o700).create(path)?;
    check_private(path, uid, true)
}

fn check_private(path: &Path, uid: u32, directory: bool) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.uid() == uid
            && meta.permissions().mode() & 0o077 == 0
            && if directory {
                meta.is_dir()
            } else {
                meta.is_file()
            },
        "{} must be an owned private {} (no symlinks)",
        path.display(),
        if directory { "directory" } else { "file" }
    );
    Ok(())
}
