use std::{
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::PathBuf,
};
use tokio::net::UnixListener;

pub(crate) struct PrivateSocket {
    pub(crate) path: PathBuf,
    identity: (u64, u64),
}

impl PrivateSocket {
    pub(crate) fn bind() -> (Self, UnixListener) {
        let uid = rustix::process::geteuid().as_raw();
        let daemon = std::fs::canonicalize("/tmp")
            .unwrap()
            .join(format!("codex-daemon-{uid}"));
        match std::fs::DirBuilder::new().mode(0o700).create(&daemon) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => panic!("{e}"),
        }
        let metadata = std::fs::symlink_metadata(&daemon).unwrap();
        assert!(metadata.is_dir() && metadata.uid() == uid && metadata.mode() & 0o777 == 0o700);
        // Only this random fixture socket is owned/removed, never the shared
        // daemon directory or an existing daemon's endpoint.
        let socket = daemon.join(format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        ));
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let meta = std::fs::symlink_metadata(&socket).unwrap();
        (
            Self {
                path: socket,
                identity: (meta.dev(), meta.ino()),
            },
            listener,
        )
    }
}

impl Drop for PrivateSocket {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.path)
            .is_ok_and(|meta| (meta.dev(), meta.ino()) == self.identity)
        {
            std::fs::remove_file(&self.path).unwrap();
        }
    }
}
