use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct ExperimentFiles(PathBuf);

impl ExperimentFiles {
    pub const RELATIVE: &str = "experiments/thermal-loop.toml";

    pub fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "lab-server-experiment-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("experiments")).unwrap();
        Self(root)
    }

    pub fn main(&self) -> PathBuf {
        self.0.join("lab-server.toml")
    }

    pub fn experiment(&self) -> PathBuf {
        self.0.join(Self::RELATIVE)
    }
}

impl Drop for ExperimentFiles {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.experiment());
        let _ = fs::remove_file(self.main());
        let _ = fs::remove_dir(self.0.join("experiments"));
        let _ = fs::remove_dir(&self.0);
    }
}
