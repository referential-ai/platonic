use crate::{AppError, AppResult};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

pub(super) const MANIFEST_FILE: &str = "00-manifest.json";
pub(super) const INPUT_FILE: &str = "01-input.md";
pub(super) const CANDIDATE_FILE: &str = "40-candidate.md";

pub(super) const PREPARE_FILES: StageFiles = StageFiles {
    name: "prepare",
    prompt: "10-prepare.prompt.md",
    result: "11-prepare.result.json",
    validation: "12-prepare.validation.json",
};

pub(super) const REFINE_FILES: StageFiles = StageFiles {
    name: "refine",
    prompt: "20-refine.prompt.md",
    result: "21-refine.result.json",
    validation: "22-refine.validation.json",
};

pub(super) const REVIEW_FILES: StageFiles = StageFiles {
    name: "review",
    prompt: "30-review.prompt.md",
    result: "31-review.result.json",
    validation: "32-review.validation.json",
};

#[cfg(test)]
pub(super) const ARTIFACT_ORDER: &[&str] = &[
    MANIFEST_FILE,
    INPUT_FILE,
    PREPARE_FILES.prompt,
    PREPARE_FILES.result,
    PREPARE_FILES.validation,
    REFINE_FILES.prompt,
    REFINE_FILES.result,
    REFINE_FILES.validation,
    REVIEW_FILES.prompt,
    REVIEW_FILES.result,
    REVIEW_FILES.validation,
    CANDIDATE_FILE,
];

#[derive(Clone, Copy)]
pub(super) struct StageFiles {
    pub(super) name: &'static str,
    pub(super) prompt: &'static str,
    pub(super) result: &'static str,
    pub(super) validation: &'static str,
}

pub(super) struct IssuePrepRun {
    dir: PathBuf,
}

impl IssuePrepRun {
    pub(super) fn start(run_dir: &Path, manifest: &impl Serialize, input: &str) -> AppResult<Self> {
        if run_dir.exists() {
            return Err(AppError::Config(format!(
                "issue-prep start requires a new run directory: {}",
                run_dir.display()
            )));
        }
        let parent = run_dir
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        fs::create_dir(run_dir).map_err(|error| {
            AppError::Config(format!(
                "cannot create issue-prep run directory {}: {error}",
                run_dir.display()
            ))
        })?;

        let run = Self {
            dir: run_dir.to_path_buf(),
        };
        run.write_json(MANIFEST_FILE, manifest)?;
        run.write_artifact(INPUT_FILE, input.as_bytes())?;
        sync_directory(&run.dir)?;
        Ok(run)
    }

    pub(super) fn write_json(&self, name: &str, value: &impl Serialize) -> AppResult<()> {
        self.write_artifact(name, &json_bytes(value)?)
    }

    pub(super) fn write_artifact(&self, name: &str, content: &[u8]) -> AppResult<()> {
        let path = self.path(name);
        if path.exists() {
            return Err(AppError::IssuePrepArtifactConflict(path));
        }

        let temp = self.dir.join(format!(".{name}.tmp"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(content)?;
        file.sync_all()?;
        drop(file);

        if path.exists() {
            fs::remove_file(&temp)?;
            return Err(AppError::IssuePrepArtifactConflict(path));
        }
        fs::rename(&temp, &path)?;
        sync_directory(&self.dir)
    }

    pub(super) fn read_json<T: DeserializeOwned>(&self, name: &str) -> AppResult<T> {
        Ok(serde_json::from_slice(&self.read(name)?)?)
    }

    pub(super) fn read_text(&self, name: &str) -> AppResult<String> {
        String::from_utf8(self.read(name)?)
            .map_err(|error| AppError::Config(format!("{name} is not UTF-8: {error}")))
    }

    pub(super) fn read(&self, name: &str) -> AppResult<Vec<u8>> {
        let path = self.path(name);
        if !path.is_file() {
            return Err(AppError::Config(format!(
                "issue-prep artifact is missing: {}",
                path.display()
            )));
        }
        Ok(fs::read(path)?)
    }

    pub(super) fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

pub(super) fn sha256(content: &[u8]) -> String {
    format!("{:x}", Sha256::digest(content))
}

fn json_bytes(value: &impl Serialize) -> AppResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> AppResult<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> AppResult<()> {
    Ok(())
}
