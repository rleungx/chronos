use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=tso.proto");
    println!("cargo:rerun-if-env-changed=CHRONOS_BUILD_COMMIT");
    if let Some(commit) = resolve_build_commit() {
        println!("cargo:rustc-env=CHRONOS_BUILD_COMMIT={commit}");
    }
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["tso.proto"], &["."])?;
    Ok(())
}

fn resolve_build_commit() -> Option<String> {
    if let Ok(commit) = std::env::var("CHRONOS_BUILD_COMMIT") {
        let trimmed = commit.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    emit_git_watch_paths();
    git_stdout(&["rev-parse", "HEAD"])
}

fn emit_git_watch_paths() {
    for name in ["HEAD", "logs/HEAD", "packed-refs"] {
        if let Some(path) = git_path(name).filter(|path| path.exists()) {
            emit_watch_path(&path);
        }
    }

    let Some(reference) = git_stdout(&["symbolic-ref", "-q", "HEAD"]) else {
        return;
    };
    let Some(reference_path) = git_path(&reference) else {
        return;
    };
    if reference_path.exists() {
        emit_watch_path(&reference_path);
        return;
    }

    let Some(refs_root) = git_path("refs").and_then(|path| path.canonicalize().ok()) else {
        return;
    };
    let Some(parent) = reference_path
        .ancestors()
        .skip(1)
        .find(|path| path.is_dir())
        .and_then(|path| path.canonicalize().ok())
    else {
        return;
    };
    if parent.starts_with(refs_root) {
        emit_watch_path(&parent);
    }
}

fn git_path(name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(git_stdout(&["rev-parse", "--git-path", name])?);
    if path.is_absolute() {
        Some(path)
    } else {
        std::env::current_dir().ok().map(|cwd| cwd.join(path))
    }
}

fn emit_watch_path(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}

fn git_stdout(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git").args(args).output().ok()?;
    let value = String::from_utf8(output.status.success().then_some(output.stdout)?).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}
