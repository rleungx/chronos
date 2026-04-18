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

    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let commit = String::from_utf8(output.stdout).ok()?;
    let trimmed = commit.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}
