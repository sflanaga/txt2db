use std::process::Command;

fn main() {
    // Fallbacks in case git is missing (e.g., from a source tarball)
    let mut git_info = "unknown".to_string();

    // git rev-parse HEAD
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| if o.status.success() { String::from_utf8(o.stdout).ok() } else { None })
        .map(|s| s.trim().to_string());

    // git status --porcelain --untracked-files=no (dirty if tracked files are changed)
    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .ok()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);

    if let Some(h) = head {
        git_info = if dirty { format!("{h}-dirty") } else { h };
    }

    println!("cargo:rustc-env=GIT_INFO={git_info}");
}
