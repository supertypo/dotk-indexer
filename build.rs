//! Stamps `git describe` into the binary for `--version`.

use vergen_git2::{Emitter, Git2};

fn main() {
    let git2 = Git2::builder().describe(true, true, None).build();
    // Outside a git checkout, report "unknown" instead of vergen's "VERGEN_IDEMPOTENT_OUTPUT".
    let emitted = Emitter::default().fail_on_error().add_instructions(&git2).and_then(|emitter| emitter.emit());
    if emitted.is_err() {
        println!("cargo::warning=no git metadata available, --version will report 'unknown'");
        println!("cargo::rustc-env=VERGEN_GIT_DESCRIBE=unknown");
    }
}
