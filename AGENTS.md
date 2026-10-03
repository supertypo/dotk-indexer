# Contributor notes for dotk-indexer

This file holds the workflow and the rules that a change must not break. README.md says what the
indexer is and how to run it.

## Workflow

1. Run `git pull` before you change anything.
2. Implement the change with its tests. If a change can regress behavior, write the regression
   test first.
3. Run `cargo test`.
4. Run `cargo clippy --all-targets -- -D warnings`, and fix the findings.
5. Run `cargo fmt`.
6. Commit on `main` with a short message and push, or open a pull request from a fork. CI runs
   these steps and `cargo audit`.

## Rules

- The API under `/v1` is a contract with its clients. Never change a path, a field, a wire name,
  a type or a status code as a side effect of other work. A new field or endpoint is a change of
  its own.
- The indexer derives everything that it serves from the chain, and a reader can prove every fact
  against a node. When it cannot prove a fact, it withholds it. It never serves a guess.
- The self-test and its repair never run while the indexer is behind the chain.
- A node answer that lacks a field that the request guarantees, or that is malformed, stops the
  work that asked for it, which retries. The indexer never fills in a default, skips the transaction or records a
  placeholder, because a stall writes nothing wrong.
- One indexer runs against a database. Nothing in the code guards against a second one.
- The schema is the sequence of files in `migrations/`, applied at every start. A schema change is
  a new numbered file. Never edit an applied migration, because sqlx refuses a changed checksum at
  boot. SQL carries no comments, in a migration or in a query string.
- A test covers a real issue: a regression, a lost fund, a wrong answer. Never a nitpick, and never
  a doc or a comment.
- A test that needs Postgres starts its own through testcontainers. Never point a test at a server
  that must already be running.
- A test that runs a self-test must be the only thing that runs one. The test setup therefore
  leaves the self-test task out, and a test that needs it opts in.
- The evictor pays fees only through the `dotk-core` assembler, whose `MAX_FEE_SOMPI` ceiling
  binds every transaction. It never submits an evict that costs more than its bounty.
- `genesis/*.json` are the generated manifests of the deployments. Never edit them by hand.
- The status page and the gateway pages are self-contained HTML with no third-party asset. Keep
  their credit footer, which NOTICE requires.
- The documentation is this file and a short README.md, beside the license files. Keep no other
  document, and no file of follow-ups.
- Code carries almost no comments. A comment states a non-obvious, important fact, in the fewest
  words that carry it. Describe the current state only, never its history. Never name a file, a
  document or a project that a reader of this repository cannot open. Open standards and
  open-source projects are fine to name.
- Documentation and comments use American spelling, simple tenses and the active voice, and no
  semicolons or em-dashes.
