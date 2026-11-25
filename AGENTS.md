# AGENTS.md

Project rules for AI agents and contributors. When in doubt, follow the
**smaller diff** — reviewable code wins over clever code.

---

## Documentation Rules

Docs describe the system **as it is now**. History, process, and change narratives belong in PRs / commits / ADRs — not here.

### Don't write

- **Change narrative**: "was X, now Y"; "adjusted because of Z"; "as requested by N"
- **Temporal words**: now / recently / newly / "as of vX"; 中文：新增了 / 现已支持 / 之前 / 原本
- **Process artifacts**: task splits, debug logs, file lists, verification steps, options considered
- **Preambles**: "This document introduces..."; "The following changes..."

### Don't create

No new doc without an explicit request.

### When updating

Rewrite the whole section. Delete what is replaced. Never keep two versions side by side.

### Self-check

Read every sentence as a first-time reader. If removing the timestamp or context makes it false, delete it.

## Comment Rules

- Write code that speaks for itself. Comment only when necessary to explain **WHY**, not WHAT. We do not need comments most of the time.
- ❌ AVOID — **Obvious Comments**(复述代码)、**Redundant Comments**、**Outdated Comments**、**Commented-out Code**、**Changelog Comments**、**Divider Comments**

## Design principles

Pick the smallest design that fits the **current** requirement, not the
most future-proof one.

- **SRP** — one responsibility per package, file, function. If the
  description needs "and", split it. Non-negotiable.
- **DRY** — extract on the third occurrence. Two near-duplicates with
  different surrounding context can stay; three identical copies is
  a smell.
- **KISS** — write the readable version first. Only optimise when a
  concrete caller or benchmark demands it.
- **YAGNI** — no config knobs, plugin points, or generic abstractions
  for hypothetical callers. Wait for the second real consumer before
  abstracting.

When these collide, prefer whichever shrinks the diff and the review.
SRP is the only hard rule.


---

## Tests

Tests exist to **describe and protect business behaviour** — not to
satisfy a coverage number or re-prove the type system.

Rules:

- **Don't test for the sake of testing.** A test whose only assertion
  is "field X equals literal Y" is noise; the compiler already proved
  it. Strip it.
- **Aim at behaviour, not literals.** If a test would still pass after
  the implementation is rewritten with a different data structure,
  it's testing literals, not behaviour.
- **One test per behaviour boundary** — empty input, error path,
  ordering tie-breaker, type-specific branch. Beats ten tests each
  poking a single field.
- **Use realistic fixtures.** Records that mirror what a real resolver
  produces, not sanitised single-character placeholders.
- **Cover the empty case.** A test demonstrating "works on no input /
  empty index / nil receiver" usually beats three happy-path literal
  tests.
- **Tests do not replace types.** Never assert "this is an int" or
  "this struct has field Y" — the type system already enforces it.

If a test does not tell a future reader something they couldn't learn
from the code, delete it.

---

## Verification

A task is done when every acceptance criterion in the task has
**evidence** recorded against the current worktree — not when the
agent believes it is done.


### Local scope

- `make tests` — every package in the module, with the race detector.
- `make tests PKGS=./driver/...` — only the packages a change touches.
- `make e2e` — the end-to-end tests of the `e2e` package, with the build
  tag `e2e` and the race detector.
- `make lint` — formatting and golangci-lint; run `make tools` first
  when the linter is not installed.

### Process

1. **Before editing**: extract every `Acceptance` / `R*` / `done
   when` line from the task description into a checklist. New
   criteria discovered during implementation go on the same list.
2. **After editing**: for each checklist item, record **one** of:
   - a shell command and its exit code,
   - a `git grep -nE` pattern and its matches,
   - a `file:line` reference whose content satisfies the criterion.
   Phrases like "verified manually" or "looks right" are not
   evidence.
3. **Blocking checks** before reporting done:
   - `make build` → exit 0
   - the scoped test run (see Local scope) → exit 0
   - `make lint` → exit 0
   - `git diff` reviewed for unrelated changes, debug code,
     weakened tests, and comment drift.
4. **Failure handling**: if a criterion cannot be satisfied, report
   it as failed with the reason. Do not silently remove it from
   the list.

### Self-check

Re-read the report as a reviewer with the same shell access but no
session memory. Each evidence line must be reproducible: same
command, same exit code, same output. If the reviewer cannot
reproduce the evidence, the evidence is wrong.

Run `make lint` and confirm that it exits 0.
