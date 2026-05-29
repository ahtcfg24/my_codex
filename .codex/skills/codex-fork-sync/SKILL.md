---
name: codex-fork-sync
description: Maintain a personal fork of openai/codex while developing on isolated branches. Use when syncing from the original Codex repository, preparing an agent-owned working branch, updating a long-lived integration branch such as deepseek, or keeping fork work rebased on upstream/main.
---

# Codex Fork Sync

## Objective

Keep a personal Codex fork synchronized with `openai/codex` while ensuring each Agent modifies code on an isolated branch.

Assume this repository may have:

- `upstream`: the original read-only Codex repository, normally `https://github.com/openai/codex.git`
- `origin`: the user's writable fork
- `main`: the fork's mirror of `upstream/main`
- `deepseek`: the user's long-lived integration branch for DeepSeek support

Never push to `upstream`.

## Preflight

Before changing branches or rebasing:

1. Run `git status --short --branch`.
2. If there are uncommitted changes, do not discard them. Ask the user whether to commit, stash, or stop unless the changes are clearly yours from the current task.
3. Run `git remote -v`.
4. If `upstream` is missing, add it:

```bash
git remote add upstream https://github.com/openai/codex.git
```

5. Fetch current refs:

```bash
git fetch upstream
git fetch origin
```

If network, authentication, or permission errors occur, report the exact failed command and stop before making branch changes.

## Sync The Fork Base

Keep `main` as a clean mirror of `upstream/main`:

```bash
git switch main
git merge --ff-only upstream/main
git push origin main
```

If `git merge --ff-only upstream/main` fails, do not create a merge commit. Stop and report that `main` has diverged from `upstream/main`.

## Update The DeepSeek Branch

When the `deepseek` branch exists, update it after `main` is synchronized:

```bash
git switch deepseek
git rebase upstream/main
```

Resolve conflicts conservatively:

- Preserve the user's DeepSeek behavior unless the user requested otherwise.
- Prefer the current upstream implementation for unrelated Codex changes.
- After resolving conflicts, run `git add <files>` and `git rebase --continue`.
- If the conflict cannot be resolved confidently, stop and ask the user.

After a successful rebase, push with lease because the branch history changed:

```bash
git push --force-with-lease origin deepseek
```

Do not use plain `--force`.

## Create An Agent Working Branch

For any local code modification, create a task-specific branch before editing.

Choose the base branch:

- Use `deepseek` when the task depends on the user's DeepSeek integration or should preserve that feature.
- Use `main` when the task should be based only on upstream Codex.
- Ask the user when the intended base is ambiguous.

Create the branch:

```bash
git switch <base-branch>
git pull --ff-only
git switch -c agent/<short-task-name>
```

Use lowercase, hyphenated branch names such as `agent/fix-deepseek-config` or `agent/update-sync-skill`.

## While Editing

Follow the repository's normal coding and verification rules.

Before committing or pushing:

1. Run focused formatting/tests required by the changed area.
2. Run `git status --short --branch`.
3. Review `git diff` to ensure the branch contains only the intended task changes.

Do not commit unless the user explicitly asked for a commit.

## Refresh A Working Branch

If upstream moves while an Agent branch is in progress:

```bash
git fetch upstream
git switch main
git merge --ff-only upstream/main
git push origin main
git switch deepseek
git rebase upstream/main
git push --force-with-lease origin deepseek
git switch <agent-branch>
git rebase deepseek
```

If the Agent branch was based on `main` instead of `deepseek`, rebase it onto `main`:

```bash
git switch <agent-branch>
git rebase main
```

## Safety Rules

- Never run `git reset --hard`, `git checkout -- <file>`, or other destructive commands unless the user explicitly approves.
- Never push to `upstream`.
- Never force-push shared branches with plain `--force`; use `--force-with-lease` only when rebasing a branch the user expects this Agent to update.
- Keep `main` free of local feature commits.
- Keep each Agent task on its own branch.
- Report any unresolved conflicts, permission problems, or divergence before continuing.
