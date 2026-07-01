# `.claude/`

Claude Code configuration for the go-redis repository.

## `settings.json` is project-shared

`settings.json` is committed and applies to **every maintainer's** Claude Code
session. Keep it to team-wide policy only — permission allowlists and other
settings that should hold for everyone working in this repo.

Per-user preferences (personal model choice, local permission grants, machine
-specific paths, etc.) belong in `settings.local.json`, which is gitignored.
Do not add them to `settings.json`. Additions here are a deliberate policy
choice for the whole team.

> Note: `settings.json` is schema-validated by Claude Code and does not accept
> comments or unknown keys, which is why this guidance lives in the README
> rather than inline.

## Layout

- `settings.json` — shared, committed Claude Code settings.
- `commands/` — repo-specific slash commands (e.g. `check-ci`).
- `skills/` — repo-specific skills (e.g. `add-command`).
- `specs/` — architecture specs referenced by skills and by maintainers.
