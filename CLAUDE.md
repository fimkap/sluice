@AGENTS.md

## Claude Code specifics

- Project settings live in `.claude/settings.json`: `go test`, `go build`, `make` and read-only git are
  pre-approved; `git push`, `terraform apply` and reading `.env`/keys are denied.
- A `PostToolUse` hook runs `gofmt -w` on Go files after every edit (`.claude/hooks/gofmt.sh`), so never spend
  a turn on formatting.
- Skills in `.claude/skills/` cover the repeatable workflows (running experiments, adding a protocol API).
  Prefer them over improvising.
- The `durability-reviewer` subagent (`.claude/agents/`) reviews any diff that touches `internal/logstore`
  or the produce path. Run it before committing such changes.
- When a task spans several packages, write the plan as a checklist first and keep it updated.
