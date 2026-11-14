#!/bin/sh
# PostToolUse hook: gofmt the Go file that was just edited.
# Claude Code passes the tool call as JSON on stdin; only tool_input.file_path matters here.
file=$(sed -n 's/.*"file_path"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
case "$file" in
  *.go) command -v gofmt >/dev/null 2>&1 && gofmt -w "$file" ;;
esac
exit 0
