# sijba-claude-headless-plugin

Claude Code in headless print mode for sijba — a `kind: git` catalog plugin for
sijba-agent-controller, but runnable on its own too. Same shape as
sijba-cursor-plugin: one `claude -p` per turn, chained with `--resume`, streamed
back to sijba (`--output-format stream-json --include-partial-messages`).

No tmux, no TUI, no long-lived process. Compared to sijba-claude-channel-plugin
(interactive claude in tmux): simpler and lighter, but nobody can approve
anything mid-turn — what the permission mode doesn't allow is denied and listed
under the reply.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install websockets

export SIJBA_URL=https://sijba.edv-bartl.at
export SIJBA_TOKEN=your-token-here
export PROJECT_NAME=my-project
export WORK_DIR=/path/to/my-project
export REGISTRY_FILE=/path/to/registry.json   # shared with the controller, for /archive and /pause

python main.py
```

Needs `claude` logged in (claude.ai subscription or API key) on the host.

| Env | Default | |
|---|---|---|
| `CLAUDE_CMD` | `claude` | absolute path under the controller (systemd PATH usually lacks `~/.local/bin`) |
| `CLAUDE_ARGS` | | extra args for every call |
| `CLAUDE_MODEL` | claude's own | catalog-wide `--model`; per project: `/model` |
| `CLAUDE_PERMISSION_MODE` | `auto` | catalog-wide `--permission-mode`; per project: `/mode` |
| `CLAUDE_TIMEOUT` | `1800` | max seconds per turn |

## Usage in sijba

Registers one channel, `claude-headless:<PROJECT_NAME>`, triggered by `@<PROJECT_NAME>`:

```
@my-project fix the failing test in api.py
```

The reply streams in; tool calls show up as `_[Bash: npm test]_` lines,
claude's own notices (e.g. a safeguard trip) as `⚠️ ...`.

In-channel commands (handled locally, never reach claude):

- `/archive` / `/pause` — set the project's status in `REGISTRY_FILE`, exit 90 / 91.
- `/reset` — forget the session id; the next turn starts a fresh conversation.
- `/kill` — abort the running turn (incl. the shell commands claude started), then reset.
  Handled immediately, not queued behind the running turn.
- `/model [name]` — show / set the model for this project (`WORK_DIR/.claude-headless.json`).
- `/mode [name]` — show / set the permission mode: `acceptEdits`, `auto`,
  `bypassPermissions`, `manual`, `dontAsk`, `plan`.
- `/log [n]` — last `n` (default 10) log lines.

## Permissions

`claude -p` can't ask. Tool calls the mode doesn't allow are denied, claude is
told so, and the reply ends with

```
⛔ Not allowed in permission mode `manual` (nobody can approve in -p mode):
- Bash: touch made.txt
```

Allow them per project in `.claude/settings.json` (`permissions.allow`), or
switch the mode with `/mode`.

## Security

- `SIJBA_TOKEN`, `SIJBA_URL` and `REGISTRY_FILE` are stripped from claude's
  environment (and so from every shell command it runs).
- The token is not written to the log (`/log` sends log lines to the chat).

## Managed by the controller

```
catalog add claude-headless --git <repo url>
```

The controller clones this repo into its own `_plugins/claude-headless/`, then runs
`main.py` with `SIJBA_URL`/`SIJBA_TOKEN`/`PROJECT_NAME`/`WORK_DIR`/`REGISTRY_FILE`
set for whichever project it's supervising.
