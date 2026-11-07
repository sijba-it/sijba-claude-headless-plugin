# TODO — Show which model actually answered each turn

> Status: done in 2faaaaf (served model + fallback footer, /model "Last turn answered by", requested=/served= in the log; verified live 2026-11-07).

## Problem

The plugin runs `claude -p --output-format stream-json` once per turn
(`HeadlessSession.send`, `main.py` ~410–535). Users keep seeing "downgraded"
notices and can only find out the current model by asking the bot
"model ?". That answer is the model describing itself, not a measurement.

Real case on rbartl.sijba.eu, channel `claude-headless:headless-recherche`
(2026-11-07): after a safeguards fallback the turns ran on Opus 4.8 instead
of Opus 5.5. Nothing in the UI said so except the bot itself when asked.

What exists today:
- `model` comes from the `system/init` event only (`main.py` ~455). That is
  the *requested* model and does not reflect a fallback mid-turn.
- `system/informational` events (e.g. "safeguards stopped the response
  above · continuing once …") are forwarded as a `⚠️` chunk, but don't say
  which model continues.
- The `usage model=… cost=…` log line (~493) uses that init `model`.
- `send()` already returns `"model"` in its result dict, but the
  caller (`_handle_message_inner`, ~811) doesn't show it anywhere.

## Goal

For every turn it must be obvious, without asking the agent, which model
(or models) actually produced the answer, and when a fallback happened.

## Tasks

1. **Capture the real model(s).** In `_pump_stdout`, on the `result` event
   read `event["modelUsage"]` (dict keyed by model id → tokens/cost). That
   is the authoritative list of models used in the turn. Also record
   `message.model` from `assistant` events, the last one = the model
   that wrote the final answer. Check the actual event shapes with a real
   `claude -p --output-format stream-json --verbose --include-partial-messages`
   run first. Don't guess field names; dump one turn's events to a file.
2. **Detect fallback.** Fallback = requested model (`current_model()` /
   init `model`) ≠ the model of the final `assistant` message, OR
   `modelUsage` contains more than one model, OR an informational/system
   event about a fallback was seen. Look for a dedicated system subtype
   (e.g. `model_fallback`) in the dump and handle it explicitly if it exists.
3. **Log it.** Extend the `usage` log line with `requested=<x> served=<y>`
   and the full `modelUsage` keys. Then `/log` shows it.
4. **Show it to the user.**
   - On fallback: append a short footer to the reply, e.g.
     `⚠️ answered by claude-opus-4-8 (requested: opus → fallback)`.
   - No footer when requested == served. It must not be noisy.
   - Remember the last served model in memory, and have `/model` (no args)
     print it next to the configured one: `Current model: opus — last turn
     served by claude-opus-5-5`.
5. **Optional:** pass `model`/`served_model` along in the reply payload's
   metadata if the plugin protocol carries metadata, so the app could show it
   later. Check `sijba_plugin_client` / how `_reply` sends first. Skip if
   it needs core changes.

## Also check (related, found in the same transcript)

`/model opus5.5` and `/model opus5` were *accepted* ("set to opus5.5")
and the next turn failed with "issue with the selected model". Commit
442c06a should reject unknown names, and `main.py` currently has
**uncommitted changes** (22+/5-). Find out whether that work is finished,
committed and deployed. The deployed version clearly didn't validate.
Tip for the reply text: suggest the full id (`claude-opus-5-5`) or the alias
(`opus`) when a name is rejected.

## Done when

- A turn that falls back shows the footer, and the log line shows
  `requested≠served`.
- A normal turn shows no footer.
- `/model` shows the last served model.
- README updated (commands / behaviour section).
- Committed, and deployed per the repo's usual path, then verified live on
  rbartl.sijba.eu with `/model` + one normal turn.
