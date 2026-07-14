# rugo CLI demo — VHS tape (https://github.com/charmbracelet/vhs)
#
# Renders a terminal recording of the `rugo` command-line tool for the
# rugo.dev landing page. VHS is dev-only tooling — it is NOT a runtime
# dependency and ships in no wheel.
#
# Render (from the opteryx-core repo root):
#
#     brew install vhs          # pulls ttyd + ffmpeg
#     vhs dev/rugo_cli_demo.tape
#
# Output: dev/rugo_cli_demo.gif  (+ .mp4 — lighter, for a <video> embed)
# Copy the asset into the site repo to embed it:
#
#     cp dev/rugo_cli_demo.gif ../rugo.dev/docs/rugo-cli-demo.gif
#
# The hidden setup block below makes `rugo` resolve from this source tree
# (no wheel install needed) by pointing PYTHONPATH at the repo root and
# shimming `rugo` to `python3 -m rugo`. A maintainer who has `pip install
# rugo` can delete the Hide/Show block and run the plain `rugo` commands.

Output dev/rugo_cli_demo.gif
Output dev/rugo_cli_demo.mp4

Require python3
Require jq

Set Shell bash
Set FontSize 22
Set Width 1200
Set Height 720
Set Padding 28
Set TypingSpeed 55ms
Set Theme { "name": "rugo", "background": "#0b0d12", "foreground": "#e6e9ef", "cursor": "#5b8cff", "selection": "#243049", "black": "#0b0d12", "red": "#ff6b6b", "green": "#4ade80", "yellow": "#fbbf24", "blue": "#5b8cff", "magenta": "#a78bfa", "cyan": "#22d3ee", "white": "#e6e9ef", "brightBlack": "#6b7280", "brightRed": "#ff8787", "brightGreen": "#86efac", "brightYellow": "#fcd34d", "brightBlue": "#82a5ff", "brightMagenta": "#c4b5fd", "brightCyan": "#67e8f9", "brightWhite": "#ffffff" }

# ── hidden setup: resolve `rugo` from the source tree, work in a temp dir ──
Hide
Type `export PYTHONPATH="$PWD"; export PS1="\[\e[38;5;111m\]$ \[\e[0m\]"`
Enter
Type `rugo() { python3 -m rugo "$@"; }`
Enter
Type `mkdir -p /tmp/rugo-demo && cp testdata/missions/space_missions.parquet /tmp/rugo-demo/ && cd /tmp/rugo-demo && rm -f missions.jsonl`
Enter
Type "clear"
Enter
Show

# ── demo: inspect → schema → preview → convert → verify ──
Sleep 800ms

Type "rugo info space_missions.parquet"
Sleep 400ms
Enter
Sleep 2s

Type "rugo schema space_missions.parquet"
Sleep 400ms
Enter
Sleep 2500ms

Type "rugo preview -n 5 -c Company,Rocket,Mission,Mission_Status space_missions.parquet"
Sleep 400ms
Enter
Sleep 3s

Type "rugo convert space_missions.parquet missions.jsonl"
Sleep 400ms
Enter
Sleep 1800ms

Type "rugo count --json missions.jsonl | jq .num_rows"
Sleep 400ms
Enter
Sleep 2500ms
