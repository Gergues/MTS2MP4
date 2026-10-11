# mts2mp4
## Purpose :
High throughput video batch conversion tool. 

Converts MTS / M2TS / TS camcorder footage to MP4 **without re-encoding**, in parallel, and writes an auditable ledger of every action taken.

The video stream is copied packet-for-packet into an MP4 container. Resolution, bitrate, chroma subsampling, interlacing and frame count are preserved exactly — the conversion changes the container, not the picture.

## Requirements

- Python 3.9+
- `ffmpeg` and `ffprobe`

**No pip packages.** The script uses the standard library only, so there is no `requirements.txt` to install.

`ffprobe` is not a separate program — it is built and shipped as part of FFmpeg, so anything that gives you one gives you both. If you have `ffmpeg` but not `ffprobe`, you have a trimmed-down build; replace it with a full one from below.

## Where to get FFmpeg

The official project is **<https://ffmpeg.org>**, and its download page is **<https://ffmpeg.org/download.html>**.

Note what that page actually offers: *FFmpeg only provides source code.* There is no official binary. The download page instead links to third-party builders it endorses, and those are what the table below points to — including the ones this script's `--install-deps portable` uses.

| Platform | Official-endorsed build | Notes |
|---|---|---|
| Source (all) | <https://ffmpeg.org/releases/> | Official releases, source tarballs only |
| Windows | <https://www.gyan.dev/ffmpeg/builds/> | "essentials" or "full" both include ffprobe |
| Windows / Linux | <https://github.com/BtbN/FFmpeg-Builds/releases> | Static and shared builds |
| macOS | <https://evermeet.cx/ffmpeg/> | ffmpeg and ffprobe are **separate downloads** here |
| Debian / Ubuntu | `apt install ffmpeg` | Distribution packages |
| Fedora / RHEL | RPM Fusion — <https://rpmfusion.org> | `dnf install ffmpeg` |
| macOS (Homebrew) | `brew install ffmpeg` | <https://brew.sh> |

Any of these works. Put the binaries on `PATH`, drop them in `~/.mts2mp4/bin`, or point at them explicitly with `--ffmpeg` / `--ffprobe`.

## First run

Just run it. If `ffmpeg` is missing, the script installs it and carries on into the job — no separate setup step.

| Situation | What happens |
|---|---|
| At a terminal | Asks once, then remembers your answer |
| Unattended / cron | Set `MTS2MP4_AUTO_INSTALL=1`, or pass `--yes` |
| Locked-down machine | `--no-auto-setup` — never installs, prints instructions instead |
| Set up ahead of time | `--install-deps [auto\|system\|portable\|print]` |

With a terminal it prefers the platform package manager (winget, brew, apt, dnf, pacman…). Unattended it prefers a portable static build in `~/.mts2mp4/bin`, which needs no admin rights and cannot stall on a sudo password. The portable route pulls from the builders listed above; each download's SHA-256 is recorded so the install can be audited afterwards.

> The portable download has not been exercised against a live server. Run `--install-deps portable` once interactively on the target machine before relying on it in a scheduled job.

## Usage

```bash
# one file
python3 mts2mp4.py clip.MTS

# a tree, 8 workers, mirrored into ./mp4
python3 mts2mp4.py ./footage -r -w 8 -o ./mp4

# plan only — logs everything, runs nothing
python3 mts2mp4.py ./footage -r --dry-run

# prove the output is pixel-identical (slow: full decode per file)
python3 mts2mp4.py ./footage -r --verify
```

Workers are clamped to 4–8. Threads by default, since each worker blocks on its own `ffmpeg` process; `--executor process` if you want OS-level isolation. Jobs run largest-first so the pool drains evenly.

## Checking the output

Two levels, for different costs:

- **Integrity — always on, cheap.** Stream counts and video packet counts compared across the remux. Demux-only, no decode. Catches truncation and silently dropped streams.
- **`--verify` — opt-in, slow.** Decodes both files and compares frame hashes. Equal hashes mean every output pixel equals every input pixel.

Verification decodes rather than hashing the copied stream on purpose: MPEG-TS carries H.264 as Annex B while MP4 uses length-prefixed AVCC, so the packet bytes legitimately differ after a remux and a packet-level hash reports a false mismatch.

A reasonable pattern on a large archive is `--verify` on a sample and the always-on packet check on the bulk.

## The ledger

Every job produces one row — 34 columns covering timestamps, worker identity, byte sizes, codecs, resolution, field order, the exact `ffmpeg` argv, exit code, attempts, and both check results. Run metadata records which `ffmpeg` binary produced the output and where it came from.

Rows stream to a `.jsonl` journal as they complete, so a crash or Ctrl-C still leaves a usable trail. At the end the same rows are exported as **CSV, JSON and Markdown** into `--ledger-dir` (default `./conversion_ledgers`).

Row statuses: `SUCCESS`, `WARNING`, `SKIPPED`, `FAILED`, `DRY_RUN`. Every input path you name gets a row, including ones that turned out not to exist.

How ffmpeg arrived on the machine — package manager commands, download URLs and their SHA-256 — is appended to `~/.mts2mp4/state.json`.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | All conversions clean |
| `1` | At least one failure |
| `2` | Setup problem — missing dependencies, bad Python, no inputs |
| `3` | Converted, but something was flagged (see `integrity` / `verification`) |

## Notes

- Existing `.mp4` outputs are skipped unless `--overwrite`.
- Source modification times are copied onto outputs by default (`--no-preserve-mtime` to disable), which keeps camcorder dates intact.
- MP4 cannot hold every MPEG-TS audio format. LPCM is re-encoded to ALAC (lossless) by default; `--audio-fallback` takes `flac`, `aac` or `fail`.
- Output is smaller than the source with no loss — MPEG-TS spends a few percent on 188-byte packet overhead that MP4 does not carry (2–5% on test footage). A much larger drop usually means a stream was dropped rather than that anything was compressed; check the `integrity` column.
- Outputs are written with `+faststart`, so the index sits at the front of the file.
