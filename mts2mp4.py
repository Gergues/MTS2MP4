#!/usr/bin/env python3
"""
mts2mp4.py — Parallel, lossless MTS -> MP4 remuxer with an auditable ledger.

Lossless by construction: the video (and normally the audio) elementary streams
are copied packet-for-packet into an MP4 container. Nothing is re-encoded, so
resolution, bitrate, chroma subsampling, interlacing and frame count are
preserved exactly.

Parallelism: a pool of 4-8 workers, each driving its own ffmpeg process.
Threads are the default (every worker blocks on an external process, so the GIL
is never the bottleneck); --executor process is available if you prefer true OS
process isolation.

Audit: every job produces one row with timestamps, sizes, codecs, the exact
ffmpeg argv and exit code. Every job also gets a cheap integrity check (stream
counts and video packet counts compared across the remux); --verify adds a
decode-level frame hash that proves the output decodes to identical pixels.
Rows are streamed to a JSONL journal as they complete (crash-safe) and exported
at the end as CSV, JSON and Markdown.

Dependencies: no third-party Python packages — the standard library only, so
there is nothing to pip install and no requirements file to maintain. It does
need the ffmpeg and ffprobe BINARIES, and it sets those up itself on first run:
just run a normal conversion and, if they are missing, it installs them and
carries straight on into the job. No separate setup command.

  at a terminal          asks once, remembers the answer
  unattended / cron      --yes, or MTS2MP4_AUTO_INSTALL=1
  never install          --no-auto-setup
  set up ahead of time   --install-deps [auto|system|portable|print]

Installs go through the platform package manager when someone is present to
approve a sudo prompt, and otherwise as a portable static build under
~/.mts2mp4/bin that needs no privileges.

Python 3.9 or newer.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

WORKER_MIN, WORKER_MAX, WORKER_DEFAULT = 4, 8, 4

SOURCE_SUFFIXES = {".mts", ".m2ts", ".ts"}

# Audio codecs that an MP4 container accepts with a straight stream copy.
MP4_SAFE_AUDIO = {"aac", "ac3", "eac3", "mp3", "alac", "mp4als", "opus"}

# Column order for every tabular export. Keep stable — downstream audit
# tooling and diffs depend on it.
LEDGER_COLUMNS = [
    "row",
    "run_id",
    "job_id",
    "status",
    "worker",
    "started_utc",
    "finished_utc",
    "elapsed_sec",
    "input_path",
    "input_bytes",
    "output_path",
    "output_bytes",
    "size_delta_pct",
    "media_duration_sec",
    "video_codec",
    "resolution",
    "frame_rate",
    "field_order",
    "audio_codec_in",
    "audio_codec_out",
    "audio_action",
    "lossless_video",
    "integrity",
    "streams_in",
    "streams_out",
    "video_packets_in",
    "video_packets_out",
    "verification",
    "input_frame_md5",
    "output_frame_md5",
    "ffmpeg_returncode",
    "attempts",
    "command",
    "message",
]

STATUS_OK = "SUCCESS"
STATUS_WARN = "WARNING"
STATUS_SKIP = "SKIPPED"
STATUS_FAIL = "FAILED"
STATUS_DRY = "DRY_RUN"


# --------------------------------------------------------------------------- #
# Job / record models  (module level so ProcessPoolExecutor can pickle them)
# --------------------------------------------------------------------------- #


@dataclass
class Job:
    job_id: str
    run_id: str
    src: str
    dst: str
    overwrite: bool = False
    skip_existing: bool = True
    verify: bool = False
    audio_fallback: str = "alac"  # alac | aac | flac | fail
    preserve_mtime: bool = True
    dry_run: bool = False
    ffmpeg: str = "ffmpeg"
    ffprobe: str = "ffprobe"


@dataclass
class Record:
    row: int = 0
    run_id: str = ""
    job_id: str = ""
    status: str = ""
    worker: str = ""
    started_utc: str = ""
    finished_utc: str = ""
    elapsed_sec: float = 0.0
    input_path: str = ""
    input_bytes: int = 0
    output_path: str = ""
    output_bytes: int = 0
    size_delta_pct: str = ""
    media_duration_sec: str = ""
    video_codec: str = ""
    resolution: str = ""
    frame_rate: str = ""
    field_order: str = ""
    audio_codec_in: str = ""
    audio_codec_out: str = ""
    audio_action: str = ""
    lossless_video: str = ""
    integrity: str = "NOT_RUN"
    streams_in: int = 0
    streams_out: int = 0
    video_packets_in: int = -1
    video_packets_out: int = -1
    verification: str = "NOT_REQUESTED"
    input_frame_md5: str = ""
    output_frame_md5: str = ""
    ffmpeg_returncode: str = ""
    attempts: int = 0
    command: str = ""
    message: str = ""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def worker_label() -> str:
    return f"pid:{os.getpid()}/thr:{threading.current_thread().name}"


def run(cmd: list[str], timeout: int | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False
    )


def tail(text: str, limit: int = 400) -> str:
    """Last N characters — ffmpeg puts the actual error at the end of stderr."""
    text = " ".join((text or "").split())
    return text[-limit:]


def clip(text: str, limit: int) -> str:
    """First N characters — for console columns, where the start carries the point."""
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def probe(path: Path, ffprobe: str) -> dict[str, Any]:
    """Return ffprobe JSON for a media file, or {} on failure."""
    proc = run(
        [
            ffprobe,
            "-v", "error",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            str(path),
        ],
        timeout=120,
    )
    if proc.returncode != 0:
        return {}
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {}


def first_stream(info: dict[str, Any], kind: str) -> dict[str, Any]:
    for stream in info.get("streams", []):
        if stream.get("codec_type") == kind:
            return stream
    return {}


def count_video_packets(path: Path, ffprobe: str) -> int:
    """
    Number of packets in the primary video stream. Demux-only, so it is cheap
    even on large files; -1 means the count could not be obtained.
    """
    proc = run(
        [
            ffprobe, "-v", "error",
            "-select_streams", "v:0",
            "-count_packets",
            "-show_entries", "stream=nb_read_packets",
            "-of", "csv=p=0",
            str(path),
        ],
        timeout=3600,
    )
    if proc.returncode != 0:
        return -1
    for line in proc.stdout.splitlines():
        token = line.strip().rstrip(",")
        if token.isdigit():
            return int(token)
    return -1


def decoded_video_md5(path: Path, ffmpeg: str) -> str:
    """
    MD5 of the fully DECODED primary video stream.

    Hashing the copied elementary stream instead would be far cheaper, but it
    reports a false mismatch: MPEG-TS carries H.264 as Annex B while MP4 uses
    length-prefixed AVCC with out-of-band extradata, so the packet bytes differ
    by container even when the coded video is untouched. Decoding normalises
    that away — equal hashes mean every output pixel equals every input pixel.
    Costs a full decode pass, which is why it sits behind --verify.
    """
    proc = run(
        [
            ffmpeg, "-v", "error", "-nostdin",
            "-i", str(path),
            "-map", "0:v:0",
            "-f", "md5", "-",
        ],
        timeout=21600,
    )
    if proc.returncode != 0:
        return ""
    for line in proc.stdout.splitlines():
        if line.upper().startswith("MD5="):
            return line.split("=", 1)[1].strip()
    return ""


def build_command(job: Job, audio_codec: str) -> list[str]:
    cmd = [
        job.ffmpeg,
        "-nostdin",
        "-hide_banner",
        "-v", "error",
        "-fflags", "+genpts",          # AVCHD timestamps are often ragged
        "-i", job.src,
        "-map", "0:v",
        "-map", "0:a?",
        "-c:v", "copy",                # never re-encode video
        "-c:a", audio_codec,
        "-copyts",
        "-avoid_negative_ts", "make_zero",
        "-movflags", "+faststart+write_colr",
        "-y" if job.overwrite else "-n",
        job.dst,
    ]
    return cmd


# --------------------------------------------------------------------------- #
# The unit of work
# --------------------------------------------------------------------------- #


def convert_one(job: Job) -> dict[str, Any]:
    """Convert a single file. Always returns a ledger row — never raises."""
    rec = Record(
        run_id=job.run_id,
        job_id=job.job_id,
        worker=worker_label(),
        started_utc=utcnow(),
        input_path=job.src,
        output_path=job.dst,
    )
    t0 = time.perf_counter()
    src, dst = Path(job.src), Path(job.dst)

    def finish(status: str, message: str = "") -> dict[str, Any]:
        rec.status = status
        rec.message = message
        rec.finished_utc = utcnow()
        rec.elapsed_sec = round(time.perf_counter() - t0, 3)
        return asdict(rec)

    try:
        if not src.is_file():
            return finish(STATUS_FAIL, "source file not found")

        rec.input_bytes = src.stat().st_size

        # ---- inspect source -------------------------------------------- #
        info = probe(src, job.ffprobe)
        vstream = first_stream(info, "video")
        astream = first_stream(info, "audio")

        rec.video_codec = vstream.get("codec_name", "")
        if vstream:
            rec.resolution = f"{vstream.get('width', '?')}x{vstream.get('height', '?')}"
            rec.frame_rate = vstream.get("r_frame_rate", "")
            rec.field_order = vstream.get("field_order", "progressive")
        rec.audio_codec_in = astream.get("codec_name", "none")
        rec.streams_in = len(info.get("streams", []))
        duration = info.get("format", {}).get("duration")
        if duration:
            rec.media_duration_sec = f"{float(duration):.3f}"

        if not vstream:
            return finish(STATUS_FAIL, "no video stream found in source")

        # ---- output guards --------------------------------------------- #
        if dst.exists() and not job.overwrite:
            if job.skip_existing:
                rec.output_bytes = dst.stat().st_size
                return finish(STATUS_SKIP, "output already exists (--overwrite to replace)")
            return finish(STATUS_FAIL, "output already exists and skipping is disabled")

        dst.parent.mkdir(parents=True, exist_ok=True)

        # ---- decide audio handling ------------------------------------- #
        acodec_in = rec.audio_codec_in
        if acodec_in == "none":
            audio_codec, rec.audio_action = "copy", "no audio stream"
        elif acodec_in in MP4_SAFE_AUDIO:
            audio_codec, rec.audio_action = "copy", "stream copy (lossless)"
        elif job.audio_fallback == "fail":
            return finish(STATUS_FAIL, f"audio codec '{acodec_in}' not MP4-compatible and fallback disabled")
        else:
            audio_codec = job.audio_fallback
            lossless = job.audio_fallback in {"alac", "flac"}
            rec.audio_action = (
                f"re-encoded to {job.audio_fallback} "
                f"({'lossless' if lossless else 'LOSSY'}) — '{acodec_in}' invalid in MP4"
            )

        cmd = build_command(job, audio_codec)
        rec.command = " ".join(cmd)

        if job.dry_run:
            rec.lossless_video = "YES (planned)"
            rec.audio_codec_out = audio_codec
            return finish(STATUS_DRY, "no command executed")

        # ---- pre-measure -------------------------------------------------- #
        rec.video_packets_in = count_video_packets(src, job.ffprobe)
        if job.verify:
            rec.input_frame_md5 = decoded_video_md5(src, job.ffmpeg)

        # ---- transcode (attempt 1: copy, attempt 2: audio fallback) ----- #
        rec.attempts = 1
        proc = run(cmd, timeout=None)
        rec.ffmpeg_returncode = str(proc.returncode)

        if proc.returncode != 0 and audio_codec == "copy" and job.audio_fallback != "fail":
            # Most common cause: PCM / unsupported audio rejected by the MP4 muxer.
            rec.attempts = 2
            audio_codec = job.audio_fallback
            rec.audio_action = (
                f"stream copy rejected by MP4 muxer; re-encoded audio to {job.audio_fallback}"
            )
            cmd = build_command(job, audio_codec)
            if "-n" in cmd:  # a partial file from attempt 1 must be replaceable
                cmd[cmd.index("-n")] = "-y"
            rec.command = " ".join(cmd)
            proc = run(cmd, timeout=None)
            rec.ffmpeg_returncode = str(proc.returncode)

        rec.audio_codec_out = audio_codec if audio_codec != "copy" else rec.audio_codec_in

        if proc.returncode != 0:
            if dst.exists() and dst.stat().st_size == 0:
                dst.unlink(missing_ok=True)
            return finish(STATUS_FAIL, tail(proc.stderr) or "ffmpeg returned a non-zero exit code")

        if not dst.is_file() or dst.stat().st_size == 0:
            return finish(STATUS_FAIL, "ffmpeg reported success but produced no output")

        rec.output_bytes = dst.stat().st_size
        if rec.input_bytes:
            delta = (rec.output_bytes - rec.input_bytes) / rec.input_bytes * 100
            rec.size_delta_pct = f"{delta:+.2f}"
        rec.lossless_video = "YES (stream copy)"

        # ---- integrity check: always, demux-only, no decode -------------- #
        out_info = probe(dst, job.ffprobe)
        rec.streams_out = len(out_info.get("streams", []))
        rec.video_packets_out = count_video_packets(dst, job.ffprobe)

        problems: list[str] = []
        if rec.video_packets_in < 0 or rec.video_packets_out < 0:
            rec.integrity = "UNAVAILABLE"
        elif rec.video_packets_in != rec.video_packets_out:
            rec.integrity = "PACKET_COUNT_DIFFERS"
            problems.append(
                f"video packets {rec.video_packets_in} in vs {rec.video_packets_out} out"
            )
        else:
            rec.integrity = "PACKETS_MATCH"

        if rec.streams_out < rec.streams_in:
            dropped = rec.streams_in - rec.streams_out
            rec.integrity = "STREAMS_DROPPED" if rec.integrity != "PACKET_COUNT_DIFFERS" else rec.integrity
            problems.append(
                f"{dropped} source stream(s) absent from the MP4 "
                "(MP4 cannot carry every MPEG-TS stream type)"
            )

        # ---- decode-level verification (opt-in) -------------------------- #
        if job.verify:
            rec.output_frame_md5 = decoded_video_md5(dst, job.ffmpeg)
            if not rec.input_frame_md5 or not rec.output_frame_md5:
                rec.verification = "UNAVAILABLE"
            elif rec.input_frame_md5 == rec.output_frame_md5:
                rec.verification = "VERIFIED_IDENTICAL"
            else:
                rec.verification = "HASH_MISMATCH"
                problems.append("decoded video differs from the source")

        if job.preserve_mtime:
            st = src.stat()
            os.utime(dst, (st.st_atime, st.st_mtime))

        if problems:
            return finish(STATUS_WARN, "remuxed, but: " + "; ".join(problems))
        return finish(STATUS_OK, "remuxed without re-encoding")

    except subprocess.TimeoutExpired:
        return finish(STATUS_FAIL, "ffmpeg timed out")
    except Exception as exc:  # noqa: BLE001 — a worker must never kill the pool
        return finish(STATUS_FAIL, f"{type(exc).__name__}: {exc}")


# --------------------------------------------------------------------------- #
# Ledger
# --------------------------------------------------------------------------- #


class Ledger:
    """Collects rows, streams a crash-safe JSONL journal, exports tabular files."""

    def __init__(self, run_id: str, outdir: Path, basename: str, meta: dict[str, Any]):
        self.run_id = run_id
        self.outdir = outdir
        self.basename = basename
        self.meta = meta
        self.rows: list[dict[str, Any]] = []
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.journal = outdir / f"{basename}.jsonl"
        self.journal.write_text("", encoding="utf-8")

    def add(self, row: dict[str, Any]) -> dict[str, Any]:
        row["row"] = len(self.rows) + 1
        self.rows.append(row)
        with self.journal.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        return row

    # -- exports ----------------------------------------------------------- #

    def to_csv(self) -> Path:
        path = self.outdir / f"{self.basename}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=LEDGER_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(self.rows)
        return path

    def to_json(self) -> Path:
        path = self.outdir / f"{self.basename}.json"
        payload = {
            "run": self.meta | {"run_id": self.run_id, "columns": LEDGER_COLUMNS},
            "summary": self.summary(),
            "records": self.rows,
        }
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return path

    def to_markdown(self, columns: Iterable[str] | None = None) -> Path:
        cols = list(columns or LEDGER_COLUMNS)
        path = self.outdir / f"{self.basename}.md"
        lines = [
            f"# MTS -> MP4 conversion ledger",
            "",
            f"- Run ID: `{self.run_id}`",
            f"- Started: {self.meta.get('started_utc')}",
            f"- Host: {self.meta.get('host')}",
            f"- Workers: {self.meta.get('workers')} ({self.meta.get('executor')})",
            f"- ffmpeg: {self.meta.get('ffmpeg_version')}",
            "",
            "| " + " | ".join(cols) + " |",
            "| " + " | ".join("---" for _ in cols) + " |",
        ]
        for row in self.rows:
            cells = [str(row.get(c, "")).replace("|", "\\|").replace("\n", " ") for c in cols]
            lines.append("| " + " | ".join(cells) + " |")
        lines += ["", "## Summary", ""]
        for key, value in self.summary().items():
            lines.append(f"- **{key}**: {value}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    # -- reporting --------------------------------------------------------- #

    def summary(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for row in self.rows:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        produced = {STATUS_OK, STATUS_WARN}
        bytes_in = sum(r["input_bytes"] for r in self.rows if r["status"] in produced)
        bytes_out = sum(r["output_bytes"] for r in self.rows if r["status"] in produced)
        return {
            "total_jobs": len(self.rows),
            **{f"status_{k.lower()}": v for k, v in sorted(counts.items())},
            "bytes_in": bytes_in,
            "bytes_out": bytes_out,
            "wall_clock_sec": self.meta.get("wall_clock_sec"),
            "cpu_sec_sum": round(sum(r["elapsed_sec"] for r in self.rows), 3),
            "packets_match": sum(1 for r in self.rows if r["integrity"] == "PACKETS_MATCH"),
            "integrity_warnings": sum(
                1 for r in self.rows
                if r["integrity"] in {"PACKET_COUNT_DIFFERS", "STREAMS_DROPPED"}
            ),
            "verified_identical": sum(
                1 for r in self.rows if r["verification"] == "VERIFIED_IDENTICAL"
            ),
            "hash_mismatch": sum(1 for r in self.rows if r["verification"] == "HASH_MISMATCH"),
        }

    def print_table(self) -> None:
        cols = ["row", "status", "input_path", "resolution", "elapsed_sec",
                "integrity", "verification", "message"]
        table = [cols] + [
            [
                str(r.get("row", "")),
                r.get("status", ""),
                clip(Path(r.get("input_path", "")).name, 32),
                r.get("resolution", ""),
                f"{r.get('elapsed_sec', 0):.1f}s",
                r.get("integrity", ""),
                r.get("verification", ""),
                clip(r.get("message", ""), 58),
            ]
            for r in self.rows
        ]
        widths = [max(len(row[i]) for row in table) for i in range(len(cols))]
        for idx, row in enumerate(table):
            print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))
            if idx == 0:
                print("  ".join("-" * w for w in widths))


# --------------------------------------------------------------------------- #
# Dependency bootstrap
# --------------------------------------------------------------------------- #

PYTHON_MIN = (3, 9)

# Private state dir: resolved binary paths and an append-only setup log. Kept
# out of the install tree so the script itself stays a single portable file.
STATE_DIR = Path(os.environ.get("MTS2MP4_HOME") or (Path.home() / ".mts2mp4"))
BIN_DIR = STATE_DIR / "bin"
STATE_FILE = STATE_DIR / "state.json"

EXE = ".exe" if os.name == "nt" else ""

# Package managers per platform: (binary, install argv, needs root).
PACKAGE_MANAGERS: dict[str, list[tuple[str, list[str], bool]]] = {
    "Darwin": [
        ("brew", ["brew", "install", "ffmpeg"], False),
        ("port", ["port", "install", "ffmpeg"], True),
    ],
    "Windows": [
        ("winget", ["winget", "install", "--id", "Gyan.FFmpeg", "-e",
                    "--accept-package-agreements", "--accept-source-agreements"], False),
        ("choco", ["choco", "install", "ffmpeg", "-y"], False),
        ("scoop", ["scoop", "install", "ffmpeg"], False),
    ],
    "Linux": [
        ("apt-get", ["apt-get", "install", "-y", "ffmpeg"], True),
        ("dnf", ["dnf", "install", "-y", "ffmpeg"], True),
        ("yum", ["yum", "install", "-y", "ffmpeg"], True),
        ("zypper", ["zypper", "--non-interactive", "install", "ffmpeg"], True),
        ("pacman", ["pacman", "-S", "--noconfirm", "ffmpeg"], True),
        ("apk", ["apk", "add", "ffmpeg"], True),
    ],
}

# Static builds used by --install-deps portable. Keyed by (system, machine).
PORTABLE_BUILDS: dict[tuple[str, str], dict[str, str]] = {
    ("Linux", "x86_64"):  {"ffmpeg": "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"},
    ("Linux", "aarch64"): {"ffmpeg": "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz"},
    ("Windows", "AMD64"): {"ffmpeg": "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"},
    ("Darwin", "x86_64"): {"ffmpeg": "https://evermeet.cx/ffmpeg/getrelease/zip",
                           "ffprobe": "https://evermeet.cx/ffprobe/getrelease/zip"},
    ("Darwin", "arm64"):  {"ffmpeg": "https://evermeet.cx/ffmpeg/getrelease/zip",
                           "ffprobe": "https://evermeet.cx/ffprobe/getrelease/zip"},
}

WANTED = ("ffmpeg", "ffprobe")


def load_state() -> dict[str, Any]:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_state(state: dict[str, Any]) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except OSError as exc:
        print(f"warning: could not write {STATE_FILE}: {exc}", file=sys.stderr)


def log_setup_event(event: dict[str, Any]) -> None:
    """Append-only record of how dependencies got onto this machine."""
    state = load_state()
    state.setdefault("setup_log", []).append({"utc": utcnow(), **event})
    state["setup_log"] = state["setup_log"][-50:]
    save_state(state)


def usable(path: str | None) -> str | None:
    """Return path if it runs and answers -version, else None."""
    if not path:
        return None
    try:
        proc = run([path, "-version"], timeout=30)
        return path if proc.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def find_tool(name: str, override: str | None) -> str | None:
    """PATH first, then a previous portable install, then a remembered path."""
    if override and override != name:
        return usable(override) or usable(shutil.which(override))
    candidates = [
        shutil.which(name),
        str(BIN_DIR / f"{name}{EXE}"),
        load_state().get("paths", {}).get(name),
    ]
    for candidate in candidates:
        resolved = usable(candidate)
        if resolved:
            return resolved
    return None


def sudo_prefix(needs_root: bool) -> list[str]:
    if not needs_root or os.name == "nt":
        return []
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        return []
    return ["sudo"] if shutil.which("sudo") else []


def available_managers() -> list[tuple[str, list[str], bool]]:
    system = platform.system()
    return [m for m in PACKAGE_MANAGERS.get(system, []) if shutil.which(m[0])]


def install_via_manager(assume_yes: bool) -> bool:
    managers = available_managers()
    if not managers:
        print("No supported package manager found on this system.", file=sys.stderr)
        return False

    name, argv, needs_root = managers[0]
    cmd = sudo_prefix(needs_root) + argv
    print(f"Installing ffmpeg with {name}:\n  {' '.join(cmd)}")
    if "sudo" in cmd:
        print("  (sudo may prompt for your password)")
    if not assume_yes and not confirm("Proceed?"):
        return False

    if name == "apt-get":
        subprocess.run(sudo_prefix(needs_root) + ["apt-get", "update"], check=False)

    # Inherit stdio: package managers want a terminal for prompts and progress.
    proc = subprocess.run(cmd, check=False)
    ok = proc.returncode == 0
    log_setup_event({"action": "package_manager_install", "manager": name,
                     "command": " ".join(cmd), "returncode": proc.returncode})
    if not ok:
        print(f"{name} exited with code {proc.returncode}.", file=sys.stderr)
    return ok


def download(url: str, dest: Path) -> str:
    """Stream a URL to dest, returning the SHA-256 of what actually arrived."""
    digest = hashlib.sha256()
    req = urllib.request.Request(url, headers={"User-Agent": "mts2mp4/1.0"})
    with urllib.request.urlopen(req, timeout=120) as response, dest.open("wb") as fh:
        while True:
            chunk = response.read(1 << 20)
            if not chunk:
                break
            fh.write(chunk)
            digest.update(chunk)
            print(f"\r  downloaded {dest.stat().st_size / 1e6:6.1f} MB", end="", flush=True)
    print()
    return digest.hexdigest()


def extract_tools(archive: Path, into: Path) -> list[str]:
    """
    Pull only ffmpeg/ffprobe out of an archive, by basename, into a flat dir.

    Members are matched on basename and written to a path we construct, so a
    crafted archive cannot traverse out of the destination directory.
    """
    into.mkdir(parents=True, exist_ok=True)
    extracted: list[str] = []
    wanted = {f"{n}{EXE}" for n in WANTED}

    def take(name: str, reader) -> None:
        base = os.path.basename(name)
        if base not in wanted or base in extracted:
            return
        target = into / base
        with target.open("wb") as out:
            shutil.copyfileobj(reader, out)
        target.chmod(0o755)
        extracted.append(base)

    if archive.suffix == ".zip" or zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as zf:
            for member in zf.infolist():
                if not member.is_dir():
                    with zf.open(member) as reader:
                        take(member.filename, reader)
    else:
        with tarfile.open(archive) as tf:
            for member in tf.getmembers():
                if member.isfile():
                    reader = tf.extractfile(member)
                    if reader:
                        take(member.name, reader)
    return extracted


def install_portable(assume_yes: bool) -> bool:
    key = (platform.system(), platform.machine())
    build = PORTABLE_BUILDS.get(key)
    if not build:
        print(f"No portable build registered for {key[0]}/{key[1]}.", file=sys.stderr)
        return False

    urls = list(dict.fromkeys(build.values()))
    print(f"Portable install into {BIN_DIR}")
    print("Source" + ("s" if len(urls) > 1 else "") + ":")
    for url in urls:
        print(f"  {url}")
    print("These are third-party static builds. The SHA-256 of each download is\n"
          "recorded in the setup log so the install can be audited later.")
    if not assume_yes and not confirm("Download and install?"):
        return False

    BIN_DIR.mkdir(parents=True, exist_ok=True)
    scratch = STATE_DIR / "downloads"
    scratch.mkdir(parents=True, exist_ok=True)
    got: set[str] = set()

    for url in urls:
        archive = scratch / f"ffmpeg-dl-{uuid.uuid4().hex[:8]}"
        try:
            print(f"Fetching {url}")
            sha = download(url, archive)
            names = extract_tools(archive, BIN_DIR)
            got.update(n.replace(EXE, "") for n in names)
            print(f"  sha256 {sha}")
            print(f"  extracted: {', '.join(names) if names else 'nothing recognised'}")
            log_setup_event({"action": "portable_download", "url": url,
                             "sha256": sha, "extracted": names})
        except (urllib.error.URLError, OSError, tarfile.TarError,
                zipfile.BadZipFile, ValueError) as exc:
            print(f"  failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            log_setup_event({"action": "portable_download", "url": url,
                             "error": f"{type(exc).__name__}: {exc}"})
        finally:
            archive.unlink(missing_ok=True)

    shutil.rmtree(scratch, ignore_errors=True)
    missing = [n for n in WANTED if n not in got]
    if missing:
        print(f"Portable install incomplete — still missing: {', '.join(missing)}", file=sys.stderr)
        return False
    return True


def env_flag(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def install_auto(assume_yes: bool, interactive: bool) -> bool:
    """
    Install by whichever route suits the situation.

    With a terminal, the system package manager first: it gives a managed,
    updatable ffmpeg and someone is there to type a sudo password. Unattended,
    portable first — a system install would stall or fail on that password
    prompt, and a static build under ~/.mts2mp4/bin needs no privileges.
    """
    routes = ([install_via_manager, install_portable] if interactive
              else [install_portable, install_via_manager])
    for route in routes:
        if route(assume_yes):
            return True
        print(f"\nFalling back to {'portable build' if route is install_via_manager else 'the package manager'}.")
    return False


def confirm(question: str) -> bool:
    if not sys.stdin or not sys.stdin.isatty():
        print(f"{question} (no terminal attached — declining; pass --yes to proceed)",
              file=sys.stderr)
        return False
    try:
        return input(f"{question} [y/N] ").strip().lower() in {"y", "yes"}
    except (EOFError, KeyboardInterrupt):
        print()
        return False


def manual_instructions() -> str:
    system = platform.system()
    managers = available_managers()
    lines = ["Install ffmpeg one of these ways, then re-run:", ""]
    if managers:
        for name, argv, needs_root in managers:
            lines.append(f"  {' '.join(sudo_prefix(needs_root) + argv)}")
    elif system == "Windows":
        lines.append("  winget install --id Gyan.FFmpeg -e")
    elif system == "Darwin":
        lines.append("  brew install ffmpeg   (Homebrew: https://brew.sh)")
    else:
        lines.append("  use your distribution's package manager")
    script = Path(sys.argv[0]).name
    if not script.endswith(".py"):  # argv[0] is '-' under `python -`, '-c', etc.
        script = "mts2mp4.py"
    lines += [
        "",
        f"  python3 {script} --install-deps portable --yes    (no admin rights needed)",
        "",
        "Or download from https://ffmpeg.org/download.html and either put it on",
        "PATH or point at it with --ffmpeg / --ffprobe.",
    ]
    return "\n".join(lines)


def remember(found: dict[str, str | None]) -> None:
    """Cache resolved binary paths, but only when they have actually changed."""
    state = load_state()
    paths = {n: found[n] for n in WANTED}
    if state.get("paths") == paths:
        return
    state["paths"] = paths
    state["last_resolved_utc"] = utcnow()
    save_state(state)


def ensure_dependencies(args: argparse.Namespace) -> tuple[str, str, str] | None:
    """
    Resolve ffmpeg and ffprobe, installing them if asked. Returns
    (ffmpeg, ffprobe, source) or None when they could not be made available.
    """
    found = {name: find_tool(name, getattr(args, name)) for name in WANTED}
    mode = args.install_deps

    if all(found.values()) and mode not in {"portable", "system"}:
        source = "portable" if str(BIN_DIR) in str(found["ffmpeg"]) else "system"
        if mode == "print":
            print(f"ffmpeg:  {found['ffmpeg']}\nffprobe: {found['ffprobe']}\n"
                  "Dependencies already satisfied; nothing to install.")
            return None
        remember(found)
        return found["ffmpeg"], found["ffprobe"], source  # type: ignore[return-value]

    missing = [n for n, p in found.items() if not p]

    if mode == "print":
        print(f"Missing: {', '.join(missing) or 'nothing'}\n")
        print(manual_instructions())
        return None

    interactive = bool(sys.stdin and sys.stdin.isatty())
    remembered = load_state().get("auto_install") is True
    asked = False

    if missing and mode is None:
        # Everything here goes to stderr: stdout is block-buffered when piped,
        # so mixing the two reorders the message.
        print(f"Required binaries not found: {', '.join(missing)}\n", file=sys.stderr)

        if args.no_auto_setup:
            print(manual_instructions(), file=sys.stderr)
            return None

        # Consent can come from a flag, the environment (for cron), a choice
        # made on a previous run, or a prompt. Any of them means "go ahead".
        consented = args.yes or env_flag("MTS2MP4_AUTO_INSTALL") or remembered
        if not consented:
            if not interactive:
                print(manual_instructions(), file=sys.stderr)
                return None
            asked = True
            if not confirm("Install ffmpeg now?"):
                print("\n" + manual_instructions(), file=sys.stderr)
                return None
        mode = "auto"

    installed = False
    if mode == "auto":
        installed = install_auto(args.yes or not interactive, interactive)
    elif mode == "system":
        installed = install_via_manager(args.yes)
    elif mode == "portable":
        installed = install_portable(args.yes)

    found = {name: find_tool(name, getattr(args, name)) for name in WANTED}
    if not all(found.values()):
        still = [n for n, p in found.items() if not p]
        print(f"\nStill missing after setup: {', '.join(still)}\n", file=sys.stderr)
        print(manual_instructions(), file=sys.stderr)
        return None

    source = "portable" if str(BIN_DIR) in str(found["ffmpeg"]) else "system"
    remember(found)
    if installed:
        print(f"\nffmpeg ready: {found['ffmpeg']}")
        print(f"Setup log: {STATE_FILE}")
        if asked and not remembered:
            # Consent is recorded so a later run that finds the binaries gone
            # repairs itself instead of stopping to ask the same question.
            state = load_state()
            state["auto_install"] = True
            save_state(state)
            print("Future runs will repair a missing ffmpeg without asking "
                  "(--no-auto-setup to turn that off).")
    return found["ffmpeg"], found["ffprobe"], source  # type: ignore[return-value]


# --------------------------------------------------------------------------- #
# Discovery & CLI
# --------------------------------------------------------------------------- #


def discover(inputs: list[str], recursive: bool) -> tuple[list[Path], list[Path]]:
    """
    Return (existing sources, missing inputs).

    Missing inputs are carried forward rather than dropped: an audit trail has
    to account for every path the operator named, including the ones that
    turned out not to be there.
    """
    found: list[Path] = []
    missing: list[Path] = []
    for item in inputs:
        path = Path(item)
        if path.is_dir():
            globber = path.rglob if recursive else path.glob
            found += [p for p in globber("*") if p.is_file() and p.suffix.lower() in SOURCE_SUFFIXES]
        elif path.is_file():
            found.append(path)
        else:
            missing.append(path)
    # Deduplicate, largest first so long jobs start early and the pool drains evenly.
    unique = sorted({p.resolve() for p in found}, key=lambda p: -p.stat().st_size)
    return unique, sorted({p for p in missing})


def destination(src: Path, outdir: Path | None, roots: list[Path], flatten: bool) -> Path:
    if outdir is None:
        return src.with_suffix(".mp4")
    if not flatten:
        for root in roots:
            try:
                return (outdir / src.relative_to(root)).with_suffix(".mp4")
            except ValueError:
                continue
    return (outdir / src.name).with_suffix(".mp4")


def tool_version(binary: str) -> str:
    proc = run([binary, "-version"])
    return proc.stdout.splitlines()[0] if proc.returncode == 0 and proc.stdout else "unknown"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="mts2mp4",
        description="Parallel lossless MTS/M2TS -> MP4 remuxer with an auditable ledger.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
  mts2mp4.py clip.MTS
  mts2mp4.py ./footage -r -w 8 -o ./mp4 --verify
  mts2mp4.py ./footage -r --dry-run --ledger-dir ./audit
""",
    )
    p.add_argument("inputs", nargs="*",
                   help="MTS/M2TS/TS files and/or directories (optional with --install-deps)")
    p.add_argument("-o", "--outdir", help="output directory (default: beside each source)")
    p.add_argument("-r", "--recursive", action="store_true", help="recurse into subdirectories")
    p.add_argument("--flatten", action="store_true", help="do not mirror source tree under --outdir")
    p.add_argument("-w", "--workers", type=int, default=WORKER_DEFAULT,
                   help=f"concurrent workers, clamped to {WORKER_MIN}-{WORKER_MAX} (default {WORKER_DEFAULT})")
    p.add_argument("--executor", choices=["thread", "process"], default="thread",
                   help="pool type (default thread; ffmpeg already runs out-of-process)")
    p.add_argument("--overwrite", action="store_true", help="replace existing .mp4 files")
    p.add_argument("--no-skip-existing", dest="skip_existing", action="store_false",
                   help="treat a pre-existing output as a failure rather than a skip")
    p.add_argument("--verify", action="store_true",
                   help="decode both files and compare frame hashes — proves pixel-identical "
                        "output, but costs a full decode pass per file (slow on large archives)")
    p.add_argument("--audio-fallback", choices=["alac", "flac", "aac", "fail"], default="alac",
                   help="what to do with audio MP4 cannot hold, e.g. LPCM (default alac, lossless)")
    p.add_argument("--no-preserve-mtime", dest="preserve_mtime", action="store_false",
                   help="do not copy the source modification time onto the output")
    p.add_argument("--dry-run", action="store_true", help="plan and log everything, run nothing")
    p.add_argument("--ledger-dir", default="./conversion_ledgers", help="where to write ledger files")
    p.add_argument("--ledger-name", help="ledger basename (default: ledger_<timestamp>_<runid>)")
    p.add_argument("--ffmpeg", default="ffmpeg", help="path to the ffmpeg binary")
    p.add_argument("--ffprobe", default="ffprobe", help="path to the ffprobe binary")

    setup = p.add_argument_group("dependency setup (no pip packages are needed)")
    setup.add_argument("--install-deps", nargs="?", const="auto",
                       choices=["auto", "system", "portable", "print"], default=None,
                       help="install the ffmpeg/ffprobe binaries: 'system' uses the platform "
                            "package manager, 'portable' downloads a static build into "
                            "~/.mts2mp4/bin with no admin rights, 'auto' tries system then "
                            "portable, 'print' only reports what is missing and exits")
    setup.add_argument("-y", "--yes", action="store_true",
                       help="answer yes to setup prompts; lets an unattended run self-install "
                            "(same as setting MTS2MP4_AUTO_INSTALL=1)")
    setup.add_argument("--no-auto-setup", action="store_true",
                       help="never install anything — if ffmpeg is missing, print instructions "
                            "and exit")
    return p.parse_args(argv)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    # Line-buffer stdout so progress and diagnostics stay in order when the
    # output is piped to a log file rather than a terminal.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except (AttributeError, OSError):
        pass

    if sys.version_info < PYTHON_MIN:
        have = ".".join(str(v) for v in sys.version_info[:3])
        need = ".".join(str(v) for v in PYTHON_MIN)
        print(f"error: Python {need}+ required, running {have}.", file=sys.stderr)
        return 2

    args = parse_args(argv)

    resolved = ensure_dependencies(args)
    if resolved is None:
        # --install-deps print always lands here; so does a declined or failed
        # install. Only the latter is an error worth a non-zero exit.
        return 0 if args.install_deps == "print" else 2
    args.ffmpeg, args.ffprobe, ffmpeg_source = resolved

    if not args.inputs:
        # Bare `--install-deps` is a setup-only run; no inputs is otherwise an error.
        if args.install_deps:
            print("Dependencies are in place. Re-run with files or a directory to convert.")
            return 0
        print("error: no inputs given. See --help.", file=sys.stderr)
        return 2

    workers = max(WORKER_MIN, min(WORKER_MAX, args.workers))
    if workers != args.workers:
        print(f"note: worker count clamped to the supported range -> {workers}")

    sources, missing = discover(args.inputs, args.recursive)
    if not sources and not missing:
        print("No MTS/M2TS/TS files found.", file=sys.stderr)
        return 1

    roots = [Path(i).resolve() for i in args.inputs if Path(i).is_dir()]
    outdir = Path(args.outdir).resolve() if args.outdir else None
    run_id = uuid.uuid4().hex[:12]
    started = utcnow()

    jobs = [
        Job(
            job_id=f"{run_id}-{n:04d}",
            run_id=run_id,
            src=str(src),
            dst=str(destination(src, outdir, roots, args.flatten)),
            overwrite=args.overwrite,
            skip_existing=args.skip_existing,
            verify=args.verify,
            audio_fallback=args.audio_fallback,
            preserve_mtime=args.preserve_mtime,
            dry_run=args.dry_run,
            ffmpeg=args.ffmpeg,
            ffprobe=args.ffprobe,
        )
        for n, src in enumerate(sources, start=1)
    ]

    basename = args.ledger_name or f"ledger_{datetime.now().strftime('%Y%m%d-%H%M%S')}_{run_id}"
    meta = {
        "started_utc": started,
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "ffmpeg_version": tool_version(args.ffmpeg),
        "ffmpeg_path": args.ffmpeg,
        "ffprobe_path": args.ffprobe,
        "ffmpeg_source": ffmpeg_source,
        "workers": workers,
        "executor": args.executor,
        "verify": args.verify,
        "dry_run": args.dry_run,
        "argv": sys.argv,
    }
    ledger = Ledger(run_id, Path(args.ledger_dir).resolve(), basename, meta)

    print(f"run {run_id}: {len(jobs)} file(s), {workers} {args.executor} worker(s)"
          f"{' [dry run]' if args.dry_run else ''}")
    print(f"journal: {ledger.journal}\n")

    # Unresolvable inputs become ledger rows before any work starts, so the
    # exported table accounts for every path the operator supplied.
    for n, path in enumerate(missing, start=len(jobs) + 1):
        ledger.add(asdict(Record(
            run_id=run_id, job_id=f"{run_id}-{n:04d}", status=STATUS_FAIL,
            started_utc=started, finished_utc=utcnow(),
            input_path=str(path),
            message="input path does not exist (not a file or directory)",
        )))
        print(f"[--] {STATUS_FAIL:<8} {path} -> input path does not exist")

    pool_cls = ProcessPoolExecutor if args.executor == "process" else ThreadPoolExecutor
    t0 = time.perf_counter()
    done = 0

    with pool_cls(max_workers=workers) as pool:
        futures = {pool.submit(convert_one, job): job for job in jobs}
        try:
            for future in as_completed(futures):
                job = futures[future]
                try:
                    row = future.result()
                except Exception as exc:  # noqa: BLE001
                    row = asdict(Record(
                        run_id=run_id, job_id=job.job_id, status=STATUS_FAIL,
                        started_utc=started, finished_utc=utcnow(),
                        input_path=job.src, output_path=job.dst,
                        message=f"worker crashed: {type(exc).__name__}: {exc}",
                    ))
                ledger.add(row)
                done += 1
                print(f"[{done}/{len(jobs)}] {row['status']:<8} {Path(row['input_path']).name} "
                      f"({row['elapsed_sec']}s)")
                if row["status"] in {STATUS_FAIL, STATUS_WARN}:
                    print(f"          -> {row['message']}")
        except KeyboardInterrupt:
            print("\ninterrupted — finishing ledger with the rows completed so far", file=sys.stderr)

    meta["wall_clock_sec"] = round(time.perf_counter() - t0, 3)
    meta["finished_utc"] = utcnow()

    print("\nLedger")
    print("=" * 140)
    ledger.print_table()
    print("=" * 140)

    paths = [ledger.to_csv(), ledger.to_json(), ledger.to_markdown(), ledger.journal]
    print("\nSummary")
    for key, value in ledger.summary().items():
        print(f"  {key}: {value}")
    print("\nExports")
    for path in paths:
        print(f"  {path}")

    # Exit codes: 0 clean, 1 at least one failure, 3 converted but with warnings.
    if any(r["status"] == STATUS_FAIL for r in ledger.rows):
        return 1
    if any(r["status"] == STATUS_WARN for r in ledger.rows):
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
