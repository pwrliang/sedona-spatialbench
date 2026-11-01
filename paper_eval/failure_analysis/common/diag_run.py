#!/usr/bin/env python3
"""Generic diagnostic runner used by all failure-analysis experiments.

Runs an arbitrary command with a hard timeout and records
  * wall-clock time,
  * exit status / signal (e.g. -9 = SIGKILL by the kernel OOM killer or by us),
  * peak RSS of the whole process tree (sampled from /proc every 0.2 s;
    no psutil dependency),
  * optionally the peak RSS of *extra* processes matched by a regex on their
    cmdline (used for PostgreSQL backends, which are not our children),
  * full stdout / stderr into log files.
A JSON record is appended to --record (JSON-lines) so every attempt is retained.

Example:
  diag_run.py --name ch_q4 --timeout 600 --record results.jsonl \
      --stdout logs/ch_q4.out --stderr logs/ch_q4.err -- clickhouse local ...
"""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

PAGE = os.sysconf("SC_PAGE_SIZE")


def _children_map():
    m = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/stat") as f:
                s = f.read()
            ppid = int(s[s.rfind(")") + 2:].split()[1])
            m.setdefault(ppid, []).append(int(pid))
        except Exception:
            pass
    return m


def _rss(pid):
    try:
        with open(f"/proc/{pid}/statm") as f:
            return int(f.read().split()[1]) * PAGE
    except Exception:
        return 0


def _tree(root):
    m = _children_map()
    out, stack = [], [root]
    while stack:
        p = stack.pop()
        out.append(p)
        stack.extend(m.get(p, []))
    return out


def _match(regex):
    pids = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\0", b" ").decode(errors="ignore")
            if re.search(regex, cmd):
                pids.append(int(pid))
        except Exception:
            pass
    return pids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--timeout", type=float, required=True)
    ap.add_argument("--record", required=True, help="JSON-lines file to append the record to")
    ap.add_argument("--stdout", required=True)
    ap.add_argument("--stderr", required=True)
    ap.add_argument("--stdin", default=None, help="file fed to stdin")
    ap.add_argument("--extra-proc-regex", default=None,
                    help="also track RSS of processes whose cmdline matches (e.g. 'postgres: .*spatialbench')")
    ap.add_argument("--meta", default="{}", help="extra JSON merged into the record")
    ap.add_argument("--max-rss-gib", type=float, default=None,
                    help="safety cap: kill the process tree (status=rss_cap) once tree+extra RSS exceeds this; "
                         "used to reproduce out-of-memory behaviour without invoking the kernel OOM killer")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    a = ap.parse_args()
    cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd

    for p in (a.stdout, a.stderr, a.record):
        os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
    fin = open(a.stdin) if a.stdin else subprocess.DEVNULL
    fout, ferr = open(a.stdout, "w"), open(a.stderr, "w")
    started = datetime.now(timezone.utc).isoformat()
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, stdin=fin, stdout=fout, stderr=ferr, start_new_session=True)

    peak = {"tree": 0, "extra": 0, "sum": 0}
    timeline = []
    stop = threading.Event()
    capped = threading.Event()

    def sampler():
        last_tl = 0.0
        while not stop.is_set():
            tree = sum(_rss(p) for p in _tree(proc.pid))
            extra = sum(_rss(p) for p in _match(a.extra_proc_regex)) if a.extra_proc_regex else 0
            peak["tree"] = max(peak["tree"], tree)
            peak["extra"] = max(peak["extra"], extra)
            peak["sum"] = max(peak["sum"], tree + extra)
            if a.max_rss_gib and (tree + extra) > a.max_rss_gib * 2**30 and not capped.is_set():
                capped.set()
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    pass
            now = time.perf_counter() - t0
            if now - last_tl >= 5.0:
                timeline.append((round(now, 1), round((tree + extra) / 2**30, 2)))
                last_tl = now
            stop.wait(0.2)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    status = "ok"
    try:
        rc = proc.wait(timeout=a.timeout)
        if rc != 0:
            status = f"exit_{rc}" if rc > 0 else f"signal_{-rc}"
    except subprocess.TimeoutExpired:
        status = "timeout"
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        rc = proc.returncode
    elapsed = time.perf_counter() - t0
    if capped.is_set():
        status = "rss_cap"
    stop.set()
    th.join()
    fout.close(); ferr.close()

    rec = {
        "name": a.name, "started_utc": started, "status": status, "returncode": rc,
        "wall_seconds": round(elapsed, 3), "timeout": a.timeout, "max_rss_gib": a.max_rss_gib,
        "peak_rss_tree_gib": round(peak["tree"] / 2**30, 3),
        "peak_rss_extra_gib": round(peak["extra"] / 2**30, 3),
        "peak_rss_total_gib": round(peak["sum"] / 2**30, 3),
        "rss_timeline_gib_every5s": timeline,
        "cmd": cmd, "stdout": a.stdout, "stderr": a.stderr,
    }
    rec.update(json.loads(a.meta))
    with open(a.record, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(f"[diag] {a.name}: {status} in {elapsed:.1f}s, peak RSS {rec['peak_rss_total_gib']} GiB", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
