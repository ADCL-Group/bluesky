#!/usr/bin/env python3
"""
adsbxchange.py

Pull ADS-B Exchange readsb-hist snapshots for a given UTC date/time window
and convert them into a BlueSky .scn (TrafScript) scenario.

Time flags ONLY:
  --start HH:MM:SS
  --stop  HH:MM:SS
Times snap UP to the 5-second grid (min 00:00:05, max 23:59:55).

Example:
 python3 adsbxchange.py 2025 11 01 --start 12:30:00 --stop 13:00:00 --bbox -81.7581221693783 28.0601167617708 -81.2420847792120 28.5411790369085 --min-alt-bbox 500 --max-alt-bbox 6000

"""

import argparse
import datetime as dt
import gzip
import io
import json
import os
import re
import signal
import sys
import urllib.request
from typing import Dict, Optional

# ---------------- Config ----------------
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/{yyyy}/{mm}/{dd}/{hh}{mi}{ss}Z.json.gz"
STEP_SECONDS = 5                  # fixed 5 s ticks
ABSENCE_TIMEOUT = 60              # seconds since last pos/msg before DEL
MISSING_CONSEC = 3                # require N consecutive missing ticks before timing out a/c
FLUSH_EVERY_TICKS = 12            # flush every ~1 minute (12 * 5 s)

# Fallback types if ADS-B 't' missing/unmapped
DEFAULT_TYPES = ["A320", "B738"]

ACID_SAFE = re.compile(r"[^A-Za-z0-9_]+")

# --------------- Helpers ----------------
def acid_from_aircraft(ac: Dict) -> str:
    """Prefer callsign (flight), else hex; sanitize to BlueSky-friendly ACID."""
    acid = (ac.get("flight") or "").strip()
    if not acid:
        acid = (ac.get("r") or "").upper()
    acid = ACID_SAFE.sub("", acid.replace(" ", "_"))
    return acid if acid else "UNK"

def typ_from_aircraft(ac: Dict) -> str:
    """Try ADS-B 't' (ICAO 8643), else a generic type."""
    t = (ac.get("t") or "").strip()
    if t:
        t = ACID_SAFE.sub("", t)
        if t:
            return t
    return DEFAULT_TYPES[0]

def num_or_none(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(x)
    except Exception:
        return None

def alt_feet(ac: Dict) -> Optional[float]:
    """Prefer geometric altitude; fallback to baro (or 0 if 'ground')."""
    alt = num_or_none(ac.get("alt_baro"))
    if alt is None:
        ab = ac.get("alt_baro")
        if isinstance(ab, str) and ab.lower() == "ground":
            return 0.0
        alt = num_or_none(ab)
    return alt

def spd_kts(ac: Dict) -> Optional[float]:
    return num_or_none(ac.get("gs"))

def hdg_deg(ac: Dict) -> Optional[float]:
    h = num_or_none(ac.get("true_heading"))
    if h is None:
        h = num_or_none(ac.get("track"))
    if h is None:
        h = num_or_none(ac.get("mag_heading"))
    if h is not None:
        return (h % 360.0 + 360.0) % 360.0
    return None

def has_valid_pos(ac: Dict) -> bool:
    return (num_or_none(ac.get("lat")) is not None
            and num_or_none(ac.get("lon")) is not None)

def url_for(ts: dt.datetime) -> str:
    return BASE_URL.format(
        yyyy=f"{ts.year:04d}", mm=f"{ts.month:02d}", dd=f"{ts.day:02d}",
        hh=f"{ts.hour:02d}", mi=f"{ts.minute:02d}", ss=f"{ts.second:02d}",
    )

def fetch_snapshot(ts: dt.datetime, verbose: bool = True) -> Optional[Dict]:
    """Fetch one snapshot. Sniffs gzip vs. plain JSON; prints the URL."""
    url = url_for(ts)
    if verbose:
        print(f"[debug] fetching {url}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "adsbx-to-bluesky/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read()
            enc = resp.headers.get("Content-Encoding", "").lower()

        # If header claims gzip, try gzip
        if "gzip" in enc:
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
                    return json.load(gz)
            except OSError as e:
                if verbose:
                    print(f"[debug] header said gzip but decompress failed: {e}; will sniff bytes")

        # Magic number sniff (1F 8B)
        if raw[:2] == b"\x1f\x8b":
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
                return json.load(gz)

        # Else assume plain JSON
        try:
            return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as e:
            if verbose:
                peek = raw[:120]
                print(f"[debug] plain JSON parse failed: {e}; first bytes: {peek!r}")
            return None

    except Exception as e:
        if verbose:
            print(f"[debug] fetch failed for {url}: {e}")
        return None

def ts_label(tseconds: int) -> str:
    h = tseconds // 3600
    m = (tseconds % 3600) // 60
    s = tseconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}.00>"

def parse_hhmmss(s: str) -> int:
    """Return seconds since 00:00:00 for 'HH:MM:SS'."""
    h, m, sec = [int(x) for x in s.split(":")]
    if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59):
        raise ValueError("HH:MM:SS out of range")
    return h*3600 + m*60 + sec

def snap_up_to_5s(seconds: int) -> int:
    """Snap UP to the next 5-second boundary; clamp to [5, 86395] then to 23:59:55."""
    if seconds < 5:
        seconds = 5
    rem = seconds % STEP_SECONDS
    if rem:
        seconds += (STEP_SECONDS - rem)
    if seconds > 86395:  # 23:59:55
        seconds = 86395
    return seconds

# ---------------- Main ------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("year", type=int)
    ap.add_argument("month", type=int)
    ap.add_argument("day", type=int)
    ap.add_argument("--start", required=True, help="UTC start time HH:MM:SS (snaps up to 5s grid)")
    ap.add_argument("--stop",  required=True, help="UTC stop time  HH:MM:SS (snaps up to 5s grid)")
    ap.add_argument("--out", default=None, help="Output .scn filename")
    ap.add_argument("--max-aircraft", type=int, default=0, help="Cap number of aircraft (0 = no cap)")
    ap.add_argument("--min-alt-ft", type=float, default=None, help="Global min altitude filter (feet)")
    ap.add_argument("--bbox", type=float, nargs=4, metavar=("MINLON","MINLAT","MAXLON","MAXLAT"),
                    help="Optional lon/lat bounding box filter")
    ap.add_argument("--min-alt-bbox", type=float, default=None,
                    help="Optional min altitude (ft) for bbox vertical filter")
    ap.add_argument("--max-alt-bbox", type=float, default=None,
                    help="Optional max altitude (ft) for bbox vertical filter")
    ap.add_argument("--quiet", action="store_true", help="Reduce debug prints")
    args = ap.parse_args()

    # Parse & snap times
    s_sec = snap_up_to_5s(parse_hhmmss(args.start))
    e_sec = snap_up_to_5s(parse_hhmmss(args.stop))
    if e_sec < s_sec:
        raise SystemExit("--stop must be >= --start (after snapping)")

    # Build actual datetimes
    day0 = dt.datetime(args.year, args.month, args.day, tzinfo=dt.timezone.utc)
    t0 = day0 + dt.timedelta(seconds=s_sec)
    t1 = day0 + dt.timedelta(seconds=e_sec)
    step = dt.timedelta(seconds=STEP_SECONDS)

    outname = args.out or f"adsbx_{args.year:04d}-{args.month:02d}-{args.day:02d}_{args.start.replace(':','')}-{args.stop.replace(':','')}.scn"

    created: set[str] = set()         # aircraft currently in scenario
    last_seen_pos: dict[str,int] = {} # epoch seconds last position
    last_seen_msg: dict[str,int] = {} # epoch seconds last any message
    gone_counter:  dict[str,int] = {} # consecutive missing ticks
    n_aircraft_total = 0
    seen_any = False
    ac_count_cap = args.max_aircraft if args.max_aircraft and args.max_aircraft > 0 else None
    verbose = not args.quiet

    with open(outname, "w", encoding="utf-8") as f:

        # Graceful Ctrl-C: ensure we flush the file before exiting
        def _graceful_sigint(signum, frame):
            try:
                f.flush()
                # os.fsync(f.fileno())  # enable if you need guaranteed durability
            except Exception:
                pass
            print("\n[info] interrupted, flushed file; exiting.")
            sys.exit(130)
        signal.signal(signal.SIGINT, _graceful_sigint)

        # Header
        f.write("# Generated from ADS-B Exchange readsb-hist snapshots (5 s)\n")
        f.write("# Time-stamped BlueSky scenario (TrafScript). Units: alt[ft], spd[kt], hdg[deg]\n")
        f.write("00:00:00.00> pan kmco\n")
        f.write("00:00:00.00> DT 1\n")
        f.write("00:00:01.00> ASAS OFF\n\n")

        tick_i = 0
        missing_streak = 0
        ts = t0
        while ts <= t1:
            relsec = int((ts - day0).total_seconds())
            prefix = ts_label(relsec)
            snap = fetch_snapshot(ts, verbose=verbose)

            if not snap or not isinstance(snap.get("aircraft"), list):
                if missing_streak == 0:
                    print(f"[warn] missing {ts.time().isoformat(timespec='seconds')}Z")
                missing_streak += 1
                tick_i += 1
                if tick_i % FLUSH_EVERY_TICKS == 0:
                    f.flush()
                ts += step
                continue

            if missing_streak:
                if verbose:
                    print(f"[info] resumed after {missing_streak} missing tick(s) at {ts.time().isoformat(timespec='seconds')}Z")
                missing_streak = 0

            now_epoch = int(ts.timestamp())
            present_acids = set()

            raw_aircraft = snap.get("aircraft", [])
            raw_count = len(raw_aircraft)

            aircraft = raw_aircraft
            if ac_count_cap is not None and len(aircraft) > ac_count_cap:
                aircraft = aircraft[:ac_count_cap]

            kept = 0
            for ac in aircraft:
                if not has_valid_pos(ac):
                    continue

                lat = num_or_none(ac.get("lat"))
                lon = num_or_none(ac.get("lon"))
                if lat is None or lon is None:
                    continue

                # Pull altitude EARLY so bbox vertical filter can use it
                alt = alt_feet(ac)

                # Combined horizontal + vertical bbox filter (if provided)
                if args.bbox:
                    minlon, minlat, maxlon, maxlat = args.bbox
                    in_horiz = (minlon <= lon <= maxlon and minlat <= lat <= maxlat)
                    in_vert  = True
                    if args.min_alt_bbox is not None or args.max_alt_bbox is not None:
                        a_ft = alt if alt is not None else 0.0
                        if args.min_alt_bbox is not None and a_ft < args.min_alt_bbox:
                            in_vert = False
                        if args.max_alt_bbox is not None and a_ft > args.max_alt_bbox:
                            in_vert = False
                    if not (in_horiz and in_vert):
                        continue

                # Optional global min-alt filter (independent of bbox)
                if args.min_alt_ft is not None and (alt is None or alt < args.min_alt_ft):
                    continue

                spd = spd_kts(ac)
                hdg = hdg_deg(ac)
                acid = acid_from_aircraft(ac)
                typ  = typ_from_aircraft(ac)

                present_acids.add(acid)
                kept += 1

                # Recency bookkeeping
                last_seen_pos[acid] = now_epoch
                seen = num_or_none(ac.get("seen"))
                if seen is not None:
                    last_seen_msg[acid] = now_epoch - int(seen)

                # Create or update
                if acid not in created:
                    a = int(round(alt if alt is not None else 0.0))
                    s = int(round(spd if spd is not None else 0))
                    h = int(round(hdg if hdg is not None else 0))
                    f.write(f"{prefix} CRE {acid},{typ},{lat:.6f},{lon:.6f},{h},{a},{s}\n")
                    created.add(acid)
                    n_aircraft_total += 1
                else:
                    if alt is not None:
                        f.write(f"{prefix} MOVE {acid},{lat:.6f},{lon:.6f},{int(round(alt))}\n")
                    else:
                        f.write(f"{prefix} MOVE {acid},{lat:.6f},{lon:.6f}\n")
                    if hdg is not None:
                        f.write(f"{prefix} HDG {acid},{int(round(hdg))}\n")
                    if spd is not None:
                        f.write(f"{prefix} SPD {acid},{int(round(spd))}\n")
                    if alt is not None:
                        f.write(f"{prefix} ALT {acid},{int(round(alt))}\n")

            if kept == 0 and raw_count > 0 and verbose:
                print(f"[info] {ts.time().isoformat(timespec='seconds')}Z: total={raw_count}, kept=0 (filters removed all)")

            # Deletion policy: aircraft not present at this tick
            for acid_active in list(created):
                if acid_active in present_acids:
                    gone_counter[acid_active] = 0
                    continue
                gone_counter[acid_active] = gone_counter.get(acid_active, 0) + 1
                if gone_counter[acid_active] < MISSING_CONSEC:
                    continue
                last_pos = last_seen_pos.get(acid_active, 0)
                last_msg = last_seen_msg.get(acid_active, 0)
                absent_for = now_epoch - max(last_pos, last_msg)
                if absent_for >= ABSENCE_TIMEOUT:
                    f.write(f"{prefix} DEL {acid_active}\n")
                    created.remove(acid_active)
                    gone_counter.pop(acid_active, None)
                    last_seen_pos.pop(acid_active, None)
                    last_seen_msg.pop(acid_active, None)

            seen_any = True
            tick_i += 1
            if tick_i % FLUSH_EVERY_TICKS == 0:
                f.flush()
            ts += step

        if seen_any:
            end_rel = int((t1 - day0).total_seconds())
            f.write(f"\n{ts_label(end_rel)} HOLD\n")
            f.flush()

    print(f"Wrote {outname}")
    if not seen_any:
        print("Warning: no snapshots processed.")

if __name__ == "__main__":
    main()

