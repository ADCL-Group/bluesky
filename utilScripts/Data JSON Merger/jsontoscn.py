import json
from pathlib import Path
from tkinter import Tk, filedialog
from datetime import datetime, timedelta


def choose_file(title, filetypes):
    Tk().withdraw()
    return Path(filedialog.askopenfilename(title=title, filetypes=filetypes))


def parse_json_to_commands(json_path, acid="VTOL1", ac_type="A320"):
    """Convert JSON trajectory into list of (time_offset_sec, command_string).
       Automatically handles time as number OR as timestamp string.
    """

    data = json.loads(Path(json_path).read_text())

    raw_time = data["time"]
    lat = data["lat_deg"]
    lon = data["lon_deg"]
    alt = data["alt_ft"]
    hdg = data["psi_deg"]
    spd = data["V_kts"]

    # --------------------------------------------------------
    # FIX: Convert time to seconds, regardless of input format
    # --------------------------------------------------------
    def convert_time(t):
        # Numeric seconds?
        try:
            return float(t)
        except:
            pass

        # Timestamp string (HH:MM:SS.xx)
        try:
            dt = datetime.strptime(t, "%H:%M:%S.%f")
        except ValueError:
            # Maybe no decimals
            dt = datetime.strptime(t, "%H:%M:%S")

        # Convert to seconds from 0
        return (dt - datetime(dt.year, dt.month, dt.day, 0, 0, 0)).total_seconds()

    time = [convert_time(t) for t in raw_time]

    # --------------------------------------------------------
    # Helpers
    # --------------------------------------------------------
    def wrap_h(h):
        return (float(h) % 360 + 360) % 360

    def fmt_alt(ft):
        return f"{ft:.0f}"

    # --------------------------------------------------------
    # Build commands
    # --------------------------------------------------------
    cmd_list = []

    # First CRE
    cmd_list.append((
        time[0],
        f"CRE {acid},{ac_type},{lat[0]:.6f},{lon[0]:.6f},{wrap_h(hdg[0]):.2f},{fmt_alt(alt[0])},{spd[0]:.1f}"
    ))

    for i in range(1, len(time)):
        t = time[i]
        cmd_list.append((t, f"MOVE {acid},{lat[i]:.6f},{lon[i]:.6f},{fmt_alt(alt[i])}"))
        cmd_list.append((t, f"HDG {acid},{wrap_h(hdg[i]):.2f}"))
        cmd_list.append((t, f"SPD {acid},{spd[i]:.1f}"))

    return cmd_list


def extract_first_cre_time(scn_lines):
    """Find the first CRE command time (HH:MM:SS.xx)."""

    for line in scn_lines:
        if "CRE" in line:
            # Format: 12:00:00.00> CRE ...
            timestamp = line.split(">")[0].strip()
            return timestamp
    raise ValueError("No CRE line found in .scn file!")


def scn_timestamp_to_datetime(stamp):
    """Convert '12:00:00.00' into a datetime object (date is arbitrary)."""
    return datetime.strptime(stamp, "%H:%M:%S.%f")


def insert_time_aligned(existing_scn_path, commands):
    """
    Insert commands at proper times.

    commands: list of (time_offset_sec, command_string)
    """

    scn_path = Path(existing_scn_path)
    lines = scn_path.read_text().splitlines()

    # Detect reference start time (first CRE in file)
    start_stamp = extract_first_cre_time(lines)
    start_dt = scn_timestamp_to_datetime(start_stamp)

    # Build new timestamped lines
    new_lines = []
    for t_offset, cmd in commands:
        new_time = start_dt + timedelta(seconds=float(t_offset))
        new_stamp = new_time.strftime("%H:%M:%S.%f")[:-4]  # keep 2 decimals like .00
        new_lines.append(f"{new_stamp}> {cmd}")

    # Merge + resort by timestamp
    combined = lines + new_lines

    # Sort by time prefix (HH:MM:SS.xx)
    def sort_key(line):
        if ">" not in line:
            return datetime(1900,1,1)
        try:
            ts = line.split(">")[0].strip()
            return scn_timestamp_to_datetime(ts)
        except:
            return datetime(1900,1,1)

    combined_sorted = sorted(combined, key=sort_key)

    out_path = scn_path.with_name(scn_path.stem + "_withVTOL.scn")
    out_path.write_text("\n".join(combined_sorted))

    return out_path


if __name__ == "__main__":
    print("Select trajectory JSON…")
    json_file = choose_file("Select JSON trajectory", [("JSON files", "*.json")])

    print("Select existing SCN file…")
    scn_file = choose_file("Select .scn", [("Scenario files", "*.scn")])

    acid = input("Callsign (default VTOL1): ").strip() or "VTOL1"
    ac_type = input("Aircraft type (default A320): ").strip() or "A320"

    print("Converting JSON → Commands…")
    commands = parse_json_to_commands(json_file, acid=acid, ac_type=ac_type)

    print("Inserting into scenario at correct timestamp…")
    out_path = insert_time_aligned(scn_file, commands)

    print("\nDone!")
    print(f"Created: {out_path}")

