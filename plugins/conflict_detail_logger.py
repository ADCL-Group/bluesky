"""
BlueSky Conflict Detail Logger
- 1 row per conflict episode
- Uses BlueSky's own lospairs 

Commands:
  LOGCONFLICT ON <path to csv> | OFF
"""

import os
import csv
import math

from bluesky import core, traf, stack, sim


def horiz_m(lat1, lon1, lat2, lon2):
    dlat = (lat1 - lat2) * 111320
    dlon = (lon1 - lon2) * 111320
    return math.sqrt(dlat*dlat + dlon*dlon)


class _ConflictDetailLogger(core.Entity):
    def __init__(self):
        super().__init__()
        self.enabled = False
        self.active = {}
        self.file = None
        self.writer = None

    def _open(self, path):
        self._close()
        os.makedirs(os.path.dirname(path), exist_ok=True)

        self.file = open(path, "w", newline="")
        self.writer = csv.writer(self.file)

        # Write CSV header
        self.writer.writerow([
            "start_time","end_time",
            "ac1","ac2",
            "ac1_start_lat","ac1_start_lon","ac1_start_alt",
            "ac2_start_lat","ac2_start_lon","ac2_start_alt",
            "ac1_end_lat","ac1_end_lon","ac1_end_alt",
            "ac2_end_lat","ac2_end_lon","ac2_end_alt",
            "min_horiz_m","min_horiz_nm","min_vert_ft",
            "ac1_min_lat","ac1_min_lon","ac1_min_alt",
            "ac2_min_lat","ac2_min_lon","ac2_min_alt",
            "los_occurred"     # BlueSky-based LOS flag
        ])
        self.enabled = True

    def _close(self):
        if self.file:
            self.file.close()
        self.file = None
        self.writer = None
        self.enabled = False
        self.active.clear()

    @core.timed_function(name="conf_detail_logger", dt=1.0)
    def update(self):
        if not self.enabled:
            return

        cd = traf.cd
        if cd is None or not hasattr(cd, "confpairs_unique"):
            return

        now = sim.simt
        ids = traf.id
        lat = traf.lat
        lon = traf.lon
        alt = traf.alt

        # BlueSky OFFICIAL LOS pairs
        lospairs = set()
        try:
            for p in cd.lospairs:
                ac1, ac2 = sorted(list(p))
                lospairs.add((ac1, ac2))
        except:
            pass

        current_pairs = set()

        # -----------------------------
        # 1. Track active conflict pairs
        # -----------------------------
        for pair in cd.confpairs_unique:
            ac1, ac2 = sorted(list(pair))
            key = (ac1, ac2)
            current_pairs.add(key)

            try:
                i1 = ids.index(ac1)
                i2 = ids.index(ac2)
            except ValueError:
                continue

            lat1, lon1, alt1 = lat[i1], lon[i1], alt[i1]
            lat2, lon2, alt2 = lat[i2], lon[i2], alt[i2]

            hm = horiz_m(lat1, lon1, lat2, lon2)
            nm = hm / 1852.0
            vf = abs(alt1 - alt2) * 3.28084

            # --- BlueSky-based LOS flag ---
            los_flag = 1 if key in lospairs else 0

            # NEW conflict → initialize
            if key not in self.active:
                self.active[key] = {
                    "start": now,
                    "ac1_start": (lat1, lon1, alt1),
                    "ac2_start": (lat2, lon2, alt2),
                    "ac1_end": (lat1, lon1, alt1),
                    "ac2_end": (lat2, lon2, alt2),
                    "min_hm": hm,
                    "min_nm": nm,
                    "min_vf": vf,
                    "ac1_min": (lat1, lon1, alt1),
                    "ac2_min": (lat2, lon2, alt2),
                    "los": los_flag
                }
            else:
                rec = self.active[key]

                # update end positions
                rec["ac1_end"] = (lat1, lon1, alt1)
                rec["ac2_end"] = (lat2, lon2, alt2)

                # update min separation
                if hm < rec["min_hm"]:
                    rec["min_hm"] = hm
                    rec["min_nm"] = nm
                    rec["min_vf"] = vf
                    rec["ac1_min"] = (lat1, lon1, alt1)
                    rec["ac2_min"] = (lat2, lon2, alt2)

                # once LOS happens, keep flag ON
                if los_flag == 1:
                    rec["los"] = 1

        # --------------------------------
        # 2. Finish & write ended conflicts
        # --------------------------------
        ended = [k for k in list(self.active.keys()) if k not in current_pairs]

        for key in ended:
            ac1, ac2 = key
            rec = self.active[key]

            self.writer.writerow([
                rec["start"], now,
                ac1, ac2,
                *rec["ac1_start"],
                *rec["ac2_start"],
                *rec["ac1_end"],
                *rec["ac2_end"],
                rec["min_hm"], rec["min_nm"], rec["min_vf"],
                *rec["ac1_min"],
                *rec["ac2_min"],
                rec["los"]     # BlueSky LOS result
            ])

            del self.active[key]

        if self.file:
            self.file.flush()


_LOGGER = None


@stack.command(name="LOGCONFLICT")
def logconflict_cmd(mode: str, path: str = ""):
    """LOGCONFLICT on <file> | off"""
    global _LOGGER
    if _LOGGER is None:
        return False, "Logger not initialized."

    mode = mode.lower()

    if mode == "on":
        if not path:
            return False, "Usage: LOGCONFLICT on <file>"
        _LOGGER._open(path)
        return True, f"Conflict logging ON → {path}"

    if mode == "off":
        _LOGGER._close()
        return True, "Conflict logging OFF"

    return False, "Usage: LOGCONFLICT on <file> | off"


def init_plugin():
    global _LOGGER
    _LOGGER = _ConflictDetailLogger()
    return {"plugin_name": "conflict_detail_logger", "plugin_type": "sim"}

