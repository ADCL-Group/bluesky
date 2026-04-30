"""BlueSky 'Corridors' plugin with pygame overlay support and colors.

Reusable corridors you can draw on the map and assign to aircraft.

Commands:
  CORR NEW <name> [width_nm]
  CORR ADD <name> <wp|lat lon|lat,lon>
  CORR CLEAR <name>
  CORR LIST [name]
  CORR DRAW <name> ON|OFF
  CORR ASSIGN <acid> <name>
  CORR COLOR <name> <r> <g> <b>
  CORR WCOLOR <name> <r> <g> <b>     # width polygon color
"""

from typing import Dict, Tuple
from bluesky import stack, navdb
import bluesky as bs
import math

_CORRIDORS: Dict[str, Dict] = {}

def _screen_ready():
    return (
        hasattr(bs, "scr")
        and bs.scr is not None
        and hasattr(bs.scr, "objtype")
        and hasattr(bs.scr, "objcolor")
        and hasattr(bs.scr, "objdata")
        and hasattr(bs.scr, "objname")
        and hasattr(bs.scr, "redrawradbg")
    )


def _mark_dirty():
    if _screen_ready():
        bs.scr.redrawradbg = True


def _find_obj_index(name: str) -> int:
    if not _screen_ready():
        return -1
    try:
        return bs.scr.objname.index(name)
    except ValueError:
        return -1


def _set_pg_object(name: str, objtype: str, objdata, color):
    """Create or update one BlueSky pygame background object."""
    if not _screen_ready():
        return

    idx = _find_obj_index(name)
    if idx >= 0:
        bs.scr.objtype[idx] = objtype
        bs.scr.objdata[idx] = objdata
        bs.scr.objcolor[idx] = color
        bs.scr.objname[idx] = name
    else:
        bs.scr.objtype.append(objtype)
        bs.scr.objdata.append(objdata)
        bs.scr.objcolor.append(color)
        bs.scr.objname.append(name)

    _mark_dirty()


def _del_pg_object(name: str):
    if not _screen_ready():
        return

    idx = _find_obj_index(name)
    if idx >= 0:
        del bs.scr.objtype[idx]
        del bs.scr.objdata[idx]
        del bs.scr.objcolor[idx]
        del bs.scr.objname[idx]
        _mark_dirty()


def init_plugin():
    return {"plugin_name": "corridors", "plugin_type": "sim", "version": "0.5"}


def _key(name: str) -> str:
    return name.upper()


def _ensure(name: str) -> str:
    k = _key(name)
    if k not in _CORRIDORS:
        _CORRIDORS[k] = {
            "pts": [],
            "width": 1.0,
            "drawn": False,
            "nseg": 0,
            "seg_names": [],
            "raw": name,
            "color": (0, 255, 255),     # cyan centerline default
            "wcolor": (255, 255, 0),    # yellow width polygon default
        }
    return k


def _is_pygame_gui():
    return _screen_ready()

def _lookup_latlon(*tokens) -> Tuple[float, float]:
    """Accept 'lat lon', 'lat,lon', or a waypoint/airport ident."""
    if len(tokens) == 1 and "," in tokens[0]:
        a, b = tokens[0].split(",", 1)
        return float(a), float(b)
    if len(tokens) >= 2:
        return float(tokens[0]), float(tokens[1])
    token = tokens[0]
    i = navdb.getaptidx(token)
    if i >= 0:
        return float(navdb.aptlat[i]), float(navdb.aptlon[i])
    j = navdb.getwpidx(token)
    if j >= 0:
        return float(navdb.wplat[j]), float(navdb.wplon[j])
    raise ValueError(f"Unknown waypoint/latlon: {token}")


def _del_line(name: str):
    try:
        stack.stack(f"DEL {name}")
    except Exception:
        pass


def _del_line_variants(base: str, idx: int):
    candidates = [
        f"{base}_S{idx}", f"{base}_s{idx}",
        f"{base.upper()}_S{idx}", f"{base.upper()}_s{idx}",
        f"{base.lower()}_S{idx}", f"{base.lower()}_s{idx}",
    ]
    for nm_ in candidates:
        _del_line(nm_)


def _pg_clear_corridor(namekey: str):
    """Remove pygame objects belonging to one corridor."""
    data = _CORRIDORS.get(namekey)
    if not data:
        return

    base = data.get("raw", namekey)

    dead = [nm for nm in list(bs.scr.objname) if nm.startswith(base + "_")] if _screen_ready() else []
    for nm in dead:
        _del_pg_object(nm)


def _hide(namekey: str):
    """Remove all drawn geometry for this corridor."""
    data = _CORRIDORS.get(namekey)
    if not data:
        return

    if _is_pygame_gui():
        _pg_clear_corridor(namekey)
    else:
        _del_line(data.get("raw", namekey))
        segs = data.get("seg_names", [])
        if segs:
            for nm_ in segs:
                _del_line(nm_)
        else:
            nprev = data.get("nseg", 0)
            base = data.get("raw", namekey)
            for i in range(1, nprev + 1):
                _del_line_variants(base, i)

    data["nseg"] = 0
    data["seg_names"] = []

def _move(lat, lon, bearing, dist_nm):
    R_nm = 3440.065
    dist_rad = dist_nm / R_nm

    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    brg = bearing

    lat2 = math.asin(
        math.sin(lat1) * math.cos(dist_rad)
        + math.cos(lat1) * math.sin(dist_rad) * math.cos(brg)
    )

    lon2 = lon1 + math.atan2(
        math.sin(brg) * math.sin(dist_rad) * math.cos(lat1),
        math.cos(dist_rad) - math.sin(lat1) * math.sin(lat2),
    )

    return math.degrees(lat2), math.degrees(lon2)


def _offset_points(lat1, lon1, lat2, lon2, width_nm):
    lat1r = math.radians(lat1)
    lon1r = math.radians(lon1)
    lat2r = math.radians(lat2)
    lon2r = math.radians(lon2)

    dlon = lon2r - lon1r
    y = math.sin(dlon) * math.cos(lat2r)
    x = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon)
    brg = math.atan2(y, x)

    left_brg = brg + math.pi / 2
    right_brg = brg - math.pi / 2

    left1 = _move(lat1, lon1, left_brg, width_nm)
    right1 = _move(lat1, lon1, right_brg, width_nm)
    left2 = _move(lat2, lon2, left_brg, width_nm)
    right2 = _move(lat2, lon2, right_brg, width_nm)

    return left1, right1, left2, right2


def _draw_centerline_stack(namekey: str):
    """Original BlueSky stack-command drawing path."""
    data = _CORRIDORS[namekey]
    pts = data["pts"]
    width = data["width"]

    if not pts or len(pts) < 2:
        _hide(namekey)
        return

    _hide(namekey)

    base = data.get("raw", namekey)
    seg_names = []
    segcount = 0

    for (lat1, lon1), (lat2, lon2) in zip(pts[:-1], pts[1:]):
        segcount += 1

        segname = f"{base}_s{segcount}"
        stack.stack(f"LINE {segname},{lat1:.6f},{lon1:.6f},{lat2:.6f},{lon2:.6f}")
        seg_names.append(segname)

        l1, r1, l2, r2 = _offset_points(lat1, lon1, lat2, lon2, width / 2)
        poly_name = f"{base}_poly{segcount}"
        stack.stack(
            f"POLY {poly_name},"
            f"{l1[0]:.6f},{l1[1]:.6f},"
            f"{l2[0]:.6f},{l2[1]:.6f},"
            f"{r2[0]:.6f},{r2[1]:.6f},"
            f"{r1[0]:.6f},{r1[1]:.6f}"
        )
        seg_names.append(poly_name)

    data["nseg"] = segcount
    data["seg_names"] = seg_names


def _draw_centerline_pygame(namekey: str):
    """Pygame-safe corridor drawing using BlueSky's built-in object lists."""
    data = _CORRIDORS[namekey]
    pts = data["pts"]
    width = data["width"]

    if not pts or len(pts) < 2:
        _hide(namekey)
        return

    _hide(namekey)

    base = data.get("raw", namekey)
    segcount = 0
    seg_names = []

    for (lat1, lon1), (lat2, lon2) in zip(pts[:-1], pts[1:]):
        segcount += 1

        segname = f"{base}_s{segcount}"
        _set_pg_object(
            segname,
            "LINE",
            [lat1, lon1, lat2, lon2],
            data["color"],
        )
        seg_names.append(segname)

        l1, r1, l2, r2 = _offset_points(lat1, lon1, lat2, lon2, width / 2)
        poly_name = f"{base}_poly{segcount}"
        _set_pg_object(
            poly_name,
            "POLY",
            [l1[0], l1[1], l2[0], l2[1], r2[0], r2[1], r1[0], r1[1]],
            data["wcolor"],
        )
        seg_names.append(poly_name)

    data["nseg"] = segcount
    data["seg_names"] = seg_names

def _draw_centerline(namekey: str):
    if _is_pygame_gui():
        _draw_centerline_pygame(namekey)
    else:
        _draw_centerline_stack(namekey)


def _parse_rgb(args):
    if len(args) != 3:
        raise ValueError("Need exactly 3 color values: r g b")
    r, g, b = map(int, args)
    if not all(0 <= c <= 255 for c in (r, g, b)):
        raise ValueError("RGB values must be between 0 and 255")
    return (r, g, b)


@stack.command
def corr(subcmd: str, name: str = "", *args):
    """CORR <subcmd> ...
       NEW <name> [width_nm]
       ADD <name> <wp|lat lon|lat,lon>
       CLEAR <name>
       LIST [name]
       DRAW <name> ON|OFF
       ASSIGN <acid> <name>
       COLOR <name> <r> <g> <b>
       WCOLOR <name> <r> <g> <b>
    """
    sc = subcmd.upper()

    if sc == "NEW":
        if not name:
            return False, "Usage: CORR NEW <name> [width_nm]"
        k = _ensure(name)
        _CORRIDORS[k].setdefault("raw", name)
        if args:
            try:
                _CORRIDORS[k]["width"] = float(args[0])
            except ValueError:
                return False, "width_nm must be numeric"
        _CORRIDORS[k]["pts"].clear()
        _CORRIDORS[k]["nseg"] = 0
        _CORRIDORS[k]["seg_names"] = []
        return True, (
            f"Corridor {_CORRIDORS[k]['raw']} created "
            f"(width {_CORRIDORS[k]['width']} NM, "
            f"line RGB {_CORRIDORS[k]['color']}, "
            f"poly RGB {_CORRIDORS[k]['wcolor']})."
        )

    if sc == "ADD":
        if not name or not args:
            return False, "Usage: CORR ADD <name> <wp|lat lon|lat,lon>"
        k = _ensure(name)
        try:
            lat, lon = _lookup_latlon(*args)
        except Exception as e:
            return False, str(e)
        _CORRIDORS[k]["pts"].append((lat, lon))
        if _CORRIDORS[k]["drawn"]:
            _draw_centerline(k)
        return True, f"Added {lat:.4f},{lon:.4f} to {_CORRIDORS[k]['raw']} (now {len(_CORRIDORS[k]['pts'])} pts)."

    if sc == "CLEAR":
        if not name:
            return False, "Usage: CORR CLEAR <name>"
        k = _key(name)
        if k in _CORRIDORS:
            _CORRIDORS[k]["pts"].clear()
            _hide(k)
            return True, f"{_CORRIDORS[k]['raw']} cleared."
        return False, f"{name} not found."

    if sc == "LIST":
        if name:
            k = _key(name)
            if k not in _CORRIDORS:
                return False, f"{name} not found."
            pts = _CORRIDORS[k]["pts"]
            lines = [
                f"{_CORRIDORS[k]['raw']} "
                f"(width {_CORRIDORS[k]['width']} NM, "
                f"{len(pts)} pts, "
                f"line RGB {_CORRIDORS[k]['color']}, "
                f"poly RGB {_CORRIDORS[k]['wcolor']}):"
            ]
            lines += [f"  {i+1}: {lat:.4f},{lon:.4f}" for i, (lat, lon) in enumerate(pts)]
            return True, "\n".join(lines)

        if not _CORRIDORS:
            return True, "No corridors defined."
        return True, "Corridors: " + ", ".join(sorted(v["raw"] for v in _CORRIDORS.values()))

    if sc == "DRAW":
        if not name or not args:
            return False, "Usage: CORR DRAW <name> ON|OFF"
        k = _key(name)
        if k not in _CORRIDORS:
            return False, f"{name} not found."
        onoff = args[0].upper()
        if onoff == "ON":
            _CORRIDORS[k]["drawn"] = True
            _draw_centerline(k)
            return True, f"{_CORRIDORS[k]['raw']} drawn."
        if onoff == "OFF":
            _CORRIDORS[k]["drawn"] = False
            _hide(k)
            return True, f"{_CORRIDORS[k]['raw']} hidden."
        return False, "Use ON or OFF."

    if sc == "COLOR":
        if not name:
            return False, "Usage: CORR COLOR <name> <r> <g> <b>"
        k = _key(name)
        if k not in _CORRIDORS:
            return False, f"{name} not found."
        try:
            _CORRIDORS[k]["color"] = _parse_rgb(args)
        except Exception as e:
            return False, str(e)
        if _CORRIDORS[k]["drawn"]:
            _draw_centerline(k)
        return True, f"{_CORRIDORS[k]['raw']} line color set to {_CORRIDORS[k]['color']}."

    if sc == "WCOLOR":
        if not name:
            return False, "Usage: CORR WCOLOR <name> <r> <g> <b>"
        k = _key(name)
        if k not in _CORRIDORS:
            return False, f"{name} not found."
        try:
            _CORRIDORS[k]["wcolor"] = _parse_rgb(args)
        except Exception as e:
            return False, str(e)
        if _CORRIDORS[k]["drawn"]:
            _draw_centerline(k)
        return True, f"{_CORRIDORS[k]['raw']} polygon color set to {_CORRIDORS[k]['wcolor']}."

    if sc == "ASSIGN":
        if not name or not args:
            return False, "Usage: CORR ASSIGN <acid> <name>"
        acid = name
        k = _key(args[0])
        if k not in _CORRIDORS or not _CORRIDORS[k]["pts"]:
            return False, f"{args[0]} not found or empty."
        stack.stack(f"DELWPT {acid} ALL")
        stack.stack(f"ORIG {acid} HERE")
        for (lat, lon) in _CORRIDORS[k]["pts"]:
            stack.stack(f"ADDWPT {acid} {lat:.6f},{lon:.6f}")
        stack.stack(f"LNAV {acid} ON")
        return True, f"Assigned {_CORRIDORS[k]['raw']} to {acid} (LNAV ON)."

    return False, "Unknown subcommand. See: CORR NEW|ADD|CLEAR|LIST|DRAW|ASSIGN|COLOR|WCOLOR"
