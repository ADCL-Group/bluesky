"""BlueSky pygame overlay drawing plugin without flicker.

Uses BlueSky pygame's built-in background object lists instead of drawing
directly to the display surface.

Commands:
  OVL LINE <name> <lat1> <lon1> <lat2> <lon2> [r g b]
  OVL CIRCLE <name> <lat> <lon> <radius_nm> [r g b]
  OVL RECT <name> <lat1> <lon1> <lat2> <lon2> [r g b]
  OVL POLY <name> <lat1> <lon1> <lat2> <lon2> ... [r g b]
  OVL DEL <name>
  OVL CLEAR
  OVL COLOR <name> <r> <g> <b>
  OVL LIST
"""

from bluesky import stack
import bluesky as bs

_SHAPES = {}


def init_plugin():
    return {
        "plugin_name": "pygameoverlay",
        "plugin_type": "sim",
        "version": "0.2",
    }


def _screen_ready():
    """Return True only when the pygame screen object is ready for object lists."""
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
    """Tell BlueSky to rebuild the radar background next frame."""
    if _screen_ready():
        bs.scr.redrawradbg = True


def _find_obj_index(name):
    """Find an existing BlueSky object index by name."""
    if not _screen_ready():
        return -1

    try:
        return bs.scr.objname.index(name)
    except ValueError:
        return -1


def _sync_shape_to_screen(name):
    """Write one shape from _SHAPES into BlueSky's pygame object lists."""
    if not _screen_ready():
        return False, "Screen not ready."

    shp = _SHAPES.get(name)
    if shp is None:
        return False, f"{name} not found."

    objtype = shp["type"]
    objdata = shp["data"]
    objcolor = shp["color"]

    idx = _find_obj_index(name)

    if idx >= 0:
        bs.scr.objtype[idx] = objtype
        bs.scr.objdata[idx] = objdata
        bs.scr.objcolor[idx] = objcolor
        bs.scr.objname[idx] = name
    else:
        bs.scr.objtype.append(objtype)
        bs.scr.objdata.append(objdata)
        bs.scr.objcolor.append(objcolor)
        bs.scr.objname.append(name)

    _mark_dirty()
    return True, None


def _remove_shape_from_screen(name):
    """Remove one named object from BlueSky's pygame object lists."""
    if not _screen_ready():
        return False, "Screen not ready."

    idx = _find_obj_index(name)
    if idx < 0:
        return False, f"{name} not found."

    del bs.scr.objtype[idx]
    del bs.scr.objdata[idx]
    del bs.scr.objcolor[idx]
    del bs.scr.objname[idx]

    _mark_dirty()
    return True, None


def _clear_all_from_screen():
    """Remove all plugin-owned objects from BlueSky's pygame object lists."""
    if not _screen_ready():
        return False, "Screen not ready."

    plugin_names = set(_SHAPES.keys())
    keep = [i for i, nm in enumerate(bs.scr.objname) if nm not in plugin_names]

    bs.scr.objtype = [bs.scr.objtype[i] for i in keep]
    bs.scr.objcolor = [bs.scr.objcolor[i] for i in keep]
    bs.scr.objdata = [bs.scr.objdata[i] for i in keep]
    bs.scr.objname = [bs.scr.objname[i] for i in keep]

    _mark_dirty()
    return True, None


def _parse_color(args, default=(0, 255, 255)):
    if len(args) >= 3:
        try:
            r = int(args[-3])
            g = int(args[-2])
            b = int(args[-1])
            if 0 <= r <= 255 and 0 <= g <= 255 and 0 <= b <= 255:
                return (r, g, b), args[:-3]
        except Exception:
            pass
    return default, args


def _make_line_data(lat1, lon1, lat2, lon2):
    return [lat1, lon1, lat2, lon2]


def _make_circle_data(lat, lon, radius_nm):
    return [lat, lon, radius_nm]


def _make_rect_data(lat1, lon1, lat2, lon2):
    # BlueSky pygame uses BOX, not RECT, with [lat0, lon0, lat1, lon1]
    return [lat1, lon1, lat2, lon2]


def _make_poly_data(vals):
    return vals[:]  # flat [lat1, lon1, lat2, lon2, ...]


@stack.command
def ovl(subcmd: str = "", name: str = "", *args):
    global _SHAPES

    sc = subcmd.upper()

    if sc == "LINE":
        if len(args) < 4:
            return False, "Usage: OVL LINE <name> <lat1> <lon1> <lat2> <lon2> [r g b]"
        color, rest = _parse_color(args)
        if len(rest) != 4:
            return False, "Need 4 coordinates for LINE"
        lat1, lon1, lat2, lon2 = map(float, rest)
        _SHAPES[name] = {
            "type": "LINE",
            "data": _make_line_data(lat1, lon1, lat2, lon2),
            "color": color,
        }
        ok, msg = _sync_shape_to_screen(name)
        return (ok, f"LINE {name} added." if ok else msg)

    if sc == "CIRCLE":
        if len(args) < 3:
            return False, "Usage: OVL CIRCLE <name> <lat> <lon> <radius_nm> [r g b]"
        color, rest = _parse_color(args)
        if len(rest) != 3:
            return False, "Need lat lon radius_nm for CIRCLE"
        lat, lon, radius_nm = float(rest[0]), float(rest[1]), float(rest[2])
        _SHAPES[name] = {
            "type": "CIRCLE",
            "data": _make_circle_data(lat, lon, radius_nm),
            "color": color,
        }
        ok, msg = _sync_shape_to_screen(name)
        return (ok, f"CIRCLE {name} added." if ok else msg)

    if sc == "RECT":
        if len(args) < 4:
            return False, "Usage: OVL RECT <name> <lat1> <lon1> <lat2> <lon2> [r g b]"
        color, rest = _parse_color(args)
        if len(rest) != 4:
            return False, "Need 4 coordinates for RECT"
        lat1, lon1, lat2, lon2 = map(float, rest)
        _SHAPES[name] = {
            "type": "BOX",
            "data": _make_rect_data(lat1, lon1, lat2, lon2),
            "color": color,
        }
        ok, msg = _sync_shape_to_screen(name)
        return (ok, f"RECT {name} added." if ok else msg)

    if sc == "POLY":
        color, rest = _parse_color(args)
        if len(rest) < 4 or len(rest) % 2 != 0:
            return False, "Usage: OVL POLY <name> <lat1> <lon1> <lat2> <lon2> ... [r g b]"
        vals = list(map(float, rest))
        _SHAPES[name] = {
            "type": "POLY",
            "data": _make_poly_data(vals),
            "color": color,
        }
        ok, msg = _sync_shape_to_screen(name)
        return (ok, f"POLY {name} added." if ok else msg)

    if sc == "COLOR":
        if len(args) != 3:
            return False, "Usage: OVL COLOR <name> <r> <g> <b>"
        if name not in _SHAPES:
            return False, f"{name} not found."
        _SHAPES[name]["color"] = (int(args[0]), int(args[1]), int(args[2]))
        ok, msg = _sync_shape_to_screen(name)
        return (ok, f"{name} color updated." if ok else msg)

    if sc == "DEL":
        if name not in _SHAPES:
            return False, f"{name} not found."
        _SHAPES.pop(name, None)
        ok, msg = _remove_shape_from_screen(name)
        # if it was already absent on screen, still consider it deleted from plugin storage
        return True, f"{name} deleted."

    if sc == "CLEAR":
        _clear_all_from_screen()
        _SHAPES.clear()
        return True, "All overlays cleared."

    if sc == "LIST":
        if not _SHAPES:
            return True, "No overlays."
        return True, "Overlays: " + ", ".join(sorted(_SHAPES.keys()))

    return False, "Use OVL LINE|CIRCLE|RECT|POLY|COLOR|DEL|CLEAR|LIST"