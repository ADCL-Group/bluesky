''' State-based conflict detection. '''
import numpy as np
from bluesky import stack
from bluesky.tools import geo
from bluesky.tools.aero import nm
from bluesky.traffic.asas import ConflictDetection


class VtolDetect(ConflictDetection):
    def detect(self, ownship, intruder, rpz, hpz, dtlookahead):
        '''Conflict detection between VTOL and all other aircraft only.'''

        # ----------------------------------------------------------------------
        # 1. Find VTOL index by callsign
        # ----------------------------------------------------------------------
        vtol_callsign = "VTOL1"          # <--- change this to your VTOL callsign
        ids = list(ownship.id)
        ntraf = ownship.ntraf

        if vtol_callsign not in ids:
            # No VTOL present: return "no conflicts" with correct shapes
            inconf = np.zeros(ntraf, dtype=bool)
            tcpamax = np.zeros(ntraf)
            empty = np.array([])
            return [], [], inconf, tcpamax, empty, empty, empty, empty, empty

        vtol_idx = ids.index(vtol_callsign)

        # ----------------------------------------------------------------------
        # 2. Original StateBased conflict detection (unchanged)
        # ----------------------------------------------------------------------
        # Identity matrix of order ntraf: avoid ownship-ownship detected conflicts
        I = np.eye(ownship.ntraf)

        # Horizontal conflict ---------------------------------------------------
        qdr, dist = geo.kwikqdrdist_matrix(
            np.asmatrix(ownship.lat), np.asmatrix(ownship.lon),
            np.asmatrix(intruder.lat), np.asmatrix(intruder.lon)
        )

        qdr = np.asarray(qdr)
        dist = np.asarray(dist) * nm + 1e9 * I

        qdrrad = np.radians(qdr)
        dx = dist * np.sin(qdrrad)  # j relative to i (east)
        dy = dist * np.cos(qdrrad)  # j relative to i (north)

        # Ownship track angle and speed
        owntrkrad = np.radians(ownship.trk)
        ownu = ownship.gs * np.sin(owntrkrad).reshape((1, ownship.ntraf))
        ownv = ownship.gs * np.cos(owntrkrad).reshape((1, ownship.ntraf))

        # Intruder track angle and speed
        inttrkrad = np.radians(intruder.trk)
        intu = intruder.gs * np.sin(inttrkrad).reshape((1, ownship.ntraf))
        intv = intruder.gs * np.cos(inttrkrad).reshape((1, ownship.ntraf))

        du = ownu - intu.T  # eastern relative speed
        dv = ownv - intv.T  # northern relative speed

        dv2 = du * du + dv * dv
        dv2 = np.where(np.abs(dv2) < 1e-6, 1e-6, dv2)
        vrel = np.sqrt(dv2)

        tcpa = -(du * dx + dv * dy) / dv2 + 1e9 * I

        dcpa2 = np.abs(dist * dist - tcpa * tcpa * dv2)

        # RPZ can differ per aircraft, get the largest per pair
        rpz = np.asarray(np.maximum(np.asmatrix(rpz), np.asmatrix(rpz).T))
        R2 = rpz * rpz
        swhorconf = dcpa2 < R2

        dxinhor = np.sqrt(np.maximum(0., R2 - dcpa2))
        dtinhor = dxinhor / vrel

        tinhor = np.where(swhorconf, tcpa - dtinhor, 1e8)
        touthor = np.where(swhorconf, tcpa + dtinhor, -1e8)

        # Vertical conflict -----------------------------------------------------
        dalt = ownship.alt.reshape((1, ownship.ntraf)) - \
            intruder.alt.reshape((1, ownship.ntraf)).T + 1e9 * I

        dvs = ownship.vs.reshape(1, ownship.ntraf) - \
            intruder.vs.reshape(1, ownship.ntraf).T
        dvs = np.where(np.abs(dvs) < 1e-6, 1e-6, dvs)

        hpz = np.asarray(np.maximum(np.asmatrix(hpz), np.asmatrix(hpz).T))
        tcrosshi = (dalt + hpz) / -dvs
        tcrosslo = (dalt - hpz) / -dvs
        tinver = np.minimum(tcrosshi, tcrosslo)
        toutver = np.maximum(tcrosshi, tcrosslo)

        # Combine vertical and horizontal conflict ------------------------------
        tinconf = np.maximum(tinver, tinhor)
        toutconf = np.minimum(toutver, touthor)

        swconfl = np.array(
            swhorconf *
            (tinconf <= toutconf) *
            (toutconf > 0.0) *
            np.asarray(tinconf < np.asmatrix(dtlookahead).T) *
            (1.0 - I),
            dtype=bool
        )

        # ----------------------------------------------------------------------
        # 3. Restrict conflicts to *only* those involving the VTOL
        # ----------------------------------------------------------------------
        # Rebuild los matrix the same way the original code does
        swlos = (dist < rpz) * (np.abs(dalt) < hpz)

        # Mask: keep only pairs where i == VTOL or j == VTOL
        vmask_conf = np.zeros_like(swconfl, dtype=bool)
        vmask_conf[vtol_idx, :] |= swconfl[vtol_idx, :]
        vmask_conf[:, vtol_idx] |= swconfl[:, vtol_idx]
        swconfl = vmask_conf

        vmask_los = np.zeros_like(swlos, dtype=bool)
        vmask_los[vtol_idx, :] |= swlos[vtol_idx, :]
        vmask_los[:, vtol_idx] |= swlos[:, vtol_idx]
        swlos = vmask_los

        # Ownship conflict flag and max tCPA (after VTOL masking)
        inconf = np.any(swconfl, 1)
        tcpamax = np.max(tcpa * swconfl, 1)

        # ----------------------------------------------------------------------
        # 4. Build conflict / LOS pair lists (now VTOL-only)
        # ----------------------------------------------------------------------
        confpairs = [(ownship.id[i], ownship.id[j])
                     for i, j in zip(*np.where(swconfl))]
        lospairs = [(ownship.id[i], ownship.id[j])
                    for i, j in zip(*np.where(swlos))]

        return confpairs, lospairs, inconf, tcpamax, \
            qdr[swconfl], dist[swconfl], np.sqrt(dcpa2[swconfl]), \
            tcpa[swconfl], tinconf[swconfl]



try:
    from bluesky.traffic.asas import cstatebased


    class CStateBased(StateBased):
        def __init__(self):
            super().__init__()
            self.detect = cstatebased.detect

except ImportError:
    pass
