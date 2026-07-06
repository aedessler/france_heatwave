#!/usr/bin/env python3
"""
Read the EURO-CORDEX EUR-11 (~12.5 km) regional-climate ensemble of daily maximum
temperature (TMax / `tasmax`) over France under **RCP4.5, 2006-2080**, and write one
NetCDF per GCM x RCM combination (ensemble member).

This is the regional-resolution companion to the NEX-GDDP-CMIP6 France TMax job
(`../NEX-GDDP/france_tmax_cmip6.py`). It reuses that script's orchestration --
per-member `ProcessPoolExecutor`, a resumable atomic cache, retry/backoff, and the
`--dry-run`/`--list-models`/`--overwrite` flags -- but swaps the data layer.

Dataset
-------
EURO-CORDEX regional downscaling, served from the **Copernicus Climate Data Store**
(CDS), dataset `projections-cordex-domains-single-levels`:
    https://cds.climate.copernicus.eu/datasets/projections-cordex-domains-single-levels

  * Retrieved with the **`cdsapi`** client, which reads credentials from
    `~/.cdsapirc` (a free CDS account + API key -- see the README). This is NOT an
    anonymous public bucket like NEX-GDDP's S3; it requires a credential.
  * `domain="europe"` (EUR-11, 0.11 deg ~= 12.5 km), `temporal_resolution="daily_mean"`,
    `variable="maximum_2m_temperature_in_the_last_24_hours"` (= `tasmax`, Kelvin).
  * **Only `experiment="rcp_4_5"`** -- no historical, no splice (unlike NEX-GDDP).
  * CDS delivers the data in **fixed 5-year blocks** (2006-2010, 2011-2015, ...,
    2076-2080) as a **zip of rotated-pole NetCDF** files. The worker unzips each
    block, clips it to France on the native rotated grid, caches the block, and
    deletes the raw download.
  * Files carry dims `rlat`,`rlon`, 2-D coords `lat(rlat,rlon)`/`lon(rlat,rlon)`, and
    a `rotated_pole` grid-mapping variable. **Output stays on this native rotated
    grid (not regridded)** -- it is clipped to a rotated bounding box covering France.

Ensemble = valid GCM x RCM pairs
--------------------------------
EUR-11 has ~6-8 driving GCMs x ~8-13 RCMs, but only a subset of pairs were actually
run for daily `tasmax` under RCP4.5. We start from a curated list (`DEFAULT_PAIRS`)
and **skip invalid combos gracefully**: if CDS returns "no data" for a member's first
5-year block, that member is logged and skipped (no output file). Use `--list-models`
to print the resolved matrix, and `--pairs` / `--gcms` / `--rcms` to override it.

NOTE on CDS controlled-vocabulary tokens: the `gcm_model` / `rcm_model` strings below
must match the CDS form's exact values. They are best-effort and flagged where the
plan and the CDS vocabulary may differ (e.g. `ipsl_ipsl_cm5a_mr`). Confirm against the
dataset's "Download" form; wrong tokens simply show up as skipped members in the log.

Parallelism
-----------
Each member (GCM x RCM) is an independent unit of work -> parallelism is across
members via `--workers N`. **CDS is queue/throttle-bound, not CPU-bound** (only a few
concurrent requests per user; each waits in CDS's queue), so keep `--workers` modest
(~4-8) -- more just pile up in the queue. `--workers 1` is serial (easiest to debug).

    # See the resolved member matrix:
    python france_tmax_cordex.py --list-models

    # Show the request plan (5-year blocks per member) without calling CDS:
    python france_tmax_cordex.py --dry-run --pairs mpi_m_mpi_esm_lr:smhi_rca4

    # Real local test: one combo, two 5-year blocks (2006-2015):
    python france_tmax_cordex.py --pairs mpi_m_mpi_esm_lr:smhi_rca4 \
        --start-year 2006 --end-year 2015 --outdir ./out_cordex_test

    # Full RCP4.5 2006-2080 ensemble, 6 members at a time (e.g. on GCP):
    python france_tmax_cordex.py --workers 6 --outdir ./tmax_france_cordex

Requirements
------------
    pip install cdsapi "xarray>=2023" netCDF4 cftime numpy pandas
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
import tempfile
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import xarray as xr

# --------------------------------------------------------------------------- #
# Dataset constants (CDS)
# --------------------------------------------------------------------------- #
DATASET = "projections-cordex-domains-single-levels"
DOMAIN = "europe"                 # EUR-11
HRES = "0_11_degree_x_0_11_degree"
TRES = "daily_mean"
VARIABLE_CDS = "maximum_2m_temperature_in_the_last_24_hours"   # = tasmax (K)
VARNAME = "tasmax"                # variable name inside the NetCDF
EXPERIMENT = "rcp_4_5"            # ONLY rcp4.5 (no historical, no splice)
ENSEMBLE = "r1i1p1"

# CDS delivers RCP runs in fixed 5-year blocks aligned to this origin/width.
BLOCK_ORIGIN = 2006
BLOCK_WIDTH = 5

# Metropolitan France bounding box (incl. Corsica), degrees east in -180..180.
FRANCE = dict(lon_w=-5.5, lon_e=9.8, lat_s=41.0, lat_n=51.5)

# Driving GCMs and RCMs, as CDS controlled-vocabulary tokens. Confirm against the CDS
# "Download" form -- wrong tokens just yield skipped members.
GCMS = [
    "cnrm_cerfacs_cm5",      # CNRM-CM5
    "ichec_ec_earth",        # EC-EARTH
    "ipsl_ipsl_cm5a_mr",     # IPSL-CM5A-MR (CDS token; plan wrote 'ipsl_cm5a_mr')
    "mohc_hadgem2_es",       # HadGEM2-ES
    "mpi_m_mpi_esm_lr",      # MPI-ESM-LR
    "ncc_noresm1_m",         # NorESM1-M
]
RCMS = [
    "clmcom_cclm4_8_17",     # CCLM4-8-17
    "knmi_racmo22e",         # RACMO22E
    "smhi_rca4",             # RCA4
    "dmi_hirham5",           # HIRHAM5
    "gerics_remo2015",       # REMO2015
    "cnrm_aladin63",         # ALADIN63
    "mohc_hadrem3_ga7_05",   # HadREM3-GA7-05
    "ictp_regcm4_6",         # RegCM4-6
]

# Curated default ensemble of GCM x RCM pairs known/expected to exist on EUR-11 for
# daily tasmax under RCP4.5. Invalid ones are skipped gracefully at runtime. Override
# with --pairs / --gcms / --rcms.
DEFAULT_PAIRS = [
    ("cnrm_cerfacs_cm5",  "smhi_rca4"),
    ("cnrm_cerfacs_cm5",  "clmcom_cclm4_8_17"),
    ("cnrm_cerfacs_cm5",  "knmi_racmo22e"),
    ("cnrm_cerfacs_cm5",  "cnrm_aladin63"),
    ("ichec_ec_earth",    "smhi_rca4"),
    ("ichec_ec_earth",    "clmcom_cclm4_8_17"),
    ("ichec_ec_earth",    "knmi_racmo22e"),
    ("ichec_ec_earth",    "dmi_hirham5"),
    ("ichec_ec_earth",    "gerics_remo2015"),
    ("ichec_ec_earth",    "cnrm_aladin63"),
    ("ipsl_ipsl_cm5a_mr", "smhi_rca4"),
    ("ipsl_ipsl_cm5a_mr", "gerics_remo2015"),
    ("ipsl_ipsl_cm5a_mr", "dmi_hirham5"),
    ("mohc_hadgem2_es",   "smhi_rca4"),
    ("mohc_hadgem2_es",   "clmcom_cclm4_8_17"),
    ("mohc_hadgem2_es",   "knmi_racmo22e"),
    ("mohc_hadgem2_es",   "dmi_hirham5"),
    ("mohc_hadgem2_es",   "cnrm_aladin63"),
    ("mohc_hadgem2_es",   "ictp_regcm4_6"),
    ("mpi_m_mpi_esm_lr",  "smhi_rca4"),
    ("mpi_m_mpi_esm_lr",  "clmcom_cclm4_8_17"),
    ("mpi_m_mpi_esm_lr",  "gerics_remo2015"),
    ("mpi_m_mpi_esm_lr",  "cnrm_aladin63"),
    ("ncc_noresm1_m",     "smhi_rca4"),
    ("ncc_noresm1_m",     "gerics_remo2015"),
    ("ncc_noresm1_m",     "dmi_hirham5"),
]

log = logging.getLogger("cordex")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
class NoDataError(Exception):
    """CDS reported no data matching the request (an invalid/absent combo or block)."""


def _is_no_data(exc: Exception) -> bool:
    """Heuristic: does this CDS exception mean 'no data for this request' (so it is
    permanent and should be skipped) rather than a transient network/queue error?"""
    msg = str(exc).lower()
    needles = ("no data", "not found", "no matching", "does not match",
               "invalid request", "404", "client has not agree", "no result")
    return any(n in msg for n in needles)


def _retry(fn, *, tries=4, delay=5.0, what="cds op"):
    """Exponential-backoff retry for transient CDS/network errors. A 'no data'
    response is permanent -> re-raised immediately as NoDataError (not retried)."""
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - classify transient vs permanent
            if _is_no_data(exc):
                raise NoDataError(str(exc)) from exc
            if attempt == tries:
                raise
            log.warning("%s failed (attempt %d/%d): %s -- retrying in %.1fs",
                        what, attempt, tries, exc, delay)
            time.sleep(delay)
            delay *= 2


def native_blocks(start: int, end: int) -> list[tuple[int, int]]:
    """The CDS native 5-year blocks (aligned to 2006) that overlap [start, end].

    e.g. start=2006,end=2080 -> [(2006,2010),(2011,2015),...,(2076,2080)] (15 blocks).
    A non-boundary start (e.g. 2008) still requests whole native blocks; the final
    series is time-sliced back to [start, end] after concatenation."""
    first = BLOCK_ORIGIN + ((start - BLOCK_ORIGIN) // BLOCK_WIDTH) * BLOCK_WIDTH
    blocks = []
    s = first
    while s <= end:
        e = s + BLOCK_WIDTH - 1
        if e >= start:
            blocks.append((s, e))
        s += BLOCK_WIDTH
    return blocks


# --------------------------------------------------------------------------- #
# France clip on the rotated-pole grid (no regridding)
# --------------------------------------------------------------------------- #
def subset_france(ds: xr.Dataset) -> xr.Dataset:
    """Clip a EUR-11 rotated-pole dataset to a rotated bounding box covering France.

    CORDEX files have dims rlat/rlon with 2-D true coordinates lat(rlat,rlon) and
    lon(rlat,rlon). We mask on the true lat/lon, then take the smallest rotated
    bounding box (rlat/rlon index slice) that contains the masked cells. This keeps a
    regular, efficient sub-array on the *native* grid (2-D lat/lon + rotated_pole
    retained, nothing regridded)."""
    lon = ((ds["lon"] + 180) % 360) - 180
    m = ((ds["lat"] >= FRANCE["lat_s"]) & (ds["lat"] <= FRANCE["lat_n"])
         & (lon >= FRANCE["lon_w"]) & (lon <= FRANCE["lon_e"]))
    # Horizontal dim names vary by RCM: most use rlat/rlon, but some (e.g. CNRM
    # ALADIN) use y/x. Derive them from the 2-D lat coordinate so the clip is
    # naming-agnostic.
    ydim, xdim = ds["lat"].dims
    ys, xs = np.where(m.values)
    if ys.size == 0:
        raise ValueError("France window selected no cells -- check lat/lon coords")
    sub = ds.isel({ydim: slice(int(ys.min()), int(ys.max()) + 1),
                   xdim: slice(int(xs.min()), int(xs.max()) + 1)})
    # Keep tasmax plus its grid-mapping variable (name varies: rotated_pole,
    # Lambert_Conformal, ...); drop bounds and any other payload.
    keep = [VARNAME]
    gm = ds[VARNAME].attrs.get("grid_mapping")
    if gm and gm in sub.variables:
        keep.append(gm)
    return sub[keep]


def _open_block_ncs(nc_paths: list[str]) -> xr.Dataset:
    """Open one or more NetCDFs extracted from a CDS block zip, clip each to France,
    and concatenate along time (a block is usually a single file)."""
    subs = []
    for p in sorted(nc_paths):
        d = xr.open_dataset(p, use_cftime=True)
        try:
            subs.append(subset_france(d).load())
        finally:
            d.close()
    if len(subs) == 1:
        return subs[0]
    # data_vars="minimal" keeps rotated_pole/lat/lon time-independent (only tasmax,
    # which already has a time dim, is concatenated); override skips redundant checks.
    return xr.concat(subs, dim="time", data_vars="minimal",
                     coords="minimal", compat="override").sortby("time")


# --------------------------------------------------------------------------- #
# CDS retrieval of one 5-year block
# --------------------------------------------------------------------------- #
def retrieve_block(client, gcm: str, rcm: str, s: int, e: int, tmpdir: str) -> xr.Dataset:
    """Download one (gcm, rcm, s-e) 5-year block zip from CDS, unzip, clip to France,
    and return the in-memory France subset. Raises NoDataError for an absent combo."""
    req = {
        "domain": DOMAIN,
        "experiment": EXPERIMENT,
        "horizontal_resolution": HRES,
        "temporal_resolution": TRES,
        "variable": VARIABLE_CDS,
        "gcm_model": gcm,
        "rcm_model": rcm,
        "ensemble_member": ENSEMBLE,
        "start_year": [str(s)],
        "end_year": [str(e)],
        "data_format": "netcdf",
        "download_format": "zip",
    }
    zip_path = os.path.join(tmpdir, f"{gcm}__{rcm}_{s}_{e}.zip")
    _retry(lambda: client.retrieve(DATASET, req, zip_path),
           what=f"retrieve {gcm}x{rcm} {s}-{e}")
    ex_dir = os.path.join(tmpdir, f"ex_{gcm}__{rcm}_{s}_{e}")
    os.makedirs(ex_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(ex_dir)
    ncs = glob.glob(os.path.join(ex_dir, "*.nc"))
    if not ncs:
        raise NoDataError(f"zip for {gcm}x{rcm} {s}-{e} contained no .nc")
    try:
        return _open_block_ncs(ncs)
    finally:
        try:
            os.remove(zip_path)
        except OSError:
            pass


# --------------------------------------------------------------------------- #
# Per-member worker
# --------------------------------------------------------------------------- #
def process_member(gcm: str, rcm: str, start_year: int, end_year: int,
                   outdir: str, overwrite: bool, dry_run: bool) -> dict:
    """Build the RCP4.5 daily-TMax France series for one GCM x RCM member and write
    <outdir>/tasmax_france_<gcm>_<rcm>_rcp45_<start>_<end>.nc.

    Returns a status dict (also used to build the member index CSV)."""
    member = f"{gcm}__{rcm}"
    out_path = os.path.join(
        outdir, f"tasmax_france_{gcm}_{rcm}_rcp45_{start_year}_{end_year}.nc")
    base = dict(member=member, gcm=gcm, rcm=rcm, out_file=os.path.basename(out_path))

    if os.path.exists(out_path) and not overwrite and not dry_run:
        return {**base, "status": "exists", "msg": f"exists, skipped ({out_path})"}

    blocks = native_blocks(start_year, end_year)
    if dry_run:
        spans = ", ".join(f"{s}-{e}" for s, e in blocks)
        return {**base, "status": "dry-run", "n_blocks": len(blocks),
                "msg": f"{len(blocks)} blocks [{spans}]"}

    import cdsapi  # lazy: --list-models/--dry-run work without cdsapi installed
    client = cdsapi.Client()

    blocks_dir = os.path.join(outdir, "blocks", member)
    os.makedirs(blocks_dir, exist_ok=True)
    for stale in glob.glob(os.path.join(blocks_dir, "*.tmp")):
        try:
            os.remove(stale)  # leftover half-write from an interrupted run
        except OSError:
            pass

    def block_path(s: int, e: int) -> str:
        return os.path.join(blocks_dir, f"tasmax_france_{member}_{s}_{e}.nc")

    enc = {VARNAME: {"zlib": True, "complevel": 4, "dtype": "float32"}}
    have = 0   # blocks already cached or freshly downloaded
    missing: list[str] = []
    t0 = time.time()

    with tempfile.TemporaryDirectory(prefix=f"cordex_{member}_") as tmp:
        for (s, e) in blocks:
            bp = block_path(s, e)
            if os.path.exists(bp) and not overwrite:
                have += 1
                continue
            try:
                sub = retrieve_block(client, gcm, rcm, s, e, tmp)
            except NoDataError as exc:
                # If nothing for this member exists yet, treat the combo as invalid
                # and skip it entirely (don't hammer CDS with 14 more dead requests).
                if have == 0:
                    log.warning("[%s] no data for first block %d-%d -- "
                                "invalid combo, skipping member (%s)", member, s, e, exc)
                    return {**base, "status": "invalid",
                            "msg": f"no data (combo absent on CDS): {exc}"}
                log.warning("[%s] no data for block %d-%d -- gap in series", member, s, e)
                missing.append(f"{s}-{e}")
                continue
            sub.to_netcdf(bp + ".tmp", encoding=enc)
            os.replace(bp + ".tmp", bp)  # atomic -> never a partial "done" block
            sub.close()
            have += 1
            log.info("[%s] cached block %d-%d (%d/%d)", member, s, e, have, len(blocks))

    present = [(s, e) for (s, e) in blocks if os.path.exists(block_path(s, e))]
    if not present:
        return {**base, "status": "invalid", "msg": "no blocks retrieved"}

    # Concatenate cached blocks into the continuous series, then clip to [start, end].
    pieces = []
    for (s, e) in present:
        d = xr.open_dataset(block_path(s, e), use_cftime=True)
        pieces.append(d.load())
        d.close()
    combined = xr.concat(pieces, dim="time", data_vars="minimal",
                         coords="minimal", compat="override").sortby("time")
    combined = combined.sel(time=slice(str(start_year), str(end_year)))

    # Horizontal dims are naming-agnostic (rlat/rlon, or y/x for some RCMs).
    spatial = [d for d in combined[VARNAME].dims if d != "time"]
    nlat = combined.sizes.get(spatial[0]) if len(spatial) >= 1 else None
    nlon = combined.sizes.get(spatial[1]) if len(spatial) >= 2 else None

    combined[VARNAME].attrs.update(
        units="K", long_name="Daily Maximum Near-Surface Air Temperature")
    combined.attrs.update(
        title=f"EURO-CORDEX EUR-11 daily TMax over France, {gcm} x {rcm}, RCP4.5",
        domain="EUR-11 (~0.11 deg / ~12.5 km, rotated pole)",
        gcm_model=gcm, rcm_model=rcm, ensemble_member=ENSEMBLE,
        experiment="rcp45",
        source="Copernicus CDS dataset projections-cordex-domains-single-levels",
        region=("France clip lon[%(lon_w)s,%(lon_e)s] lat[%(lat_s)s,%(lat_n)s] "
                "(incl. Corsica)" % FRANCE),
        grid_note="Native model grid, NOT regridded; clipped to the bounding box of "
                  "grid cells covering France. 2-D lat/lon + grid_mapping retained.",
        missing_blocks=("none" if not missing else ",".join(missing)),
        history=f"Retrieved RCP4.5 daily tasmax from CDS, unzipped, clipped to France "
                f"on {time.strftime('%Y-%m-%d')} by france_tmax_cordex.py",
    )

    # Atomic write: a worker killed mid-write (e.g. OOM) must never leave a
    # truncated file at out_path that a later run would treat as "exists, skip".
    combined.to_netcdf(out_path + ".tmp", encoding=enc)
    os.replace(out_path + ".tmp", out_path)
    dt = time.time() - t0
    nt = combined.sizes["time"]
    res = {**base, "status": "ok", "n_days": nt, "nlat": nlat, "nlon": nlon,
           "n_blocks": len(present),
           "msg": (f"OK -> {out_path} ({nt} days, {nlat}x{nlon} grid, "
                   f"{len(present)} blocks, {dt/60:.1f} min"
                   + (f", missing {missing}" if missing else "") + ")")}
    combined.close()
    return res


# --------------------------------------------------------------------------- #
# CLI / orchestration
# --------------------------------------------------------------------------- #
def resolve_pairs(args) -> list[tuple[str, str]]:
    """Resolve the GCM x RCM member matrix from --pairs / --gcms / --rcms / default."""
    if args.pairs:
        pairs = []
        for tok in args.pairs.split(","):
            tok = tok.strip()
            if not tok:
                continue
            if ":" not in tok:
                raise SystemExit(f"--pairs entry must be gcm:rcm, got '{tok}'")
            g, r = tok.split(":", 1)
            pairs.append((g.strip(), r.strip()))
        return pairs
    if args.gcms or args.rcms:
        gcms = [g.strip() for g in (args.gcms or ",".join(GCMS)).split(",") if g.strip()]
        rcms = [r.strip() for r in (args.rcms or ",".join(RCMS)).split(",") if r.strip()]
        return [(g, r) for g in gcms for r in rcms]
    return list(DEFAULT_PAIRS)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Read the EURO-CORDEX EUR-11 ensemble of France daily TMax "
                    "(tasmax) under RCP4.5, 2006-2080, via the Copernicus CDS.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--start-year", type=int, default=2006,
                   help="First year (RCP4.5 starts 2006; no historical).")
    p.add_argument("--end-year", type=int, default=2080)
    p.add_argument("--pairs", default="",
                   help="Explicit members as comma-separated gcm:rcm tokens "
                        "(overrides --gcms/--rcms and the default matrix).")
    p.add_argument("--gcms", default="",
                   help="Comma-separated GCM tokens to cross-product with --rcms.")
    p.add_argument("--rcms", default="",
                   help="Comma-separated RCM tokens to cross-product with --gcms.")
    p.add_argument("--outdir", default="./tmax_france_cordex")
    p.add_argument("--workers", type=int, default=1,
                   help="Members processed simultaneously. CDS is queue-bound, so "
                        "keep this modest (~4-8); more just wait in CDS's queue.")
    p.add_argument("--executor", choices=["process", "thread"], default="process",
                   help="Parallel backend when --workers > 1 (CDS work is IO-bound, "
                        "so 'thread' is also fine and lighter).")
    p.add_argument("--overwrite", action="store_true",
                   help="Re-create output files (and re-download blocks) that exist.")
    p.add_argument("--dry-run", action="store_true",
                   help="Report the per-member 5-year block plan without calling CDS.")
    p.add_argument("--list-models", action="store_true",
                   help="Print the resolved GCM x RCM member matrix and exit.")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    pairs = resolve_pairs(args)

    if args.list_models:
        print(f"# {len(pairs)} GCM x RCM members (RCP4.5, EUR-11):")
        for g, r in pairs:
            print(f"{g}:{r}")
        return 0

    os.makedirs(args.outdir, exist_ok=True)
    log.info("Ensemble: %d members, experiment=rcp45, years %d-%d, workers=%d (%s)",
             len(pairs), args.start_year, args.end_year,
             args.workers, args.executor if args.workers > 1 else "serial")

    work = dict(start_year=args.start_year, end_year=args.end_year,
                outdir=args.outdir, overwrite=args.overwrite, dry_run=args.dry_run)

    results: list[dict] = []
    if args.workers <= 1:
        for g, r in pairs:
            try:
                results.append(process_member(g, r, **work))
            except Exception as exc:  # noqa: BLE001 - keep going on per-member errors
                results.append(dict(member=f"{g}__{r}", gcm=g, rcm=r,
                                    status="error", msg=f"ERROR: {exc}"))
            log.info("[%s__%s] %s", g, r, results[-1]["msg"])
    else:
        Pool = ProcessPoolExecutor if args.executor == "process" else ThreadPoolExecutor
        with Pool(max_workers=args.workers) as ex:
            futs = {ex.submit(process_member, g, r, **work): (g, r) for g, r in pairs}
            for fut in as_completed(futs):
                g, r = futs[fut]
                try:
                    results.append(fut.result())
                except Exception as exc:  # noqa: BLE001
                    results.append(dict(member=f"{g}__{r}", gcm=g, rcm=r,
                                        status="error", msg=f"ERROR: {exc}"))
                log.info("[%s__%s] %s", g, r, results[-1]["msg"])

    # Member index CSV (skip for dry-run, which retrieves nothing).
    if not args.dry_run:
        idx = pd.DataFrame([{k: r.get(k) for k in
                             ("gcm", "rcm", "status", "n_days", "nlat", "nlon",
                              "n_blocks", "out_file", "msg")} for r in results])
        idx_path = os.path.join(args.outdir, "members_index.csv")
        idx.sort_values(["status", "gcm", "rcm"]).to_csv(idx_path, index=False)
        log.info("wrote member index -> %s", idx_path)

    print("\n===== Summary =====")
    n_ok = sum(1 for r in results if r["status"] == "ok")
    for r in sorted(results, key=lambda d: (d["status"], d["member"])):
        print(f"[{r['member']}] {r['msg']}")
    print(f"\n{n_ok}/{len(results)} members produced an output file.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
