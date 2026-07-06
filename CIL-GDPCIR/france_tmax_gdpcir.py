#!/usr/bin/env python3
"""
Read the CIL-GDPCIR ensemble of daily maximum temperature (TMax / `tasmax`) over
France, 2000-2080, and write one NetCDF per model (ensemble member).

Dataset
-------
CIL Global Downscaled Projections for Climate Impacts Research (CIL-GDPCIR),
served from the **Microsoft Planetary Computer**:
    https://planetarycomputer.microsoft.com/dataset/group/cil-gdpcir

  * Bias-corrected + statistically downscaled CMIP6, delivered on a common global
    0.25-degree grid -- a sibling product to NEX-GDDP-CMIP6. This worker is the
    Planetary-Computer companion to ../NEX-GDDP/france_tmax_cmip6.py and reuses that
    script's orchestration (per-model ProcessPoolExecutor, a resumable atomic per-year
    cache, retry/backoff, --dry-run/--list-models/--overwrite). It swaps only the data
    layer: AWS S3 per-year NetCDF -> Planetary Computer STAC + Azure Zarr.
  * Access is via the STAC API, signed with `planetary_computer.sign_inplace`. The
    public collections need **no credential/secret** (unlike EURO-CORDEX's CDS).
  * Data lives as **Zarr** on Azure Blob (account `rhgeuwest`, West Europe). Each STAC
    item is one (model, scenario); its `tasmax`/`tasmin`/`pr` assets are Zarr stores.
  * Three collections split the ensemble by licence (we search all three at once so we
    never hardcode which model is where):
        cil-gdpcir-cc0     -> FGOALS-g3, INM-CM4-8, INM-CM5-0      (public domain)
        cil-gdpcir-cc-by   -> 21 models                            (CC-BY-4.0)
        cil-gdpcir-cc-by-sa-> CanESM5                              (CC-BY-SA-4.0)
  * `tasmax` units are Kelvin. Longitudes are already in the -180..180 convention
    (-179.875..179.875) and latitudes ascend, so France is a direct `.sel()` -- no
    rewrap (contrast NEX-GDDP, whose 0..360 grid needs rewrapping).
  * Scenarios: `historical` covers 1950-2014, the SSPs (ssp126/245/370/585) cover
    2015-2099/2100. This script splices `historical` (years <= 2014) with the chosen
    SSP (years >= 2015) into one continuous per-model series.

Why this is cheaper than the NEX-GDDP S3 path
---------------------------------------------
The GDPCIR Zarr stores are chunked at 365 days x 90 deg lat x 90 deg lon, so reading
the France window only transfers the handful of chunks that cover France (a lazy,
dask-backed `.sel`) -- not the whole globe per day. So we open each store once and pull
just the France column of chunks, year by year.

Parallelism
-----------
Each model is an independent unit of work (one ensemble member -> one output file), so
parallelism is across models via --workers N. --workers 1 (the default) runs serially,
which is easiest for debugging. On a cloud VM bump --workers up to saturate the network.

    # Debug locally, serial, two models, a short year range:
    python france_tmax_gdpcir.py --models CanESM5,MIROC6 \
        --start-year 2000 --end-year 2003 --outdir ./out

    # See what would be read without reading anything:
    python france_tmax_gdpcir.py --dry-run

    # Full ensemble, 2000-2080, ssp245, 8 members in parallel (e.g. on GCP):
    python france_tmax_gdpcir.py --scenario ssp245 \
        --start-year 2000 --end-year 2080 --workers 8 --outdir ./tmax_france

Requirements
------------
    pip install planetary-computer pystac-client "xarray>=2023" zarr adlfs dask \
        netCDF4 cftime
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

# --------------------------------------------------------------------------- #
# Dataset constants
# --------------------------------------------------------------------------- #
STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTIONS = [
    "cil-gdpcir-cc0",       # FGOALS-g3, INM-CM4-8, INM-CM5-0 (public domain)
    "cil-gdpcir-cc-by",     # the 21-model bulk of the ensemble (CC-BY-4.0)
    "cil-gdpcir-cc-by-sa",  # CanESM5 (CC-BY-SA-4.0)
]
VARIABLE = "tasmax"               # daily maximum near-surface air temperature
HISTORICAL = "historical"
HISTORICAL_END = 2014             # historical runs end in 2014; SSPs start 2015

# Metropolitan France bounding box (incl. Corsica), degrees east in -180..180.
# Identical window to the NEX-GDDP and EURO-CORDEX siblings.
FRANCE = dict(lon_w=-5.5, lon_e=9.8, lat_s=41.0, lat_n=51.5)

# The 25 GCMs in CIL-GDPCIR, across the three licence collections. Models lacking the
# requested scenario are skipped gracefully at runtime. Override with --models.
DEFAULT_MODELS = [
    # cil-gdpcir-cc0 (public domain)
    "FGOALS-g3", "INM-CM4-8", "INM-CM5-0",
    # cil-gdpcir-cc-by
    "ACCESS-CM2", "ACCESS-ESM1-5", "BCC-CSM2-MR", "CMCC-CM2-SR5", "CMCC-ESM2",
    "EC-Earth3", "EC-Earth3-AerChem", "EC-Earth3-CC", "EC-Earth3-Veg",
    "EC-Earth3-Veg-LR", "GFDL-CM4", "GFDL-ESM4", "HadGEM3-GC31-LL", "MIROC-ES2L",
    "MIROC6", "MPI-ESM1-2-HR", "MPI-ESM1-2-LR", "NESM3", "NorESM2-LM", "NorESM2-MM",
    "UKESM1-0-LL",
    # cil-gdpcir-cc-by-sa
    "CanESM5",
]

log = logging.getLogger("gdpcir")


def _silence_fsspec_teardown():
    """adlfs/fsspec print a harmless `RuntimeError: Loop is not running` from their
    async finalizers when the interpreter (or a worker process) shuts down and the
    shared IO loop has already stopped. The data is fully written by then. Swallow just
    that one error -- via both the unraisable hook (fires for __del__) and the thread
    hook (fires in fsspec's loop thread) -- so it doesn't clutter the run log. Installed
    at import so spawned ProcessPoolExecutor workers inherit it too."""
    import threading

    def _is_loop_err(exc) -> bool:
        return isinstance(exc, RuntimeError) and "Loop is not running" in str(exc)

    # weakref._exitfunc routes finalizer errors through sys.excepthook at interpreter
    # exit -- this is the one that actually prints the fsspec teardown traceback.
    _orig_excepthook = sys.excepthook

    def _excepthook(exc_type, exc, tb):
        if _is_loop_err(exc):
            return
        _orig_excepthook(exc_type, exc, tb)

    sys.excepthook = _excepthook

    _orig_unraisable = sys.unraisablehook

    def _unraisable(args):
        if _is_loop_err(getattr(args, "exc_value", None)):
            return
        _orig_unraisable(args)

    sys.unraisablehook = _unraisable

    _orig_thread = threading.excepthook

    def _thread(args):
        if _is_loop_err(getattr(args, "exc_value", None)):
            return
        _orig_thread(args)

    threading.excepthook = _thread


_silence_fsspec_teardown()


# --------------------------------------------------------------------------- #
# Planetary Computer STAC access
# --------------------------------------------------------------------------- #
def make_catalog():
    """Open a STAC client that auto-signs assets (injects the Azure SAS token).

    Create one *inside* each worker process -- the signing client must not be shared
    across a fork (same caveat as s3fs in the NEX-GDDP sibling). Imported lazily so
    --list-models works without the third-party stack installed."""
    import planetary_computer
    import pystac_client

    return pystac_client.Client.open(
        STAC_API, modifier=planetary_computer.sign_inplace,
    )


def find_item(catalog, model: str, experiment: str):
    """Return the STAC item for one (model, experiment), searching all three licence
    collections, or None if that combination does not exist (model lacks the scenario).
    """
    search = catalog.search(
        collections=COLLECTIONS,
        query={"cmip6:source_id": {"eq": model},
               "cmip6:experiment_id": {"eq": experiment}},
    )
    items = list(search.items())
    return items[0] if items else None


def _retry(fn, *, tries=4, delay=3.0, what="op", on_retry=None):
    """Exponential-backoff retry for flaky network reads. `on_retry`, if given, runs
    between attempts -- used here to re-sign (re-open) a store whose ~1h SAS token may
    have expired mid-read."""
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - transient network/SAS errors
            if attempt == tries:
                raise
            log.warning("%s failed (attempt %d/%d): %s -- retrying in %.1fs",
                        what, attempt, tries, exc, delay)
            time.sleep(delay)
            delay *= 2
            if on_retry is not None:
                try:
                    on_retry()
                except Exception as re_exc:  # noqa: BLE001
                    log.warning("%s re-sign failed: %s", what, re_exc)


# --------------------------------------------------------------------------- #
# Subsetting
# --------------------------------------------------------------------------- #
def subset_france(ds):
    """Clip a global GDPCIR dataset to the France window (lazy / no compute).

    GDPCIR longitudes are already -180..180, so unlike NEX-GDDP no rewrap is needed.
    We only flip a coordinate if it happens to descend, then slice the bbox."""
    if float(ds.lat[0]) > float(ds.lat[-1]):
        ds = ds.isel(lat=slice(None, None, -1))
    if float(ds.lon[0]) > float(ds.lon[-1]):
        ds = ds.isel(lon=slice(None, None, -1))
    sub = ds.sel(
        lat=slice(FRANCE["lat_s"], FRANCE["lat_n"]),
        lon=slice(FRANCE["lon_w"], FRANCE["lon_e"]),
    )
    return sub[[VARIABLE]]


def open_france(item):
    """Lazily open the tasmax Zarr store of one signed STAC item and clip it to France.

    Uses the asset's own `xarray:open_kwargs` (engine=zarr, consolidated=True,
    chunks={}, storage_options incl. the signed SAS credential); adds use_cftime so
    every model keeps its native calendar. Returns a lazy, dask-backed France subset."""
    import xarray as xr

    asset = item.assets[VARIABLE]
    open_kwargs = dict(asset.extra_fields.get("xarray:open_kwargs", {}))
    open_kwargs.setdefault("engine", "zarr")
    open_kwargs.setdefault("consolidated", True)
    open_kwargs.setdefault("chunks", {})
    open_kwargs["use_cftime"] = True
    ds = xr.open_dataset(asset.href, **open_kwargs)
    return subset_france(ds)


def _strip_encoding(ds):
    """Drop Zarr-specific encoding (chunks/compressor/filters) so the NetCDF engine
    can re-encode cleanly on write."""
    for name in list(ds.variables):
        ds[name].encoding = {}
    return ds


# --------------------------------------------------------------------------- #
# Per-model worker
# --------------------------------------------------------------------------- #
def process_model(model: str, scenario: str, start_year: int, end_year: int,
                  outdir: str, overwrite: bool, dry_run: bool) -> str:
    """Build the spliced historical+SSP France TMax series for one model and write it
    to <outdir>/tasmax_france_<model>_<scenario>_<start>_<end>.nc. Returns a short
    human-readable status string."""
    out_path = os.path.join(
        outdir, f"tasmax_france_{model}_{scenario}_{start_year}_{end_year}.nc")
    if os.path.exists(out_path) and not overwrite and not dry_run:
        return f"[{model}] exists, skipped ({out_path})"

    import xarray as xr

    hist_years = [y for y in range(start_year, end_year + 1) if y <= HISTORICAL_END]
    ssp_years = [y for y in range(start_year, end_year + 1) if y > HISTORICAL_END]

    catalog = make_catalog()
    hist_item = find_item(catalog, model, HISTORICAL) if hist_years else None
    ssp_item = find_item(catalog, model, scenario) if ssp_years else None
    if hist_item is None and ssp_item is None:
        return f"[{model}] no tasmax item for historical/{scenario}, skipped"

    # year -> (which scenario label, STAC item), in chronological order.
    plan: list[tuple[int, str, object]] = []
    for y in hist_years:
        if hist_item is not None:
            plan.append((y, HISTORICAL, hist_item))
    for y in ssp_years:
        if ssp_item is not None:
            plan.append((y, scenario, ssp_item))
    if not plan:
        return f"[{model}] no files in requested range, skipped"

    if dry_run:
        coll = (ssp_item or hist_item).collection_id
        spans = []
        if hist_item is not None and hist_years:
            spans.append(f"historical {hist_years[0]}-{hist_years[-1]}")
        if ssp_item is not None and ssp_years:
            spans.append(f"{scenario} {ssp_years[0]}-{ssp_years[-1]}")
        return f"[{model}] collection={coll} {len(plan)} years: " + " + ".join(spans)

    # Per-year cache: each year's France subset is saved as its own .nc as soon as it
    # is read, so a crash/preemption resumes at YEAR granularity (only the missing
    # years are re-read) instead of restarting the whole model.
    enc = {VARIABLE: {"zlib": True, "complevel": 4, "dtype": "float32"}}
    yearly_dir = os.path.join(outdir, "yearly", model)
    os.makedirs(yearly_dir, exist_ok=True)
    for stale in glob.glob(os.path.join(yearly_dir, "*.tmp")):
        try:
            os.remove(stale)  # leftover half-write from an interrupted run
        except OSError:
            pass

    def yearly_path(scen: str, year: int) -> str:
        return os.path.join(yearly_dir, f"tasmax_france_{model}_{scen}_{year}.nc")

    todo = [(y, s, it) for (y, s, it) in plan
            if overwrite or not os.path.exists(yearly_path(s, y))]
    cached = len(plan) - len(todo)
    log.info("[%s] %d/%d yearly files cached; reading %d (%d-%d)...",
             model, cached, len(plan), len(todo), plan[0][0], plan[-1][0])

    t0 = time.time()
    # Lazy store opened once per scenario; re-opened (re-signed) on a token-expiry retry.
    holders: dict[str, dict] = {}

    def get_ds(scen: str, item):
        if scen not in holders:
            holders[scen] = {"ds": open_france(item)}
        return holders[scen]

    missing: list[int] = []
    for (year, scen, item) in todo:
        holder = get_ds(scen, item)

        def attempt():
            return holder["ds"].sel(time=str(year)).load()

        def on_retry(scen=scen):  # re-sign: re-open the store with a fresh SAS token
            holder["ds"] = open_france(find_item(make_catalog(), model, scen))

        try:
            sub = _retry(attempt, what=f"read {model} {scen} {year}", on_retry=on_retry)
        except Exception as exc:  # noqa: BLE001 - keep going; record the gap
            log.warning("[%s] failed year %d: %s", model, year, exc)
            missing.append(year)
            continue
        if sub.sizes.get("time", 0) == 0:
            missing.append(year)  # year out of this model's coverage
            continue
        _strip_encoding(sub)
        yp = yearly_path(scen, year)
        sub.to_netcdf(yp + ".tmp", encoding=enc)
        os.replace(yp + ".tmp", yp)  # atomic rename -> never a partial "done" file
        sub.close()

    if missing:
        log.warning("[%s] missing years (no data): %s", model, sorted(set(missing)))

    # Release the lazy Zarr stores while the adlfs async event loop is still alive --
    # otherwise their finalizers fire at interpreter teardown and print a noisy
    # "Loop is not running" RuntimeError (harmless, but it clutters the run log).
    for h in holders.values():
        try:
            h["ds"].close()
        except Exception:  # noqa: BLE001
            pass

    # Concatenate the per-year files that are present into the full series.
    present = [(y, s) for (y, s, _) in plan if os.path.exists(yearly_path(s, y))]
    if not present:
        return f"[{model}] no data retrieved, skipped"

    pieces = []
    for (year, scen) in present:
        d = xr.open_dataset(yearly_path(scen, year), use_cftime=True)
        pieces.append(d[[VARIABLE]].load())
        d.close()
    combined = xr.concat(pieces, dim="time").sortby("time")
    _strip_encoding(combined)
    combined[VARIABLE].attrs.update(
        units="K", long_name="Daily Maximum Near-Surface Air Temperature")
    src_item = ssp_item or hist_item
    combined.attrs.update(
        title=f"CIL-GDPCIR daily TMax over France, {model}",
        model=model,
        scenario=f"{HISTORICAL}+{scenario}",
        licence_collection=src_item.collection_id,
        source="CIL-GDPCIR (Climate Impact Lab Global Downscaled Projections for "
               "Climate Impacts Research) via Microsoft Planetary Computer",
        stac_api=STAC_API,
        region=("France bbox lon[%(lon_w)s,%(lon_e)s] lat[%(lat_s)s,%(lat_n)s]" % FRANCE),
        history=f"Subset to France and spliced historical+{scenario} on "
                f"{time.strftime('%Y-%m-%d')} by france_tmax_gdpcir.py",
    )

    combined.to_netcdf(out_path, encoding=enc)
    dt = time.time() - t0
    nt = combined.sizes["time"]
    return (f"[{model}] OK -> {out_path} "
            f"({nt} days, {combined.sizes['lat']}x{combined.sizes['lon']} grid, "
            f"{dt/60:.1f} min)")


# --------------------------------------------------------------------------- #
# CLI / orchestration
# --------------------------------------------------------------------------- #
def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Read the CIL-GDPCIR ensemble of France daily TMax (tasmax) from "
                    "the Microsoft Planetary Computer.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--scenario", default="ssp245",
                   choices=["ssp126", "ssp245", "ssp370", "ssp585"],
                   help="SSP scenario used for years > 2014 (2000-2014 always uses historical).")
    p.add_argument("--start-year", type=int, default=2000)
    p.add_argument("--end-year", type=int, default=2080)
    p.add_argument("--models", default="all",
                   help='Comma-separated model names, or "all" for the default ensemble.')
    p.add_argument("--outdir", default="./tmax_france")
    p.add_argument("--workers", type=int, default=1,
                   help="Number of ensemble members to process simultaneously "
                        "(>1 turns on parallelism).")
    p.add_argument("--executor", choices=["process", "thread"], default="process",
                   help="Parallel backend when --workers > 1.")
    p.add_argument("--overwrite", action="store_true",
                   help="Re-create output files that already exist.")
    p.add_argument("--dry-run", action="store_true",
                   help="Resolve STAC items and report the plan without reading data.")
    p.add_argument("--list-models", action="store_true",
                   help="Print the default ensemble (25 models) and exit.")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    # The Azure/adlfs stack logs every blob HTTP request at INFO -- thousands of lines
    # per model. Silence them so our own progress logs stay readable.
    for noisy in ("azure", "azure.core.pipeline.policies.http_logging_policy",
                  "adlfs", "aiohttp", "asyncio", "fsspec"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    if args.list_models:
        for m in DEFAULT_MODELS:
            print(m)
        return 0

    models = (DEFAULT_MODELS if args.models == "all"
              else [m.strip() for m in args.models.split(",") if m.strip()])
    log.info("Ensemble: %d models, scenario=%s, years %d-%d, workers=%d (%s)",
             len(models), args.scenario, args.start_year, args.end_year,
             args.workers, args.executor if args.workers > 1 else "serial")

    if not args.dry_run:
        os.makedirs(args.outdir, exist_ok=True)
    work = dict(scenario=args.scenario, start_year=args.start_year,
                end_year=args.end_year, outdir=args.outdir,
                overwrite=args.overwrite, dry_run=args.dry_run)

    results: list[str] = []
    if args.workers <= 1:
        for m in models:
            try:
                results.append(process_model(m, **work))
            except Exception as exc:  # noqa: BLE001 - keep going on per-model errors
                results.append(f"[{m}] ERROR: {exc}")
            log.info(results[-1])
    else:
        Pool = ProcessPoolExecutor if args.executor == "process" else ThreadPoolExecutor
        with Pool(max_workers=args.workers) as ex:
            futs = {ex.submit(process_model, m, **work): m for m in models}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    results.append(fut.result())
                except Exception as exc:  # noqa: BLE001
                    results.append(f"[{m}] ERROR: {exc}")
                log.info(results[-1])

    print("\n===== Summary =====")
    for r in sorted(results):
        print(r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
