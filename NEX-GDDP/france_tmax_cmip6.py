#!/usr/bin/env python3
"""
Read the NEX-GDDP-CMIP6 ensemble of daily maximum temperature (TMax / `tasmax`)
over France, 2000-2080, and write one NetCDF per model (ensemble member).

Dataset
-------
NASA Earth Exchange Global Daily Downscaled Projections (NEX-GDDP-CMIP6)
    https://registry.opendata.aws/nex-gddp-cmip6/

  * Public S3 bucket: s3://nex-gddp-cmip6  (region us-west-2, anonymous access,
    egress sponsored by the AWS Open Data program -> free to download).
  * Object layout:
        NEX-GDDP-CMIP6/<model>/<scenario>/<variant>/<variable>/
            <variable>_day_<model>_<scenario>_<variant>_<grid>_<year>[_v<x.y>].nc
  * One NetCDF file per (model, scenario, variable, year). NetCDF4/HDF5,
    CF-1.7 conventions.
  * All models are delivered on a common global 0.25-degree grid
    (lon 0.125..359.875 in 0..360 convention, lat -59.875..89.875).
  * `tasmax` units are Kelvin. Time is "days since 1850-01-01"; calendars vary
    by model (noleap vs proleptic_gregorian), so each model is written to its
    own file rather than forced onto a shared time axis.
  * Scenarios: `historical` covers 1950-2014, the SSPs (ssp126/245/370/585)
    cover 2015-2100. This script splices `historical` (years <= 2014) with the
    chosen SSP (years >= 2015) into one continuous per-model series.

Why one global file is downloaded per year
-------------------------------------------
The `tasmax` variable is HDF5-chunked as (time=1, lat=600, lon=1440): each daily
chunk spans the whole globe. Reading the France window therefore touches every
time chunk, i.e. the whole file must be transferred regardless. So we simply pull
each yearly file, clip it to the France bounding box (~42 x 61 grid cells), keep
that, and discard the global file. Output per model is a few hundred MB.

Parallelism
-----------
Each model is an independent unit of work (one ensemble member -> one output
file), so parallelism is across models. Use --workers N to download/process N
ensemble members simultaneously. --workers 1 (the default) runs serially, which
is easiest for debugging. On Google Cloud, bump --workers up to saturate the VM's
network/CPU (e.g. --workers 16).

    # Debug locally, serial, just two models, a short year range:
    python france_tmax_cmip6.py --models ACCESS-CM2,MIROC6 \
        --start-year 2000 --end-year 2003 --outdir ./out

    # See what would be downloaded without downloading anything:
    python france_tmax_cmip6.py --dry-run

    # Full ensemble, 2000-2080, ssp245, 16 members in parallel (e.g. on GCP):
    python france_tmax_cmip6.py --scenario ssp245 \
        --start-year 2000 --end-year 2080 --workers 16 --outdir ./tmax_france

Requirements
------------
    pip install "xarray>=2023" s3fs h5netcdf netCDF4 dask
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import s3fs
import xarray as xr

# --------------------------------------------------------------------------- #
# Dataset constants
# --------------------------------------------------------------------------- #
BUCKET = "nex-gddp-cmip6"
ROOT = "NEX-GDDP-CMIP6"            # top-level prefix inside the bucket
VARIABLE = "tasmax"               # daily maximum near-surface air temperature
HISTORICAL = "historical"
HISTORICAL_END = 2014             # historical runs end in 2014; SSPs start 2015

# Metropolitan France bounding box (incl. Corsica), degrees east in -180..180.
FRANCE = dict(lon_w=-5.5, lon_e=9.8, lat_s=41.0, lat_n=51.5)

# The 35 model directories in the bucket, minus those unusable for an ssp245
# tasmax run:
#   * CESM2, CESM2-WACCM, IITM-ESM -> ship tas but NOT tasmax/tasmin (skipped).
#   * GFDL-CM4_gr2                  -> alternate grid of GFDL-CM4 (double-counts).
#   * HadGEM3-GC31-MM              -> has historical/ssp126/ssp585 but NO ssp245,
#                                     so it cannot make a full 2000-2080 series.
# Models lacking the variable/scenario are also skipped at runtime. Override with
# --models.
DEFAULT_MODELS = [
    "ACCESS-CM2", "ACCESS-ESM1-5", "BCC-CSM2-MR", "CMCC-CM2-SR5", "CMCC-ESM2",
    "CNRM-CM6-1", "CNRM-ESM2-1", "CanESM5", "EC-Earth3", "EC-Earth3-Veg-LR",
    "FGOALS-g3", "GFDL-CM4", "GFDL-ESM4", "GISS-E2-1-G", "HadGEM3-GC31-LL",
    "INM-CM4-8", "INM-CM5-0", "IPSL-CM6A-LR",
    "KACE-1-0-G", "KIOST-ESM", "MIROC-ES2L", "MIROC6", "MPI-ESM1-2-HR",
    "MPI-ESM1-2-LR", "MRI-ESM2-0", "NESM3", "NorESM2-LM", "NorESM2-MM",
    "TaiESM1", "UKESM1-0-LL",
]

# Filename: <var>_day_<model>_<scenario>_<variant>_<grid>_<year>[_v<maj>.<min>].nc
_FNAME_RE = re.compile(r"_(\d{4})(?:_v(\d+)\.(\d+))?\.nc$")

log = logging.getLogger("tmax")


# --------------------------------------------------------------------------- #
# S3 discovery
# --------------------------------------------------------------------------- #
def make_fs() -> s3fs.S3FileSystem:
    """Anonymous, read-only S3 filesystem. Create one *inside* each worker
    process -- s3fs connections must not be shared across a fork."""
    return s3fs.S3FileSystem(anon=True)


def _retry(fn, *, tries=4, delay=2.0, what="s3 op"):
    """Tiny exponential-backoff retry for flaky network/S3 calls."""
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except FileNotFoundError:
            raise  # a missing key/prefix is not transient -- don't retry
        except Exception as exc:  # noqa: BLE001 - transient S3/network errors
            if attempt == tries:
                raise
            log.warning("%s failed (attempt %d/%d): %s -- retrying in %.1fs",
                        what, attempt, tries, exc, delay)
            time.sleep(delay)
            delay *= 2


@dataclass
class ScenarioFiles:
    variant: str
    grid: str
    keys_by_year: dict[int, str] = field(default_factory=dict)


def discover_scenario(fs, model: str, scenario: str, years: list[int]) -> ScenarioFiles | None:
    """Find, for one (model, scenario), the best tasmax file per requested year.

    Returns None if the model/scenario has no tasmax at all. Resolves variant and
    grid labels (which differ across models) from S3 instead of hardcoding them,
    and de-duplicates the corrected `_vX.Y` reruns by keeping the highest version.
    """
    if not years:
        return None
    base = f"{BUCKET}/{ROOT}/{model}/{scenario}"
    try:
        variant_dirs = [p.rsplit("/", 1)[-1] for p in _retry(
            lambda: fs.ls(base), what=f"ls {base}")]
    except FileNotFoundError:
        log.warning("[%s] no %s scenario directory -- skipping", model, scenario)
        return None

    # Pick the first variant that actually carries tasmax.
    for variant in sorted(variant_dirs):
        var_dir = f"{base}/{variant}/{VARIABLE}"
        try:
            files = _retry(lambda: fs.ls(var_dir), what=f"ls {var_dir}")
        except FileNotFoundError:
            continue
        if not files:
            continue

        # year -> (version_tuple, key); keep the highest version per year.
        best: dict[int, tuple[tuple[int, int], str]] = {}
        grid = None
        for full in files:
            name = full.rsplit("/", 1)[-1]
            m = _FNAME_RE.search(name)
            if not m:
                continue
            year = int(m.group(1))
            if year not in years:
                continue
            ver = (int(m.group(2)), int(m.group(3))) if m.group(2) else (1, 0)
            key = full[len(BUCKET) + 1:]  # strip "bucket/" -> object key
            if year not in best or ver > best[year][0]:
                best[year] = (ver, key)
            if grid is None:
                # grid label is the token just before the year in the filename
                parts = name.split("_")
                # ..._<variant>_<grid>_<year>...  -> grid is parts[-2] for plain,
                # or parts[-3] when a _vX.Y suffix is present.
                grid = parts[-3] if parts[-1].startswith("v") else parts[-2]
        if best:
            return ScenarioFiles(
                variant=variant,
                grid=grid or "gn",
                keys_by_year={y: k for y, (_, k) in best.items()},
            )

    log.warning("[%s] %s has no %s variable -- skipping",
                model, scenario, VARIABLE)
    return None


# --------------------------------------------------------------------------- #
# Subsetting
# --------------------------------------------------------------------------- #
def subset_france(ds: xr.Dataset) -> xr.Dataset:
    """Clip a global dataset to the France window.

    The grid uses a 0..360 longitude convention and France straddles the prime
    meridian, so longitudes are first rewrapped to -180..180 and re-sorted.
    """
    ds = ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180)).sortby("lon")
    return ds.sel(
        lat=slice(FRANCE["lat_s"], FRANCE["lat_n"]),
        lon=slice(FRANCE["lon_w"], FRANCE["lon_e"]),
    )


def load_year(fs, key: str, tmpdir: str) -> xr.Dataset:
    """Download one yearly global file, clip to France, return the tasmax slice
    in memory (the global temp file is deleted before returning)."""
    local = os.path.join(tmpdir, key.rsplit("/", 1)[-1])
    _retry(lambda: fs.get(f"{BUCKET}/{key}", local), what=f"get {key}")
    try:
        # use_cftime=True keeps every model's native calendar consistent.
        ds = xr.open_dataset(local, use_cftime=True)
        sub = subset_france(ds)[[VARIABLE]].load()
        ds.close()
    finally:
        try:
            os.remove(local)
        except OSError:
            pass
    return sub


# --------------------------------------------------------------------------- #
# Per-model worker
# --------------------------------------------------------------------------- #
def process_model(model: str, scenario: str, start_year: int, end_year: int,
                  outdir: str, overwrite: bool, dry_run: bool) -> str:
    """Build the spliced historical+SSP France TMax series for one model and
    write it to <outdir>/tasmax_france_<model>_<scenario>_<start>_<end>.nc.

    Returns a short human-readable status string."""
    out_path = os.path.join(
        outdir, f"tasmax_france_{model}_{scenario}_{start_year}_{end_year}.nc")
    if os.path.exists(out_path) and not overwrite and not dry_run:
        return f"[{model}] exists, skipped ({out_path})"

    fs = make_fs()
    hist_years = [y for y in range(start_year, end_year + 1) if y <= HISTORICAL_END]
    ssp_years = [y for y in range(start_year, end_year + 1) if y > HISTORICAL_END]

    hist = discover_scenario(fs, model, HISTORICAL, hist_years)
    ssp = discover_scenario(fs, model, scenario, ssp_years)
    if hist is None and ssp is None:
        return f"[{model}] no tasmax available, skipped"

    # year -> (which scenario, object key), in chronological order.
    plan: list[tuple[int, str, str]] = []
    for y in sorted(hist_years):
        if hist and y in hist.keys_by_year:
            plan.append((y, HISTORICAL, hist.keys_by_year[y]))
    for y in sorted(ssp_years):
        if ssp and y in ssp.keys_by_year:
            plan.append((y, scenario, ssp.keys_by_year[y]))

    missing = sorted(set(range(start_year, end_year + 1)) - {y for y, _, _ in plan})
    if missing:
        log.warning("[%s] missing years (no file found): %s", model, missing)
    if not plan:
        return f"[{model}] no files in requested range, skipped"

    if dry_run:
        variant = (ssp or hist).variant
        grid = (ssp or hist).grid
        return (f"[{model}] variant={variant} grid={grid} "
                f"{len(plan)} files {plan[0][0]}-{plan[-1][0]}")

    # Per-year cache: each year's France subset is saved as its own .nc as soon as
    # it is processed, so a crash/preemption resumes at YEAR granularity (only the
    # missing years are re-downloaded) instead of restarting the whole model.
    enc = {"tasmax": {"zlib": True, "complevel": 4, "dtype": "float32"}}
    yearly_dir = os.path.join(outdir, "yearly", model)
    os.makedirs(yearly_dir, exist_ok=True)

    def yearly_path(scen: str, year: int) -> str:
        return os.path.join(yearly_dir, f"tasmax_france_{model}_{scen}_{year}.nc")

    for stale in glob.glob(os.path.join(yearly_dir, "*.tmp")):
        try:
            os.remove(stale)  # leftover half-write from an interrupted run
        except OSError:
            pass

    todo = [(y, s, k) for (y, s, k) in plan
            if overwrite or not os.path.exists(yearly_path(s, y))]
    cached = len(plan) - len(todo)
    log.info("[%s] %d/%d yearly files already saved; downloading %d (%d-%d)...",
             model, cached, len(plan), len(todo), plan[0][0], plan[-1][0])

    t0 = time.time()
    with tempfile.TemporaryDirectory(prefix=f"nexgddp_{model}_") as tmp:
        for i, (year, scen, key) in enumerate(todo, 1):
            sub = load_year(fs, key, tmp)
            yp = yearly_path(scen, year)
            sub.to_netcdf(yp + ".tmp", encoding=enc)
            os.replace(yp + ".tmp", yp)  # atomic rename -> never a partial "done" file
            sub.close()
            if i % 10 == 0 or i == len(todo):
                log.info("[%s]   downloaded %d/%d (%s)", model, i, len(todo), year)

    # Concatenate the per-year files (all present now) into the full series.
    pieces = []
    for (year, scen, key) in plan:
        d = xr.open_dataset(yearly_path(scen, year), use_cftime=True)
        pieces.append(d[[VARIABLE]].load())
        d.close()
    combined = xr.concat(pieces, dim="time").sortby("time")
    combined = combined.rename({VARIABLE: "tasmax"})
    combined["tasmax"].attrs.update(units="K", long_name="Daily Maximum Near-Surface Air Temperature")
    combined.attrs.update(
        title=f"NEX-GDDP-CMIP6 daily TMax over France, {model}",
        model=model,
        scenario=f"{HISTORICAL}+{scenario}",
        variant=(ssp or hist).variant,
        grid_label=(ssp or hist).grid,
        source="NEX-GDDP-CMIP6 (https://registry.opendata.aws/nex-gddp-cmip6/)",
        region=("France bbox lon[%(lon_w)s,%(lon_e)s] lat[%(lat_s)s,%(lat_n)s]" % FRANCE),
        history=f"Subset to France and spliced historical+{scenario} on "
                f"{time.strftime('%Y-%m-%d')} by france_tmax_cmip6.py",
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
        description="Read the NEX-GDDP-CMIP6 ensemble of France daily TMax (tasmax).",
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
                   help="Resolve files and report the plan without downloading.")
    p.add_argument("--list-models", action="store_true",
                   help="Print every model directory in the bucket and exit.")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.list_models:
        fs = make_fs()
        for p in fs.ls(f"{BUCKET}/{ROOT}"):
            name = p.rsplit("/", 1)[-1]
            if not name.endswith(".csv"):
                print(name)
        return 0

    models = (DEFAULT_MODELS if args.models == "all"
              else [m.strip() for m in args.models.split(",") if m.strip()])
    log.info("Ensemble: %d models, scenario=%s, years %d-%d, workers=%d (%s)",
             len(models), args.scenario, args.start_year, args.end_year,
             args.workers, args.executor if args.workers > 1 else "serial")

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
