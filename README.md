# France temperatures — CMIP6 downscaled daily TMax ensembles

Extractions of **daily maximum near-surface air temperature (`tasmax`) over
France** from three different CMIP6-era downscaling products. Each pipeline produces
**one NetCDF per ensemble member** and runs on a Google Cloud spot VM.

This file is the overview and the home for everything that applies to all the
pipelines. Each subdirectory has its own README with the pipeline-specific details.

---

## Directory structure

```
France temperatures/
├── README.md                      ← this overview (cross-cutting info)
│
├── compute_daily_tmax_csv.py      reads all NetCDFs, applies France shapefile mask,
│                                  writes france_daily_tmax.csv
├── plot_france_tmax_timeseries.py reads the CSV, produces two plots:
│                                  1. all individual models  2. ensemble max
├── france_daily_tmax.csv          daily France-averaged TMax (°C), 64 columns
├── france_tmax_timeseries.png     all models, colored by dataset
├── france_tmax_ensemble_max.png   one line per dataset (annual max across models)
│
├── NEX-GDDP/                       NASA NEX-GDDP-CMIP6 — 0.25° (~25 km) statistical downscaling
│   ├── france_tmax_cmip6.py        France daily TMax, 2000–2080, historical+ssp245)
│   ├── tmax_france_results/        30 output .nc (~4.4 GB), one per model
│   ├── gcp_startup.sh              boot/resume script for VM `france-tmax`
│   ├── requirements.txt
│   └── README.md                   ← NEX-GDDP details (S3 access, splice, runbook)
│
├── CIL-GDPCIR/                     CIL Global Downscaled Projections — bias-corrected & downscaled
│   ├── france_tmax_gdpcir.py       France daily TMax, 2000–2080, ssp245    
│   ├── tmax_france_results/        23 output .nc, one per model
│   ├── gcp_startup_gdpcir.sh
│   ├── requirements.txt
│   └── README.md
│
├── EURO-CORDEX/                    EURO-CORDEX EUR-11 — ~12.5 km dynamical downscaling
│   ├── france_tmax_cordex.py       France daily TMax, RCP4.5, 2006–2080 
│   ├── tmax_france_cordex_results/ 11 output .nc, one per GCM×RCM pair
│   ├── gcp_startup_cordex.sh
│   ├── requirements.txt
│   └── README.md                   ← EURO-CORDEX details (CDS access, rotated grid, runbook)
│
└── extra stuff/
    └── global average/             monthly global-mean tas, 1950–2100 (NEX-GDDP-derived)
        ├── global_monthly_tas_cmip6.py · gcp_startup_globaltas.sh
        └── tas_global_results/     33 model .nc + .csv (area-weighted 60°S–90°N mean)
```

---

## Analysis workflow

### Step 1: `compute_daily_tmax_csv.py`

Reads all NetCDF files from the three datasets, applies a **Natural Earth 10m shapefile
mask** (metropolitan France incl. Corsica), computes a cos(lat)-weighted spatial average
for each day, and writes `france_daily_tmax.csv`. Column names use `Dataset__Model`
format; values are °C. Takes ~10 minutes.

```bash
python compute_daily_tmax_csv.py              # all datasets (default)
python compute_daily_tmax_csv.py --nex        # NEX-GDDP only
python compute_daily_tmax_csv.py --cil        # CIL-GDPCIR only
python compute_daily_tmax_csv.py --euro       # EURO-CORDEX only
python compute_daily_tmax_csv.py --nex --cil  # combine any subset
python compute_daily_tmax_csv.py --all        # explicit all (same as no flags)
```

### Step 2: `plot_france_tmax_timeseries.py`

Reads the CSV and produces two plots:

- **`france_tmax_timeseries.png`** — annual max of daily France-mean TMax for every
  individual model, colored by dataset (blue = NEX-GDDP, orange = CIL-GDPCIR,
  green = EURO-CORDEX).
- **`france_tmax_ensemble_max.png`** — one line per dataset: for each year, the hottest
  France-averaged day across all models in the ensemble.

```bash
python plot_france_tmax_timeseries.py                    # all datasets, max only
python plot_france_tmax_timeseries.py --mean             # add ensemble-mean lines (dotted)
python plot_france_tmax_timeseries.py --members          # add individual model lines (gray)
python plot_france_tmax_timeseries.py --mean --members   # both overlays
python plot_france_tmax_timeseries.py --ERA5             # overlay observed ERA5-GFS line
python plot_france_tmax_timeseries.py --nex --euro       # only NEX-GDDP + EURO-CORDEX
```

| Flag | Effect (both scripts) |
|---|---|
| `--nex` | Include NEX-GDDP |
| `--cil` | Include CIL-GDPCIR |
| `--euro` | Include EURO-CORDEX |
| `--all` | Include all (default if no dataset flag given) |

Plot-only flags:

| Flag | Effect |
|---|---|
| `--mean` | Plot 2: dotted lines — mean of each model's annual max |
| `--members` | Plot 2: gray lines — each individual model's annual max |
| `--ERA5` | Both plots: solid black line — observed annual-max TMax from `ERA5-GFS_france_annual_tmax_2000-2026.csv` |

---

## The three France-TMax datasets at a glance

| | **NEX-GDDP** | **CIL-GDPCIR** | **EURO-CORDEX** |
|---|---|---|---|
| Product | NASA NEX-GDDP-CMIP6 (statistical) | CIL Global Downscaled (bias-corrected) | EURO-CORDEX EUR-11 (dynamical / RCM) |
| Resolution | 0.25° (~25 km), regular lat/lon | 0.25° (~25 km), regular lat/lon | 0.11° (~12.5 km), **rotated-pole** grid |
| Source | AWS S3 (anonymous) | Microsoft Planetary Computer (STAC) | Copernicus CDS (`cdsapi`) |
| Scenario | historical+**ssp245** | **ssp245** | **rcp_4_5** |
| Period | 2000–2080 | 2000–2080 | 2006–2080 |
| Members | 30 models | 23 models | 11 GCM×RCM pairs |

Output filename convention: `tasmax_france_<member>_<scenario>_<startYear>_<endYear>.nc`.

---

## The France window (shared)

Metropolitan France incl. Corsica, in −180…180° east:

```
lon  −5.5 … 9.8      lat  41.0 … 51.5
```

The bounding box is used for the initial spatial subset in each pipeline. The analysis
scripts (`compute_daily_tmax_csv.py`) then apply a **shapefile mask** (Natural Earth 10m)
to exclude ocean and neighboring-country grid cells within the box.

---

## Shared engineering patterns

These are common to every pipeline here; the subfolder READMEs only note deviations.

**Parallelism across ensemble members.** Each member is an independent unit of work, so
parallelism is across members via `--workers N` (a `ProcessPoolExecutor`). `--workers 1`
is serial (easiest to debug).

**Resumable atomic cache.** Each unit of work (a year, or a 5-year block) is written to a
per-member cache file via a temp file + `os.replace` atomic rename, so a crash or spot
preemption never leaves a half-written file that looks complete. On restart the worker
**skips already-cached units** and only re-fetches the missing ones, then concatenates
the cache into the final per-member NetCDF.

**Google Cloud spot-VM pattern.** Every pipeline runs the same way:
- A **SPOT** VM with `--instance-termination-action=STOP` — a preemption *stops* (does
  not delete) the VM, so the disk and all cached work survive.
- The startup script runs on every boot, installs deps, and relaunches the job unless
  complete → **resume after preemption is just `gcloud … start`**.
- Outputs live on the VM's **local disk** (no bucket); you `gcloud compute scp` them down.

---

## Toolchain & credentials

- Python 3, `xarray` + `netCDF4`/`h5netcdf` + `cftime`; see each folder's
  `requirements.txt`. Calendars vary by model (standard / `noleap` / `360_day`), so each
  member is written to its **own** file with `use_cftime=True` rather than forced onto a
  shared time axis.
- **NEX-GDDP** needs no credential (anonymous S3).
- **CIL-GDPCIR** needs no credential (public Planetary Computer).
- **EURO-CORDEX** needs a CDS API key in `~/.cdsapirc` — **never commit it**.

---

## Status (2026-06-27)

| Pipeline | Status |
|---|---|
| NEX-GDDP France TMax | 30 models in `NEX-GDDP/tmax_france_results/` |
| CIL-GDPCIR France TMax | 23 models in `CIL-GDPCIR/tmax_france_results/` |
| EURO-CORDEX France TMax | 11 GCM×RCM pairs in `EURO-CORDEX/tmax_france_cordex_results/` |
| Global-mean `tas` | 33 models in `extra stuff/global average/tas_global_results/` |
| Analysis CSV + plots | `france_daily_tmax.csv` + 2 PNGs |
