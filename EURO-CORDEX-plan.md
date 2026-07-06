# Plan: France daily TMax from EURO-CORDEX — RCP4.5, 2006–2080 (via Copernicus CDS)

## Context
The user wants to repeat the NEX-GDDP-CMIP6 France daily-TMax effort, but using
**EURO-CORDEX** — the dynamically-downscaled regional climate model (RCM) ensemble over
Europe at **EUR-11 (~12.5 km)**, i.e. much finer regional detail than NEX-GDDP's 0.25°.
**Scope (per user): RCP4.5 only, starting 2006 — no historical data, no splice.**

**Key research finding (drives the whole approach):** the AWS Open Data bucket
`s3://euro-cordex` does **not** currently hold daily-max-temperature *projections*. It has
only (a) CMIP5 CORDEX **monthly** `tas`/`pr` (EUR-11, Zarr, historical+RCPs) and (b) CMIP6
CORDEX **ERA5 evaluation** hindcasts (18 Zarr stores, EUR-12, one daily store with `tasmax`).
The 1,687 daily-`tasmax` rows in the bucket's intake catalog point to Jülich local disk
(`/mnt/...`), not S3. The genuine EURO-CORDEX **daily `tasmax` projection ensemble** lives on
the **Copernicus Climate Data Store (CDS)**, dataset
[`projections-cordex-domains-single-levels`](https://cds.climate.copernicus.eu/datasets/projections-cordex-domains-single-levels),
downloaded via the **`cdsapi`** client. **The user chose this CDS path.**

Outcome: one **RCP4.5 daily-`tasmax`** NetCDF per GCM–RCM pair, **2006–2080**, clipped to
France on the native rotated-pole grid — the NEX-GDDP deliverable at regional resolution.

## Step 0 — Reorganize the directory FIRST (isolate the two data sources)
Before any EURO-CORDEX work, separate the existing NEX-GDDP material so the new pipeline can
never mix with or overwrite it. Move **all** current files into a new `NEX-GDDP/` folder and
do EURO-CORDEX work in a separate `EURO-CORDEX/` folder:
```
France temperatures/
├── NEX-GDDP/                     # everything that exists today, moved here
│   ├── README.md
│   ├── france_tmax_cmip6.py
│   ├── global_monthly_tas_cmip6.py
│   ├── gcp_startup.sh
│   ├── gcp_startup_globaltas.sh
│   ├── requirements.txt
│   ├── tmax_france_results/      # the 30 France TMax .nc (4.3 GB) — move carefully
│   ├── tas_global_results/       # global-tas outputs
│   └── out_resume_test/
└── EURO-CORDEX/                  # all new work lives here
    ├── france_tmax_cordex.py
    ├── gcp_startup_cordex.sh
    ├── requirements.txt
    ├── README.md
    └── tmax_france_cordex_results/   # outputs land here
```
- Use plain `mv` (same filesystem → instant, no copy); verify the 4.3 GB
  `tmax_france_results/` arrived intact (count = 30 `.nc`) before continuing.
- Drop `__pycache__/` (regenerates). After the move, update path references in the NEX-GDDP
  `README.md` (the `gcloud --metadata-from-file` commands assume the script sits in the
  working dir → run them from inside `NEX-GDDP/`), and update the project memory's "Repo
  files / outputs" paths to the new `NEX-GDDP/...` locations.

## Approach
New self-contained worker **`france_tmax_cordex.py`**, reusing the proven orchestration of
`NEX-GDDP/france_tmax_cmip6.py` but swapping the S3/`s3fs` data layer for `cdsapi`, and the
simple bbox `.sel` for a rotated-pole clip. **No historical experiment — only `rcp_4_5`,
2006 onward.**

**Reused from the existing worker (same structure):**
- `process_model(...)` per-member orchestration + `ProcessPoolExecutor` + `--workers N`
  CLI (france_tmax_cmip6.py ~:240, :347–368) — here a "member" is a **GCM×RCM combo**.
- The **resumable atomic cache** pattern (france_tmax_cmip6.py ~:281–320): write each
  downloaded chunk to `…/blocks/<member>/*.nc.tmp` then `os.replace` to final; on restart
  skip cached chunks; concat all chunks → one output. (Cache unit = a **5-year block**,
  since CDS delivers fixed 5-year files.)
- `_retry()` backoff (france_tmax_cmip6.py:117), output encoding (`zlib`/float32), and the
  `--dry-run`/`--list-models`/`--overwrite` flags. (The historical→scenario splice logic is
  **dropped** — single scenario, no boundary.)
- The whole GCP spot-VM pattern: `gcp_startup.sh` (metadata-staged code, STOP-on-preempt,
  resume-on-boot) and the **self-stop-on-`.complete` systemd watcher** used last run.

**New data layer — CDS retrieval (replaces `make_fs`/`discover_scenario`/`load_year`):**
```python
import cdsapi
c = cdsapi.Client()                      # reads ~/.cdsapirc (URL + key)
c.retrieve("projections-cordex-domains-single-levels", {
    "domain": "europe",                  # EUR-11
    "experiment": "rcp_4_5",             # ONLY rcp4.5 (no historical)
    "horizontal_resolution": "0_11_degree_x_0_11_degree",
    "temporal_resolution": "daily_mean",
    "variable": "maximum_2m_temperature_in_the_last_24_hours",   # = tasmax (K)
    "gcm_model": gcm, "rcm_model": rcm, "ensemble_member": "r1i1p1",
    "start_year": [s], "end_year": [e],  # one fixed 5-year block, e.g. "2006","2010"
    "data_format": "netcdf", "download_format": "zip",
}, zip_path)
```
- CDS returns a **zip of rotated-pole NetCDF** files (one per 5-year block). Worker unzips
  to temp scratch, opens with `xarray` (`use_cftime=True`), clips to France, writes the
  block to the resumable cache, deletes the raw zip/NetCDF.
- **Ensemble = valid GCM×RCM combinations.** EURO-CORDEX EUR-11 has ~8 GCMs × ~13 RCMs but
  only a subset of pairs exist. Start from a curated list (e.g. GCMs
  `cnrm_cerfacs_cm5`, `ichec_ec_earth`, `ipsl_cm5a_mr`, `mohc_hadgem2_es`,
  `mpi_m_mpi_esm_lr`, `ncc_noresm1_m`; RCMs `clmcom_cclm4_8_17`, `knmi_racmo22e`,
  `smhi_rca4`, `dmi_hirham5`, `gerics_remo2015`, `cnrm_aladin63`, …) and **skip invalid
  combos gracefully** (CDS "no data" / 400 → log + skip), like the existing skip logic.
  `--list-models` prints the resolved matrix.
- **Time range: RCP4.5, 2006–2080**, in fixed 5-year blocks: `2006–2010, 2011–2015, …,
  2076–2080` (15 blocks per member). No historical, no splice. (End year configurable;
  default 2080 to match the NEX-GDDP run.)

**France clip on the rotated-pole grid (replaces `subset_france`):**
CORDEX files have dims `rlat`,`rlon`, 2-D coords `lat(rlat,rlon)`/`lon(rlat,rlon)`, and a
`rotated_pole` grid_mapping. Clip by masking on the true lat/lon then taking the bounding
rotated box (keeps a regular, efficient sub-array covering France; 2-D lat/lon retained,
**not regridded** — preserves the native RCM grid):
```python
FRANCE = dict(lon_w=-5.5, lon_e=9.8, lat_s=41.0, lat_n=51.5)   # incl. Corsica
lon = ((ds.lon + 180) % 360) - 180
m = (ds.lat>=41.0)&(ds.lat<=51.5)&(lon>=-5.5)&(lon<=9.8)
ys, xs = np.where(m.values)
sub = ds.isel(rlat=slice(ys.min(), ys.max()+1), rlon=slice(xs.min(), xs.max()+1))
```
(Optional later add-on: regrid to a regular 0.1° lat/lon with `xesmf` — **not** in scope by
default, to keep the native ~12.5 km grid and avoid a heavy dependency.)

**Output:** `tasmax_france_<gcm>_<rcm>_rcp45_2006_2080.nc` (tasmax in K, rotated-grid France
subset, 2-D lat/lon + `rotated_pole` retained), provenance attrs noting CDS source, domain
EUR-11, scenario RCP4.5, and the rotated-grid/clip caveat. Plus a combined index CSV of members.

## Prerequisite — CDS credential (NOT anonymous)
Unlike the public S3 bucket, CDS requires a **free account + API key** in `~/.cdsapirc`
(`url` + `key`). The user must create this once. On GCP it is a **secret**: pass the key via
instance metadata and have the startup script write `~/.cdsapirc` (project-private, but still
a credential — flag it; do not commit it to the repo). Add **`cdsapi`** to `requirements.txt`.

## GCP deployment (smaller, CDS-throttled)
- **CDS is queue/throttle-bound**, not CPU-bound: only a few concurrent requests per user, and
  each waits in CDS's queue (minutes→hours under load). So:
  - Use a **small spot VM** (e.g. `n2-standard-4`/`-8`) — 32 vCPUs are wasted here.
  - `--workers` modest (≈4–8); effective parallelism is capped by CDS, the rest queue.
  - Prefer zone **`europe-west*`** (CDS/Copernicus is in Europe); egress is free (open data).
- Everything else reuses last run: SPOT + `--instance-termination-action=STOP`, metadata-
  staged code, resume-on-boot, **self-stop systemd watcher on `.complete`**, local-disk
  output (user scp's), no bucket.

## Files
- **First:** the `NEX-GDDP/` ↔ `EURO-CORDEX/` reorg (Step 0).
- **New `EURO-CORDEX/france_tmax_cordex.py`** (worker)
- **New `EURO-CORDEX/gcp_startup_cordex.sh`** (boot/resume; installs `cdsapi`, writes
  `~/.cdsapirc` from metadata, runs the worker to `/opt/cordex/tmax_france_cordex/`)
- **New `EURO-CORDEX/requirements.txt`** (`cdsapi`, xarray, netCDF4, cftime, numpy, pandas)
  and **`EURO-CORDEX/README.md`** (CDS path, RCP4.5/2006 scope, credential setup, rotated-grid
  caveat, CDS-throttling note)
- Reuse (copy/adapt into `EURO-CORDEX/`): the per-member orchestration + resumable cache from
  `NEX-GDDP/france_tmax_cmip6.py`, and the self-stop systemd watcher pattern from last run.

## Local test (before GCP)
1. Confirm `~/.cdsapirc` works: a tiny `cdsapi` retrieve (1 combo, 1 five-year block).
2. `--dry-run` over 2–3 known-valid combos → confirm the request matrix + skip logic.
3. Real run, **1 combo (e.g. `mpi_m_mpi_esm_lr` × `smhi_rca4`), 2006–2015** (two RCP4.5
   blocks): verify unzip → rotated clip → cache → concat, the France subarray (~140×140
   cells), `tasmax` ≈ 270–310 K daily, and resume (re-run skips the cached block).

## Verification (end-to-end)
- **Finished:** `/opt/cordex/.complete` + `run.log` Summary block.
- **Complete set:** one `tasmax_france_<gcm>_<rcm>_rcp45_2006_2080.nc` per resolved valid
  combo (count matches `--list-models` minus logged skips).
- **Valid:** open each → continuous daily series **2006-01→2080-12** (calendar-aware), no NaN
  over land, `tasmax` in a plausible 250–320 K envelope, 2-D lat/lon covers France incl. Corsica.
- Retrieve via `gcloud compute scp`; tear down the VM after download (verify-then-delete, per
  last run's morning flow).

## Key caveats / risks
- **Speed:** CDS queueing makes wall-clock unpredictable (hours–days for a full ensemble),
  unlike the bandwidth-bound S3 job. The resumable cache makes this safe to run over a long
  period / across preemptions.
- **Combo validity & ensemble size:** the exact set of valid EUR-11 GCM×RCM pairs with daily
  `tasmax` under RCP4.5 must be resolved against the CDS form; expect ~10–25 usable members.
- **Rotated grid:** output stays on the native rotated grid (not regridded) by default.
- **Credential handling** for the CDS key on the VM (secret in metadata).
