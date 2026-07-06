# CIL-GDPCIR France daily TMax (Planetary Computer)

> See [`../README.md`](../README.md) for the project overview and cross-cutting info
> (directory map, the shared GCP spot-VM / resumable-cache pattern, the float32-mean
> gotcha, the France window). This file covers the CIL-GDPCIR pipeline's details.

A **third parallel effort** alongside [`../NEX-GDDP/`](../NEX-GDDP) and
[`../EURO-CORDEX/`](../EURO-CORDEX), reading **France daily TMax (`tasmax`), 2000–2080**
from the **CIL Global Downscaled Projections for Climate Impacts Research
(CIL-GDPCIR)** ensemble on the **Microsoft Planetary Computer**. Like NEX-GDDP it is a
global 0.25° statistically-downscaled CMIP6 daily product that splices `historical`
(≤ 2014) + an SSP (≥ 2015) and clips to a France bounding box — so it gives a third
independent ensemble for comparison. It reuses NEX-GDDP's orchestration (per-model
`ProcessPoolExecutor`, resumable atomic per-year cache, retry/backoff, the
`--dry-run`/`--list-models`/`--overwrite` flags) and swaps **only the data layer**:
AWS S3 per-year NetCDF → Planetary Computer STAC + Azure Zarr.

- Worker: [`france_tmax_gdpcir.py`](france_tmax_gdpcir.py)
- Boot/resume: [`gcp_startup_gdpcir.sh`](gcp_startup_gdpcir.sh) · VM `gdpcir-tmax`
- Python deps: [`requirements.txt`](requirements.txt)

---

## 1. The data

**CIL-GDPCIR** — <https://planetarycomputer.microsoft.com/dataset/group/cil-gdpcir>

| | |
|---|---|
| Catalog | STAC API `https://planetarycomputer.microsoft.com/api/stac/v1` |
| Access | **Sign with `planetary_computer.sign_inplace`** — free, **no account / no secret** (contrast EURO-CORDEX's CDS credential) |
| Storage | **Zarr** on Azure Blob (account `rhgeuwest`, West Europe), read via `adlfs` |
| Grid | common global **0.25°** grid, **already −180…180 longitude** (−179.875…179.875), lat ascending −89.875…89.875 |
| One STAC item = | one (model, scenario); assets `tasmax` / `tasmin` / `pr`, each a Zarr store |
| `tasmax` units | **Kelvin**; **noleap (365-day) calendar** — GDPCIR drops leap days for every model |

**Three licence collections** (the worker searches all three at once, so it never has
to know which model is where):

| Collection | Models | Licence |
|---|---|---|
| `cil-gdpcir-cc0` | FGOALS-g3, INM-CM4-8, INM-CM5-0 | CC0 (public domain) |
| `cil-gdpcir-cc-by` | 21-model bulk (ACCESS-*, EC-Earth3-*, GFDL-*, MPI-ESM*, MIROC*, NorESM2-*, …) | CC-BY-4.0 |
| `cil-gdpcir-cc-by-sa` | CanESM5 | CC-BY-SA-4.0 |

→ **25 GCMs** in the default ensemble. Each output records its source collection in the
`licence_collection` global attribute. **Scenarios:** `historical` 1950–2014, the SSPs
(`ssp126/245/370/585`) 2015–2099/2100. Default SSP here is **ssp245**, spliced with
historical into a continuous 2000–2080 series.

### Why this is much cheaper than the NEX-GDDP S3 path
NEX-GDDP's NetCDF is chunked one **global** slab per day, so clipping to France still
transfers the whole yearly file. GDPCIR's Zarr is chunked **365 days × 90° lat × 90°
lon**, so a France `.sel()` is a lazy, dask-backed read that pulls **only the handful of
chunks covering France** — no whole-globe transfer. The script opens each store once and
pulls the France window year by year.

### Real-world quirks the script handles
- **No longitude rewrap.** GDPCIR longitudes are already −180…180, so France (which
  straddles 0° E) is a direct `.sel(lon=slice(-5.5, 9.8))` — unlike NEX-GDDP's 0–360
  rewrap. (The worker still defensively flips a coordinate if it ever comes back
  descending.)
- **SAS-token expiry.** Planetary Computer signing tokens last ~1 h. A transient read
  failure triggers `_retry`, which **re-signs** (re-opens the store with a fresh token)
  before retrying. Per-year reads are small, so this rarely fires.
- **Models lacking a scenario are skipped.** `find_item` returns `None` for an absent
  (model, scenario) and that model is logged and skipped — no crash.
- **adlfs teardown noise.** fsspec/adlfs print a harmless `RuntimeError: Loop is not
  running` from async finalizers at interpreter exit (the data is fully written by
  then). The worker installs a `sys.excepthook` filter that swallows **only** that
  message, keeping the log clean.

List the default ensemble anytime: `python france_tmax_gdpcir.py --list-models` (works
offline — no deps needed).

---

## 2. The France window

Metropolitan France incl. Corsica, in −180…180 degrees east (identical to the two
siblings):

```
lon −5.5 … 9.8     lat 41.0 … 51.5
```

Resulting subset is a **42 × 61** grid (verified). Edit `FRANCE` near the top of
`france_tmax_gdpcir.py` to change it.

---

## 3. The worker script

`france_tmax_gdpcir.py` processes each model end-to-end and writes one NetCDF:

```
tmax_france/tasmax_france_<model>_<scenario>_<start>_<end>.nc
```

For each model it: resolves the `historical` + SSP STAC items across the three
collections → opens each tasmax Zarr lazily → clips to France → reads year by year into
a per-model cache → concatenates 2000–2014 (historical) + 2015–2080 (ssp) into one
continuous series → writes a compressed NetCDF (`tasmax` in K) with provenance
attributes (model, scenario, `licence_collection`, region, history).

### CLI options
| Flag | Default | Meaning |
|---|---|---|
| `--scenario` | `ssp245` | SSP for years > 2014 (`ssp126/245/370/585`) |
| `--start-year` / `--end-year` | `2000` / `2080` | inclusive year range |
| `--models` | `all` | comma list, or `all` = the default 25 |
| `--outdir` | `./tmax_france` | where outputs are written |
| `--workers` | `1` | **ensemble members processed simultaneously** (the parallel switch) |
| `--executor` | `process` | `process` (true parallelism) or `thread` |
| `--overwrite` | off | re-create existing outputs (otherwise skipped → **resume**) |
| `--dry-run` | off | resolve STAC items + per-year plan without reading data |
| `--list-models` | off | print the default 25-model ensemble and exit (offline) |

**Parallelism.** `--workers N` runs N models at once. The bottleneck is network/Azure
reads (not CPU), so a modest VM with `--workers 8` is plenty — see [§7](#7-sizingcost-notes).

**Resumability** is built in: a model whose output `.nc` exists is skipped unless
`--overwrite`, and within a model each year is cached atomically (see
[§4](#4-the-google-cloud-run)). Re-running the same command continues where it stopped.

### Local quick start
```bash
pip install -r requirements.txt

# Debug: two models, a short range spanning the historical->ssp splice, serial
python france_tmax_gdpcir.py --models MIROC6,CanESM5 --start-year 2013 --end-year 2016 --outdir ./out

# Preview the plan without reading data
python france_tmax_gdpcir.py --dry-run

# Full ensemble, 8 in parallel
python france_tmax_gdpcir.py --workers 8 --outdir ./tmax_france
```

---

## 4. The Google Cloud run

A small **spot VM** runs all 25 models with `--workers 8`, storing outputs on the VM's
local disk (no bucket).

> **Region: `us-west1-b`.** GDPCIR is hosted on **Azure West Europe** (`rhgeuwest`), so
> on latency alone a European GCP region would be ideal — **but this project has an org
> policy (`constraints/gcp.resourceLocations`) that restricts resources to US locations**,
> so `europe-west1` is blocked (its VPC has no subnet there). We therefore run in
> `us-west1-b` — the same region/zone as the `cordex-tmax` VM, with proven spot capacity.
> Reads cross the Atlantic to Azure-Europe (a bit higher per-request latency) but are
> still free; the job is resumable, so the only cost is some extra wall-clock.

### What to launch
| | |
|---|---|
| Instance | `gdpcir-tmax` |
| Project / zone | `bullet-climate-analysis` / `us-west1-b` |
| Machine | `e2-standard-8` (8 vCPU, 32 GB RAM) — network-bound, no need for 32 vCPU |
| Provisioning | **SPOT**, `--instance-termination-action=STOP` |
| Boot disk | 50 GB pd-ssd |
| Output dir on VM | `/opt/gdpcir/tmax_france/` |
| Logs on VM | `/opt/gdpcir/startup.log`, `/opt/gdpcir/run.log` |

Create command (run **from inside `CIL-GDPCIR/`**):
```bash
gcloud compute instances create gdpcir-tmax \
  --project=bullet-climate-analysis --zone=us-west1-b \
  --machine-type=e2-standard-8 \
  --provisioning-model=SPOT --instance-termination-action=STOP \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=50GB --boot-disk-type=pd-ssd \
  --metadata-from-file=startup-script=gcp_startup_gdpcir.sh,gdpcir-code=france_tmax_gdpcir.py
```
**No credential metadata** is needed (unlike the EURO-CORDEX VM's `cdsapirc`).

### How resume-on-preemption works
- **`--instance-termination-action=STOP`**: if Google reclaims the spot VM it **stops**
  (not deletes) — the disk and all saved outputs survive.
- The worker code travels as the **`gdpcir-code` instance metadata** value.
- [`gcp_startup_gdpcir.sh`](gcp_startup_gdpcir.sh) runs on **every boot**: installs deps
  once (`.setup_done` marker), rewrites the worker from metadata, and (unless
  `/opt/gdpcir/.complete` exists or the job is already running) relaunches it detached.
- **Resume is at YEAR granularity.** Each year's France subset is written to
  `tmax_france/yearly/<model>/...` via an **atomic** temp-file rename, so a crash never
  leaves a half-written "done" file. On restart the worker skips cached years and reads
  only the missing ones, then concatenates them into the final per-model file.
- **Self-stop on completion.** On a clean exit the job writes `/opt/gdpcir/.complete`; a
  detached `selfstop.sh` watcher then powers the VM **off once** (guarded by
  `.stopped_once`) so compute billing stops while the output disk is preserved. Start
  the VM again to scp the data — it will not immediately re-stop.
- **Update the worker on a live/stopped VM** (takes effect next boot):
  ```bash
  gcloud compute instances add-metadata gdpcir-tmax --zone=us-west1-b \
    --metadata-from-file=gdpcir-code=france_tmax_gdpcir.py
  ```

Resume after a preemption is one command:
```bash
gcloud compute instances start gdpcir-tmax --zone=us-west1-b
```

---

## 5. Operating runbook

**Check progress**
```bash
gcloud compute ssh gdpcir-tmax --zone=us-west1-b --command \
  'echo "outputs: $(ls /opt/gdpcir/tmax_france/*.nc 2>/dev/null | wc -l)/25"; tail -n 30 /opt/gdpcir/run.log'
```

**Is it done?**
```bash
gcloud compute ssh gdpcir-tmax --zone=us-west1-b --command \
  '[ -f /opt/gdpcir/.complete ] && echo COMPLETE || echo "running ($(ls /opt/gdpcir/tmax_france/*.nc 2>/dev/null | wc -l) outputs)"'
```
(If the VM self-stopped on completion, `start` it first, then check.)

**Retrieve the data to your laptop** (VM must be running)
```bash
gcloud compute instances start gdpcir-tmax --zone=us-west1-b   # if self-stopped
gcloud compute scp --recurse \
  gdpcir-tmax:/opt/gdpcir/tmax_france ./tmax_france_gdpcir_results \
  --zone=us-west1-b
```

**Tear down when finished** (stops billing for compute **and** disk)
```bash
gcloud compute instances delete gdpcir-tmax --zone=us-west1-b --quiet
```
The external IP can change across stop/start — always address the VM by name.

---

## 6. Testing completion

Three levels: the run **finished**, it produced **all expected files**, and those files
are **scientifically valid**.

### a) The run finished
`/opt/gdpcir/.complete` is written **only on a clean exit (code 0)**, and the log ends
with a `===== Summary =====` block. On the VM:
```bash
[ -f /opt/gdpcir/.complete ] && echo "COMPLETE" || echo "NOT done yet"
grep -aE "OK ->|ERROR|skipped" /opt/gdpcir/run.log   # per-model outcomes
```
Each model ends in `OK -> ...`, `skipped` (already done), or `ERROR: ...` (rerun
resumes it). A model that lacks ssp245 is logged as `no tasmax item ... skipped`.

### b) All expected files are present
```bash
ls /opt/gdpcir/tmax_france/*.nc | wc -l        # expect up to 25
# list default-ensemble models with NO output file (run from /opt/gdpcir):
cd /opt/gdpcir
comm -23 \
  <(python3 -c "import france_tmax_gdpcir as f;print('\n'.join(sorted(f.DEFAULT_MODELS)))") \
  <(ls /opt/gdpcir/tmax_france/*.nc 2>/dev/null | sed -E 's#.*/tasmax_france_(.*)_ssp245_.*#\1#' | sort)
```
Re-running the same command regenerates only the missing/failed models.

### c) Files are scientifically valid
Open each file and check dimensions, the continuous 2000–2080 span (historical→ssp
splice with no gap), and plausible values. **Use float64 for any mean** (the project's
float32-mean gotcha):
```bash
python3 - <<'PY'
import glob, xarray as xr
for f in sorted(glob.glob("/opt/gdpcir/tmax_france/*.nc")):  # or your local dir
    ds = xr.open_dataset(f, use_cftime=True)
    tx = ds.tasmax
    t0, t1 = str(ds.time.values[0])[:10], str(ds.time.values[-1])[:10]
    kmin, kmax = float(tx.min()), float(tx.max())        # min/max fine in float32
    mean_c = float(tx.astype("float64").mean()) - 273.15  # mean MUST be float64
    ok = (tx.sizes["lat"] == 42 and tx.sizes["lon"] == 61
          and t0 == "2000-01-01" and t1.startswith("2080")
          and 220 < kmin and kmax < 330)
    print(f"{'OK ' if ok else 'BAD'} {f.split('/')[-1]:52s} "
          f"days={tx.sizes['time']} {t0}->{t1} K[{kmin:.1f},{kmax:.1f}] mean={mean_c:.1f}C")
    ds.close()
PY
```
Expected per file: `lat=42, lon=61`, span `2000-01-01 -> 2080-12-31`, **29,565 days**
(81 yr × 365 — GDPCIR is noleap for *every* model, so no calendar branching), `tasmax`
≈ 240–320 K, area mean ≈ 14–16 °C. (Verified smoke test on MIROC6 2013–2016: 1460 days,
42×61, mean 15.7 °C, seamless across the 2014→2015 splice.)

---

## 7. Sizing / cost notes
- **Reads are free** — Planetary Computer serves these as signed open data (no account,
  no egress fee). The only real cost is the GCP VM.
- **Network-bound, not CPU-bound.** Each model pulls only the France column of 90°×90°
  Zarr chunks (~0.5 min/year when the VM is in Europe; somewhat slower from `us-west1`
  across the Atlantic). A model is ~81 years; `--workers 8` finishes the 25-model
  ensemble in a few hours. An `e2-standard-8` is ample — no need for a 32-vCPU VM like
  the NEX-GDDP job.
- **RAM**: each worker decompresses a couple of 90° chunks at a time (~hundreds of MB) →
  8 workers fit comfortably in 32 GB.
- **Disk**: outputs are small (~each model is 42×61×29,565 float32 ≈ a few hundred MB
  compressed; the whole ensemble is a handful of GB) → a 50 GB boot disk is plenty.
- **Region**: this project's org policy restricts resources to **US locations**, so the
  VM runs in `us-west1-b` (alongside `cordex-tmax`). A European region would give faster
  reads but is blocked here; reads are free regardless.
