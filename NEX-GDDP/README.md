# NEX-GDDP-CMIP6 ensemble extractions

> See [`../README.md`](../README.md) for the project overview and cross-cutting info
> (directory map, the shared GCP spot-VM / resumable-cache pattern, the float32-mean
> gotcha, the France window). This file covers the NEX-GDDP pipelines' details.

> **Location note.** This pipeline now lives in its own `NEX-GDDP/` folder (separated
> from the new `EURO-CORDEX/` work). Run all the `gcloud`/`scp` commands below **from
> inside `NEX-GDDP/`** — they reference the worker scripts by relative path. The
> second (global-mean `tas`) pipeline's files were moved to
> `../extra stuff/global average/`; the §8 links point there.

Two parallel pipelines over the NASA NEX-GDDP-CMIP6 downscaled-CMIP6 ensemble, each
running on its own Google Cloud spot VM:

1. **France daily TMax** (`tasmax`) over France, **2000–2080** — the main focus of
   this README (§1–§7).
   - Worker: [`france_tmax_cmip6.py`](france_tmax_cmip6.py)
   - Boot/resume: [`gcp_startup.sh`](gcp_startup.sh) · VM `france-tmax`
2. **Monthly global-mean `tas`** per model, **1950–2100** (area-weighted over the
   dataset's 60°S–90°N domain) — see [§8](#8-second-pipeline-monthly-global-mean-tas).
   - Worker: [`global_monthly_tas_cmip6.py`](../extra%20stuff/global%20average/global_monthly_tas_cmip6.py)
   - Boot/resume: [`gcp_startup_globaltas.sh`](../extra%20stuff/global%20average/gcp_startup_globaltas.sh) · VM `global-tas`

Shared Python deps: [`requirements.txt`](requirements.txt).

---

## 1. The data

**NASA Earth Exchange Global Daily Downscaled Projections (NEX-GDDP-CMIP6)** —
<https://registry.opendata.aws/nex-gddp-cmip6/>

| | |
|---|---|
| Bucket | `s3://nex-gddp-cmip6` (AWS region `us-west-2`) |
| Access | **Anonymous** (`s3fs.S3FileSystem(anon=True)`), egress sponsored by AWS Open Data → free to download |
| Format | NetCDF4 / HDF5, CF-1.7 |
| Grid | common global **0.25°** grid (lon 1440 × lat 600), **0–360 longitude** convention |
| Object layout | `NEX-GDDP-CMIP6/<model>/<scenario>/<variant>/<variable>/<var>_day_<model>_<scenario>_<variant>_<grid>_<year>[_v<x.y>].nc` |
| One file = | one (model, scenario, variable, year) |
| `tasmax` units | **Kelvin**; time is `days since 1850-01-01` |

**Scenarios.** `historical` covers 1950–2014; the SSPs (`ssp126/245/370/585`)
cover 2015–2100. For a continuous 2000–2080 series we **splice** `historical`
(years ≤ 2014) with the chosen SSP (years ≥ 2015). Default SSP here is **ssp245**.

### Real-world quirks the script handles
- **Longitude 0–360 + prime meridian.** France straddles 0° E, so longitudes are
  rewrapped to −180…180 and re-sorted before clipping to the bounding box.
- **Variant / grid / version vary per model.** Variant labels are `r1i1p1f1`,
  `r1i1p1f2`, `r4i1p1f1`, …; grid labels are `gn` or `gr`; some years have a
  corrected `_v1.1` / `_v2.0` rerun alongside the original. All of these are
  **discovered from S3**, not hardcoded, and corrected versions win.
- **Calendars differ** (noleap vs proleptic_gregorian), so each model is written
  to its **own** file rather than forced onto a shared time axis.
- **Missing temperature.** `CESM2` and `CESM2-WACCM` ship **no** temperature
  variables in this dataset → auto-skipped. `GFDL-CM4_gr2` is GFDL-CM4 on an
  alternate grid → excluded from the default list to avoid double-counting.

### How many models
- **35** model directories in the bucket
- **33** have `tasmax`
- **32** in the default ensemble (also dropping the duplicate `GFDL-CM4_gr2`)

List the live set anytime: `python france_tmax_cmip6.py --list-models`.

### Why whole yearly files are downloaded
`tasmax` is HDF5-chunked `(time=1, lat=600, lon=1440)` — one global slab per day.
Clipping to France still touches every daily chunk, so the whole yearly file
transfers regardless. The script therefore pulls each yearly file, clips it to the
France window (~42 × 61 cells), keeps that, and deletes the global file. The unit
of parallelism is the **model**, not the file.

---

## 2. The France window

Metropolitan France incl. Corsica, in −180…180 degrees east:

```
lon −5.5 … 9.8     lat 41.0 … 51.5
```

Resulting subset is a **42 × 61** grid. Edit `FRANCE` near the top of
`france_tmax_cmip6.py` to change it.

---

## 3. The worker script

`france_tmax_cmip6.py` processes each model end-to-end and writes one NetCDF:

```
tmax_france/tasmax_france_<model>_<scenario>_<start>_<end>.nc
```

For each model it: discovers the per-year files on S3 → downloads each yearly
global file to temp scratch → clips to France → concatenates 2000–2014
(historical) + 2015–2080 (ssp) into one continuous series → writes a compressed
NetCDF (`tasmax` in K) with provenance attributes (model, variant, grid, region,
history).

### CLI options
| Flag | Default | Meaning |
|---|---|---|
| `--scenario` | `ssp245` | SSP for years > 2014 (`ssp126/245/370/585`) |
| `--start-year` / `--end-year` | `2000` / `2080` | inclusive year range |
| `--models` | `all` | comma list, or `all` = the default 32 |
| `--outdir` | `./tmax_france` | where outputs are written |
| `--workers` | `1` | **ensemble members processed simultaneously** (the parallel switch) |
| `--executor` | `process` | `process` (true parallelism) or `thread` |
| `--overwrite` | off | re-create existing outputs (otherwise they're skipped → **resume**) |
| `--dry-run` | off | resolve the file plan without downloading |
| `--list-models` | off | print every model dir in the bucket and exit |

**Parallelism.** `--workers N` runs N models at once. `--workers 1` is serial
(easiest for debugging). On a big VM, set `--workers` to ≈ the vCPU count.

**Resumability** is built in: a model whose output `.nc` already exists is skipped
unless `--overwrite`. Re-running the same command continues where it stopped.

### Local quick start
```bash
pip install -r requirements.txt

# Debug: two models, a short range, serial
python france_tmax_cmip6.py --models ACCESS-CM2,MIROC6 --start-year 2000 --end-year 2003

# Preview the file plan without downloading
python france_tmax_cmip6.py --dry-run

# Full ensemble, parallel
python france_tmax_cmip6.py --workers 16 --outdir ./tmax_france
```

---

## 4. The Google Cloud run

A single **`n2-standard-32` spot VM** runs all 30 models with `--workers 32`,
storing outputs **on the VM's local disk** (no bucket).

> **Preferred region: `us-west1` (The Dalles, Oregon).** Both this job and the
> global-`tas` job run here, in zone **`us-west1-b`**. Two reasons: (1) it is
> **co-located with the source data** — the NEX-GDDP-CMIP6 Open Data bucket lives in
> AWS `us-west-2` (Oregon), so cross-cloud reads are fast and low-latency; and (2) in
> practice it has had the **most spot capacity** of the West-Coast regions. We
> originally launched in `us-west2` (Los Angeles) but abandoned it on 2026-06-26 after
> repeated spot preemptions; `us-west1` has run preemption-free since. If a specific
> zone is capacity-constrained, `us-west1-a` / `us-west1-c` are fine fallbacks — keep
> the region, change the zone.

### What was launched
| | |
|---|---|
| Instance | `france-tmax` |
| Project / zone | `bullet-climate-analysis` / `us-west1-b` (The Dalles, Oregon — moved from us-west2-a/LA on 2026-06-26 after repeated spot preemptions; us-west1 has more spot capacity and is co-located with the AWS `us-west-2` Oregon bucket) |
| Machine | `n2-standard-32` (32 vCPU, 128 GB RAM) |
| Provisioning | **SPOT**, `--instance-termination-action=STOP` |
| Boot disk | 100 GB pd-ssd |
| Output dir on VM | `/opt/france/tmax_france/` |
| Logs on VM | `/opt/france/startup.log`, `/opt/france/run.log` |

Create command (already run):
```bash
gcloud compute instances create france-tmax \
  --project=bullet-climate-analysis --zone=us-west1-b \
  --machine-type=n2-standard-32 \
  --provisioning-model=SPOT --instance-termination-action=STOP \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=100GB --boot-disk-type=pd-ssd \
  --metadata-from-file=startup-script=gcp_startup.sh,france-code=france_tmax_cmip6.py
```

### How resume-on-preemption works
- **`--instance-termination-action=STOP`**: if Google reclaims the spot VM it
  **stops** (does not delete) — the disk and all saved outputs survive.
- The worker code travels as the **`france-code` instance metadata** value.
- [`gcp_startup.sh`](gcp_startup.sh) is the **startup script**, which GCP runs on
  **every boot**. Each boot it: installs deps once (`.setup_done` marker), rewrites
  the worker code from metadata, clears any orphaned `/tmp/nexgddp_*` scratch from
  an interrupted run, and (unless `/opt/france/.complete` exists or the job is
  already running) relaunches the job detached via `setsid`.
- **Resume is at YEAR granularity.** As each year is processed it is written to a
  per-model cache `tmax_france/yearly/<model>/tasmax_france_<model>_<scen>_<year>.nc`
  via an **atomic** temp-file rename (so a crash never leaves a half-written file
  that looks complete). On restart the worker **skips any year already cached** and
  re-downloads only the missing ones; once all of a model's years are present it
  concatenates them into the final `tasmax_france_<model>_...nc`. A finished model
  (final file present) is skipped entirely.
- **Updating the worker code on a live/stopped VM**: push new code to metadata and it
  takes effect on the next boot, without disturbing a running process:
  ```bash
  gcloud compute instances add-metadata france-tmax --zone=us-west1-b \
    --metadata-from-file=france-code=france_tmax_cmip6.py
  ```

So after a preemption, resuming is one command (it picks up from the cached years):
```bash
gcloud compute instances start france-tmax --zone=us-west1-b
```

The job does **not** auto-shutdown on completion; it writes `/opt/france/.complete`
and leaves the VM running so you can pull the data.

---

## 5. Operating runbook

**Check progress**
```bash
gcloud compute ssh france-tmax --zone=us-west1-b --command \
  'echo "outputs: $(ls /opt/france/tmax_france/*.nc 2>/dev/null | wc -l)/30"; tail -n 30 /opt/france/run.log'
```

**Is it done?** — quick check (see [§6](#6-testing-completion) for full verification):
```bash
gcloud compute ssh france-tmax --zone=us-west1-b --command \
  '[ -f /opt/france/.complete ] && echo COMPLETE || echo "running ($(ls /opt/france/tmax_france | wc -l) outputs)"'
```

**Retrieve the data to your laptop** (run on your machine, VM must be running)
```bash
gcloud compute scp --recurse \
  france-tmax:/opt/france/tmax_france ./tmax_france_results \
  --zone=us-west1-b
```

**Resume after a spot preemption**
```bash
gcloud compute instances start france-tmax --zone=us-west1-b
# startup script auto-resumes; no other action needed
```

**Tear down when finished** (stops billing for compute **and** disk)
```bash
gcloud compute instances delete france-tmax --zone=us-west1-b --quiet
```
Use `stop` instead of `delete` if you want to keep the disk to resume later (you
still pay for the disk while stopped). The external IP can change across stop/start
— always address the VM by name, as the commands above do.

---

## 6. Testing completion

Completion has three levels: the run **finished**, it produced **all expected
files**, and those files are **scientifically valid**. Check all three before you
tear the VM down.

### a) The run finished
The job writes `/opt/france/.complete` **only on a clean exit (code 0)**, and the
log ends with a `===== Summary =====` block listing every model. On the VM:
```bash
[ -f /opt/france/.complete ] && echo "COMPLETE" || echo "NOT done yet"
tail -n 40 /opt/france/run.log          # look for the Summary block
grep -aE "OK ->|ERROR|skipped" /opt/france/run.log   # per-model outcomes
```
Each model ends in exactly one of: `OK -> ...` (written), `skipped` (e.g. CESM2 /
CESM2-WACCM have no tasmax), or `ERROR: ...` (failed — rerun resumes it).

### b) All expected files are present
The default ensemble is **30 models → 30 files** (`.complete` only appears if the
process exited 0, but a model that hit `ERROR` is logged and *not* re-attempted in
the same run, so always confirm the count and re-run if short):
```bash
ls /opt/france/tmax_france/*.nc | wc -l        # expect 30
# list any default-ensemble models that have NO output file (run from /opt/france):
cd /opt/france
comm -23 \
  <(python3 -c "import france_tmax_cmip6 as f;print('\n'.join(sorted(f.DEFAULT_MODELS)))") \
  <(ls /opt/france/tmax_france/ | sed -E 's/tasmax_france_(.*)_ssp245_.*/\1/' | sort)
```
If any are missing, just re-run the same command (`bash /opt/france/run.sh`, or
restart the VM) — existing files are skipped and only the missing models download.

### c) Files are scientifically valid
Open each file and sanity-check the dimensions, the continuous 2000–2080 span
(historical→ssp splice with no gap), and physically plausible values. Run this on
the VM, or on your laptop after the scp in [§5](#5-operating-runbook):
```bash
python3 - <<'PY'
import glob, xarray as xr
bad = []
for f in sorted(glob.glob("/opt/france/tmax_france/*.nc")):  # or your local dir
    ds = xr.open_dataset(f, use_cftime=True)
    tx = ds.tasmax
    t0, t1 = str(ds.time.values[0])[:10], str(ds.time.values[-1])[:10]
    kmin, kmax = float(tx.min()), float(tx.max())
    ok = (tx.sizes["lat"] == 42 and tx.sizes["lon"] == 61
          and t0 == "2000-01-01" and t1.startswith("2080")
          and 220 < kmin and kmax < 330)        # plausible K range over France
    print(f"{'OK ' if ok else 'BAD'} {f.split('/')[-1]:55s} "
          f"days={tx.sizes['time']} {t0}->{t1} K[{kmin:.1f},{kmax:.1f}]")
    if not ok: bad.append(f)
    ds.close()
print("\nALL VALID" if not bad else f"\n{len(bad)} FILE(S) FAILED CHECKS")
PY
```
Expected per file: `lat=42, lon=61`, span `2000-01-01 -> 2080-12-31`, and `tasmax`
in Kelvin roughly 240–320 K (≈ −33 to +47 °C). Total days are calendar-dependent:
**29,565** for `noleap` models (365 × 81) and **29,586** for standard calendars
(81 years + 21 leap days, 2000–2080). A file that is `BAD`, truncated, or missing
days indicates an interrupted write — delete it and re-run to regenerate.

---

## 7. Sizing / cost notes
- **Transfer**: ~30 models × 81 yearly files × ~236 MB ≈ **~570 GB** pulled from
  S3 (free egress). Outputs total only ~**5 GB**.
- **RAM**: each worker accumulates ~0.3–0.4 GB of clipped data before writing;
  32 workers ≈ ~12 GB → 128 GB VM is comfortable.
- **Scratch disk**: ~236 MB × `--workers` in `/tmp` at peak (≈ 7.5 GB) → 100 GB
  boot disk is plenty alongside the ~5 GB of outputs.
- **Network** is the wall-clock bottleneck; running in `us-west1` (Oregon, the same
  region as the AWS `us-west-2` bucket) keeps it fast.

---

## 8. Second pipeline: monthly global-mean `tas`

[`global_monthly_tas_cmip6.py`](../extra%20stuff/global%20average/global_monthly_tas_cmip6.py)
(now in `../extra stuff/global average/`) computes, for each model,
a **monthly area-weighted mean `tas`** (daily-mean near-surface temperature) series,
**1950–2100** (historical + ssp245). It reuses the same S3 discovery / download /
splice machinery; the only difference is it replaces the France clip with a
cos-latitude-weighted spatial mean, then a monthly resample.

**Two caveats baked into the output attributes:**
- **Starts 1950**, not 1850 — NEX-GDDP-CMIP6 has no pre-1950 data (a true
  mid-19th-century baseline would need raw, non-downscaled CMIP6).
- **Domain is 60°S–90°N** (the dataset's grid: lat −59.875…89.875). This is an
  area-weighted mean over that band, **not** a true global mean — Antarctica and the
  Southern Ocean below 60°S are excluded, which biases the value slightly warm
  (~14.5 °C vs the ~14 °C true global mean).

**Model count.** *Every* model dir has `tas` (unlike `tasmax`, which CESM2 /
CESM2-WACCM / IITM-ESM lack), so before exclusions there are 34 (35 dirs minus the
duplicate `GFDL-CM4_gr2`). `HadGEM3-GC31-MM` is also dropped — it has only
historical/ssp126/ssp585 (no ssp245) — leaving a default ensemble of **33** under
ssp245. (For ssp126/ssp585 you could add HadGEM3-GC31-MM back via `--models`.)

**Output** (in `--outdir`, default `./tas_global`):
- `globalmean_tas_<model>_ssp245_1950_2100.nc` — monthly `tas` (K), native calendar
- `globalmean_tas_<model>_ssp245_1950_2100.csv` — `year, month, tas_K, tas_C`
- `globalmean_tas_all_models_ssp245_1950_2100.csv` — combined tidy table across models
- `yearly/tas_<model>/...` — per-year monthly-mean cache (see resume note below)

**Resume is at YEAR granularity** (same mechanism as the France job): each year's
monthly-mean series is written to `tas_global/yearly/tas_<model>/` via an atomic
rename as soon as it is reduced, so a crash/preemption re-downloads only the missing
years on restart, then concatenates the cache into the final file.

**CLI** mirrors the France script, plus `--variable` (default `tas`; accepts
`tasmax`/`tasmin`). Parallel switch is the same `--workers N` (one model per worker).

```bash
# Debug: two models (one gregorian, one noleap), short range
python global_monthly_tas_cmip6.py --models ACCESS-CM2,GISS-E2-1-G \
    --start-year 2014 --end-year 2015 --workers 2 --outdir ./out_tas_test

# Full run, 33 models in parallel
python global_monthly_tas_cmip6.py --workers 32 --outdir ./tas_global
```

### GCP run (separate VM `global-tas`, zone `us-west1-b`)
Created like `france-tmax` but in **`us-west1`** (The Dalles, Oregon — more spot
capacity than us-west2/LA, and co-located with the AWS `us-west-2` Oregon bucket for
fast downloads), with the global-tas startup script and code (run this
`--metadata-from-file` command **from inside `../extra stuff/global average/`**, where
those two files now live):
```bash
gcloud compute instances create global-tas \
  --project=bullet-climate-analysis --zone=us-west1-b \
  --machine-type=n2-standard-32 \
  --provisioning-model=SPOT --instance-termination-action=STOP \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=100GB --boot-disk-type=pd-ssd \
  --metadata-from-file=startup-script=gcp_startup_globaltas.sh,globaltas-code=global_monthly_tas_cmip6.py
```
Job dir on the VM is `/opt/globaltas/` (outputs in `/opt/globaltas/tas_global/`,
logs `run.log` / `startup.log`, done-marker `/opt/globaltas/.complete`). Resume,
retrieve, and teardown are identical to [§5](#5-operating-runbook) — just substitute
`global-tas` for `france-tmax`, `/opt/globaltas` for `/opt/france`, and
`--zone=us-west1-b`. The two VMs are independent and run concurrently.

**Operate it:**
```bash
# progress
gcloud compute ssh global-tas --zone=us-west1-b --command \
  'echo "done: $(ls /opt/globaltas/tas_global/*.nc 2>/dev/null | wc -l)/33"; tail -n 20 /opt/globaltas/run.log'
# retrieve (tiny: a few MB)
gcloud compute scp --recurse global-tas:/opt/globaltas/tas_global ./tas_global_results --zone=us-west1-b
# tear down
gcloud compute instances delete global-tas --zone=us-west1-b --quiet
```

**Validity check** — each file should have **1812 months** (151 yr × 12), span
`1950-01 → 2100-12`, no NaN, and `tas` ≈ 270–300 K monthly. Sizing is similar to the
France job but transfers more (~32 × 151 × ~230 MB ≈ **~1.1 TB**, free egress); outputs
are only a few MB. Per-worker memory stays low because the spatial mean is computed
with dask `chunks={"time": 30}` rather than loading whole global years.
