# EURO-CORDEX France daily TMax — RCP4.5, 2006–2080

> See [`../README.md`](../README.md) for the project overview and cross-cutting info
> (directory map, the shared GCP spot-VM / resumable-cache pattern, the float32-mean
> gotcha, the France window). This file covers EURO-CORDEX-specific details.

Regional-resolution companion to the NEX-GDDP-CMIP6 France TMax job (in `../NEX-GDDP/`).
Instead of NASA's statistically-downscaled 0.25° product, this pulls the
**dynamically-downscaled EURO-CORDEX EUR-11 (~12.5 km)** regional-climate ensemble and
clips daily `tasmax` to France — one NetCDF per **GCM × RCM** combination.

| | |
|---|---|
| Worker | [`france_tmax_cordex.py`](france_tmax_cordex.py) |
| Boot/resume | [`gcp_startup_cordex.sh`](gcp_startup_cordex.sh) · VM `cordex-tmax` |
| Deps | [`requirements.txt`](requirements.txt) |
| Outputs | `tmax_france_cordex_results/` (after you `scp` them down) |

**Scope (fixed):** `experiment = rcp_4_5` **only**, years **2006–2080**. No historical
run, **no splice** — RCP4.5 begins in 2006, so the series starts there.

---

## 1. The data

**EURO-CORDEX**, served from the **Copernicus Climate Data Store (CDS)**, dataset
[`projections-cordex-domains-single-levels`](https://cds.climate.copernicus.eu/datasets/projections-cordex-domains-single-levels).

| | |
|---|---|
| Access | **`cdsapi`** client + a **free CDS account/API key** in `~/.cdsapirc` (NOT anonymous — see §2) |
| Domain | `europe` = **EUR-11**, 0.11° (~12.5 km), **rotated-pole** grid |
| Variable | `maximum_2m_temperature_in_the_last_24_hours` = `tasmax` (**Kelvin**) |
| Temporal | `daily_mean` |
| Experiment | **`rcp_4_5`** only |
| Delivery | a **zip of NetCDF**, in **fixed 5-year blocks** (2006–2010, …, 2076–2080) |
| Grid in file | dims `rlat`,`rlon`; 2-D `lat(rlat,rlon)`/`lon(rlat,rlon)`; a `rotated_pole` grid-mapping var |

**Why this is on CDS and not S3.** The AWS `s3://euro-cordex` Open Data bucket does
**not** hold the daily `tasmax` *projections* — only CMIP5-CORDEX *monthly* `tas`/`pr`
and CMIP6-CORDEX ERA5 *evaluation* hindcasts. The genuine EUR-11 daily `tasmax`
projection ensemble lives on CDS, hence the `cdsapi` path.

### The ensemble — valid GCM × RCM pairs
EUR-11 was produced by ~6 driving GCMs × ~8 RCMs, but **only a subset of pairs** were
actually run for daily `tasmax` under RCP4.5. The worker starts from a curated list
(`DEFAULT_PAIRS`, 26 candidates) and **skips invalid combos gracefully**: if CDS
returns "no data" (a `400 "not a valid combination"`) for a member's *first* 5-year
block, that member is logged and skipped with no output.

**Actual result of the 2026-06-27 run: 11 of the 26 candidates were valid** (the other
15 don't exist on CDS and were skipped). The 11 members produced:

| Driving GCM | RCMs that exist for RCP4.5 daily tasmax |
|---|---|
| `cnrm_cerfacs_cm5` (CNRM-CM5) | `cnrm_aladin63`, `knmi_racmo22e`, `smhi_rca4` |
| `ichec_ec_earth` (EC-EARTH) | `knmi_racmo22e` |
| `mohc_hadgem2_es` (HadGEM2-ES) | `dmi_hirham5`, `knmi_racmo22e`, `smhi_rca4` |
| `mpi_m_mpi_esm_lr` (MPI-ESM-LR) | `smhi_rca4` |
| `ncc_noresm1_m` (NorESM1-M) | `dmi_hirham5`, `gerics_remo2015`, `smhi_rca4` |

To grow the ensemble, add known-valid pairs to `DEFAULT_PAIRS` (or pass `--pairs`); the
graceful-skip logic makes probing new combinations cheap. `members_index.csv` (written to
`--outdir`) records every candidate's final status (ok / exists / invalid).

> **CDS token caveat.** The `gcm_model` / `rcm_model` strings in the worker
> (`GCMS`/`RCMS`) must match the CDS form's exact controlled-vocabulary values. They
> are best-effort (e.g. `ipsl_ipsl_cm5a_mr`, which the original plan wrote as
> `ipsl_cm5a_mr`). Preview the matrix with `--list-models` and confirm tokens against
> the dataset's **Download** form; a wrong token simply appears as a skipped member.

### The France clip (native grid, **not** regridded)
Most CORDEX files are on a rotated-pole grid (dims `rlat`/`rlon`), but **some RCMs use
other grids** — e.g. CNRM-ALADIN delivers on a Lambert-Conformal grid with dims `y`/`x`.
The worker is **naming-agnostic**: it masks on the true 2-D `lat`/`lon` (rewrapping
longitude to −180…180), derives the horizontal dim names from `ds["lat"].dims`, then
takes the **smallest index box** on those dims containing France incl. Corsica
(`lon −5.5…9.8`, `lat 41.0…51.5`). Output stays on the **native model grid** with 2-D
`lat`/`lon` + the model's `grid_mapping` variable (`rotated_pole`, `Lambert_Conformal`,
…) retained — no interpolation. France sub-arrays are ~112×114 cells (rotated) or
~103×104 (ALADIN's y/x). (Optional future add-on: regrid to a regular 0.1° grid with
`xesmf`; out of scope to keep the native ~12.5 km grid and avoid a heavy dependency.)

---

## 2. Prerequisite — CDS credential (one-time)

Unlike NEX-GDDP's anonymous S3, CDS needs an account + API key:

1. Create a free account at <https://cds.climate.copernicus.eu> and log in.
2. Copy your key from your CDS profile page.
3. Write `~/.cdsapirc`:
   ```
   url: https://cds.climate.copernicus.eu/api
   key: <UID>:<API-KEY>
   ```
4. On the dataset page, **accept the dataset's licence/terms** once (otherwise
   retrievals 403 until you do).

> **Secret handling.** `~/.cdsapirc` is a credential — do **not** commit it. On GCP it
> is passed as private instance metadata (`cdsapirc` attribute) and the startup script
> writes it to `/root/.cdsapirc` (chmod 600). See §4.

---

## 3. The worker script

`france_tmax_cordex.py` processes each member end-to-end and writes:
```
tmax_france_cordex/tasmax_france_<gcm>_<rcm>_rcp45_2006_2080.nc
```
Per member it: requests each 5-year block from CDS → unzips → clips to France on the
rotated grid → **caches the block** (`blocks/<member>/…nc`, atomic temp-rename) →
concatenates the cached blocks into one continuous 2006–2080 series → writes a
compressed NetCDF (`tasmax` in K) with provenance attributes (domain, GCM, RCM,
ensemble, source, the rotated-grid/clip caveat, and any missing blocks). A
`members_index.csv` summarizing every member (status, days, grid, blocks) is written
to `--outdir` at the end.

### CLI options
| Flag | Default | Meaning |
|---|---|---|
| `--start-year` / `--end-year` | `2006` / `2080` | inclusive year range (RCP4.5 ⇒ ≥ 2006) |
| `--pairs` | — | explicit members, comma-separated `gcm:rcm` (overrides the matrix) |
| `--gcms` / `--rcms` | — | comma lists to **cross-product** into a matrix |
| `--outdir` | `./tmax_france_cordex` | where outputs + cache + index land |
| `--workers` | `1` | members processed at once — CDS is queue-bound, but **peak RAM ≈ workers × ~3 GB** (the per-member concat), so size to memory: 3 on a 16 GB box |
| `--executor` | `process` | `process` or `thread` (CDS work is IO-bound ⇒ `thread` is fine too) |
| `--overwrite` | off | re-create outputs **and re-download blocks** |
| `--dry-run` | off | print the per-member 5-year-block plan **without** calling CDS |
| `--list-models` | off | print the resolved GCM × RCM matrix and exit |

**Resumability.** Each 5-year block is cached as soon as it downloads via an atomic
temp-rename, and the **final per-member output is written the same way** (`.tmp` +
`os.replace`) so a worker killed mid-write (e.g. OOM) can never leave a truncated `.nc`
that a later run mistakes for "done". A crash/preemption only re-requests in-flight
blocks; a member whose final `.nc` exists is skipped unless `--overwrite`.

> **Rebuild a member from its cache without re-downloading:** delete its output `.nc`
> and re-run for that member **without** `--overwrite` (e.g.
> `--pairs gcm:rcm`). The cached blocks are reused (no CDS calls) and only the
> concat+write reruns — ~0.5 min. (`--overwrite` would force re-downloading all 15
> blocks, which you usually don't want.)

**`--dry-run` and `--list-models` work without `cdsapi` installed** (the import is
lazy), so you can inspect the plan anywhere.

### Local quick start
```bash
pip install -r requirements.txt

# Resolved member matrix:
python france_tmax_cordex.py --list-models

# Per-member block plan, no network:
python france_tmax_cordex.py --dry-run --pairs mpi_m_mpi_esm_lr:smhi_rca4

# Real smoke test: one combo, two blocks (2006–2015):
python france_tmax_cordex.py --pairs mpi_m_mpi_esm_lr:smhi_rca4 \
    --start-year 2006 --end-year 2015 --outdir ./out_cordex_test
```

---

## 4. The Google Cloud run

**CDS is throttle/queue-bound, not CPU-bound** — only a few concurrent requests per
user, each waiting in CDS's queue (minutes→hours under load). So this job uses a
**small** spot VM, not the 32-vCPU box the bandwidth-bound NEX-GDDP job used. Since the
work is queue-bound (not transfer-bound), VM↔CDS proximity barely matters; we run in
**us-west1** (where the project's default VPC has a subnet — its custom-mode default
network has no Europe subnet, and adding one wasn't worth it for a queue-bound job).

> **Memory, not CPU, sets the machine size.** Each member's final step concatenates all
> 15 cached blocks in memory (~2–3 GB for a full 2006–2080 member), so peak RAM ≈
> `--workers × ~3 GB`. The first run on an **`e2-standard-2` (8 GB)** with `--workers 6`
> **OOM-killed** a worker, which broke the whole `ProcessPoolExecutor` and failed 7
> members. The proven config is **`e2-standard-4` (16 GB) with `--workers 3`** (the
> startup script uses `--workers 3`). The full 11-member run then took ~1 hour wall-clock
> for ~$1–2.

| | |
|---|---|
| Instance | `cordex-tmax` |
| Project / zone | `bullet-climate-analysis` / `us-west1-b` |
| Machine | `e2-standard-4` (16 GB — sized for the per-member concat memory, **not** CPU) |
| Workers | `--workers 3` (set in `gcp_startup_cordex.sh`; keep `workers × ~3 GB ≤ RAM`) |
| Provisioning | **SPOT**, `--instance-termination-action=STOP` |
| Boot disk | 40 GB pd-balanced (outputs are small; raw blocks deleted per-block) |
| Output dir on VM | `/opt/cordex/tmax_france_cordex/` |
| Logs on VM | `/opt/cordex/startup.log`, `/opt/cordex/run.log` |

Create (pass the worker, the startup script, **and the CDS secret** as metadata):
```bash
gcloud compute instances create cordex-tmax \
  --project=bullet-climate-analysis --zone=us-west1-b \
  --machine-type=e2-standard-4 \
  --provisioning-model=SPOT --instance-termination-action=STOP \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=40GB --boot-disk-type=pd-balanced \
  --metadata-from-file=startup-script=gcp_startup_cordex.sh,cordex-code=france_tmax_cordex.py,cdsapirc=$HOME/.cdsapirc
```

### How resume + self-stop work
- **`--instance-termination-action=STOP`**: a spot preemption *stops* (doesn't delete)
  the VM — disk and all cached blocks survive.
- [`gcp_startup_cordex.sh`](gcp_startup_cordex.sh) runs on **every boot**: installs
  deps once (`.setup_done`), writes `/root/.cdsapirc` from the `cdsapirc` metadata
  secret, rewrites the worker from `cordex-code` metadata, clears orphaned
  `/tmp/cordex_*` scratch, and (unless `/opt/cordex/.complete` exists or the job is
  already running) relaunches the job detached.
- **Resume is at 5-year-block granularity** via the atomic block cache
  (`tmax_france_cordex/blocks/<member>/…`).
- **Self-stop on completion.** A detached `selfstop.sh` watcher (launched with
  `setsid` from the startup script) waits for `/opt/cordex/.complete`, then powers the
  VM off **once** (guarded by a `.stopped_once` marker) so compute billing stops while
  the output disk is preserved. When you later `start` the VM to download, it does
  **not** power off again. (It is deliberately **not** a systemd unit: a unit ordered
  around `multi-user.target` started via `systemctl --now` *from* the startup script
  deadlocks against `google-startup-scripts.service` — that bug stuck the very first
  boot; the `setsid` background pattern, the same one the job launch uses, avoids it.)
- **Update the worker on a live/stopped VM** (takes effect next boot):
  ```bash
  gcloud compute instances add-metadata cordex-tmax --zone=us-west1-b \
    --metadata-from-file=cordex-code=france_tmax_cordex.py
  ```

---

## 5. Operating runbook

```bash
# Progress
gcloud compute ssh cordex-tmax --zone=us-west1-b --command \
  'echo "outputs: $(ls /opt/cordex/tmax_france_cordex/*.nc 2>/dev/null | wc -l)"; tail -n 30 /opt/cordex/run.log'

# Done? The VM self-stops on completion, BUT a spot VM also goes TERMINATED on
# preemption — so TERMINATED is ambiguous. Disambiguate via the operations log:
#   guestTerminate (and no later preempt) = self-stopped = DONE
#   *preempt* with no later guestTerminate = preempted mid-run -> just `start` to resume
gcloud compute operations list --filter="targetLink~cordex-tmax" \
  --format='table(operationType,insertTime)' | tail -6
# (when RUNNING, `[ -f /opt/cordex/.complete ]` over ssh also confirms completion)

# Resume after a preemption (or to access the VM after self-stop)
gcloud compute instances start cordex-tmax --zone=us-west1-b

# Retrieve ONLY the outputs + index (NOT the blocks/ cache, which is several GB):
gcloud compute scp \
  cordex-tmax:'/opt/cordex/tmax_france_cordex/*.nc' \
  cordex-tmax:'/opt/cordex/tmax_france_cordex/members_index.csv' \
  ./tmax_france_cordex_results/ --zone=us-west1-b

# Tear down when finished (stops compute + disk billing)
gcloud compute instances delete cordex-tmax --zone=us-west1-b --quiet
```
The external IP changes across stop/start — always address the VM by name.

---

## 6. Verification

1. **Finished:** `/opt/cordex/.complete` exists and `run.log` ends with a
   `===== Summary =====` block; `members_index.csv` lists each member's status.
2. **Complete set:** one `tasmax_france_<gcm>_<rcm>_rcp45_2006_2080.nc` per resolved
   **valid** combo (count = `--list-models` minus logged skips).
3. **Valid:** open each file →
   - continuous daily series **2006-01 → 2080-12** (calendar-aware). Day counts depend
     on the model's calendar: **27,394** (standard/Gregorian, 2006–2080 incl. leaps),
     **27,375** (`noleap`, 75×365), **27,000** (`360_day`, 75×360). **A zero-length time
     axis = a corrupt/truncated file** — re-build it from its cached blocks (see §3).
   - `tasmax` mostly **270–305 K** with extremes ~**220–325 K** (rare cold high-Alps
     cells dip to ~222 K), no NaN. **Compute any mean in float64** — float32 means on
     these ~46 M-value fields are catastrophically wrong (see the root README).
   - 2-D `lat`/`lon` covering France incl. Corsica; horizontal dims are **`rlat`/`rlon`
     (~112×114)** for most RCMs or **`y`/`x` (~103×104)** for ALADIN — don't hardcode.
   ```bash
   python3 - <<'PY'
   import glob, xarray as xr, numpy as np
   for f in sorted(glob.glob("./tmax_france_cordex_results/*.nc")):
       ds = xr.open_dataset(f, use_cftime=True); tx = ds.tasmax
       nt = tx.sizes.get("time", 0)
       if nt == 0:
           print(f"CORRUPT (empty time): {f}"); ds.close(); continue
       sp = [d for d in tx.dims if d != "time"]          # rlat/rlon or y/x
       t0, t1 = str(ds.time.values[0])[:10], str(ds.time.values[-1])[:10]
       mean_c = float(tx.astype("float64").mean()) - 273.15   # float64!
       print(f"{f.split('/')[-1]:58s} {nt}d {t0}->{t1} "
             f"{sp[0]}x{sp[1]}={tx.sizes[sp[0]]}x{tx.sizes[sp[1]]} "
             f"mean={mean_c:.1f}C K[{float(tx.min()):.0f},{float(tx.max()):.0f}]")
       ds.close()
   PY
   ```

Retrieve via `scp`, verify, then delete the VM (verify-then-delete).

---

## 7. Key caveats / risks
- **Speed is CDS-queue-bound** → wall-clock is unpredictable, unlike the bandwidth-bound
  NEX-GDDP S3 job. (The 2026-06-27 run happened to clear the queue fast: ~1 hour for the
  full ensemble. It can be much longer under load.) The block cache makes the run safe to
  spread over a long period / across preemptions.
- **Ensemble is small:** of the 26 curated candidates, **only 11 exist on CDS** (§1). Many
  plausible-looking GCM×RCM pairs simply weren't run for daily RCP4.5 `tasmax`.
- **Memory-bound machine sizing:** the per-member concat needs ~2–3 GB, so RAM (not CPU)
  caps `--workers`; undersizing OOM-kills a worker and breaks the whole pool (§4).
- **Native grid, mixed conventions:** output is on each model's native grid (**not**
  regridded) — `rlat/rlon` for most RCMs, `y/x` for ALADIN. Don't assume rotated-pole.
- **Credential:** the CDS key is a secret (instance metadata on GCP; never committed).
