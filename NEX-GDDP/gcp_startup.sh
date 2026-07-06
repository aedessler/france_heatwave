#!/bin/bash
# Startup script for the France TMax worker VM.
# Runs as root on EVERY boot, so a spot preemption -> STOP -> `instances start`
# automatically resumes the job. The Python worker skips any model whose output
# .nc already exists, so resume happens at model granularity.
set -u
JOBDIR=/opt/france
OUT=$JOBDIR/tmax_france
mkdir -p "$JOBDIR" "$OUT"
exec >>"$JOBDIR/startup.log" 2>&1
echo "===== startup $(date -u) ====="

# --- one-time environment setup (idempotent) -------------------------------
if [ ! -f "$JOBDIR/.setup_done" ]; then
  echo "installing python + deps..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y python3-pip python3-venv
  python3 -m venv "$JOBDIR/venv"
  "$JOBDIR/venv/bin/pip" install --upgrade pip
  "$JOBDIR/venv/bin/pip" install \
    "xarray>=2023.1" "s3fs>=2023.1" "h5netcdf>=1.1" "netCDF4>=1.6" \
    "cftime>=1.6" "dask>=2023.1"
  touch "$JOBDIR/.setup_done"
fi

# --- fetch the worker code from instance metadata (fresh each boot) ---------
curl -s -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/france-code" \
  -o "$JOBDIR/france_tmax_cmip6.py"

# --- write the run wrapper --------------------------------------------------
cat >"$JOBDIR/run.sh" <<RUNEOF
#!/bin/bash
cd "$JOBDIR"
# clear any temp scratch left over from an interrupted (preempted) run
rm -rf /tmp/nexgddp_* 2>/dev/null || true
echo "===== job start \$(date -u) ====="
"$JOBDIR/venv/bin/python" "$JOBDIR/france_tmax_cmip6.py" \
    --scenario ssp245 --start-year 2000 --end-year 2080 \
    --workers 32 --executor process --outdir "$OUT"
rc=\$?
echo "===== job exit code \$rc at \$(date -u) ====="
if [ \$rc -eq 0 ]; then touch "$JOBDIR/.complete"; fi
RUNEOF
chmod +x "$JOBDIR/run.sh"

# --- launch (skip if already finished or already running) -------------------
if [ -f "$JOBDIR/.complete" ]; then
  echo "job already complete; nothing to do."
  exit 0
fi
if pgrep -f "france_tmax_cmip6.py" >/dev/null; then
  echo "job already running; leaving it alone."
  exit 0
fi
echo "launching job in background..."
setsid bash "$JOBDIR/run.sh" >>"$JOBDIR/run.log" 2>&1 </dev/null &
echo "launched."
