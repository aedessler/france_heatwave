#!/bin/bash
# Startup script for the CIL-GDPCIR France TMax worker VM.
# Runs as root on EVERY boot, so a spot preemption -> STOP -> `instances start`
# automatically resumes the job. The Python worker caches each year's France subset
# atomically and skips cached years on restart, so resume happens at YEAR granularity.
#
# Unlike the EURO-CORDEX sibling, GDPCIR needs NO secret/credential -- Planetary
# Computer asset signing is free and token-less for these public collections.
#
# On clean completion the job writes /opt/gdpcir/.complete, and a detached watcher
# (selfstop.sh, installed below) powers the VM off ONCE so it stops billing compute
# while the output disk is preserved for you to start + scp. A guard marker
# (.stopped_once) ensures it does not power off again when you restart it to download.
#
# Note: GDPCIR data is hosted on Azure West Europe (account rhgeuwest); a European VM
# would minimise latency, but this project's org policy restricts resources to US
# locations, so the VM runs in us-west1-b (reads cross the Atlantic but are still free).
set -u
JOBDIR=/opt/gdpcir
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
    "planetary-computer>=1.0" "pystac-client>=0.7" "xarray>=2023.1" \
    "zarr>=2.13" "adlfs>=2023.1" "dask>=2023.1" "netCDF4>=1.6" "cftime>=1.6"
  touch "$JOBDIR/.setup_done"
fi

# --- fetch the worker code from instance metadata (fresh each boot) ---------
curl -s -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/gdpcir-code" \
  -o "$JOBDIR/france_tmax_gdpcir.py"

# --- write the run wrapper --------------------------------------------------
cat >"$JOBDIR/run.sh" <<RUNEOF
#!/bin/bash
cd "$JOBDIR"
echo "===== job start \$(date -u) ====="
"$JOBDIR/venv/bin/python" "$JOBDIR/france_tmax_gdpcir.py" \
    --scenario ssp245 --start-year 2000 --end-year 2080 \
    --workers 8 --executor process --outdir "$OUT"
rc=\$?
echo "===== job exit code \$rc at \$(date -u) ====="
if [ \$rc -eq 0 ]; then touch "$JOBDIR/.complete"; fi
RUNEOF
chmod +x "$JOBDIR/run.sh"

# --- self-stop watcher: power off ONCE when .complete appears ---------------
# Run as a detached background process (NOT a systemd unit): a unit ordered around
# multi-user.target would deadlock `systemctl --now` against this very startup script,
# which runs as google-startup-scripts.service while that target is still activating.
cat >"$JOBDIR/selfstop.sh" <<'STOPEOF'
#!/bin/bash
JOBDIR=/opt/gdpcir
while [ ! -f "$JOBDIR/.complete" ]; do sleep 60; done
if [ ! -f "$JOBDIR/.stopped_once" ]; then
  touch "$JOBDIR/.stopped_once"   # one-shot: don't re-stop after a manual restart
  echo "job complete $(date -u) -- powering off to stop compute billing." \
    >>"$JOBDIR/startup.log"
  sleep 10
  /sbin/shutdown -h now
fi
STOPEOF
chmod +x "$JOBDIR/selfstop.sh"
# launch the watcher detached, once (skip if already watching or already stopped once)
if [ ! -f "$JOBDIR/.stopped_once" ] && ! pgrep -f "$JOBDIR/selfstop.sh" >/dev/null; then
  setsid bash "$JOBDIR/selfstop.sh" >>"$JOBDIR/startup.log" 2>&1 </dev/null &
  echo "self-stop watcher armed."
fi

# --- launch the job (skip if already finished or already running) -----------
if [ -f "$JOBDIR/.complete" ]; then
  echo "job already complete; nothing to do."
  exit 0
fi
if pgrep -f "france_tmax_gdpcir.py" >/dev/null; then
  echo "job already running; leaving it alone."
  exit 0
fi
echo "launching job in background..."
setsid bash "$JOBDIR/run.sh" >>"$JOBDIR/run.log" 2>&1 </dev/null &
echo "launched."
