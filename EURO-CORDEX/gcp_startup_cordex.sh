#!/bin/bash
# Startup script for the EURO-CORDEX France TMax worker VM.
# Runs as root on EVERY boot, so a spot preemption -> STOP -> `instances start`
# automatically resumes the job. The worker caches each 5-year block atomically and
# skips cached blocks on restart, so resume happens at 5-year-block granularity.
#
# On clean completion the job writes /opt/cordex/.complete, and a systemd watcher
# (installed below) powers the VM off ONCE so it stops billing compute while the
# output disk is preserved for you to start + scp. A guard marker (.stopped_once)
# ensures it does not immediately power off again when you restart it to download.
set -u
JOBDIR=/opt/cordex
OUT=$JOBDIR/tmax_france_cordex
mkdir -p "$JOBDIR" "$OUT"
exec >>"$JOBDIR/startup.log" 2>&1
echo "===== startup $(date -u) ====="

md() {  # read an instance-metadata attribute
  curl -s -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"
}

# --- one-time environment setup (idempotent) -------------------------------
if [ ! -f "$JOBDIR/.setup_done" ]; then
  echo "installing python + deps..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y python3-pip python3-venv unzip
  python3 -m venv "$JOBDIR/venv"
  "$JOBDIR/venv/bin/pip" install --upgrade pip
  "$JOBDIR/venv/bin/pip" install \
    "cdsapi>=0.7" "xarray>=2023.1" "netCDF4>=1.6" \
    "cftime>=1.6" "numpy>=1.23" "pandas>=1.5"
  touch "$JOBDIR/.setup_done"
fi

# --- CDS credential (SECRET): write ~/.cdsapirc from instance metadata -------
# Pass the credential as the `cdsapirc` metadata attribute (its full contents:
#   url: https://cds.climate.copernicus.eu/api
#   key: <UID>:<API-KEY>
# ). It is project-private metadata, but still a secret -- do NOT commit it.
if md cdsapirc > /root/.cdsapirc.new 2>/dev/null && [ -s /root/.cdsapirc.new ]; then
  mv /root/.cdsapirc.new /root/.cdsapirc
  chmod 600 /root/.cdsapirc
  echo "wrote /root/.cdsapirc from metadata."
else
  rm -f /root/.cdsapirc.new
  echo "WARNING: no 'cdsapirc' metadata found -- CDS retrieval will fail until set."
fi

# --- fetch the worker code from instance metadata (fresh each boot) ----------
curl -s -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/cordex-code" \
  -o "$JOBDIR/france_tmax_cordex.py"

# --- write the run wrapper --------------------------------------------------
cat >"$JOBDIR/run.sh" <<RUNEOF
#!/bin/bash
cd "$JOBDIR"
export HOME=/root            # so cdsapi finds /root/.cdsapirc
rm -rf /tmp/cordex_* 2>/dev/null || true   # clear scratch from an interrupted run
echo "===== job start \$(date -u) ====="
"$JOBDIR/venv/bin/python" "$JOBDIR/france_tmax_cordex.py" \
    --start-year 2006 --end-year 2080 \
    --workers 3 --executor process --outdir "$OUT"
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
JOBDIR=/opt/cordex
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
if pgrep -f "france_tmax_cordex.py" >/dev/null; then
  echo "job already running; leaving it alone."
  exit 0
fi
echo "launching job in background..."
setsid bash "$JOBDIR/run.sh" >>"$JOBDIR/run.log" 2>&1 </dev/null &
echo "launched."
