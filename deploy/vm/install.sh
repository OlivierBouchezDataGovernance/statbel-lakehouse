#!/usr/bin/env bash
# Installs the daily Statbel mirror on the VM (run on the VM, as olivier, from a copy of this repository).
#
#   bash deploy/vm/install.sh            # install or update, enable the timer
#   systemctl start statbel-mirror       # first run now
#   journalctl -u statbel-mirror -n 50   # result
#
# Before: the service account of the VM needs roles/storage.objectUser on the bucket (docs/install_eu.md, section 5).
#
# © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com
set -euo pipefail

SRC="$(cd "$(dirname "$0")/../.." && pwd)"
APP=/home/olivier/statbel-mirror
VENV=/home/olivier/.venvs/statbel-mirror

# only what the script reads: no code is pulled from GitHub by the service
mkdir -p "$APP/scripts" "$APP/config"
install -m 644 "$SRC/scripts/mirror_statbel.py" "$APP/scripts/"
install -m 644 "$SRC/config/datasets.yml" "$APP/config/"

[ -x "$VENV/bin/python" ] || python3 -m venv "$VENV"
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet "google-auth>=2.30" "requests>=2.32" "pyyaml>=6.0" "rdflib>=7.0"

sudo install -m 644 "$SRC/deploy/vm/statbel-mirror.service" "$SRC/deploy/vm/statbel-mirror.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now statbel-mirror.timer
systemctl list-timers statbel-mirror.timer --no-pager
