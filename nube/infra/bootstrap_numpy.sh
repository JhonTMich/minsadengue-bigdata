#!/bin/bash
set -euo pipefail
if ! python3 -m pip --version >/dev/null 2>&1; then
  sudo dnf install -y -q python3-pip
fi
sudo python3 -m pip install --quiet "numpy==1.26.4"
python3 -c "import numpy; print('numpy', numpy.__version__)"
