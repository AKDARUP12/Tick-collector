#!/usr/bin/env bash
set -e
echo "--- checking python ---"
python3 --version

echo "--- creating virtual environment in .venv ---"
if [ ! -d .venv ]; then
  python3 -m venv .venv
else
  echo ".venv already exists, reusing"
fi

echo "--- installing packages ---"
./.venv/bin/pip install --upgrade pip -q
./.venv/bin/pip install -r requirements.txt

echo ""
echo "Done. Activate with:"
echo "    source .venv/bin/activate"
echo ""
echo "Copy env template and edit credentials:"
echo "    cp .env.example .env && nano .env"
echo ""
echo "Then verify:"
echo "    .venv/bin/python -c 'from pyarrow_client import ArrowClient; print(\"ok\")'"
