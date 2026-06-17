#!/usr/bin/env bash
set -euo pipefail

echo "==> Killing port-forwards..."
pkill -f "kubectl port-forward" 2>/dev/null || true

echo "==> Deleting kind cluster..."
kind delete cluster --name es-demo

echo "Done."
