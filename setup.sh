#!/usr/bin/env bash
set -euo pipefail

CLUSTER=es-demo

echo "==> Tearing down any existing cluster..."
kind delete cluster --name "$CLUSTER" 2>/dev/null || true

echo "==> Creating kind cluster..."
kind create cluster --name "$CLUSTER"

echo "==> Applying infra manifests..."
kubectl apply -f infra.yaml

echo "==> Waiting for deployments to be ready..."
kubectl rollout status deployment/postgres --timeout=120s
kubectl rollout status deployment/nats     --timeout=120s
kubectl rollout status deployment/redis    --timeout=120s

echo "==> Starting port-forwards (background)..."
kubectl port-forward svc/postgres 5432:5432 &
kubectl port-forward svc/nats     4222:4222 &
kubectl port-forward svc/redis    6379:6379 &

# Give port-forwards a moment to establish
sleep 2

echo ""
echo "Infrastructure ready:"
echo "  PostgreSQL  localhost:5432"
echo "  NATS        localhost:4222"
echo "  Redis       localhost:6379"
echo ""
echo "Start the server with:"
echo "  go mod tidy && go run ./cmd/server"
