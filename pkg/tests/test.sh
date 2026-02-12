#!/bin/bash
#
# test.sh - Deploy and run comprehensive KeyDB Redis 8 tests
#
# This script automatically updates the test job with the current service ClusterIP
# to bypass VPN DNS issues that prevent service name resolution in k3s.
#

set -e

NAMESPACE="${KEYDB_NAMESPACE:-default}"
SERVICE_NAME="${KEYDB_SERVICE:-keydb}"
TEST_YAML="$(dirname "$0")/test.yaml"

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║     KeyDB Redis 8 - Comprehensive Test Deployment Script            ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# Fetch the service ClusterIP
echo "📡 Fetching service ClusterIP..."
SERVICE_IP=$(kubectl -n "$NAMESPACE" get svc "$SERVICE_NAME" -o jsonpath='{.spec.clusterIP}' 2>/dev/null)

if [ -z "$SERVICE_IP" ]; then
    echo "❌ Error: Service '$SERVICE_NAME' not found in namespace '$NAMESPACE'"
    echo "   Please ensure KeyDB is deployed:"
    echo "   helm -n $NAMESPACE install keydb ./pkg/helm"
    exit 1
fi

echo "✅ Found service: $SERVICE_NAME"
echo "   ClusterIP: $SERVICE_IP"
echo ""

# Clean up previous test job and all pods
echo "🗑️  Cleaning up previous test job..."
kubectl -n "$NAMESPACE" delete job keydb-comprehensive-test 2>/dev/null || true
kubectl -n "$NAMESPACE" delete pods -l app=keydb-test 2>/dev/null || true
kubectl -n "$NAMESPACE" delete configmap keydb-comprehensive-tests 2>/dev/null || true
sleep 2

# Create temporary test.yaml with the service IP
echo "🚀 Deploying comprehensive test job..."
sed "s/KEYDB_SERVICE_IP_PLACEHOLDER/$SERVICE_IP/g" "$TEST_YAML" | kubectl -n "$NAMESPACE" apply -f -

# Wait for the job to complete or fail
echo "⏳ Waiting for test to complete (timeout: 300s)..."
echo ""

# Wait for job to have a pod
POD_NAME=""
for i in {1..30}; do
  POD_NAME=$(kubectl -n "$NAMESPACE" get pods -l app=keydb-test --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)
  if [ -n "$POD_NAME" ]; then
    break
  fi
  sleep 1
done

if [ -z "$POD_NAME" ]; then
  echo "❌ No test pod found after 30 seconds"
  exit 1
fi

echo "📋 Monitoring pod: $POD_NAME"
echo ""

# Wait for pod to be created and ready to stream logs
kubectl -n "$NAMESPACE" wait --for=condition=ready pod/$POD_NAME --timeout=30s 2>/dev/null || true

# Stream logs in background
kubectl -n "$NAMESPACE" logs -f $POD_NAME 2>&1 &
LOG_PID=$!

# Wait for job to complete or fail
JOB_COMPLETE=0
JOB_FAILED=0

for i in {1..300}; do
  # Check if job is complete
  if kubectl -n "$NAMESPACE" get job keydb-comprehensive-test -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null | grep -q "True"; then
    JOB_COMPLETE=1
    break
  fi
  
  # Check if job failed
  if kubectl -n "$NAMESPACE" get job keydb-comprehensive-test -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null | grep -q "True"; then
    JOB_FAILED=1
    break
  fi
  
  # Check if pod is done
  POD_PHASE=$(kubectl -n "$NAMESPACE" get pod $POD_NAME -o jsonpath='{.status.phase}' 2>/dev/null)
  if [ "$POD_PHASE" = "Succeeded" ] || [ "$POD_PHASE" = "Failed" ]; then
    # Give it a moment for job status to update
    sleep 2
    if kubectl -n "$NAMESPACE" get job keydb-comprehensive-test -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null | grep -q "True"; then
      JOB_COMPLETE=1
    else
      JOB_FAILED=1
    fi
    break
  fi
  
  sleep 1
done

# Stop log streaming
kill $LOG_PID 2>/dev/null || true
wait $LOG_PID 2>/dev/null || true

echo ""

if [ $JOB_COMPLETE -eq 1 ]; then
  echo "✅ Test job completed successfully"
elif [ $JOB_FAILED -eq 1 ]; then
  echo "❌ Test job failed (some tests did not pass)"
else
  echo "❌ Test job did not complete in time"
  echo ""
  echo "Pod status:"
  kubectl -n "$NAMESPACE" get pod $POD_NAME
  exit 1
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                          TEST RESULTS                                ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# Show the test results from the pod
if [ -n "$POD_NAME" ]; then
  kubectl -n "$NAMESPACE" logs $POD_NAME 2>&1
else
  kubectl -n "$NAMESPACE" logs -l app=keydb-test --tail=100 2>&1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Exit with appropriate code
if [ $JOB_COMPLETE -eq 1 ]; then
  echo "✅ All tests passed!"
  exit 0
else
  echo "❌ Some tests failed (see logs above)"
  exit 1
fi

