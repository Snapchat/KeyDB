#!/bin/bash
#
# deploy_and_test.sh - Deploy KeyDB to k3s and run comprehensive tests
#
# This script:
# 1. Verifies the Docker image exists
# 2. Cleans up any existing deployment
# 3. Deploys KeyDB using Helm
# 4. Waits for pods to be ready
# 5. Runs comprehensive tests
#

set -e

NAMESPACE="${KEYDB_NAMESPACE:-default}"
RELEASE_NAME="${KEYDB_RELEASE:-keydb}"
IMAGE_TAG="${KEYDB_IMAGE_TAG:-8.2.3}"

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║       KeyDB Redis 8 - Kubernetes Deployment & Test Pipeline         ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Namespace:  $NAMESPACE"
echo "Release:    $RELEASE_NAME"
echo "Image Tag:  $IMAGE_TAG"
echo ""

# Step 1: Verify Docker image
echo "🔍 Step 1: Verifying Docker image..."
if docker manifest inspect "vainkop/keydb8:${IMAGE_TAG}" >/dev/null 2>&1; then
    echo "✅ Image found: vainkop/keydb8:${IMAGE_TAG}"
    docker manifest inspect "vainkop/keydb8:${IMAGE_TAG}" | grep -E "(architecture|os)" | head -6
else
    echo "❌ Error: Image vainkop/keydb8:${IMAGE_TAG} not found"
    echo "   Please ensure the Docker build completed successfully"
    exit 1
fi
echo ""

# Step 2: Set kubeconfig context
echo "🔧 Step 2: Setting Kubernetes context..."
kubectl config use-context local || {
    echo "⚠️  Warning: Could not set context to 'local', using current context"
}
echo ""

# Step 3: Clean up existing deployment
echo "🧹 Step 3: Cleaning up existing deployment..."
if helm list -n "$NAMESPACE" | grep -q "$RELEASE_NAME"; then
    echo "   Uninstalling existing Helm release..."
    helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" || true
    sleep 5
fi

# Delete StatefulSet first (required before PVCs can be deleted)
echo "   Deleting StatefulSet (if exists)..."
kubectl delete statefulset "$RELEASE_NAME" -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true
sleep 3

# Clean up PVCs (can only be deleted after StatefulSet is gone)
echo "   Cleaning up PVCs..."
kubectl delete pvc -l app.kubernetes.io/name=keydb -n "$NAMESPACE" 2>/dev/null || true
sleep 3
echo ""

# Step 4: Deploy with Helm
echo "🚀 Step 4: Deploying KeyDB to k3s..."
cd "$(dirname "$0")"
helm install "$RELEASE_NAME" ./pkg/helm \
    -n "$NAMESPACE" \
    --set imageTag="${IMAGE_TAG}"

# Wait for pods to be ready (more reliable than Helm's --wait for StatefulSets)
echo "⏳ Waiting for pods to be ready..."
if kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keydb -n "$NAMESPACE" --timeout=120s; then
    echo "✅ All pods are ready!"
else
    echo "❌ Pods did not become ready in time"
    kubectl get pods -l app.kubernetes.io/name=keydb -n "$NAMESPACE"
    exit 1
fi

echo ""
echo "📊 Step 5: Checking deployment status..."
kubectl get pods -l app.kubernetes.io/name=keydb -n "$NAMESPACE" -o wide
echo ""

kubectl get pvc -l app.kubernetes.io/name=keydb -n "$NAMESPACE" 2>/dev/null || echo "No PVCs found"
echo ""

kubectl get svc -l app.kubernetes.io/name=keydb -n "$NAMESPACE"
echo ""

# Step 6: Wait for pods to be ready
echo "⏳ Step 6: Waiting for pods to be ready (timeout: 3 minutes)..."
if kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keydb -n "$NAMESPACE" --timeout=180s; then
    echo "✅ All pods are ready!"
else
    echo "❌ Pods did not become ready in time"
    echo ""
    echo "Pod status:"
    kubectl get pods -l app.kubernetes.io/name=keydb -n "$NAMESPACE"
    echo ""
    echo "Pod logs:"
    kubectl logs -l app.kubernetes.io/name=keydb -n "$NAMESPACE" --tail=20
    exit 1
fi
echo ""

# Step 7: Run comprehensive tests
echo "🧪 Step 7: Running comprehensive tests..."
cd pkg/tests
if [ -f test.sh ]; then
    chmod +x test.sh
    ./test.sh
    TEST_EXIT_CODE=$?
    
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        echo ""
        echo "╔══════════════════════════════════════════════════════════════════════╗"
        echo "║                    ✅ ALL TESTS PASSED!                              ║"
        echo "╚══════════════════════════════════════════════════════════════════════╝"
    else
        echo ""
        echo "╔══════════════════════════════════════════════════════════════════════╗"
        echo "║                    ❌ SOME TESTS FAILED                               ║"
        echo "╚══════════════════════════════════════════════════════════════════════╝"
        exit $TEST_EXIT_CODE
    fi
else
    echo "❌ Error: test.sh not found in pkg/tests/"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "✅ Deployment and testing complete!"
echo ""
echo "To access KeyDB:"
echo "  kubectl port-forward -n $NAMESPACE svc/$RELEASE_NAME 6379:6379"
echo ""
echo "To view logs:"
echo "  kubectl logs -l app.kubernetes.io/name=keydb -n $NAMESPACE -f"
echo ""
echo "To uninstall:"
echo "  helm uninstall $RELEASE_NAME -n $NAMESPACE"
echo ""

