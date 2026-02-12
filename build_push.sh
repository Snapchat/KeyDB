#!/bin/bash
#
# build_push.sh - Build and push KeyDB Redis 8 multi-arch image to Docker Hub
#
# Usage:
#   ./build_push.sh [tag] [platforms]
#
# Examples:
#   ./build_push.sh                    # Pushes as 'latest' (amd64 + arm64)
#   ./build_push.sh 8.2.3              # Pushes as '8.2.3' and 'latest' (amd64 + arm64)
#   ./build_push.sh 8.2.3 linux/amd64  # Single platform build
#

set -e

REPO="vainkop/keydb8"
TAG="${1:-latest}"
PLATFORMS="${2:-linux/amd64,linux/arm64}"

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║      Building KeyDB Redis 8 Multi-Arch Docker Image                ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Repository: ${REPO}"
echo "Tag: ${TAG}"
echo "Platforms: ${PLATFORMS}"
echo ""

# Check if Dockerfile exists
if [ ! -f "Dockerfile" ]; then
    echo "❌ Error: Dockerfile not found in current directory"
    exit 1
fi

# Check if buildx is available
if ! docker buildx version > /dev/null 2>&1; then
    echo "❌ Error: docker buildx is required for multi-arch builds"
    echo "Install it with: docker buildx install"
    exit 1
fi

# Check if logged in to Docker Hub
if ! docker info 2>/dev/null | grep -q "Username:"; then
    echo "⚠️  Not logged in to Docker Hub"
    echo "Please run: docker login"
    exit 1
fi

# Create builder instance if it doesn't exist
BUILDER_NAME="keydb-multiarch"
if ! docker buildx ls | grep -q "$BUILDER_NAME"; then
    echo "📦 Creating buildx builder: $BUILDER_NAME"
    docker buildx create --name "$BUILDER_NAME" --driver docker-container --use
    docker buildx inspect --bootstrap
else
    echo "📦 Using existing builder: $BUILDER_NAME"
    docker buildx use "$BUILDER_NAME"
fi

# Build and push multi-arch image
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Building multi-arch image (this may take 20-40 minutes)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

docker buildx build \
    --platform "${PLATFORMS}" \
    --tag "${REPO}:${TAG}" \
    --push \
    --progress=plain \
    .

# Tag as latest if specific version was provided
if [ "${TAG}" != "latest" ]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Tagging as latest..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    docker buildx build \
        --platform "${PLATFORMS}" \
        --tag "${REPO}:latest" \
        --push \
        --progress=plain \
        .
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                    ✅ BUILD COMPLETE!                                ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Multi-arch images pushed:"
echo "  • ${REPO}:${TAG} (${PLATFORMS})"
if [ "${TAG}" != "latest" ]; then
    echo "  • ${REPO}:latest (${PLATFORMS})"
fi
echo ""
echo "Verify with:"
echo "  docker manifest inspect ${REPO}:${TAG}"
echo ""
echo "Pull with:"
echo "  docker pull ${REPO}:${TAG}"
echo ""
echo "Docker will automatically select the correct architecture!"
echo ""
echo "Deploy to Kubernetes:"
echo "  helm install keydb ./pkg/helm"
echo ""
