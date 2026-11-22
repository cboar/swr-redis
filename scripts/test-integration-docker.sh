#!/usr/bin/env bash
set -euo pipefail

PORT=16379
CONTAINER_NAME="swr-redis-test-${RANDOM}"

echo "Starting Redis test container (${CONTAINER_NAME}) on port ${PORT}..."
docker run -d --rm --name "${CONTAINER_NAME}" -p "${PORT}:6379" redis:alpine > /dev/null

cleanup() {
  echo "Cleaning up container ${CONTAINER_NAME}..."
  docker stop "${CONTAINER_NAME}" > /dev/null 2>&1 || true
}
trap cleanup EXIT

# Wait for redis to be ready
echo "Waiting for Redis to be ready..."
for i in {1..30}; do
  if docker exec "${CONTAINER_NAME}" redis-cli ping > /dev/null 2>&1; then
    echo "Redis is ready!"
    break
  fi
  sleep 0.2
done

echo "Running integration tests..."
REQUIRE_REDIS=1 REDIS_PORT="${PORT}" node --test tests/integration/*.test.ts
echo "Integration tests passed successfully!"
