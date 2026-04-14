#!/bin/bash
# Downloads ES index and mapping definitions from sunbird-devops and applies them
# against the local Elasticsearch container.
#
# Usage: ./init-elasticsearch.sh [BRANCH]
#   BRANCH: branch of sunbird-devops to use (default: release-8.0.0)
#
# Prerequisites: docker must be running with the sunbird_es container up and healthy.

set -e

BRANCH="${1:-release-8.0.0}"
ES_HOST="http://localhost:9200"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADS_DIR="${SCRIPT_DIR}/.es-migrations"
REPO_URL="https://github.com/project-sunbird/sunbird-devops.git"
REPO_PATH="ansible/roles/es7-mapping/files"

INDICES=(
    "compositesearch"
)

echo "Downloading ES index/mapping definitions (branch: ${BRANCH})..."
rm -rf "${DOWNLOADS_DIR}"
if ! git clone --depth 1 --branch "${BRANCH}" --filter=blob:none --sparse "${REPO_URL}" "${DOWNLOADS_DIR}"; then
    echo "ERROR: Failed to clone ${REPO_URL} (branch: ${BRANCH})"
    exit 1
fi
cd "${DOWNLOADS_DIR}"
if ! git sparse-checkout set "${REPO_PATH}"; then
    echo "ERROR: Failed to sparse-checkout ${REPO_PATH}"
    exit 1
fi
cd "${SCRIPT_DIR}"

# Wait for Elasticsearch to be ready (max 60 seconds)
echo "Waiting for Elasticsearch at ${ES_HOST}..."
RETRIES=0
MAX_RETRIES=30
until curl -s "${ES_HOST}/_cluster/health" > /dev/null 2>&1; do
    RETRIES=$((RETRIES + 1))
    if [ ${RETRIES} -ge ${MAX_RETRIES} ]; then
        echo "ERROR: Elasticsearch not reachable after ${MAX_RETRIES} attempts."
        exit 1
    fi
    sleep 2
done
echo "Elasticsearch is ready."

FAILED=0
for index in "${INDICES[@]}"; do
    index_file="${DOWNLOADS_DIR}/${REPO_PATH}/indices/${index}.json"
    mapping_file="${DOWNLOADS_DIR}/${REPO_PATH}/mappings/${index}-mapping.json"

    # Create index
    if [ -f "${index_file}" ]; then
        STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "${ES_HOST}/${index}" \
            -H "Content-Type: application/json" \
            -d @"${index_file}")
        if [ "${STATUS}" = "200" ] || [ "${STATUS}" = "201" ]; then
            echo "OK: index ${index} created"
        elif [ "${STATUS}" = "400" ]; then
            echo "SKIP: index ${index} already exists"
        else
            echo "FAIL: index ${index} (HTTP ${STATUS})"
            FAILED=$((FAILED + 1))
            continue
        fi
    else
        echo "SKIP: ${index}.json not found"
        continue
    fi

    # Apply mapping
    if [ -f "${mapping_file}" ]; then
        STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "${ES_HOST}/${index}/_mapping" \
            -H "Content-Type: application/json" \
            -d @"${mapping_file}")
        if [ "${STATUS}" = "200" ] || [ "${STATUS}" = "201" ]; then
            echo "OK: mapping for ${index} applied"
        else
            echo "FAIL: mapping for ${index} (HTTP ${STATUS})"
            FAILED=$((FAILED + 1))
        fi
    else
        echo "SKIP: ${index}-mapping.json not found"
    fi
done

rm -rf "${DOWNLOADS_DIR}"

echo ""
if [ ${FAILED} -gt 0 ]; then
    echo "${FAILED} operation(s) failed."
    exit 1
else
    echo "All ES indices and mappings created successfully."
fi
