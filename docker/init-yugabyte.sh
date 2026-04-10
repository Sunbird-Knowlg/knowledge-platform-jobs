#!/bin/bash
# Downloads CQL migration scripts from sunbird-spark-installer and runs them
# against the local YugabyteDB container.
#
# Usage: ./init-yugabyte.sh [ENVIRONMENT] [BRANCH]
#   ENVIRONMENT: keyspace prefix (default: dev)
#   BRANCH:      branch of sunbird-spark-installer to use (default: develop)
#
# Prerequisites: docker must be running with the yugabyte container up.

set -e

ENV="${1:-dev}"
BRANCH="${2:-develop}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="${SCRIPT_DIR}/.migrations"
REPO_URL="https://github.com/Sunbird-Spark/sunbird-spark-installer.git"
REPO_BASE="scripts/sunbird-yugabyte-migrations"

echo "Downloading CQL migration scripts (branch: ${BRANCH})..."
rm -rf "${MIGRATIONS_DIR}"
if ! git clone --depth 1 --branch "${BRANCH}" --filter=blob:none --sparse "${REPO_URL}" "${MIGRATIONS_DIR}"; then
    echo "ERROR: Failed to clone ${REPO_URL} (branch: ${BRANCH})"
    exit 1
fi
cd "${MIGRATIONS_DIR}"
if ! git sparse-checkout set "${REPO_BASE}/sunbird-knowlg" "${REPO_BASE}/sunbird-inquiry"; then
    echo "ERROR: Failed to sparse-checkout migration directories"
    exit 1
fi
cd "${SCRIPT_DIR}"

FAILED=0

run_migrations() {
    local module="$1"
    shift
    local cql_files=("$@")
    local repo_path="${REPO_BASE}/${module}"

    echo ""
    echo "Running ${module} migrations with ENV=${ENV}..."

    for cql_file in "${cql_files[@]}"; do
        src="${MIGRATIONS_DIR}/${repo_path}/${cql_file}"
        if [ ! -f "${src}" ]; then
            echo "SKIP: ${module}/${cql_file} not found"
            continue
        fi

        tmp="/tmp/${module}_${cql_file}"
        sed "s/\${ENV}/${ENV}/g" "${src}" > "${tmp}"

        docker cp "${tmp}" yugabyte:/tmp/"${cql_file}"
        if docker exec yugabyte /home/yugabyte/bin/ycqlsh 127.0.0.1 9042 \
            -u cassandra -p cassandra \
            -f /tmp/"${cql_file}" 2>&1; then
            echo "OK: ${module}/${cql_file}"
        else
            echo "FAIL: ${module}/${cql_file}"
            FAILED=$((FAILED + 1))
        fi
        rm -f "${tmp}"
    done
}

# sunbird-knowlg migrations
run_migrations "sunbird-knowlg" \
    "sunbird.cql" \
    "lock_db.cql" \
    "dialcodes.cql" \
    "content_store.cql" \
    "contentstore.cql" \
    "category_store.cql" \
    "dialcode_store.cql" \
    "hierarchy_store.cql" \
    "platform_db.cql" \
    "script_store.cql"

# sunbird-inquiry migrations
run_migrations "sunbird-inquiry" \
    "hierarchy_store.cql" \
    "question_store.cql"

rm -rf "${MIGRATIONS_DIR}"

echo ""
if [ ${FAILED} -gt 0 ]; then
    echo "${FAILED} migration(s) failed."
    exit 1
else
    echo "All migrations completed successfully."
fi
