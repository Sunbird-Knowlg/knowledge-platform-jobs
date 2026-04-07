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
REPO_PATH="scripts/sunbird-yugabyte-migrations/sunbird-knowlg"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Ordered list of CQL files to execute
CQL_FILES=(
    "sunbird.cql"
    "lock_db.cql"
    "dialcodes.cql"
    "content_store.cql"
    "contentstore.cql"
    "category_store.cql"
    "dialcode_store.cql"
    "hierarchy_store.cql"
    "platform_db.cql"
    "script_store.cql"
)

echo -e "${YELLOW}Downloading CQL migration scripts...${NC}"
rm -rf "${MIGRATIONS_DIR}"
git clone --depth 1 --branch "${BRANCH}" --filter=blob:none --sparse "${REPO_URL}" "${MIGRATIONS_DIR}" 2>/dev/null
cd "${MIGRATIONS_DIR}"
git sparse-checkout set "${REPO_PATH}" 2>/dev/null
cd "${SCRIPT_DIR}"

echo -e "${YELLOW}Running migrations with ENV=${ENV}...${NC}"
echo ""

FAILED=0
for cql_file in "${CQL_FILES[@]}"; do
    src="${MIGRATIONS_DIR}/${REPO_PATH}/${cql_file}"
    if [ ! -f "${src}" ]; then
        echo -e "${YELLOW}SKIP: ${cql_file} not found${NC}"
        continue
    fi

    # Replace ${ENV} placeholder with actual environment prefix
    tmp="/tmp/knowlg_${cql_file}"
    sed "s/\${ENV}/${ENV}/g" "${src}" > "${tmp}"

    # Copy into container and execute
    docker cp "${tmp}" yugabyte:/tmp/"${cql_file}"
    if docker exec yugabyte /home/yugabyte/bin/ycqlsh 127.0.0.1 9042 \
        -u cassandra -p cassandra \
        -f /tmp/"${cql_file}" 2>&1; then
        echo -e "${GREEN}OK: ${cql_file}${NC}"
    else
        echo -e "${RED}FAIL: ${cql_file}${NC}"
        FAILED=$((FAILED + 1))
    fi
    rm -f "${tmp}"
done

# Cleanup
rm -rf "${MIGRATIONS_DIR}"

echo ""
if [ ${FAILED} -gt 0 ]; then
    echo -e "${RED}${FAILED} migration(s) failed.${NC}"
    exit 1
else
    echo -e "${GREEN}All migrations completed successfully.${NC}"
fi
