#!/bin/sh
# Install an openworkflow build that has the `openworkflow/backend` and
# `openworkflow/testing` exports, until a release on npm includes them.
#
#   OPENWORKFLOW_DIR  an openworkflow checkout to build (default: ../openworkflow)
#   OPENWORKFLOW_REF  if the checkout is missing, clone this ref of
#                     cliftonc/openworkflow instead (default: backend-api)
#
# Extra arguments are installed alongside it, e.g. `drizzle-orm@rc`; a separate
# `npm install --no-save` would put the npm openworkflow back.
#
# Delete this script, and its CI step, once the exports are released.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
src=${OPENWORKFLOW_DIR:-$root/../openworkflow}
vendor=$root/.vendor

if [ ! -f "$src/packages/openworkflow/package.json" ]; then
  src=$vendor/openworkflow
  rm -rf "$src"
  git clone --quiet --depth 1 --branch "${OPENWORKFLOW_REF:-backend-api}" \
    https://github.com/cliftonc/openworkflow.git "$src"
fi

(cd "$src" && npm ci --silent --no-audit --no-fund && npx turbo build --filter=openworkflow >/dev/null)
mkdir -p "$vendor"
rm -f "$vendor"/openworkflow-*.tgz
tarball=$(cd "$src" && npm pack -w openworkflow --pack-destination "$vendor" --silent | tail -1)
npm install --no-save --no-audit --no-fund "$vendor/$tarball" "$@"
echo "installed openworkflow from $src"
