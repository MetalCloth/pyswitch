#!/bin/sh
set -eu

if [ "${PYSWITCH_STORAGE_BACKEND:-memory}" = "postgres" ]; then
    echo "Applying PostgreSQL migrations"
    alembic upgrade head
fi

exec "$@"
