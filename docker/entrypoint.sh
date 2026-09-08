#!/bin/sh
set -eu

if [ "${PYSWITCH_STORAGE_BACKEND:-memory}" = "postgres" ]; then
    echo "Applying PostgreSQL migrations"
    # ponytail: fixed 15-attempt retry covers startup DNS/connection races;
    # use a separate migration job for prolonged database outages.
    attempt=1
    while ! alembic upgrade head; do
        if [ "$attempt" -ge 15 ]; then
            echo "PostgreSQL migrations failed after ${attempt} attempts" >&2
            exit 1
        fi
        echo "Migration attempt ${attempt} failed; retrying in 2s" >&2
        attempt=$((attempt + 1))
        sleep 2
    done
fi

exec "$@"
