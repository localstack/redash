#!/bin/bash
set -e

export REDASH_DATABASE_URL="postgresql://${POSTGRES_USERNAME}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}/${POSTGRES_DATABASE}"

exec supervisord -c /app/ecs/supervisord.conf
