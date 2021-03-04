#!/usr/bin/env bash

function log() {

  echo -e "$(date '+%Y-%m-%d %H:%M:%S') [${1}] ${2}";

}

db=${1}
table=${2}

log "INFO" "Executing INVALIDATE METADATA statement on $db.$table"
impala-shell -d "$db" -q "INVALIDATE METADATA $table"
log "INFO" "Executed INVALIDATE METADATA statement on $db.$table"