#!/usr/bin/env bash
# Get list of versions to be used by tests/check_for_new_version

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x

get_versions(){
    curl -s http://archive.apache.org/dist/spark/ |
    egrep -i -o 'href="spark-[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+' |
    sed 's/href="spark-//'
}

get_versions
