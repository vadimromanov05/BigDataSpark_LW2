#!/usr/bin/env bash
set -euo pipefail

JARS_DIR="$(cd "$(dirname "$0")" && pwd)/jars"
mkdir -p "$JARS_DIR"

PG_JAR="postgresql-42.7.1.jar"
PG_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/${PG_JAR}"

CH_JAR="clickhouse-jdbc-0.6.0-patch5-all.jar"
CH_URL="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0-patch5/${CH_JAR}"

download() {
    local out="$1"; local url="$2"
    if [ -f "$JARS_DIR/$out" ]; then
        echo "✓ $out already present"
    else
        echo "→ downloading $out"
        curl -fSL "$url" -o "$JARS_DIR/$out"
    fi
}

download "$PG_JAR" "$PG_URL"
download "$CH_JAR" "$CH_URL"

echo "Done. JARs in $JARS_DIR"
ls -lh "$JARS_DIR"
