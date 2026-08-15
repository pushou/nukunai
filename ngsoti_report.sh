#!/usr/bin/env bash
# Résumé des alertes kunai pour chaque échantillon malware NGSOTI.
# Pour chaque logs/ngsoti/<hash>/kunai.jsonl.gz, on extrait les compteurs
# par famille ainsi que le détail des alertes des familles à faible volume.
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${1:-$JCODE_SCRATCH_DIR/ngsoti_out}"
mkdir -p "$OUT_DIR"

cd "$DIR"
mapfile -t files < <(ls logs/ngsoti/*/kunai.jsonl.gz)

process() {
    local f="$1"
    local hash
    hash="$(basename "$(dirname "$f")")"
    local out="$OUT_DIR/$hash.txt"
    timeout 300 nu kunai_detect_compromise.nu "$f" > "$out" 2>&1
    local rc=$?
    echo "=== $hash rc=$rc ==="
    grep -E "≡≡  Famille" "$out" | sed -E 's/\x1b\[[0-9;]*m//g'
    echo
}

export -f process
export OUT_DIR
# parallélise sur le CPU disponible, max 4 à la fois pour ne pas saturer
nproc=$(nproc)
p=$(( nproc < 4 ? nproc : 4 ))
printf '%s\n' "${files[@]}" | xargs -P "$p" -I{} bash -c 'process "$@"' _ {}
