#!/usr/bin/env bash
# Traite séquentiellement (un à la fois) tous les échantillons NGSOTI.
# Pour chacun : affiche le rapport détaillé dans la console ET écrit le fichier
# markdown dans logs/ngsoti/scanresult<TS>/<hash>.md. Le TS est unique pour
# toute cette exécution, donc les 16 rapports sont regroupés dans le même dossier.
set -u
cd "$(dirname "$0")"

TS="$(date +%Y%m%d_%H%M%S)"
echo "Session de scan : scanresult$TS"
echo "Rapports markdown -> logs/ngsoti/scanresult$TS/"

count=0
fail=0
for d in logs/ngsoti/*/; do
  f="${d}kunai.jsonl.gz"
  [ -f "$f" ] || { echo "SKIP (pas de jsonl.gz) : $d"; continue; }
  count=$((count+1))
  hash="$(basename "$(dirname "$f")")"
  echo ""
  echo "=================================================================="
  echo ">>> [$count] $hash"
  echo "=================================================================="
  FILE="$f" SCAN_TS="$TS" nu ngsoti_detail.nu
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "!! ÉCHEC sur $hash (rc=$rc)"
    fail=$((fail+1))
  fi
done
echo ""
echo "======================================================"
echo "Terminé : $count échantillon(s), $fail échec(s)."
echo "Résultats : logs/ngsoti/scanresult$TS/"
echo "======================================================"
exit "$fail"
