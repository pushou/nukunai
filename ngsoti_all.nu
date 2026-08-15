#!/usr/bin/env nu
# Traite séquentiellement (un à la fois) tous les échantillons NGSOTI.
# Pour chacun : exécute ngsoti_detail.nu (affiche le rapport en console ET écrit
# le fichier markdown logs/ngsoti/scanresult<TS>/<hash>.md). Le TS est unique
# pour toute cette exécution, donc tous les rapports sont regroupés dans le même dossier.
#
# Usage : nu ngsoti_all.nu

def main [] {
  # Répertoire des logs analysés (ajustable)
  let logs_dir = $"($env.FILE_PWD)/logs/ngsoti"
  let detail_script = $"($env.FILE_PWD)/ngsoti_detail.nu"

  let ts = (date now | format date "%Y%m%d_%H%M%S")
  print $"Session de scan : scanresult($ts)"
  print $"Rapports markdown -> ($logs_dir)/scanresult($ts)/"

  mut count = 0
  mut fail = 0

  # Parcourt chaque échantillon (un fichier kunai.jsonl.gz par sous-dossier).
  for f in (glob $"($logs_dir)/*/kunai.jsonl.gz") {
    $count = $count + 1
    let hash = ($f | path dirname | path basename)

    print ""
    print "=================================================================="
    print $">>> [($count)] ($hash)"
    print "=================================================================="
    # Exécute ngsoti_detail.nu (même dossier) avec FILE et SCAN_TS fixés.
    let res = (with-env { FILE: $f, SCAN_TS: $ts } { nu $detail_script } | complete)
    let rc = $res.exit_code
    if $rc != 0 {
      print $"!! ÉCHEC sur ($hash) rc=($rc)"
      $fail = $fail + 1
    }
  }

  print ""
  print "======================================================"
  print $"Terminé : ($count) échantillons, ($fail) échecs."
  print $"Résultats : ($logs_dir)/scanresult($ts)/"
  print "======================================================"
  exit $fail
}
