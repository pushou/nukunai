#!/usr/bin/env nu
# Rapporte le détail des alertes (tableau markdown, compact) pour un fichier kunai NGSOTI.
# Le fichier est passé via la variable d'env FILE.
# Usage: FILE=logs/ngsoti/<hash>/kunai.jsonl.gz nu ngsoti_detail.nu
#
# Deux sorties :
#  - affichage console en direct des alertes (avec couleurs),
#  - fichier markdown écrit dans logs/ngsoti/scanresult<TS>/<descriptif>.md (dans
#    la directory des logs). Le fichier est nommé à partir des familles de
#    détection détectées (famille+nombre) précédées du hash court de l'échantillon.
#    Le TS commun à toute une exécution est passé par la variable d'env SCAN_TS
#    (fixé par le wrapper) ; sinon généré ici.
use kunai_detect_compromise.nu *

let file = $env.FILE
# Timestamp commun de la session de scan (fourni par le wrapper pour regrouper
# tous les échantillons dans le même dossier scanresult<TS>).
let ts = ($env.SCAN_TS? | default (date now | format date "%Y%m%d_%H%M%S"))
# nombre max de lignes affichées par famille ; au-delà, seul le décompte est montré
let max_rows = 25

# Matérialise le fichier UNE seule fois en DataFrame en mémoire (polars collect,
# sans into-nu). Chaque détection filtre ensuite ce DataFrame en RAM au lieu de
# re-dérouler le plan build_base (open + unnests) à chaque famille. C'est ce qui
# évite de re-scanner les centaines de milliers de lignes x 9 fois et de saturer
# la charge CPU (le plugin paralélisait chaque re-collect sur tous les cœurs).
let base = (build_base $file 200000 | polars collect)

let fams = {
  execve:        {|b| detect_execve $b}
  file_create:   {|b| detect_file_create $b}
  connect:       {|b| detect_connect $b}
  send_data:     {|b| detect_send_data $b}
  dns_query:     {|b| detect_dns_query $b}
  kill:          {|b| detect_kill $b}
  bpf_prog_load: {|b| detect_bpf $b}
  mmap_exec:     {|b| detect_mmap_exec $b}
  prctl:         {|b| detect_prctl $b}
}

# Buffer markdown du rapport final (sortie fichier .md).
mut md_lines = [ $"# Détail des alertes — ($file)" "" ]

# Nombre d'alertes par famille de détection (pour le nom du fichier de sortie).
mut fam_counts = {}

print $"(ansi cyan)###### Fichier: ($file)(ansi reset)"
for fam in ($fams | columns) {
  # On reste en polars le plus longtemps possible : les comptages ne sont jamais
  # matérialisés en nushell (shape = [lignes, colonnes], léger), et on ne convertit
  # en nushell (into-nu) que les max_rows premières lignes. Évite d'éclater la
  # mémoire/CPU pour les familles massives (ex. send_data à 300k+ lignes).
  let fam_frame = (do (($fams | get $fam)) $base)
  let shape = ($fam_frame | polars shape | polars into-nu)
  let n = ($shape.0.rows | into int)
  $fam_counts = ($fam_counts | insert $fam $n)
  print $"(ansi green)== ($fam) — ($n) alertes(ansi reset)"
  $md_lines = ($md_lines | append $"## ($fam) — ($n) alertes" "")
  if $n > 0 {
    let l2 = ($fam_frame | polars first $max_rows | polars collect | polars into-nu)
    let more = if $n > $max_rows { $n - $max_rows } else { 0 }
    if $n <= $max_rows {
      print ($l2 | to md)
      $md_lines = ($md_lines | append (($l2 | to md) | split row "\n"))
    } else {
      print ($l2 | to md)
      $md_lines = ($md_lines | append (($l2 | to md) | split row "\n"))
      print $"(ansi yellow)… et ($more) autres alertes non affichées(ansi reset)"
      $md_lines = ($md_lines | append $"… et ($more) autres alertes non affichées")
    }
  }
  print ""
  $md_lines = ($md_lines | append "")
}

# Bilan consolidé (IP/ports et URLs de téléchargement) : généré par la procédure
# report_bilan de kunai_detect_compromise.nu et ajouté en fin de rapport.
let bilan = (report_bilan $base 50)
$md_lines = ($md_lines | append $bilan)

# Écrit le rapport markdown dans la directory des logs, sous
# logs/ngsoti/scanresult<TS>/<nom>.md. Le chemin du log est dérivé du fichier
# d'entrée (ex. logs/ngsoti/<hash>/kunai.jsonl.gz → dossier logs de l'entrée).
let log_dir = ($file | path dirname | path dirname)
let out_dir = ($log_dir | path join $"scanresult($ts)")
mkdir $out_dir
# Nom du fichier : délégué à la procédure report_basename de
# kunai_detect_compromise.nu (nom descriptif depuis les familles détectées).
let hash = ($file | path dirname | path basename)
let out_file = ($out_dir | path join $"(report_basename $hash $fam_counts).md")
$md_lines | str join "\n" | save --force $out_file
print $"(ansi cyan)Rapport markdown écrit : ($out_file)(ansi reset)"
