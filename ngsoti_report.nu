#!/usr/bin/env nu
# Résumé des alertes kunai pour chaque échantillon malware NGSOTI.
# Pour chaque logs/ngsoti/<hash>/kunai.jsonl.gz, exécute kunai_detect_compromise.nu,
# capture le résultat dans <OUT_DIR>/<hash>.txt puis affiche un résumé des compteurs
# par famille (en retirant les couleurs ANSI).
#
# Usage : nu ngsoti_report.nu [répertoire_sortie]
# Par défaut le répertoire de sortie est $JCODE_SCRATCH_DIR/ngsoti_out (ou ./ngsoti_out).

def main [output_dir?: path] {
  let logs_dir = $"($env.FILE_PWD)/logs/ngsoti"
  let scratch = (if ($env.JCODE_SCRATCH_DIR? == null) { $env.PWD } else { $env.JCODE_SCRATCH_DIR? })
  let out_dir = (if ($output_dir == null) {
    $"($scratch)/ngsoti_out"
  } else {
    $output_dir
  })
  mkdir $out_dir

  let detect_script = $"($env.FILE_PWD)/kunai_detect_compromise.nu"

  # Nombre de cœurs à utiliser, plafonné à 4 pour ne pas saturer la machine.
  let nproc = (sys cpu | length)
  let p = (if $nproc < 4 { $nproc } else { 4 })

  let files = (glob $"($logs_dir)/*/kunai.jsonl.gz")

  # Processus un fichier : exécute kunai_detect_compromise.nu (timeout 300 s),
  # écrit la sortie complète (stdout+stderr) dans <out>/<hash>.txt puis imprime
  # le résumé des familles en retirant les codes ANSI.
  def process [f: path, out: path, script: path] {
    let hash = ($f | path dirname | path basename)
    let out_file = ($out | path join $"($hash).txt")
    let res = (^timeout 300 nu $script $f | complete)
    # rc = code de retour de timeout (124 si dépassé, sinon celui de nu)
    let rc = $res.exit_code
    ($res.stdout | str join "\n") | save --force $out_file
    if not ($res.stderr | is-empty) {
      ($res.stderr | str join "\n") | save --append $out_file
    }
    print $"=== ($hash) rc=($rc) ==="
    open --raw $out_file
      | lines
      | where ($it | str contains "≡≡  Famille")
      | each {|l| $l | str replace -a -r '\x1b\[[0-9;]*m' '' | print $in }
    print ""
  }

  $files
    | par-each --threads $p {|f| process $f $out_dir $detect_script }
    | ignore
}
