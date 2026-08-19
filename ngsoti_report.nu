#!/usr/bin/env nu
# Summary of the kunai alerts for each NGSOTI malware sample.
# For each logs/ngsoti/<hash>/kunai.jsonl.gz, runs kunai_detect_compromise.nu,
# captures the result in <OUT_DIR>/<hash>.txt then displays a summary of the
# counters per family (removing the ANSI colors).
#
# Usage: nu ngsoti_report.nu [output_dir]
# By default the output directory is $JCODE_SCRATCH_DIR/ngsoti_out (or ./ngsoti_out).

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

  # Number of cores to use, capped at 4 to avoid saturating the machine.
  let nproc = (sys cpu | length)
  let p = (if $nproc < 4 { $nproc } else { 4 })

  let files = (glob $"($logs_dir)/*/kunai.jsonl.gz")

  # Processes one file: runs kunai_detect_compromise.nu (300 s timeout),
  # writes the full output (stdout+stderr) into <out>/<hash>.txt then prints
  # the family summary removing the ANSI codes.
  def process [f: path, out: path, script: path] {
    let hash = ($f | path dirname | path basename)
    let out_file = ($out | path join $"($hash).txt")
    let res = (^timeout 300 nu $script $f | complete)
    # rc = return code of timeout (124 if exceeded, otherwise the nu one)
    let rc = $res.exit_code
    ($res.stdout | str join "\n") | save --force $out_file
    if not ($res.stderr | is-empty) {
      ($res.stderr | str join "\n") | save --append $out_file
    }
    print $"=== ($hash) rc=($rc) ==="
    open --raw $out_file
      | lines
      | where ($it | str contains "≡≡  Family")
      | each {|l| $l | str replace -a -r '\x1b\[[0-9;]*m' '' | print $in }
    print ""
  }

  $files
    | par-each --threads $p {|f| process $f $out_dir $detect_script }
    | ignore
}
