#!/usr/bin/env nu
# Processes sequentially (one at a time) all the NGSOTI samples.
# For each: runs ngsoti_detail.nu (displays the report on console AND writes
# the markdown file logs/ngsoti/scanresult<TS>/<hash>.md). The TS is unique
# for the whole run, so all reports are grouped in the same folder.
#
# Usage: nu ngsoti_all.nu

def main [] {
  # Analyzed logs directory (adjustable)
  let logs_dir = $"($env.FILE_PWD)/logs/ngsoti"
  let detail_script = $"($env.FILE_PWD)/ngsoti_detail.nu"

  let ts = (date now | format date "%Y%m%d_%H%M%S")
  print $"Scan session: scanresult($ts)"
  print $"Markdown reports -> ($logs_dir)/scanresult($ts)/"

  mut count = 0
  mut fail = 0

  # Iterates over each sample (one kunai.jsonl.gz per subfolder).
  for f in (glob $"($logs_dir)/*/kunai.jsonl.gz") {
    $count = $count + 1
    let hash = ($f | path dirname | path basename)

    print ""
    print "=================================================================="
    print $">>> [($count)] ($hash)"
    print "=================================================================="
    # Runs ngsoti_detail.nu (same folder) with FILE and SCAN_TS set.
    let res = (with-env { FILE: $f, SCAN_TS: $ts } { nu $detail_script } | complete)
    let rc = $res.exit_code
    if $rc != 0 {
      print $"!! FAILED on ($hash) rc=($rc)"
      $fail = $fail + 1
    }
  }

  print ""
  print "======================================================"
  print $"Done: ($count) samples, ($fail) failures."
  print $"Results: ($logs_dir)/scanresult($ts)/"
  print "======================================================"
  exit $fail
}
