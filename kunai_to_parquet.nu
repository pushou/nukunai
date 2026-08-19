
# kunai_to_parquet.nu
#
# Converts a kunai events file (raw ndjson .jsonl or compressed .gz)
# to the Parquet format (.parquet), for a much faster lazy polars processing
# (ndjson parsing + schema inference are only done once).
#
# Usage:
#   nu kunai_to_parquet.nu kunai.jsonl
#   nu kunai_to_parquet.nu kunai.jsonl.gz
#   nu kunai_to_parquet.nu kunai.jsonl --output out.parquet
#   nu kunai_to_parquet.nu kunai.jsonl --eager
#   nu kunai_to_parquet.nu kunai.jsonl --noflat
#
# Options:
#   --infer-schema N   rows to infer the schema (default 200000 ;
#                      below 200000 inference may fail on large batches)
#   --eager            conversion in eager mode (6x faster but very
#                      RAM-hungry: saturates memory on large gz files;
#                      lazy is the default because it works everywhere)
#   --noflat           do not flatten data/info (keep the raw structure)
#   --output FILE      exact name of the output parquet (default: next to the
#                      source, unambiguous name, cf. default_output)
#
# OUTPUT NAME: a .gz and its decompressed .jsonl converge to the SAME parquet:
#   kunai.jsonl    -> kunai.jsonl.parquet
#   kunai.jsonl.gz -> kunai.jsonl.parquet
#
# NON-DESTRUCTIVE: the .gz/.jsonl source is NEVER deleted nor overwritten. In
# eager mode, the .gz is decompressed to a temporary file (polars cannot read
# the compressed gz in eager mode), converted, then the temporary is deleted
# (even on failure). In lazy mode (default), the gz is read directly, without
# a temporary.

# Default output name: so that a .gz and its decompressed .jsonl converge to
# the SAME parquet, we only strip an eventual `.gz` suffix, then keep the rest
# of the name and append `.parquet`:
#   test.jsonl.gz -> test.jsonl.parquet
#   test.jsonl    -> test.jsonl.parquet
def default_output [eventslog: string] {
    let ext = ($eventslog | path parse | get extension)
    if $ext == 'gz' {
        let no_gz = ($eventslog | str replace -r '\.gz$' '')
        $"($no_gz).parquet"
    } else if $ext == 'parquet' {
        $eventslog
    } else {
        $"($eventslog).parquet"
    }
}

export def save_into_parquet [
    eventslog: string
    eager_param: string
    infer_schema_num: int
    noflat_param: string
    output: string
] {
    let parquetfile = $output
    print $"converting  ($eventslog) to ($parquetfile) ($eager_param) infer-schema=($infer_schema_num) flat=($noflat_param)"

    let frame = if $eager_param == "--lazy" {
        polars open --infer-schema $infer_schema_num -t ndjson $eventslog
    } else {
        polars open --infer-schema $infer_schema_num -t ndjson $eventslog --eager
    }
    let flat = if $noflat_param == 'flat' { $frame | polars unnest data info } else { $frame }
    $flat | polars save -t parquet $parquetfile
}

export def main [
    kunai_events_log_file: string
    --infer-schema: int = 200000
    --eager                      # eager = 6x faster but very RAM-hungry; lazy by default
    --noflat                     # do not flatten data/info (keep the raw structure)
    --output: string             # exact name of the output parquet (default: by default)
] {
    let eager_param = if $eager { "--eager" } else { "--lazy" }
    let noflat_param = if $noflat { "noflat" } else { "flat" }

    # the source file must exist
    if not ($kunai_events_log_file | path exists) {
        return $"file ($kunai_events_log_file) not found"
    }

    let file_extension = ($kunai_events_log_file | path parse | get extension)
    if $file_extension == 'parquet' {
        return $"skipping parquet file ($kunai_events_log_file) already converted"
    }

    let out = ($output | default (default_output $kunai_events_log_file))

    # In eager mode, a compressed .gz cannot be read directly by polars.
    # It is decompressed to a TEMPORARY file (the .gz source stays intact),
    # the temp is converted to the final parquet, then the temp is deleted.
    # The temp is deleted in ALL cases (even if the conversion fails, e.g.
    # memory exhaustion of the polars plugin): the status is captured with try/
    # catch, we clean up, then re-emit the eventual error.
    if $file_extension == 'gz' and $eager_param == "--eager" {
        let ori_dir = ($kunai_events_log_file | path dirname)
        let temp_unzipped = ($ori_dir | path join ($kunai_events_log_file | path basename | path parse | get stem)) + ".unzip.tmp"
        ^gzip -dc $kunai_events_log_file o> $temp_unzipped
        print $"unzipped - non-destructive - from ($kunai_events_log_file) to ($temp_unzipped)"
        let status = (try {
            save_into_parquet $temp_unzipped $eager_param $infer_schema $noflat_param $out
            'ok'
        } catch { |err| $err.msg })
        rm -f $temp_unzipped
        if $status != 'ok' {
            error make { msg: $"conversion failed: ($status)", label: { text: "conversion", span: (metadata $kunai_events_log_file).span } }
        }
    } else {
        save_into_parquet $kunai_events_log_file $eager_param $infer_schema $noflat_param $out
    }
    print $"parquet saved: ($out)"
}
