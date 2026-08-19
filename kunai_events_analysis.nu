#!/usr/bin/env nu
#Usage:
# > nu kunai_events_analysis.nu events.log.1373.parquet or gzipped   
# print the number of events by event_name in a kunai events log file
#Flags:
#   -h, --help - Display the help message for this command
#Parameters:
 # kunai events log file <string>

# Counts the events by name from an already flattened Parquet file.
def count_by_events_name_parquet [
    eventslogparquet: string ] {
    let kunai_polars_frame = (polars open $eventslogparquet)
    $kunai_polars_frame|polars get event|polars unnest event|polars get name|polars value-counts |polars sort-by [count] -r [true]
}

# Counts the events by name directly from a .gz / ndjson, WITHOUT going through
# the parquet format: polars decompresses the .gz natively via the ndjson reader.
# Unnest of data/info to expose the event column.
def count_by_events_name_ndjson [
    eventslog: string
    infer_schema: int ] {
    let frame = (polars open --infer-schema $infer_schema -t ndjson $eventslog
        | polars unnest data info)
    $frame
        | polars get event
        | polars unnest event
        | polars get name
        | polars value-counts
        | polars sort-by [count] -r [true]
        | polars collect
}

def main [
    kunai_events_log_file: string 
    --infer-schema:  int = 200000 # Number of rows to infer schema. under 200000 it failed
    --lazy # kept for backward compatibility (no-op: gz read directly, always collected)
    ] {
    
    # file exists check
    try {ls ($kunai_events_log_file)} catch {return $"file ($kunai_events_log_file) not found"}
    let file_extension = ($kunai_events_log_file | path parse | get extension)

    if ($file_extension == 'parquet') {
        count_by_events_name_parquet $kunai_events_log_file
    } else {
        # .gz (or raw ndjson): direct read, no parquet conversion
        count_by_events_name_ndjson $kunai_events_log_file $infer_schema
    }
}
