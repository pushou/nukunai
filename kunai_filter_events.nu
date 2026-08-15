#!/usr/bin/env nu
#Usage:
# > nu ./events.nu ./eventsreg.log -e 1,5,6 
# > nu ./events.nu ./eventsreg.log -e 1,5,6 -s
#Flags:
#   -h, --help - Display the help message for this command
#Parameters:
 # kunai events log file <string>
 # kunai event id <int> id=0 => no filter

# Ouvre la source directement :
#  - .parquet : polars open (déjà aplati),
#  - .gz / ndjson : lecteur ndjson (polars décompresse les .gz nativement),
#    avec unnest de data/info pour exposer la colonne event comme le parquet.
# Retourne TOUJOURS un eager dataframe (polars collect) pour que le type soit
# concret en aval et que les renames conditionnels fonctionnent.
def open_source [
    kunai_events_log_file: string
    infer_schema: int ] {
    let file_extension = ($kunai_events_log_file | path parse | get extension)
    if $file_extension == 'parquet' {
        polars open $kunai_events_log_file | polars collect
    } else {
        polars open --infer-schema $infer_schema -t ndjson $kunai_events_log_file
            | polars unnest data info
            | polars collect
    }
}

def filter_events [
    frame                       # polars frame (lazy ou eager) de la source
    events_id: string ] {
    print $"filter event_id: ($events_id)"
    let events_list = ($events_id|split row ","|each {$in|into int})
    let cols = ($frame | polars schema | columns)
    let f = if "id" in $cols { $frame | polars rename id main_id } else { $frame }
    let f = if "name" in $cols { $f | polars rename name main_name } else { $f }
    $f | polars unnest event
       | polars rename [source id name uuid batch] [event_source event_id event_name event_uuid event_batch]
       | polars with-column ((polars col event_id) 
       | polars is-in $events_list
       | polars as match_id) 
       | polars filter (polars col match_id) 
       | polars drop match_id      
    }

def exploring_fdf [
    frame
    events_id: string ] {
    filter_events $frame $events_id| polars collect |polars into-nu |flatten --all|flatten --all|explore
}

def save_filtered_parquet [
    frame
    kunai_events_log_file: string
    events_id: string ] {
    let events_chunks = "_" + ($events_id | str replace --all "," "_") + "."
    let stem = ($kunai_events_log_file | path parse | get stem)
    let filtered_file = $"($stem)($events_chunks)parquet"
    print $"save filtered_events in: ($filtered_file)"
    filter_events $frame $events_id | polars collect | polars save ($filtered_file)
    try {ls $filtered_file} catch {print $"error creating parquet file ($filtered_file)" ; exit 10} 
    exit 0
}
    
def main [
    kunai_events_log_file: string 
    --events_id (-e): string = "1" # event id to filter, default is 1 => execve
    --infer-schema:  int = 200000 # Number of rows to infer schema. under 200000 it failed
    --lazy # kept for backward compatibility (no-op: the source is always read eagerly)
    --save (-s) # save filtered events in a parquet file and do not xplore dataframe
] {
    let save_param  = match $save {
        true => {true}
        false => {false}
    }  
      
    # file exists check
    try {ls ($kunai_events_log_file)} catch {return $"file ($kunai_events_log_file) not found"}
    let file_extension = ($kunai_events_log_file | path parse | get extension)

    print $"main_file_extension = ($file_extension)"

    if $file_extension not-in ['parquet', 'gz', 'log', 'jsonl', 'ndjson'] {
        print $"file must be parquet or gz";exit 0
    }

    # lecture directe de la source (gz lus sans conversion parquet)
    let frame = (open_source $kunai_events_log_file $infer_schema)
   
    if $save_param {save_filtered_parquet $frame $kunai_events_log_file $events_id } else {
        exploring_fdf $frame $events_id 
    }
}
