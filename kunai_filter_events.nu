#Usage:
# > nu ./events.nu ./eventsreg.log -e 1,5,6 
# > nu ./events.nu ./eventsreg.log -e 1,5,6 -s
#Flags:
#   -h, --help - Display the help message for this command
#Parameters:
 # kunai events log file <string>
 # kunai event id <int> id=0 => no filter

use ./kunai_to_parquet.nu 

def filter_kunai_parquet [
    kunai_events_log_parquet: string 
    events_id: string ] {
    print $"parquet_file: ($kunai_events_log_parquet)"
    let kunai_polars_frame = (polars open $kunai_events_log_parquet)
    print $"filter event_id: ($events_id)"
    let events_list = ($events_id|split row ","|each {$in|into int})
    $kunai_polars_frame 
           | if ("id" in ($in | polars columns)) { polars rename id main_id } else { $in }
           | if ("name" in ($in | polars columns)) { polars rename name main_name } else { $in }
           | polars unnest event
           | polars rename [source id name uuid batch] [event_source event_id event_name event_uuid event_batch]
           | polars with-column ((polars col event_id) 
           | polars is-in $events_list
           | polars as match_id) 
           | polars filter (polars col match_id) 
           | polars drop match_id      
    }

def exploring_fdf [
    kunai_events_log_file: string 
    file_extension: string
    parquet_file: string
    infer_schema: int 
    events_id: string ] {
    print $"kunai_events_log_file file_extension parquet_file infer_schema events_id: ($kunai_events_log_file) ($file_extension) ($parquet_file) ($infer_schema) ($events_id)"

    if ($file_extension != 'parquet') {
                # flattenize one level and convert to parquet
                kunai_to_parquet $kunai_events_log_file 
         }
    # filter the parquet file
     filter_kunai_parquet  $parquet_file $events_id| polars collect |polars into-nu |flatten --all|flatten --all|explore
}

def save_filtered_parquet [
    kunai_events_log_file: string
    unzipped_file: string
    file_extension: string
    parquet_file: string
    infer_schema: int 
    events_id: string ] {
    print $"kunai_events_log_file unzipped_file file_extension parquet_file infer_schema events_id: ($kunai_events_log_file) ($unzipped_file) ($file_extension) ($parquet_file) ($infer_schema) ($events_id)"
      
    let events_chunks = $events_id 
    let events_chunks = "_" + ($events_chunks | str replace --all "," "_") + "." 

    let $filtered_file = ($parquet_file|str replace ".parquet" "" ) + $events_chunks + $file_extension 
    print $"save filtered_events in: ($filtered_file)"
    print $"parquet_file: ($parquet_file)"
    if ($file_extension != 'parquet') {
                # flattenize one level and convert to parquet
                kunai_to_parquet $kunai_events_log_file 
         }
    # filter the parquet file
    filter_kunai_parquet  $parquet_file $events_id| polars save ($filtered_file)
    exit 0
}
    
def main [
    kunai_events_log_file: string 
    --events_id (-e): string = "1" # event id to filter, default is 1 => execve
    --infer-schema:  int = 200000 # Number of rows to infer schema. under 200000 it failed
    --lazy # eager mode is the default (*6 faster than lazy mode ... use ram) 
    --save (-s) # save filtered events in a parquet file and do not xplore dataframe
] {
    let eager_param  = match $lazy {
        true => {"--lazy"}
        false => {"--eager"}
    }  

    let save_param  = match $save {
        true => {true}
        false => {false}
    }  
      
    # file exists check
    try {ls ($kunai_events_log_file)} catch {return $"file ($kunai_events_log_file) not found"}
    let target_dir = ($kunai_events_log_file | path dirname)
    let unzipped_file = ($target_dir | path join ($kunai_events_log_file |path basename |path parse |get stem))
    let kunai_events_log_parquet = $unzipped_file + ".parquet"
    let file_extension = ($kunai_events_log_file | path parse | get extension)

    print $"main_file_extension = ($file_extension)"

    let parquet_file = match $file_extension {
                          'gz' => {$kunai_events_log_file | path basename |split column '.' |insert fich_name {$in.column1 + '.' + $in.column2 + '.' + $in.column3 + '.parquet'}|get fich_name.0},
                          'parquet' => { $kunai_events_log_file}
                          _ => {"" }
                         }
                       
    if ($parquet_file == "") {print $"file must be parquet or gz";exit 0}

    print $"parquet_file_main ($parquet_file)"
   
    if ($save_param) {save_filtered_parquet $kunai_events_log_file $unzipped_file $file_extension $parquet_file $infer_schema $events_id } else {
        exploring_fdf $kunai_events_log_file $file_extension $parquet_file  $infer_schema $events_id 
    }
}
