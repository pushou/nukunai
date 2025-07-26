#Usage:
# > nu kunai_events_analysis.nu events.log.1373.parquet or gzipped   
# print the number of events by event_name in a kunai events log file
#Flags:
#   -h, --help - Display the help message for this command
#Parameters:
 # kunai events log file <string>
use ./kunai_to_parquet.nu 

def count_by_events_name_parquet [
    eventslogparquet: string ] {
    let kunai_polars_frame = (polars open $eventslogparquet)
    $kunai_polars_frame|polars get event|polars unnest event|polars get name|polars value-counts |polars sort-by [count] -r [true]
}


def main [
    kunai_events_log_file: string 
    --infer-schema:  int = 200000 # Number of rows to infer schema. under 200000 it failed
    --lazy # eager mode is the default (*6 faster than lazy mode but use a lot of ram) 
    ] {
    
    let eager_param  = match $lazy {
        true => {"--lazy"}
        false => {"--eager"}
    }  
    
    # file exists check
    try {ls ($kunai_events_log_file)} catch {return $"file ($kunai_events_log_file) not found"}
    let file_extension = ($kunai_events_log_file | path parse | get extension)

    # let table_events = kunai config --list-events|from ssv --noheaders |insert events_name {each {$in.column0|str replace ':' ''}}|rename column1 events_id|move events_id --after events_name|reject column1
    # let list_events_id = $table_events | get events_id | into int
    # let list_events_name = $table_events | get events_name    
    
    if ($file_extension == 'parquet') { count_by_events_name_parquet $kunai_events_log_file } else {
                            let target_dir = ($kunai_events_log_file | path dirname)
                            let unzipped_file = ($target_dir | path join ($kunai_events_log_file |path basename |path parse |get stem))
                            let kunai_events_log_parquet = $unzipped_file + ".parquet"
                            # flattenize and convert to parquet
                            kunai_to_parquet $kunai_events_log_file --infer-schema $infer_schema
                            # extract events
                            count_by_events_name_parquet $kunai_events_log_parquet }
    }
