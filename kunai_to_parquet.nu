
export def setFilename [
    eventslog: string     
] {
let extension = ($eventslog|path parse|get extension)

match $extension {
  'gz' => {$eventslog | path basename |split column '.' |insert fich_name {$in.column1 + '_' + $in.column3 + '.parquet'}|get fich_name.0},
  _ => {$eventslog + ".parquet"}     
  }   
}

def save_into_parquet [
    eventslog: string 
    eager_param: string
    infer_schema_num: int 
    noflat_param: string
] {
let parquetfile = setFilename $eventslog
print $"converting  ($eventslog) to ($parquetfile) ($eager_param) infer-schema=($infer_schema_num) flat=($noflat_param)"

try {  
    if $eager_param == "--lazy" {polars open --infer-schema ($infer_schema_num) -t ndjson ($eventslog)} else  {
            polars open --infer-schema ($infer_schema_num) -t ndjson ($eventslog) --eager} 
    |if $noflat_param == 'flat' {polars unnest data info} else {$in}
    | polars save -t parquet $parquetfile } #catch { $"converting ($eventslog) to ($parquetfile) failed - verify input file have .log or .gz extension" } 
}


export def main [
    kunai_events_log_file: string 
    --infer-schema:  int = 200000 # Number of rows to infer schema. under 200000 it failed
    --lazy # lazy is the Default  but eager mode is *6 faster than lazy mode use a lot of ram 
    --noflat # do not flattenize at all, just convert to parquet
] {
    let eager_param  = match $lazy {
        true => {"--lazy"}
        false => {"--eager"}
    }  

    let noflat_param  = match $noflat {
        true => {"noflat"}
        false => {"flat"}
    }

    # file exists check
    try {ls ($kunai_events_log_file)} catch {return $"file ($kunai_events_log_file) not found"}

    
    # in eager mode, the file must not be compressed or polars open failed
    let file_extension = ($kunai_events_log_file | path parse | get extension)
    if $file_extension == 'parquet' {
        return $"skipping parquet file ($kunai_events_log_file) already converted"} 
    if $file_extension == 'gz' and $eager_param == "--eager" {
        try {gzip -d --force $kunai_events_log_file } catch {'you must install gzip!'}
        let ori_dir = ($kunai_events_log_file | path dirname)
        let events_log_unzipped = ($ori_dir | path join ($kunai_events_log_file |path basename |path parse |get stem))
        print $"unzipped file  from ($kunai_events_log_file) to ($events_log_unzipped)"
        save_into_parquet $events_log_unzipped $eager_param $infer_schema $noflat_param  
    } else {save_into_parquet $kunai_events_log_file $eager_param $infer_schema $noflat_param}
}
