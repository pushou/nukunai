
# Convert kunai events log file to flatten parquet file - this will not be working for all kunai events log files
# usage: ls  *.gz |get name |each {nu kunai_to_parquet.nu $in}
# usage: nu kunai_to_flatten_parquet.nu events.log.4858.parquet
# polars open  events.log.4858.parquet |polars into-nu |explore
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
] {
let parquetfile = setFilename $eventslog
print $"converting  ($eventslog) to ($parquetfile) ($eager_param) infer-schema=($infer_schema_num)"

try {  
    if $eager_param == "--lazy" {polars open --infer-schema ($infer_schema_num) -t ndjson ($eventslog)} else  {
                                                polars open --infer-schema ($infer_schema_num) -t ndjson ($eventslog) --eager} 
    | polars unnest data info
    | polars rename [command_line task flags path exe] [main_command_line main_task main_flags main_path main_exe]
    | if ("id" in ($in | polars columns)) { polars rename id main_id } else { $in } #  slowest way if ("id" in ($in | polars columns)) {polars with-column {main_id: (polars col id)}} else {$in}|polars collect 
    | if ("main_exe" in ($in | polars columns)) {polars unnest main_exe 
                            | polars rename [path md5 sha1 sha256 sha512 size error] [
                                main_exe_path main_exe_md5 main_exe_sha1 main_exe_sha256 main_exe_sha512 main_exe_size main_exe_error]} else { $in }
    | if ("name" in ($in | polars columns)) { polars rename name main_name } else { $in } 
    | polars unnest dst |polars rename [ip port] [dst_ip dst_port]
    | polars unnest socket
    | polars rename [domain proto type] [socket_domain socket_proto socket_type]
    | polars unnest src
    | polars rename [hostname ip port public is_v6] [src_hostname src_ip src_port scr_public src_is_v6]
    | polars unnest target
    | polars rename [command_line exe task ] [target_command_line target_exe target_task]
    | polars unnest target_task
    | polars rename [name pid tgid guuid uid user gid group namespaces flags zombie] [
                     target_task_name target_task_pid target_task_tgid target_task_guuid 
                     target_task_uid target_task_user target_task_gid target_task_group target_task_namespaces target_task_flags target_task_zombie]
    | polars unnest target_exe 
    | polars rename path target_exe_path 
    | polars unnest main_task
    | polars rename [name pid tgid guuid uid user gid group namespaces flags zombie] [
                         main_task_name main_task_pid main_task_tgid main_task_guuid main_task_uid main_task_user main_task_gid main_task_group main_task_namespaces main_task_flags main_task_zombie ]
    | polars unnest main_task_namespaces
    | polars rename mnt main_task_namespaces_mnt
    | polars unnest host
    | polars rename [uuid name container] [host_uuid host_name host_container]
    | polars unnest event
    | polars rename [source id name uuid batch] [event_source event_id event_name event_uuid event_batch]
    | if ("parent_task" in ($in | polars columns)) {polars unnest parent_task 
                        | polars rename [name pid tgid guuid uid user gid group namespaces flags zombie] [
                          parent_task_name parent_task_pid parent_task_tgid parent_task_guuid parent_task_uid 
                          parent_task_user parent_task_gid parent_task_group parent_task_namespaces parent_task_flags parent_task_zombie ]} else { $in } 
    | if ("prog_type" in ($in | polars columns)) {polars unnest prog_type | polars  rename [id name] [prog_type_id prog_type_name]} else { $in } 
    | if ("host_container" in ($in | polars columns)) {polars unnest  host_container 
                           | polars rename [name type] [host_container_name host_container_type]} else { $in } 
    | if ("mapped" in ($in | polars columns)) {polars unnest  mapped 
                           | polars rename [path md5 sha1 sha256 sha512 size error] [mapped_path mapped_path_md5 
                             mapped_path_sha1 mapped_path_sha256 mapped_path_sha512 mapped_path_size mapped_path_error]} else { $in }                        
    | if ("dns_server" in ($in | polars columns)) {polars unnest dns_server 
                           | polars rename [ip port public is_v6] [dns_server_ip dns_server_port dns_server_public dns_server_is_v6 ]} else { $in } 
    | if ("target_task_namespaces" in ($in | polars columns)) {polars unnest target_task_namespaces 
                           | polars rename [mnt] [target_task_namespaces_mnt] } else { $in }
    | if ("parent_task_namespaces" in ($in | polars columns)) {polars unnest  parent_task_namespaces 
                           | polars rename [mnt] [parent_task_namespaces_mnt] } else { $in }
    | if ("interpreter" in ($in | polars columns)) {polars unnest interpreter 
                           | polars rename [path md5 sha1 sha256 sha512 size error] [interpreter_path interpreter_md5 
                             interpreter_sha1 interpreter_sha256 interpreter_sha512 interpreter_size interpreter_error]} else { $in }
    | if ("bpf_prog" in ($in | polars columns)) {polars unnest bpf_prog 
                           | polars rename [md5 sha1 sha256 sha512 size] [bpf_prog_md5 bpf_prog_sha1 bpf_prog_sha256 bpf_prog_sha512 bpf_prog_size]} else { $in } 
    | polars save -t parquet $parquetfile } catch {|err| $err.msg ; $"converting ($eventslog) to ($parquetfile) failed" }  
}

export def main [
    kunai_events_log_file: string 
    --infer-schema:  int = 200000 # Number of rows to infer schema. under 200000 it failed
    --lazy # lazy is the Default  but eager mode is *6 faster than lazy mode use a lot of ram 
] {
    let eager_param  = match $lazy {
        true => {"--lazy"}
        false => {"--eager"}
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
        save_into_parquet $events_log_unzipped $eager_param $infer_schema  
    } else {save_into_parquet $kunai_events_log_file $eager_param $infer_schema}
}

