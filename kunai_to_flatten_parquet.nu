#!/usr/bin/env nu
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

def has-col [
    data: any
    col: string
] {
    let cols: list<string> = ($data | polars columns)
    $col in $cols
}

def rename-cols [df: any, specs: list<list<string>>] {
    mut acc = $df
    let cols: list<string> = ($df | polars columns)
    for spec in $specs {
        let src = ($spec | get 0)
        let dst = ($spec | get 1)
        if $src in $cols {
            $acc = ($acc | polars rename $src $dst)
        }
    }
    $acc
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
    | rename-cols $in [ ["command_line" "main_command_line"], ["task" "main_task"], ["flags" "main_flags"], ["path" "main_path"], ["exe" "main_exe"] ]
    | if (has-col $in "id") { polars rename id main_id } else { $in } #  slowest way if (has-col $in "id") {polars with-column {main_id: (polars col id)}} else {$in}|polars collect 
    | if (has-col $in "main_exe") {polars unnest main_exe 
                            | rename-cols $in [ ["path" "main_exe_path"], ["md5" "main_exe_md5"], ["sha1" "main_exe_sha1"], ["sha256" "main_exe_sha256"], ["sha512" "main_exe_sha512"], ["size" "main_exe_size"], ["error" "main_exe_error"] ]} else { $in }
    | if (has-col $in "name") { polars rename name main_name } else { $in } 
    | polars unnest dst |rename-cols $in [ ["ip" "dst_ip"], ["port" "dst_port"] ]
    | polars unnest socket
    | rename-cols $in [ ["domain" "socket_domain"], ["proto" "socket_proto"], ["type" "socket_type"] ]
    | polars unnest src
    | rename-cols $in [ ["hostname" "src_hostname"], ["ip" "src_ip"], ["port" "src_port"], ["public" "scr_public"], ["is_v6" "src_is_v6"] ]
    | polars unnest target
    | rename-cols $in [ ["command_line" "target_command_line"], ["exe" "target_exe"], ["task" "target_task"] ]
    | polars unnest target_task
    | rename-cols $in [ ["name" "target_task_name"], ["pid" "target_task_pid"], ["tgid" "target_task_tgid"], ["guuid" "target_task_guuid"], ["uid" "target_task_uid"], ["user" "target_task_user"], ["gid" "target_task_gid"], ["group" "target_task_group"], ["namespaces" "target_task_namespaces"], ["flags" "target_task_flags"], ["zombie" "target_task_zombie"] ]
    | polars unnest target_exe 
    | polars rename path target_exe_path 
    | polars unnest main_task
    | rename-cols $in [ ["name" "main_task_name"], ["pid" "main_task_pid"], ["tgid" "main_task_tgid"], ["guuid" "main_task_guuid"], ["uid" "main_task_uid"], ["user" "main_task_user"], ["gid" "main_task_gid"], ["group" "main_task_group"], ["namespaces" "main_task_namespaces"], ["flags" "main_task_flags"], ["zombie" "main_task_zombie"] ]
    | polars unnest main_task_namespaces
    | polars rename mnt main_task_namespaces_mnt
    | polars unnest host
    | rename-cols $in [ ["uuid" "host_uuid"], ["name" "host_name"], ["container" "host_container"] ]
    | polars unnest event
    | rename-cols $in [ ["source" "event_source"], ["id" "event_id"], ["name" "event_name"], ["uuid" "event_uuid"], ["batch" "event_batch"] ]
    | if (has-col $in "parent_task") {polars unnest parent_task 
                        | rename-cols $in [ ["name" "parent_task_name"], ["pid" "parent_task_pid"], ["tgid" "parent_task_tgid"], ["guuid" "parent_task_guuid"], ["uid" "parent_task_uid"], ["user" "parent_task_user"], ["gid" "parent_task_gid"], ["group" "parent_task_group"], ["namespaces" "parent_task_namespaces"], ["flags" "parent_task_flags"], ["zombie" "parent_task_zombie"] ]} else { $in } 
    | if (has-col $in "prog_type") {polars unnest prog_type | rename-cols $in [ ["id" "prog_type_id"], ["name" "prog_type_name"] ]} else { $in } 
    | if (has-col $in "host_container") {polars unnest  host_container 
                           | rename-cols $in [ ["name" "host_container_name"], ["type" "host_container_type"] ]} else { $in } 
    | if (has-col $in "mapped") {polars unnest  mapped 
                           | rename-cols $in [ ["path" "mapped_path"], ["md5" "mapped_path_md5"], ["sha1" "mapped_path_sha1"], ["sha256" "mapped_path_sha256"], ["sha512" "mapped_path_sha512"], ["size" "mapped_path_size"], ["error" "mapped_path_error"] ]} else { $in }                        
    | if (has-col $in "dns_server") {polars unnest dns_server 
                           | rename-cols $in [ ["ip" "dns_server_ip"], ["port" "dns_server_port"], ["public" "dns_server_public"], ["is_v6" "dns_server_is_v6"] ]} else { $in } 
    | if (has-col $in "target_task_namespaces") {polars unnest target_task_namespaces 
                           | rename-cols $in [ ["mnt" "target_task_namespaces_mnt"] ] } else { $in }
    | if (has-col $in "parent_task_namespaces") {polars unnest  parent_task_namespaces 
                           | rename-cols $in [ ["mnt" "parent_task_namespaces_mnt"] ] } else { $in }
    | if (has-col $in "interpreter") {polars unnest interpreter 
                           | rename-cols $in [ ["path" "interpreter_path"], ["md5" "interpreter_md5"], ["sha1" "interpreter_sha1"], ["sha256" "interpreter_sha256"], ["sha512" "interpreter_sha512"], ["size" "interpreter_size"], ["error" "interpreter_error"] ]} else { $in }
    | if (has-col $in "bpf_prog") {polars unnest bpf_prog 
                           | rename-cols $in [ ["md5" "bpf_prog_md5"], ["sha1" "bpf_prog_sha1"], ["sha256" "bpf_prog_sha256"], ["sha512" "bpf_prog_sha512"], ["size" "bpf_prog_size"] ]} else { $in } 
    | polars save -t parquet $parquetfile } catch {|err| $err.msg ; $"converting ($eventslog) to ($parquetfile) failed" }  
}

export def main [
    kunai_events_log_file: string 
    --infer-schema:  int = 200000 # Number of rows to infer schema. under 200000 it failed
    --eager # eager mode is *6 faster than lazy mode but use a lot of ram ; lazy is the default (works everywhere)
] {
    let eager_param  = if $eager { "--eager" } else { "--lazy" }  

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

