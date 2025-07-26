cat  /var/log/kunai/events.log 
   | from json --objects
   | get data
   | filter {$in.command_line !~ "sshd" and $in.command_line !~ "/usr/sbin/tc" and $in.command_line !~ "mk_docker"}
   | default "nosocket" socket 
   | where socket != 'nosocket'
   | flatten --all
   | group-by command_line
   | explore

cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.data.command_line !~ "sshd" and $in.data.command_line !~ "/usr/sbin/tc" and $in.data.command_line !~ "mk_docker"}
   | flatten --all|flatten --all|flatten --all
   | default "nip" "ip"
   | where ip != "nip" 
   | explore

cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.data.command_line !~ "sshd" and $in.data.command_line !~ "/usr/sbin/tc" and $in.data.command_line !~ "mk_docker"}
   | flatten --all|flatten --all|flatten --all
   | where event_name == "dns_query"
   | explore


cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.data.command_line !~ "sshd" and $in.data.command_line !~ "/usr/sbin/tc" and $in.data.command_line !~ "mk_docker"}
   | flatten --all
   | flatten --all
   | flatten --all
   | group-by   event_name --to-table
   | get event_name
╭────┬──────────────╮
│  0 │ prctl        │
│  1 │ exit_group   │
│  2 │ read_config  │
│  3 │ kill         │
│  4 │ file_unlink  │
│  5 │ clone        │
│  6 │ file_create  │
│  7 │ file_rename  │
│  8 │ write_config │
│  9 │ execve       │
│ 10 │ send_data    │
│ 11 │ connect      │
│ 12 │ dns_query    │
│ 13 │ mmap_exec    │
│ 14 │ exit
╰────┴──────────────

cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.data.command_line !~ "sshd" and $in.data.command_line !~ "/usr/sbin/tc" and $in.data.command_line !~ "mk_docker"}
   | flatten --all|flatten --all|flatten --all
   | group-by   event_name --to-table
   | where event_name == "execve"
   | explore
# selection sur events
# requête dns
cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.info.event.name == "dns_query"} 
   | group-by data.query
   | columns
# group-by   event_name --to-table
ssh -t registry.iutbeziers.fr r#'sudo cat  /var/log/kunai/events.log'#| from json --objects |filter {$in.info.event.name == "dns_query"}|flatten --all|flatten --all|flatten --all

cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.info.event.name == "execve"}
   | get data 
   | flatten --all 
   | get command_linegroup-by   event_name --to-table
   | uniq -c

cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.info.event.name == "execve"}
   | get data
   | flatten --all
   | get command_line
   | uniq 
   | split column " "
   | get column1i
   | uniq -c

cat  /var/log/kunai/events.log
   | from json --objects
   | filter {$in.info.event.name == "connect"}i
   | flatten --all
   | select command_line socket src dst
   | flatten --all
   | explore


cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.info.event.name == "connect"}
   | flatten --all
   | select command_line socket src dst 
   | flatten --all
   | get dst_ip
   | uniq -c

cat  /var/log/kunai/events.log
   | from json --objects
   | filter {$in.info.event.name == "connect"}
   | flatten --all
   | select command_line socket src dst 
   | flatten --all
   | get dst_port
   | uniq -c

cat  /var/log/kunai/events.log 
   | from json --objects
   | filter {$in.info.event.name == "file_create"}
   | get data 
   | flatten --all
   | get path
   | uniq 
   | path parse
   | get extension
   | uniq -c 
   | sort-by -r count 
   | first 20

def is-numeric []: string -> bool 
     { $in like '^[+-]?\d+(\.\d*)?$' }
cat  /var/log/kunai/events.log
  | from json --objects
  | filter {$in.info.event.name == "file_create"}
  | get data
  | flatten --all
  | get path
  | uniq
  | path parse
  | get extension
  | uniq
  | where { not ($in | is-numeric) }

cat  /var/log/kunai/events.log
  | from json --objects
  | filter {$in.info.event.name == "file_create"}
  | get data
  | flatten --all
  | get path
  | uniq
  | path parse
  | get extension
  | uniq
  | where { try { into float; false } catch { true } }

cat  /var/log/kunai/events.log 
  | from json --objects
  | filter {$in.info.event.name == "kill"}
  | get data.target.exe
  | uniq -c
  | sort-by -r count

cat  /var/log/kunai/events.log 
  | from json --objects
  | filter {$in.info.event.name == "send_data"} 
  | select data.dst data.src 
  | flatten --all

cat  /var/log/kunai/events.log 
  | from json --objects
  | filter {$in.info.event.name == "send_data"} 
  | select data.dst data.src 
  | flatten --all
  | get port 
  | uniq -c
  | sort-by -r count
  | first 20
╭────┬───────┬───────╮
│  # │ value │ count │
├────┼───────┼───────┤
│  0 │  1514 │ 18299 │
│  1 │  9200 │ 14666 │
│  2 │ 64294 │ 12196 │
│  3 │ 47016 │  8943 │
│  4 │ 43182 │  5272 │
│  5 │ 38644 │   437 │
│  6 │ 55062 │   361 │
│  7 │ 43010 │   361 │
│  8 │ 60800 │   347 │
│  9 │ 44952 │   345 │
│ 10 │   443 │   305 │
│ 11 │ 38840 │   290 │
│ 12 │ 35250 │   282 │
│ 13 │ 39114 │   189 │
│ 14 │ 42990 │    97 │
│ 15 │ 43068 │    85 │
│ 16 │ 49222 │    77 │
│ 17 │  8090 │    38 │
│ 18 │ 56742 │    30 │
│ 19 │ 42996 │    23 │
╰────┴───────┴───────╯
cat  /var/log/kunai/events.log 
  | from json --objects
  | filter {$in.info.event.name == "send_data"} 
  | select data.dst data.src 
  | flatten --all
  | get ip |uniq -c|sort-by -r count
  | first 20

cat  /var/log/kunai/events.log 
  | from json --objects
  | filter {$in.info.event.name == "send_data"} 
  | select data.dst data.src 
  | flatten --all
  | group-by hostname

def filter_kunai_logs [
    events_log: string 
    event_id: int] {
    # build events table from kunai config
    print $"event_id: ($event_id)"
    let table_events = kunai config --list-events
                      | from ssv --noheaders 
                      | insert events_name {each {$in.column0|str replace ':' ''}}
                      | rename column1 events_id
                      | move events_id --after events_name
                      | reject column1
    let list_events_id = $table_events | get events_id | into int
    let list_events_name = $table_events | get events_name
    # generate dataframe from events logfile 
    let kunai_polars_frame = (polars open $events_log --infer-schema 200000 -t ndjson
                              | polars unnest data info
                              | polars unnest event
                              | polars rename [name id] [event_name event_id])
    # filter event based on events ID
    match $event_id {                          
        $list_events_id =>  ($kunai_polars_frame | polars filter-with ((polars col event_id) == $event_id))
        0 =>  ($kunai_polars_frame)
    }
}    

