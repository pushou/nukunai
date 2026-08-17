# nukunai
nushell polars scripts to analyse/filter kunai logs (jsonl to parquet file).

<img src="images/explore.gif" width="150%" >

## requirements 
Nushell and its blazzing fast polars plugins, kunai logs (install kunai (https://github.com/kunai-project/) or see ngsoti malware dataset)
(it offers satisfactory performance with 100MB Kunai log files) 


## transform kunai jsonl files log file to parquet file 
```
nu kunai_to_parquet.nu events.log.1408.gz
╭────┬──────────────────────────────────────────────────────────────────────────────────────────────╮
│ 39 │ unzipped file  from events.log.1408.gz to events.log.1408                                    │
│    │ converting  events.log.1408 to events.log.1408.parquet --eager infer-schema=200000 flat=flat │
╰────┴──────────────────────────────────────────────────────────────────────────────────────────────
ls |get name|each {nu kunai_to_parquet.nu $in}
```

## explore dataset interactively
```
polars open  events.log.1408.parquet
     | polars collect
     | polars into-nu 
     | flatten --all | flatten --all
     | explore
```

## remember kunai events code:
```
❯ nu kunai_print_events_table.nu
╭────┬───────────────────┬───────────╮
│  # │    events_name    │ events_id │
├────┼───────────────────┼───────────┤
│  0 │ execve            │ 1         │
│  1 │ execve_script     │ 2         │
│  2 │ exit              │ 4         │
│  3 │ exit_group        │ 5         │
│  4 │ clone             │ 6         │
│  5 │ prctl             │ 7         │
│  6 │ kill              │ 8         │
│  7 │ ptrace            │ 9         │
│  8 │ init_module       │ 20        │
│  9 │ bpf_prog_load     │ 21        │
│ 10 │ bpf_socket_filter │ 22        │
│ 11 │ mprotect_exec     │ 40        │
│ 12 │ mmap_exec         │ 41        │
│ 13 │ connect           │ 60        │
│ 14 │ dns_query         │ 61        │
│ 15 │ send_data         │ 62        │
│ 16 │ read              │ 81        │
│ 17 │ read_config       │ 82        │
│ 18 │ write             │ 83        │
│ 19 │ write_config      │ 84        │
│ 20 │ file_rename       │ 85        │
│ 21 │ file_unlink       │ 86        │
│ 22 │ write_close       │ 87        │
│ 23 │ file_create       │ 88        │
│ 24 │ io_uring_sqe      │ 100       │
│ 25 │ file_scan         │ 500       │
├────┼───────────────────┼───────────┤
│  # │    events_name    │ events_id │
╰────┴───────────────────┴───────────╯
```

## filter kunai events 

The filter accepts both a gzipped kunai log (`.gz`) and an already flattened
`.parquet` file. A `.gz` input is read directly (polars decompresses it natively
via the ndjson reader), so **no intermediate parquet file is created** here,
unlike the compromise detection engine (see below), which converts
`.gz`/`.jsonl` inputs to a parquet file before analysing them.

### explore
```
nu kunai_filter_events.nu ./events.log.1521.gz -e 1,2,61
main_file_extension = gz
filter event_id: 1,2,61
```
This opens an interactive `explore` on the filtered events.

### -s save filters events to parquet file 
```
nu kunai_filter_events.nu ./events.log.1521.gz -e 1,2,61 -s
main_file_extension = gz
save filtered_events in: events.log.1521_1_2_61.parquet
filter event_id: 1,2,61

polars open  ./events.log.1521_1_2_61.parquet | polars collect | polars into-nu |explore
```

## display events count from file

```
nu kunai_events_analysis.nu ./events.log.1503.gz
╭────┬───────────────┬────────╮
│  # │     name      │ count  │
├────┼───────────────┼────────┤
│  0 │ prctl         │ 102434 │
│  1 │ send_data     │  90791 │
│  2 │ mmap_exec     │  79647 │
│  3 │ clone         │  61173 │
│  4 │ exit_group    │  52130 │
│  5 │ kill          │  50042 │
│  6 │ execve        │  15732 │
│  7 │ read_config   │  13552 │
│  8 │ connect       │   4479 │
│  9 │ dns_query     │   3118 │
│ 10 │ file_unlink   │   3041 │
│ 11 │ file_create   │   2422 │
│ 12 │ file_rename   │    791 │
│ 13 │ exit          │    316 │
│ 14 │ bpf_prog_load │    117 │
│ 15 │ execve_script │     80 │
╰────┴───────────────┴────────╯
```



## usefull oneliners

```
polars open events.log.5319.parquet | polars shape
╭───┬────────┬─────────╮
│ # │  rows  │ columns │
├───┼────────┼─────────┤
│ 0 │ 412240 │      44 │
╰───┴────────┴─────────╯
```

```
 polars open kunai.jsonl.parquet    | polars schema
╭──────────────┬─────────────────────────────────╮
│ ancestors    │ str                             │
│ command_line │ str                             │
│              │ ╭────────┬─────╮                │
│ exe          │ │ file   │ str │                │
│              │ │ md5    │ str │                │
│              │ │ sha1   │ str │                │
│              │ │ sha256 │ str │                │
│              │ │ sha512 │ str │                │
│              │ │ size   │ i64 │                │
│              │ │ error  │ str │                │
│              │ ╰────────┴─────╯                │
│              │ ╭──────────┬──────╮             │
│ dst          │ │ hostname │ str  │             │
│              │ │ ip       │ str  │             │
│              │ │ port     │ i64  │             │
│              │ │ public   │ bool │             │
│              │ │ is_v6    │ bool │             │
│              │ ╰──────────┴──────╯             
```

### flatten host_container 
```
❯ polars open events.log.1411.parquet
   | polars rename [name] [main_name]
   | polars unnest host
   | polars rename [uuid name container] [host_uuid host_name host_container]
   | polars collect

```
### right way to select containers events
```
timeit {polars open events.log.1380.parquet 
        | polars rename [name] [main_name]
        | polars unnest host
        | polars rename [uuid name container] [host_uuid host_name host_container]
        | polars unnest host_container| polars rename [name type] [container_name container_type]
        | polars filter ((polars col container_type) == "docker")| polars collect|polars into-nu}
16sec 154ms 189µs 325ns
```
### select src dst address dst port
```
 polars open events.log.1375.parquet
     | polars select src dst
     | polars unnest src
     | polars rename [ip port ] [src_ip src_port] 
     | polars unnest dst
     | polars rename [hostname ip port public is_v6]  [dst_hostname dst_ip dst_port dst_public dst_is_v6]
     | polars group-by (polars col dst_ip)
     | polars agg [(polars col dst_port|polars unique)]
     | polars into-nu
╭────┬─────────────────────────────────────────┬────────────────╮
│  # │                 dst_ip                  │    dst_port    │
├────┼─────────────────────────────────────────┼────────────────┤
│  0 │ 2a05:d018:19bb:ea02:8107:67e6:6dd9:97da │ ╭───┬────╮     │
│    │                                         │ │ 0 │ 53 │     │
│    │                                         │ ╰───┴────╯     │
│  1 │ 10.255.255.135                          │ ╭───┬─────╮    │
│    │                                         │ │ 0 │ 443 │    │
│    │                                         │ ╰───┴─────╯    │
│  2 │ 52.212.210.86                           │ ╭───┬─────╮    │
│    │                                         │ │ 0 │  53 │    │
│    │                                         │ │ 1 │ 443 │    │
│    │                                         │ ╰───┴─────╯    │
│  3 │ 54.220.9.34                             │ ╭───┬────╮     │
│    │                                         │ │ 0 │ 53 │     │
│    │                                         │ ╰───┴────╯
```

### dns

```
polars open events.log.1373.parquet 
    | polars  select query response dns_server_ip 
    | polars drop-nulls 
    | polars collect
    | polars into-nu

polars open events.log.1373.parquet
    | polars  select query response dns_server_ip
    | polars drop-nulls
    | polars group-by query
    | polars agg [(polars col response|polars count) (polars col dns_server_ip|polars unique)]
    | polars collect
╭───┬────────────────────────┬──────────┬──────────────────────╮
│ # │         query          │ response │    dns_server_ip     │
├───┼────────────────────────┼──────────┼──────────────────────┤
│ 0 │ api.crowdsec.net       │        4 │ ╭───┬──────────────╮ │
│   │                        │          │ │ 0 │ 10.6.255.106 │ │
│   │                        │          │ ╰───┴──────────────╯ │
│ 1 │ registry.local         │     2086 │ ╭───┬──────────────╮ │
│   │                        │          │ │ 0 │ 127.0.0.11   │ │
│   │                        │          │ │ 1 │ 10.6.255.106 │ │
│   │                        │          │ ╰───┴──────────────╯ │
│ 2 │ gitlab.com             │        2 │ ╭───┬──────────────╮ │
│   │                        │          │ │ 0 │ 10.6.255.106 │ │
│   │                        │          │ ╰───┴──────────────╯ │

polars open kunai.jsonl_61.parquet
    | polars  select query response dns_server
    | polars drop-duplicates
    | polars group-by query
    | polars agg [(polars col response|polars count) (polars col dns_server|polars unique)]
    | polars collect
╭───┬───────────────┬──────────┬──────────────────────────────────────────╮
│ # │     query     │ response │                dns_server                │
├───┼───────────────┼──────────┼──────────────────────────────────────────┤
│ 0 │ api.ipify.org │        2 │ ╭───┬──────────┬──────┬────────┬───────╮ │
│   │               │          │ │ # │    ip    │ port │ public │ is_v6 │ │
│   │               │          │ ├───┼──────────┼──────┼────────┼───────┤ │
│   │               │          │ │ 0 │ 10.0.2.3 │   53 │ false  │ false │ │
│   │               │          │ ╰───┴──────────┴──────┴────────┴───────╯ │
│ 1 │ kunai-sandbox │        1 │ ╭───┬──────────┬──────┬────────┬───────╮ │
│   │               │          │ │ # │    ip    │ port │ public │ is_v6 │ │
│   │               │          │ ├───┼──────────┼──────┼────────┼───────┤ │
│   │               │          │ │ 0 │ 10.0.2.3 │   53 │ false  │ false │ │
│   │               │          │ ╰───┴──────────┴──────┴────────┴───────╯ │
╰───┴───────────────┴──────────┴──────────────────────────────────────────╯
```

## filter command lines

```
polars open events.log.5319.parquet 
    | polars filter (polars col command_line|polars contains "curl|wget")
    | polars get command_line
    | polars collect
╭───────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│     # │                                                                command_line                                                                │
├───────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│     0 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     1 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     2 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     3 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     4 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     5 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     6 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     7 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     8 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     9 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│   ... │ ...                                                                                                                                        │
│ 23716 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23717 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23718 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23719 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23720 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23721 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23722 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23723 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23724 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23725 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
╰───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## filter zombie processes
```
polars open  events.log.1502.parquet 
     | polars rename [command_line task flags path exe name] [main_command_line main_task main_flags main_path main_exe main_name] 
     | polars unnest main_task
     | polars rename [name pid tgid guuid uid  gid  namespaces flags] [t_name t_pid t_tgid t_guuid t_uid  t_gid  t_namespaces t_flags] 
     | polars filter ((polars col zombie) == 'true')
     | polars into-nu 
     | flatten --all
     | explore
```

---

## Compromise detection engine (`kunai_detect_compromise.nu`)

`kunai_detect_compromise.nu` is the detection engine behind the NGSOTI
analysis. It reads kunai event files in lazy polars and produces readable
reports.

**The processing goes through the creation of a Parquet file.** The three
source formats accepted are raw or compressed JSON lines (`.jsonl` /
`.jsonl.gz`) and the already-flattened Parquet format (`.parquet`). A
`.gz`/`.jsonl` input is **automatically converted to a `.parquet` file**
(`ensure_parquet`, via `kunai_to_parquet.nu`) before analysis: it is written
next to the source (or into `--cache-dir`), then re-read as Parquet. This is
far faster than re-parsing the raw ndjson on every run (benchmark ~14x on a
typical sample). A `.parquet` input is read directly, since its `data`/`info`
fields are already flattened. The script exposes reusable procedures used by
the example scripts below.

### The 9 detection families

`execve`, `file_create`, `connect`, `send_data`, `dns_query`, `kill`,
`bpf_prog_load`, `mmap_exec`, `prctl`.

Each line is classified as **benign** (legitimate platform noise: agents,
standard utilities, local IPs) or **suspicious** based on the task and the
process chain. Hijackable utilities (`docker`, `chmod`, `curl`, `bash`…) are
only benign when they come from a legitimate chain.

### Engine usage

```
# default: the 2 most recent .gz / .parquet files
nu kunai_detect_compromise.nu

# explicit files
nu kunai_detect_compromise.nu fichier1.gz fichier2.gz

# analyse a Parquet file
nu kunai_detect_compromise.nu kunai.jsonl.parquet

# display only 1 line per family
nu kunai_detect_compromise.nu -n 1

# run a single family
nu kunai_detect_compromise.nu -f execve

# interactive dataframe display
nu kunai_detect_compromise.nu --explore
```

Options : `--infer-schema <n>` (default 200000), `-n/--num <n>` (lines per
family, default 20), `-f/--family <fam>`, `-x/--explore`, `--no-json`
(writes the markdown only).

## Vendored rule repositories (submodules)

Two upstream rule repositories are embedded as git **submodules** so that the
nukunai engine can be cross-checked against the official kunai rules and their
phenotypes reused in `kunai_rules.nu` (generic pot commun):

| Submodule | Upstream | Content |
|-----------|----------|---------|
| `kunai_rules/vendored/community-rules` | https://github.com/kunai-project/community-rules | Official community rules, gene `.kun` |
| `kunai_rules/vendored/kunai-rules` | https://github.com/digisquad-repo/kunai-rules | 306 detection/dependency rules `.yaml` (same gene DSL) |

Clone them with their submodules:

```
git submodule update --init --recursive
```

They are read-only references for cross-checking. The engine itself stays
self-contained in `kunai_rules.nu` / `kunai_rules_local.nu` and the local
`kunai_rules/rules_v0.1/*.detection.kun` pot commun (same gene syntax as the
upstreames), so the analysis never depends on network access.

### Validating rules with `kunai replay`

If a kunai binary is installed, `kunai replay -r <rule>` can act as a syntax
validator for the detection rules:

```
kunai replay -r kunai_rules/rules_v0.1/c2_unusual_port.connect.detection.kun /tmp/events.jsonl
```

**Known quirk (kunai gene parser, reproducible up to 0.6.2):** a bare
integer literal on the RHS of `==` (e.g. `.data.dst.port == 31337`) is
rejected with `expected value or indirect_field_path`, and this is
**reproducible on the upstream rules** (digisquad
`net_c2_port.connect.detection.yaml` fails the same way). **Fix:** quote the
literal (`== '31337'`, `> '1000000'`, `> '7.5'`) — gene still compares
numerically. Our nushell/polars engine is therefore the source of truth for
analysis and handles integers/ranges/regex cleanly; use `kunai replay` only
as an auxiliary syntax check, not as the gate.

## Example: NGSOTI logs

The scripts below use the engine to analyse the kunai logs of the
[ngsoti](https://github.com/cert-orangecyberdefense/ngsoti) malware dataset and
detect suspicious behaviour.

### Files

| File | Role |
|------|------|
| `ngsoti_detail.nu` | Detailed report for one sample (one file via `FILE`) → `.md` + `.json` |
| `ngsoti_all.nu` | Processes every sample of `logs/ngsoti/*/kunai.jsonl.gz` sequentially |
| `ngsoti_report.nu` | Summary of alerts per family for **all** samples (parallel, 4 cores) |

### Produced reports

Each run writes its reports into `scanresult<TS>/` (a directory shared by the
whole session), with a descriptive name based on the detected families and the
short hash of the sample:

```
scanresult20260816_000812/15e67237_execve1_connect35_send_data9286_dns_query2_prctl3.md
scanresult20260816_000812/15e67237_execve1_connect35_send_data9286_dns_query2_prctl3.json
```

- **`.md`** : alert details per family (25 first lines max) + consolidated
  balance sheet (most active IPs/ports, download URLs, dropped files).
- **`.json`** (default, use `--no-json` to disable) : complete structure that
  can be consumed by programs:

```json
{
  "schema_version": 1,
  "scan_ts": "20260816_000812",
  "sample": { "file": "logs/ngsoti/<hash>/kunai.jsonl.gz", "hash": "<sha256>", "short_hash": "15e67237" },
  "fam_counts": { "execve": 1, "connect": 35, "send_data": 9286, "dns_query": 2, "prctl": 3 },
  "families": {
    "execve": [ "..." ],
    "send_data": [ "..." ]
  },
  "bilan": { "ipports": [ "..." ], "urls": [ "..." ], "files": [ "..." ] }
}
```

### Bulk analysis

**Detailed report for one sample** (via the `FILE` environment variable):
```
FILE=logs/ngsoti/<hash>/kunai.jsonl.gz nu ngsoti_detail.nu
```

**All samples, sequentially** (same `scanresult<TS>` directories):
```
nu ngsoti_all.nu
```

**Quick summary for all samples** (parallel, shows families + counters):
```
nu ngsoti_report.nu                    # output into $JCODE_SCRATCH_DIR/ngsoti_out (else ./ngsoti_out)
nu ngsoti_report.nu my_folder          # output into my_folder
```


