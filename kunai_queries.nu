#!/usr/bin/env nu
# kunai_queries.nu - structured ad-hoc queries on kunai logs (event-driven EDR).
#
# Centralizes and structures (as subcommands with options) the scattered one-liners
# of `kunai_requests.nu`, modeled after the official kunai examples/scripts
# (filter_connect/exec/kill/send, count_event_types, view_network_events,
# view_command_lines). Each subcommand is a query = filtering on the event name
# + aggregation (count / top-N) + sort.
#
# Usage:
#   nu kunai_queries.nu <query> <file> [--top N] [--filter "regex"] [--all] [--infer-schema N]
#   nu kunai_queries.nu help
#   nu kunai_queries.nu <query> --help   (detailed help for the subcommand)
#
# `<file>` can be a `.parquet` (already flattened) or a `.gz`/`.jsonl`/`.log`
# (kunai ndjson, transparently decompressed by polars).
#
# Queries (each subcommand exposes its option variants, see --help):
#   events           count of events per name                  (count_event_types)
#   dns              DNS queries grouped by query              (dns query)
#   command-lines    most frequent execve command_line         (view_command_lines)
#   exes             first word of execve command_line         (exe palette)
#   connect-ips      top dst_ip of connections (--public)      (filter_connect)
#   connect-ports    top dst_port of connections
#   network          network view: command_line + dst (--filter "regex")
#   dst-ports        ports + unique ancestors per destination IP (--all)
#   file-extensions  extensions of created paths               (filter_write)
#   file-creates     created files (--skip-benign to hide system noise)
#   kill-targets     killed targets (kill)                     (filter_kill)
#   prctl-options    prctl options per process
#   file-renames     old->new renames (file_rename)
#   file-unlinks     deleted files (file_unlink)
#   mmap-execs       RX-mapped files, drop-and-run (--suspicious --mprotect -s)
#   bpf-progs        loaded eBPF programs (bpf_prog_load)
#   send-ports       top send_data ports                       (filter_send)
#   send-ips         top send_data IPs (--public)
#   iocs             consolidated IOC view (network/ports/dns/files/exec) (--public)
#
# NB: nushell/polars conventions and pitfalls documented in MEMORIES.md:
#  - polars `or`/`and` Exprs are wrapped in parentheses, never `polars or`;
#  - Int128 columns (error_code...) are NEVER selected before a
#    `collect`/`into-nu`: only safe columns are kept (cols_keep helper);
#  - kunai formats vary: `unnestif` / `cols_keep` helpers (see the engine
#    kunai_detect_compromise.nu) for conditionally present columns.

# ---------------------------------------------------------------------------
# gz/jsonl -> parquet conversion in cache (same logic as the engine).
# A .gz/.jsonl is first converted to .parquet (next to the source), then read
# from the parquet: far faster than parsing the ndjson on every run.
# Reuses the cache if it is newer than the source. Returns the path.
def ensure_parquet [file: string, infer_schema: int] {
    # already parquet: nothing to convert.
    if ($file | path parse | get extension) == 'parquet' { return $file }

    # Convergent target path (same rule as the engine / kunai_to_parquet.nu):
    #   test.jsonl    -> test.jsonl.parquet
    #   test.jsonl.gz -> test.jsonl.parquet   (only the `.gz` is stripped)
    let ext = ($file | path parse | get extension)
    let target = if $ext == 'gz' {
        let no_gz = ($file | str replace -r '\.gz$' '')
        $"($no_gz).parquet"
    } else {
        $"($file).parquet"
    }

    # Reuses the cache if it exists, is sane (PAR1 footer) and newer than the
    # source. A truncated/corrupt parquet (missing footer) is removed and
    # regenerated: a conversion can sometimes leave an incomplete file.
    if ($target | path exists) {
        let sane = ((^tail -c 4 $target) == 'PAR1')
        if $sane {
            let src_m = ((ls $file).0.modified?)
            let dst_m = ((ls $target).0.modified?)
            if ($dst_m | is-not-empty) and ($src_m | is-not-empty) and ($dst_m >= $src_m) {
                return $target
            }
        }
        # parquet absent, corrupt, or outdated: regenerate it.
        rm -f $target
    }

    print $"(ansi yellow)⚙ converting (($file)) → (($target))(ansi reset)"
    let script = ($env.FILE_PWD | path join "kunai_to_parquet.nu")
    let res = (^nu $script $file --output $target --infer-schema $infer_schema | complete)
    if $res.exit_code != 0 {
        error make { msg: $"conversion of ($file) failed: ($res.stderr)" }
    }
    $target
}

# Opens the source as a FLATTENED lazy frame with no column collision:
#  - .parquet assumed after any .gz / .jsonl / .log conversion,
#  - `event` is renamed `event_info` BEFORE the unnest so it does not collide
#    with `data.name` / `data.id` (some kunai versions have a `name` at the root
#    and inside `event` -> only one `name` column must remain).
def open_source [file: string, infer_schema: int] {
    let src = (ensure_parquet $file $infer_schema)
    let base = (polars open $src)
    let cols = ($base | polars schema | columns)
    let renamed = if 'event' in $cols { $base | polars rename event event_info } else { $base }
    $renamed
        | unnestif $in 'event_info'
        | unnestif $in 'host'
        | unnestif $in 'task'
        | unnestif $in 'parent_task'
}

# Returns the event name column to use (unique):
# `event_info_name` (from the renamed event) if present, else `name` (data).
def event_name_col [base] {
    let cols = ($base | polars schema | columns)
    if 'event_info_name' in $cols { 'event_info_name' } else if 'name' in $cols { 'name' } else { '' }
}

# conditional unnest: only unroll the column if it exists (varying kunai formats).
def unnestif [base, col: string] {
    let cols = ($base | polars schema | columns)
    if $col in $cols { $base | polars unnest $col -s "_" } else { $base }
}

# select among a list keeping only the columns actually present (avoids
# referencing an Int128 / missing column before collect). Returns a lazy frame.
def cols_keep [cols: list<string>] {
    let df = $in
    let present = ($df | polars schema | columns)
    let keep = ($cols | where {|c| $c in $present })
    if (($keep | length) == 0) {
        $df | polars select [event_name]
    } else {
        $df | polars select $keep
    }
}

# expects True/False on stdin and prints a `✗ no data` otherwise (avoids a silent empty table).
def maybe_note [s: bool] { if $s { } else { print $"(ansi yellow)✗ no row matches this filter on this file(ansi reset)" } }

# validates the presence and existence of the source file; prints the error otherwise. Returns a bool.
def require_file [file: string] {
    if ($file | is-empty) {
        print $"(ansi red)✗ missing file: provide a kunai .parquet / .gz / .jsonl path(ansi reset)"
        false
    } else if not ($file | path exists) {
        print $"(ansi red)✗ file not found: ($file)(ansi reset)"
        false
    } else { true }
}

# Registry of queries (subcommands) for help and argument validation.
# Defined here (top of file) because it is referenced by `check_args_pos`: in
# nushell, a variable referenced in a `def` must exist BEFORE that `def`
# (evaluation happens in load order).
const REQUESTS = {
    events:            { desc: 'count of events per name',                       arg: '' }
    dns:               { desc: 'DNS queries grouped by query',                   arg: '--top/--all' }
    'command-lines':   { desc: 'most frequent execve command_line',              arg: '--top/--all' }
    exes:              { desc: 'executable palette (1st word of command_line)',  arg: '--top/--all' }
    'connect-ips':     { desc: 'top dst_ip of connections',                      arg: '--top/--all' }
    'connect-ports':   { desc: 'top dst_port of connections',                    arg: '--top/--all' }
    network:           { desc: 'network view command_line + dst connect',        arg: '--filter "regex"' }
    'dst-ports':       { desc: 'ports + unique ancestors grouped by destination IP', arg: '--top/--all' }
    'file-extensions': { desc: 'extensions of created files',                    arg: '--top/--all' }
    'file-creates':    { desc: 'created files: path + writing binary',           arg: '--top/--all' }
    'kill-targets':    { desc: 'killed targets (kill)',                          arg: '--top/--all' }
    'prctl-options':   { desc: 'prctl options per process',                      arg: '--top/--all' }
    'file-renames':    { desc: 'old->new renames (file_rename)',                 arg: '--top/--all' }
    'file-unlinks':    { desc: 'deleted files (file_unlink)',                    arg: '--top/--all' }
    'mmap-execs':      { desc: 'RX-mapped files / drop-and-run',                 arg: '--top/--all/--mprotect/-s' }
    'bpf-progs':       { desc: 'loaded eBPF programs (rootkit)',                 arg: '--top/--all' }
    'send-ports':      { desc: 'top send_data ports',                            arg: '--top/--all' }
    'send-ips':        { desc: 'top send_data IPs',                              arg: '--top/--all' }
    iocs:              { desc: 'consolidated IOC view (egress / dns / execve)',  arg: '--public/--top/--all' }
}

# Clear message when unexpected positional arguments follow the query
# (the native nushell "Extra positional argument" error is cryptic and does not
# say what to do). Each subcommand accepts only ONE positional `<file>` (default
# ''); any surplus lands in `rest`. If the 1st positional (`file`) is itself
# another known query, we flag that two queries are not nested.
def check_args_pos [name: string, file: string, rest: list<string>] {
    let is_req = ($file in ($REQUESTS | columns)) and ($file != $name)
    if (($rest | length) > 0) or $is_req {
        let extra = (([$file] | append $rest | where {|p| ($p | is-not-empty) }) | str join ' ')
        let msg = ([
            ($"(ansi red)✗ unexpected positional arguments after '($name)': '($extra)'(ansi reset)")
            '  Usage: nu kunai_queries.nu <query> <file> [options]'
            '  The kunai <file> goes right after the query, then options; only one query per run.'
            (if $is_req { $"\n  '($file)' is ANOTHER query: queries are not nested, run one pass at a time." } else { '' })
        ] | where {|l| $l != '' } | str join "\n")
        error make { msg: $msg }
    }
}

# common base: flattened frame (data/info/event_info) filtered on the event name.
# Returns the filtered lazy frame (not collected yet).
def events_frame [file: string, infer_schema: int, evname: string] {
    let base = (open_source $file $infer_schema)
    let ncol = (event_name_col $base)
    if $ncol != '' {
        $base | polars filter ((polars col $ncol) == $evname)
    } else {
        # name column missing (rare): nothing to filter, return a safe empty frame.
        $base | polars filter (polars lit false)
    }
}

# Sorts a value-counts frame (count) by count descending then by value.
# Lazy -> collect -> into-nu.
def render_top [df, top: int, valuecol: string] {
    ($df
        | polars group-by $valuecol
        | polars agg [(polars col $valuecol | polars count)]
        | polars sort-by [count $valuecol] -r [true false]
        | polars collect
        | polars into-nu
        | first $top)
}

# True if an IP (text) is really public: excludes RFC1918 ranges
# (10/8, 172.16/12, 192.168/16), loopback (127.), link-local (169.254.),
# multicast/broadcast and the IPv4-mapped addresses of internal Docker
# networks (::ffff:10. / ::ffff:192.168. / ::ffff:172.16..31.).
# Text is enough here (dst_public is unreliable in IPv4-mapped form).
def is_public_ip [ip: string] {
    let i = ($ip | str trim)
    not (
        ($i == '0.0.0.0') or ($i == '::') or ($i == '::1')
        or ($i == '255.255.255.255')
        or (($i | str starts-with '10.') or ($i | str starts-with '192.168.'))
        or ($i =~ '^172\.(1[6-9]|2[0-9]|3[01])\.')
        or ($i starts-with '127.') or ($i starts-with '169.254.')
        or ($i starts-with '::ffff:10.') or ($i starts-with '::ffff:192.168.')
        or ($i =~ '^::ffff:172\.(1[6-9]|2[0-9]|3[01])\.')
        or ($i starts-with '::ffff:127.')
    )
}

# ------------------------------------------------- queries (subcommands)

# kunai event type -> sub-query + option variants (shown by events).
# Each subcommand lists here the real options it exposes (--public,
# --skip-benign, --suspicious/--mprotect, --filter...), so they can be seen at a
# glance. `iocs` is the consolidated view; its --public only applies to the
# network egress (connect/send_data), hence the annotation on those lines.
const EVENT_QUERY = {
    execve:        'command-lines / exes / iocs'
    execve_script: 'command-lines / exes / iocs'
    connect:       'connect-ips (--public) / connect-ports / network (--filter "regex") / dst-ports / iocs (--public)'
    send_data:     'send-ips (--public) / send-ports / iocs (--public)'
    dns_query:     'dns / iocs'
    file_create:   'file-extensions / file-creates (--skip-benign)'
    kill:          'kill-targets'
    prctl:         'prctl-options'
    file_rename:   'file-renames'
    file_unlink:   'file-unlinks'
    mmap_exec:     'mmap-execs (--suspicious --mprotect) / iocs'
    mprotect_exec: 'mmap-execs (--suspicious --mprotect) / iocs'
    bpf_prog_load: 'bpf-progs'
}

# Count of events per name (count_event_types).
def "main events" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'events' $file $rest
    if not (require_file $file) { return }
    let base = (open_source $file $infer_schema)
    let ncol = (event_name_col $base)
    if $ncol == '' {
        print $"(ansi yellow)✗ missing event name column(ansi reset)"
        return
    }
    # unify the column under `name` (event_info_name of the converted parquet, or name).
    let rows = ($base
        | polars select $ncol
        | polars rename $ncol name
        | polars drop-nulls
        | polars get name
        | polars value-counts
        | polars sort-by [count name] -r [true false]
        | polars collect
        | polars into-nu)
    if (($rows | length) == 0) {
        print $"(ansi yellow)✗ no events in this file(ansi reset)"
    } else {
        # Append the event type -> processing sub-query link (visible).
        $rows | each {|r| $r | merge {query: ($EVENT_QUERY | get -o $r.name | default '—')} }
    }
}

# DNS queries grouped by query (dns_query event), top-N or all.
def "main dns" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'dns' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'dns_query')
    let cols = ($base | polars schema | columns)
    if 'query' not-in $cols {
        print $"(ansi yellow)✗ 'query' column missing for dns_query on this format(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select query
        | polars drop-nulls
        | polars get query
        | polars value-counts
        | polars sort-by [count query] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no DNS queries in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# most frequent execve command_line (view_command_lines).
def "main command-lines" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'command-lines' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'execve')
    let cols = ($base | polars schema | columns)
    if 'command_line' not-in $cols {
        print $"(ansi yellow)✗ 'command_line' column missing for execve(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select command_line
        | polars drop-nulls
        | polars get command_line
        | polars value-counts
        | polars sort-by [count command_line] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no execve in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# First word of each execve command_line (palette of invoked executables).
def "main exes" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'exes' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'execve')
    let cols = ($base | polars schema | columns)
    if 'command_line' not-in $cols {
        print $"(ansi yellow)✗ 'command_line' column missing for execve(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select command_line
        | polars drop-nulls
        | polars get command_line
        | polars collect
        | polars into-nu
        | each {|r| $r.command_line | split row ' ' | first }
        | uniq -c
        | sort-by count -r
        | rename count exe)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no execve in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Top destination IPs of connections. `--public` only shows really public IPs
# (excludes RFC1918 / loopback / internal Docker networks, including the
# IPv4-mapped ::ffff:).
def "main connect-ips" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --public (-p)         # keep only really public IPs
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'connect-ips' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'connect')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    if 'dst_ip' not-in $cols {
        print $"(ansi yellow)✗ no dst_ip column for connect on this format(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select dst_ip
        | polars drop-nulls
        | polars get dst_ip
        | polars value-counts
        | polars sort-by [count dst_ip] -r [true false]
        | polars collect
        | polars into-nu
        | where {|r| if not $public { true } else { (is_public_ip ($r.dst_ip | into string)) } })
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no connections in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Direct `connect-ips` call (after `source kunai_queries.nu`), alias of `main connect-ips`.
def connect-ips [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --public (-p)         # keep only really public IPs
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'connect-ips' $file $rest
    main connect-ips $file --public=$public --top=$top --all=$all --infer-schema=$infer_schema
}

# Top dst_port of connections.
def "main connect-ports" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'connect-ports' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'connect')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    if 'dst_port' not-in $cols {
        print $"(ansi yellow)✗ no dst_port column for connect on this format(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select dst_port
        | polars drop-nulls
        | polars get dst_port
        | polars value-counts
        | polars sort-by [count dst_port] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no connections in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Ports per destination IP: groups the dst addresses and aggregates their unique ports.
# Accepts both a parquet with NESTED `dst` (non-flattened source format, e.g.
# eventsreg.log.parquet) and an already flattened parquet (`dst_ip` / `dst_port` at
# root level). The NESTED form reproduces the wanted polars pipeline:
#   polars get dst | drop-nulls | unnest dst | select ip port | collect
#   | group-by ip | agg (unique port) | collect
def "main dst-ports" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 20       # number of rows to show
    --all (-a)            # show everything (instead of the first --top)
    --infer-schema: int = 200000
] {
    check_args_pos 'dst-ports' $file $rest
    if not (require_file $file) { return }
    let base = (open_source $file $infer_schema)
    let cols = ($base | polars schema | columns)
    if 'dst' in $cols {
        # source format with nested `dst`: unnest -> ip / port, then group-by ip.
        # `ancestors` (pipe-separated string) is aggregated to uniques like the port.
        let all_rows = ($base
            | polars select dst ancestors
            | polars drop-nulls
            | polars unnest dst
            | polars select ip port ancestors
            | polars collect
            | polars group-by (polars col ip)
            | polars agg [(polars col port | polars unique) (polars col ancestors | polars unique)]
            | polars sort-by ip
            | polars collect
            | polars into-nu)
        if (($all_rows | length) == 0) {
            print $"(ansi yellow)✗ no destination ip in this file(ansi reset)"
        } else if $all { $all_rows } else { $all_rows | first $top }
    } else if 'dst_ip' in $cols {
        # already flattened parquet flat=flat: dst_ip / dst_port at root level,
        # `ancestors` (pipe-separated string) remains a root column.
        let all_rows = ($base
            | polars select dst_ip dst_port ancestors
            | polars drop-nulls
            | polars collect
            | polars group-by (polars col dst_ip | polars as ip)
            | polars agg [(polars col dst_port | polars unique | polars as port) (polars col ancestors | polars unique)]
            | polars sort-by ip
            | polars collect
            | polars into-nu)
        if (($all_rows | length) == 0) {
            print $"(ansi yellow)✗ no destination ip in this file(ansi reset)"
        } else if $all { $all_rows } else { $all_rows | first $top }
    } else {
        print $"(ansi red)✗ neither a nested `dst` column nor `dst_ip`/`dst_port` in this file(ansi reset)"
    }
}

# Network view: command_line + dst (connect), grouped by command_line
# (uniques of dst_ip / dst_port / dst_hostname / dst_public), filterable on command_line.
def "main network" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --filter: string = ''  # regex on command_line; e.g. --filter "ssh"
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'network' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'connect')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    let keep = ($cols | where {|c| $c in ['command_line' 'dst_ip' 'dst_port' 'dst_hostname' 'dst_public'] })
    if ($keep | length) == 0 {
        print $"(ansi yellow)✗ no displayable column available(ansi reset)"
        return
    }
    # columns to aggregate as uniques = all except the command_line key
    let dstcols = ($keep | where {|c| $c != 'command_line' })
    let aggs = ($dstcols | each {|c| (polars col $c | polars unique) })
    let all_rows = ($base
        | cols_keep $keep
        | polars collect
        | polars group-by (polars col command_line)
        | polars agg $aggs
        | polars sort-by command_line
        | polars collect
        | polars into-nu)
    if ($filter | is-empty) { $all_rows } else { $all_rows | where {|r| $r.command_line =~ $filter} }
}

# List of created files (file_create): created path + writing binary.
# `path` = created path (e.g. /root/bin), `exe_path` = binary that writes (e.g. /tmp/sample.bin).
# `--skip-benign` hides the writes of known benign system agents / utilities
# (sftp-server/restic, systemd-journald, osqueryd, bash, cp, tar...) to keep only
# the file creations that do not fall into expected noise.
def "main file-creates" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 20       # number of rows
    --all (-a)            # all rows
    --skip-benign (-b)    # hide known benign system/utility writers
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'file-creates' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'file_create')
    let cols = ($base | polars schema | columns)
    if 'path' not-in $cols {
        print $"(ansi yellow)✗ path column missing for file_create(ansi reset)"
        return
    }
    # the created path (path) is renamed to avoid colliding with the writing
    # binary path when unnesting the `exe` struct (same logic as
    # event -> event_info). The inner `exe` field differs by format:
    # `path` (registry/parquet) or `file` (ngsoti/jsonl) -> rename the right one
    # to writer_path.
    let renamed = if 'exe' in $cols { $base | polars rename path created_path } else { $base }
    let unnested = ($renamed | unnestif $in 'exe')
    let after = ($unnested | polars schema | columns)
    let writer_col = (if 'exe_path' in $after { 'exe_path' } else if 'exe_file' in $after { 'exe_file' } else { '' })
    let df = ($unnested
        | (if $writer_col != '' { polars rename $writer_col writer_path } else { $in })
        | cols_keep [created_path writer_path command_line]
        | polars drop-nulls
        | polars unique -s [created_path]
        | polars sort-by created_path
        | polars collect
        | polars into-nu)
    if (($df | length) == 0) {
        print $"(ansi yellow)✗ no file_create in this file(ansi reset)"
        return
    }
    if $skip_benign {
        # local equivalent of the engine's legit_agents + benign_utilities, binary
        # names of the writer: sftp-server/restic, journald, systemd, osquery,
        # shell switches and mundane utilities of the system/backup chain.
        let benign = ['sftp-server','systemd-journald','systemd-logind','systemd',
                      'systemd-xdg-autostart-generator','osqueryd','wazuh-agentd',
                      'cmk-agent-ctl','kunai','dash','bash','sh','cp','tar','mktemp',
                      'gzip','xz','mv','rm','touch','mkdir']
        let kept = ($df | where {|r|
            let w = ($r.writer_path | path basename)
            $w not-in $benign and (not ($r.created_path =~ 'restic-temp'))
        })
        if (($kept | length) == 0) {
            print $"(ansi yellow)✗ no non-benign file_create (--skip-benign) in this file(ansi reset)"
            return
        }
        if $all { $kept } else { $kept | first $top }
        return
    }
    if $all { $df } else { $df | first $top }
}

# Extensions of created paths (file_create / filter_write).
def "main file-extensions" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'file-extensions' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'file_create')
    let cols = ($base | polars schema | columns)
    if 'path' not-in $cols {
        print $"(ansi yellow)✗ path column missing for file_create(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select path
        | polars drop-nulls
        | polars get path
        | polars collect
        | polars into-nu
        | each {|r| $r.path | path parse | get extension | str trim | if ($in == '') { '(no ext)' } else { $in } }
        | group-by { $in }
        | transpose key extension
        | each {|r| { extension: $r.key, count: ($r.extension | length) } }
        | sort-by count -r)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no file_create in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Killed targets (kill / filter_kill): target* column once unrolled by unnestif.
def "main kill-targets" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'kill-targets' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'kill')
    let base = (unnestif $base 'target')
    let cols = ($base | polars schema | columns)
    let tcol = (if 'target_executable' in $cols { 'target_executable' } else if 'target_exe' in $cols { 'target_exe' } else if 'target' in $cols { 'target' } else { '' })
    if $tcol == '' {
        print $"(ansi yellow)✗ target \(target*\) column missing for kill on this format(ansi reset)"
        return
    }
    let all_rows = ($base
        | cols_keep [$tcol]
        | polars drop-nulls
        | polars get $tcol
        | polars value-counts
        | polars sort-by [count $tcol] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no kill in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Most frequent prctl options, grouped by (task_name, option): shows WHO calls
# WHICH prctl (PR_SET_* hardening, PR_CAPBSET_READ reads...).
def "main prctl-options" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'prctl-options' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'prctl')
    let cols = ($base | polars schema | columns)
    if 'option' not-in $cols {
        print $"(ansi yellow)✗ 'option' column missing for prctl on this format(ansi reset)"
        return
    }
    let grp = if 'task_name' in $cols { [task_name option] } else { [option] }
    let all_rows = ($base
        | polars select $grp
        | polars drop-nulls
        | polars group-by $grp
        | polars agg [(polars col (if 'option' in $grp { 'option' } else { $grp.0 }) | polars count | polars as count)]
        | polars sort-by [count option] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no prctl in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Renames (file_rename): most frequent (task_name, old, new) pairs.
def "main file-renames" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'file-renames' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'file_rename')
    let cols = ($base | polars schema | columns)
    if 'old' not-in $cols or 'new' not-in $cols {
        print $"(ansi yellow)✗ 'old'/'new' columns missing for file_rename on this format(ansi reset)"
        return
    }
    let grp = (if 'task_name' in $cols { [task_name old new] } else { [old new] })
    let all_rows = ($base
        | polars select $grp
        | polars drop-nulls
        | polars group-by $grp
        | polars agg [(polars col old | polars count | polars as count)]
        | polars sort-by [count old] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no file_rename in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# File deletions (file_unlink): most deleted paths.
def "main file-unlinks" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'file-unlinks' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'file_unlink')
    let cols = ($base | polars schema | columns)
    if 'path' not-in $cols {
        print $"(ansi yellow)✗ 'path' column missing for file_unlink on this format(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select path
        | polars drop-nulls
        | polars get path
        | polars value-counts
        | polars sort-by [count path] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no file_unlink in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Executable memory mapping (mmap_exec / mprotect_exec): RX-mapped files.
# GROUPED ON mapped_path (the mapped file), NOT exe_path: the latter is only the
# running binary (sh/who/grep...) that maps its libs normally -> noise.
# The useful signal is the MAPPED file, especially from a temp/fd/memfd path
# (drop-and-run, cf. the engine's r_mmapexec_from_tmp). --suspicious filters on it.
def "main mmap-execs" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --mprotect            # also include mprotect_exec
    --suspicious (-s)     # keep only temporary mapped_path (tmp/fd/memfd)
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'mmap-execs' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'mmap_exec')
    let cols = ($base | polars schema | columns)
    if 'mapped' not-in $cols and 'mapped_file' not-in $cols {
        print $"(ansi yellow)✗ 'mapped'/'mapped_file' columns missing for mmap_exec on this format(ansi reset)"
        return
    }
    # mprotect_exec: same columns, concatenated to mmap_exec if requested.
    let base = if $mprotect {
        let mp = (events_frame $file $infer_schema 'mprotect_exec')
        if (($mp | polars schema | columns | length) > 0) { $base | polars concat $mp } else { $base }
    } else { $base }
    # unroll the mapped struct -> mapped_path (old format: direct mapped_file).
    let base = ($base
        | unnestif $in 'mapped'
        | if ('mapped_file' in ($in | polars schema | columns)) { $in | polars rename mapped_file mapped_path } else { $in })
    # drop-and-run filter: temporary mapped path (same pattern as the engine).
    let base = if $suspicious {
        $base | polars filter ((polars col mapped_path) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/|/proc/self/fd/|memfd:)")
    } else { $base }
    let all_rows = ($base
        | polars select [task_name mapped_path]
        | polars drop-nulls
        | polars group-by [task_name mapped_path]
        | polars agg [(polars col mapped_path | polars count | polars as count)]
        | polars sort-by [count mapped_path] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no mmap_exec in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Loaded eBPF programs (bpf_prog_load): grouped by type + name + process.
# Useful to spot malicious/rootkit BPF (kprobe, tracepoint, cgroup).
# The nested prog_type column is unrolled by unnest (prog_type_name).
def "main bpf-progs" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'bpf-progs' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'bpf_prog_load')
    let cols = ($base | polars schema | columns)
    if 'prog_type' not-in $cols {
        print $"(ansi yellow)✗ 'prog_type' column missing for bpf_prog_load on this format(ansi reset)"
        return
    }
    let has_task = ('task_name' in $cols)
    # group columns: prog_type_name and name / the group-by indexers.
    let all_rows = ($base
        | polars unnest prog_type -s "_"
        | polars select (if $has_task { [task_name prog_type_name name] } else { [prog_type_name name] })
        | polars drop-nulls
        | polars group-by (if $has_task { [task_name prog_type_name name] } else { [prog_type_name name] })
        | polars agg [(polars col name | polars count | polars as count)]
        | polars sort-by [count name] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no bpf_prog_load in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Top send_data ports (filter_send).
def "main send-ports" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'send-ports' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'send_data')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    if 'dst_port' not-in $cols {
        print $"(ansi yellow)✗ no dst_port column for send_data on this format(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select dst_port
        | polars drop-nulls
        | polars get dst_port
        | polars value-counts
        | polars sort-by [count dst_port] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no send_data in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Top send_data destination IPs (dst_ip after unrolling dst).
# `--public` only shows really public IPs (excludes RFC1918 / loopback /
# internal Docker networks, including the IPv4-mapped ::ffff:).
def "main send-ips" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --public (-p)         # keep only really public IPs
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'send-ips' $file $rest
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'send_data')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    if 'dst_ip' not-in $cols {
        print $"(ansi yellow)✗ no dst_ip column for send_data on this format(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select dst_ip
        | polars drop-nulls
        | polars get dst_ip
        | polars value-counts
        | polars sort-by [count dst_ip] -r [true false]
        | polars collect
        | polars into-nu
        | where {|r| if not $public { true } else { (is_public_ip ($r.dst_ip | into string)) } })
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ no send_data in this file(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Direct `send-ips` call (after `source kunai_queries.nu`), alias of `main send-ips`.
def send-ips [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --public (-p)         # keep only really public IPs
    --top: int = 10       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'send-ips' $file $rest
    main send-ips $file --public=$public --top=$top --all=$all --infer-schema=$infer_schema
}

# Consolidated IOC extraction (compromise indicators), nushell equivalent of
# `view_iocs` of the official kunai scripts. Groups in one view the indicators
# carried by high-signal events:
#   connect / send_data -> destination IP and port (egress)
#   dns_query            -> queried domain + resolved IPs (response)
#   execve / mmap_exec   -> exotic executable (outside system paths)
# Each row indicates the IOC TYPE, the value, the responsible binary and its
# ancestor chain. `--public` keeps only really public IPs (not the
# private/mapped ranges, cf. dst_public unreliable in IPv4-mapped form).
def "main iocs" [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --public (-p)         # keep only really public IPs (egress)
    --top: int = 30       # number of rows
    --all (-a)            # all rows
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'iocs' $file $rest
    if not (require_file $file) { return }
    let base = (open_source $file $infer_schema)

    # Responsible binary: column from the `exe` unnest (ngsoti `exe.file`
    # -> exe_file, registry `exe.path` -> exe_path). Missing -> '' (execve skipped).
    let allcols = ($base | polars schema | columns)
    let bin_col = (if 'exe' in $allcols {
        let e = ($base | polars unnest exe -s "_" | polars schema | columns)
        if 'exe_path' in $e { 'exe_path' } else if 'exe_file' in $e { 'exe_file' } else { '' }
    } else { '' })
    let base = (if 'exe' in $allcols { $base | polars unnest exe -s "_" } else { $base })
    let evcol = (event_name_col $base)

    # columns to select per branch (avoids referencing a missing column).
    # `src` is not always present (varying kunai formats) -> it is only
    # selected/unrolled if it exists, otherwise src_port stays empty.
    let has_src = ('src' in $allcols)
    let net_keep = (['ancestors' 'dst'] | append (if $has_src { ['src'] } else { [] }) | append (if $bin_col != '' { [$bin_col] } else { [] }))
    let dns_keep = (['query' 'response' 'ancestors'] | append (if $bin_col != '' { [$bin_col] } else { [] }))
    let exe_keep = (['ancestors' $evcol] | append (if $bin_col != '' { [$bin_col] } else { [] }))

    # ---- connect & send_data: egress IP + port ----
    let netsrc = ($base
        | polars filter (((polars col $evcol) == 'connect') or ((polars col $evcol) == 'send_data'))
        | polars select $net_keep
        | polars unnest dst -s "_")
    let netsrc = (if $has_src { $netsrc | polars unnest src -s "_" } else { $netsrc })
    let netcols = ($netsrc | polars schema | columns)
    let net = (if ('dst_ip' in $netcols) {
        ($netsrc
            | polars collect
            | polars into-nu
            | each {|r|
                let ev = ($r | get -o $evcol | default '')
                {
                    type: (if $ev == 'connect' { 'egress-ip' } else { 'send-ip' })
                    indicator: ($r.dst_ip | into string)
                    dest_port: ($r | get -o dst_port | default '' | into string)
                    src_port: ($r | get -o src_port | default '' | into string)
                    binary: (if $bin_col != '' { $r | get $bin_col | default '' } else { '' })
                    ancestors: ($r.ancestors | default '' | into string)
                }
            })
    } else { [] })
    # `--public`: keep only truly public outbound traffic (to a non-private/
    # local IP). Text is enough here: we exclude RFC1918 / loopback / internal
    # Docker networks (including the IPv4-mapped ::ffff:).
    let net = ($net | where {|r| if not $public { true } else { (is_public_ip $r.indicator) } })
    let net = ($net | where {|r| $r.indicator != 'null' and ($r.indicator | is-not-empty) })

    # ---- dns_query: queried domain + response IPs ----
    let dnsrc = ($base
        | polars filter ((polars col $evcol) == 'dns_query')
        | polars select $dns_keep)
    let dnscols = ($dnsrc | polars schema | columns)
    let dns = (if ('query' in $dnscols) {
        ($dnsrc
            | polars collect
            | polars into-nu
            | each {|r|
                {
                    type: 'dns'
                    indicator: ($r.query | default '' | into string)
                    dest_port: ($r.response | default '' | into string)
                    src_port: ''
                    binary: (if $bin_col != '' { $r | get $bin_col | default '' } else { '' })
                    ancestors: ($r.ancestors | default '' | into string)
                }
            })
    } else { [] })
    let dns = ($dns | where {|r| $r.indicator != 'null' and ($r.indicator | is-not-empty) })

    # ---- execve / mmap_exec: executable (file IOC) ----
    let exesrc = ($base
        | polars filter (((polars col $evcol) == 'execve') or ((polars col $evcol) == 'mmap_exec'))
        | polars select $exe_keep)
    let execols = ($exesrc | polars schema | columns)
    let exes = (if ($bin_col != '' and $bin_col in $execols) {
        ($exesrc
            | polars collect
            | polars into-nu
            | each {|r|
                {
                    type: (if (($r | get $evcol | default '') == 'execve') { 'execve' } else { 'mmap-exec' })
                    indicator: ($r | get $bin_col | default '' | into string)
                    dest_port: ''
                    src_port: ''
                    binary: ($r | get $bin_col | default '' | into string)
                    ancestors: ($r.ancestors | default '' | into string)
                }
            })
    } else { [] })
    let exes = ($exes | where {|r| $r.indicator != 'null' and ($r.indicator | is-not-empty) })

    let rows = ($net | append $dns | append $exes)

    if (($rows | length) == 0) {
        if $public { print $"(ansi yellow)✗ no public IOC in this file(ansi reset)" } else { print $"(ansi yellow)✗ no IOC in this file(ansi reset)" }
        return
    }

    # Aggregates the rows per (type, indicator) to output uniques: ports and
    # responsible binaries concatenated, most frequent ancestors first.
    # `ports` column = DESTINATION ports (dst.port for connect/send_data, DNS
    # response for dns). For network we sort numerically (service ports first) and
    # bound the display to the first 4 (+N others, all with --all): ephemeral
    # ports (>= 32768) that appear on the dst side of a server-side send_data
    # (local source = service, distant peer ephemeral) must not drown the real
    # service ports.
    let sep = ' '
    let agg = ($rows
        | group-by {|r| $"($r.type)|($r.indicator)" }
        | items {|k, v|
            let parts = ($k | split row '|')
            let mkports = {|clist|
                let clean = ($clist
                    | each {|p| if $p == null { '' } else { $p | into string } }
                    | where {|p| ($p | is-not-empty) and $p != '' }
                    | uniq)
                if ($parts.0 == 'send-ip' or $parts.0 == 'egress-ip') {
                    let sorted = ($clean
                        | each {|p| { n: (if ($p =~ '^\d+$') { $p | into int } else { 999999 }), s: $p } }
                        | sort-by n | get s)
                    if ((not $all) and (($sorted | length) > 4)) {
                        let vis = ($sorted | first 4 | str join $sep)
                        let hidden = (($sorted | length) - 4)
                        $"(ansi green)($vis)(ansi reset) (ansi dark_gray)+($hidden) others(ansi reset)"
                    } else { $sorted | str join $sep }
                } else { $clean | str join $sep }
            }
            let ports    = (do $mkports $v.dest_port)
            let srcports = (do $mkports $v.src_port)
            let bins  = ($v.binary | where {|b| ($b | is-not-empty) and $b != '' } | uniq | str join ' ')
            {
                type: ($parts.0)
                indicator: ($parts.1)
                count: ($v | length)
                ports: $ports
                src_ports: $srcports
                binary: $bins
                ancestors: ($v.ancestors
                    | where {|a| ($a | is-not-empty) and $a != '' }
                    | group-by {|a| $a }
                    | items {|k, g| { value: $k, count: ($g | length) } }
                    | sort-by count -r
                    | first 1
                    | get -o value
                    | default '')
            }
        })
    let agg = ($agg | sort-by type count -r)
    if $all { $agg } else { $agg | first $top }
}

# Direct `iocs` call (after `source kunai_queries.nu`), alias of `main iocs`:
# handy to pipe into `explore` from the nushell REPL.
def iocs [
    file: string = ''     # kunai .parquet / .gz / .jsonl file
    ...rest: string       # surplus positional args -> clear message
    --public (-p)         # keep only really public IPs (egress)
    --top: int = 30       # number of rows
    --all (-a)            # all rows (and all ports, no bound)
    --infer-schema: int = 200000  # ndjson schema inference rows
] {
    check_args_pos 'iocs' $file $rest
    main iocs $file --public=$public --top=$top --all=$all --infer-schema=$infer_schema
}


# ---------------------------------------------------------------- CLI

def print_help [] {
    print $"(ansi cyan)kunai_queries.nu — ad-hoc queries on kunai logs(ansi reset)"
    print $""
    print $"Each query is a subcommand with its own help:"
    print $"  nu kunai_queries.nu <query> --help"
    print $""
    print $"(ansi green)Queries:(ansi reset)"
    for k in ($REQUESTS | columns) {
        let info = $REQUESTS | get $k
        print $"  (ansi yellow)($k)(ansi reset)  ($info.desc)"
        if $info.arg != '' { print $"         options: ($info.arg)" }
    }
    print $""
    print $"Accepted source file: .parquet already flattened, or .gz / .jsonl / .log ndjson"
    print $"kunai. The --infer-schema option, default 200000, controls the ndjson reader's"
    print $"schema inference."
}

# Shows the summary of available queries (nu kunai_queries.nu help).
def "main help" [] {
    print_help
}

# Entry point: the real queries are subcommands (main <query>).
# This generic `main` only handles the case where no argument is a known subcommand:
#  - `-h`/`--help` -> summary,
#  - a file path passed alone -> default `events` query,
#  - otherwise -> unknown query.
def main [
    query: string        # query type: events | dns | command-lines | exes | connect-ips | connect-ports | network | dst-ports | file-extensions | file-creates | kill-targets | prctl-options | file-renames | file-unlinks | mmap-execs | bpf-progs | send-ports | send-ips | help
] {
    if $query == '-h' or $query == '--help' {
        print_help
    } else if ($query | path exists) {
        main events $query
    } else {
        print $"(ansi red)✗ unknown query: '($query)'(ansi reset)"
        print_help
    }
}
