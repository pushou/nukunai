#!/usr/bin/env nu
# kunai_detect_compromise.nu
#
# Compromise detection on the kunai logs of the "registry" machine.
# Analyzes compressed kunai event files (.gz), raw JSON lines (.jsonl) and the
# Parquet format (.parquet), in lazy polars mode.
# The .parquet (from kunai_to_parquet.nu) is read directly without going through
# the ndjson separator: data/info are already flattened there.
#
# Usage:
#   nu kunai_detect_compromise.nu                                # the 2 most recent registry files
#   nu kunai_detect_compromise.nu file1.gz file2.gz        # explicit files
#   nu kunai_detect_compromise.nu file.parquet                # analyze a parquet
#   nu kunai_detect_compromise.nu -n 1                           # display only 1 line per family
#   nu kunai_detect_compromise.nu -f execve                      # run only one family
#   nu kunai_detect_compromise.nu --explore                      # interactive dataframe display
#
# Families (--family): all, execve, file_create, connect, send_data,
#                      dns_query, kill, bpf_prog_load, mmap_exec, prctl

# =====================================================================
# ---- "registry" machine context -------------------------------------
# The machine's LEGITIMATE context (agents, services, networks, benign paths,
# allowlists) has been refactored out of the engine into the
# kunai_rules_local.nu module (machine LOCAL rules). The executable common pool
# of generic rules lives in kunai_rules.nu. The engine consumes here
# both modules and composes generic + local.
# =====================================================================
# The two `use` below resolve their paths RELATIVE to this file's directory
# (not to the launch cwd): the repo is therefore portable, wherever it is
# cloned. NB: nushell requires a CONSTANT path at parse time (not `use $var`),
# which is why no variable is used here for the rules directory.
use ./kunai_rules_local.nu local_cfg
# executable common pool: generic rules (no machine context)
use ./kunai_rules.nu *


# boolean polars expression: true if the task (task_name) is in the allowlisted
# list passed (list of benign process names, converted to a df).
export def is_in_df [col, values: list<string>] {
    ((polars col $col) | polars is-in ($values | polars into-df))
}

# not_legit: expression "this line is NOT legitimate" (to keep with filter).
# A line is benign if:
#   - the task is itself a legitimate agent (legit_agents), OR
#   - the task is a benign utility (benign_utilities, standard command) and its
#     PARENT is legitimate (agent OR utility, parent chain).
# NB: the PARENT alone is no longer enough to make a line benign. Otherwise a rootkit
# that daemonizes its binary under systemd (e.g. perfctl → "oom_reaper", parent systemd)
# would be masked: it is neither an agent nor a utility, so it stays DETECTED whatever
# its parent is.
# A docker/chmod/curl/perl/python3 launched by an unknown task therefore stays detected.
# The lists come from the machine LOCAL context (kunai_rules_local.nu).
export def not_legit [] {
    let legit = ((local_cfg legit_agents) | polars into-df)
    let utils = ((local_cfg benign_utilities) | polars into-df)
    let task_legit   = ((polars col task_name) | polars is-in $legit)
    let task_utils   = ((polars col task_name) | polars is-in $utils)
    let parent_legit = ((polars col parent_task_name) | polars is-in $legit)
    let parent_utils = ((polars col parent_task_name) | polars is-in $utils)
    # "clean" parent = legitimate agent OR benign utility
    let parent_ok  = ($parent_legit or $parent_utils)
    let benign     = ($task_legit or ($task_utils and $parent_ok))
    $benign | polars expr-not
}

# boolean polars expression: true if the string column `col` STARTS-WITH one of
# the `prefixes` (allowlist of benign public paths / networks).
# The dst_ip field is a string, so we compare by prefix start ("151.101.",
# "/tmp/cargo-", …); equivalent to a CIDR range / path match pragmatically.
# We build an ANCHORED regex alternative (^) with each prefix escaped
# as a literal: polars `contains` expects a regex, so the ^(p1|p2|…) form avoids
# any false positive (e.g. 'ee151.101' does not match '151.101').
export def starts_with_any [col, prefixes: list<string>] {
    # An EMPTY allowlist must never match everything (otherwise `^(...)` -> `^()` matches
    # the empty string and therefore every row, which would neutralize the filter).
    if ($prefixes | is-empty) { return (polars lit false) }
    # escapes each prefix: the regex metacharacters (. * + ? ( ) [ ] { } ^ $ | \) are
    # prefixed with a backslash to be treated as literals.
    let escaped = ($prefixes | each {|p| $p | str replace -a -r '([\.\+\*\?\(\)\[\]\{\}\$\^\|\\])' '\$1' })
    let re = ('^(' + ($escaped | str join '|') + ')')
    ((polars col $col) | polars contains $re)
}

# boolean polars expression: true if the string column `col` CONTAINS (anywhere)
# one of the `prefixes`. Non-anchored variant of starts_with_any, useful
# when the prefix is not at the start of the value (e.g. command_line = "ln -rs
# /var/tmp/mkinitramfs_GHWtJR/..."). Escapes the regex metacharacters as literals.
export def contains_any [col, prefixes: list<string>] {
    # An EMPTY allowlist must return false (never match everything via an empty `(?:)`).
    if ($prefixes | is-empty) { return (polars lit false) }
    let escaped = ($prefixes | each {|p| $p | str replace -a -r '([\.\+\*\?\(\)\[\]\{\}\$\^\|\\])' '\$1' })
    let re = ('(?:' + ($escaped | str join '|') + ')')
    ((polars col $col) | polars contains $re)
}

# boolean polars expression: true (== the dst_ip field designates a PRIVATE /
# loopback / link-local IPv4 address, in raw OR IPv4-mapped `::ffff:` form).
# GENERIC FIX for a kunai bug: kunai renders dst_public=true for the IPv4-mapped
# `::ffff:` addresses even when they are RFC1918 (internal docker network
# 10./172.16-31./192.168., loopback 127., link-local 169.254.). Such a
# destination is NEVER public egress: it is internal host/docker traffic.
# We can therefore NOT rely on the source `dst_public` column alone to qualify
# exfiltration: a mapped RFC1918 is never an external C2.
export def is_private_dst [] {
    # RFC1918: 10/8, 172.16/12, 192.168/16 ; loopback 127/8 ; link-local 169.254/16.
    # Each range appears in raw AND IPv4-mapped `::ffff:` form (the exact format
    # kunai renders in dst_ip for docker/host traffic).
    let raw = (
        ['127.', '10.', '169.254.', '192.168.']
        | append (16..31 | each {|b| $'172.($b).'})
    )
    let mapped = ($raw | each {|p| $'::ffff:($p)' })
    let priv = ($raw | append $mapped)
    (starts_with_any 'dst_ip' $priv)
}

# boolean polars expression: true if dst_ip is a WILDCARD/LOOPBACK address
# (`0.0.0.0`, `::`, `::1`) that a daemon uses to BIND its socket (listening
# on all interfaces / loopback), and which is therefore NEVER exfiltration.
# These connect meta-events (kunai renders dst_public=true) correspond to the
# preparation of a listening socket, not to a real outbound connection. It is a
# FLOW criterion (destination address class), not process identity.
export def is_wildcard_dst [] {
    (is_in_df 'dst_ip' ['0.0.0.0' '::' '::1'])
}

# =====================================================================
# ---- NOISE mode ------------------------------------------------------
# =====================================================================
# By default the engine reports ALL real traffic/flow (including benign system
# noise: chronyd NTP sync, iptables reverse-DNS, ELK/docker service
# resolutions). This is an ANALYST choice: see the noise rather than
# nothing, and remove it explicitly if needed.
# `--no-noise` (main flag -> $env.KUNAI_NOISE) switches to SUPPRESSION mode:
# the known benign flows (NTP port 123, dns allowlist + reverse-PTR) are then
# filtered, leaving only the real anomalies (the behavior of commits
# e968143/39d1226).
export def noise_off [] { (($env.KUNAI_NOISE? | default "") == "true") }
# Reversed detection FOR polars: "noise present" (true WHEN NOISE OFF)
# used to neutralize a criterion when we want to keep the noise by default.
export def noise_keep [] { (noise_off | polars expr-not) }

# =====================================================================
# ---- Common lazy base (basic unnests) -----------------------------
# =====================================================================
export def build_base [file: string, infer_schema: int] {
    # -- Reading the source file according to its format -----------------------
    # .gz / .jsonl   : compressed JSON lines or not, data/info to unfold.
    # .parquet       : already flattened (flat data/info), no more ndjson separator.
    let ext = ($file | path parse | get extension)
    let base = if $ext == 'parquet' {
        polars open $file
    } else {
        polars open $file -t ndjson --infer-schema $infer_schema
        | polars unnest data info
    }
    # -- Structural unnest (event/task/host/parent_task) ----------------------
    # rename event -> event_info BEFORE the unnest to avoid the collision
    # between data.name/data.id (bpf) and event.name/event.id.
    # The unnests are conditional: the fully flattened parquet
    # (kunai_to_flatten_parquet.nu) no longer has these columns.
    let cols = ($base | polars schema | columns)
    let b = if 'event' in $cols { $base | polars rename event event_info } else { $base }
    let b = ($b
        | unnestif $in 'event_info'
        | unnestif $in 'host'
        | unnestif $in 'task'
        | unnestif $in 'parent_task')
    # path -> main_path (file path / main target).
    # This field exists only if at least one event in the batch exposes a `path`
    # (e.g. file_create): the conditional rename avoids a crash when the
    # file contains none (e.g. a 100% connect / execve batch).
    let cols = ($b | polars schema | columns)
    if "path" in $cols { $b | polars rename [path] [main_path] } else { $b }
}

# =====================================================================
# ---- Automatic gz/jsonl -> parquet conversion (default mode) ----
# =====================================================================
# A .gz/.jsonl is first converted to a .parquet (next to the source, or in
# --cache-dir), then analyzed on the parquet: much faster than parsing
# the intact ndjson at each run (~14x benchmark on a typical sample).
# DEFAULT behavior; --no-convert (global flag) disables it to go back to
# the historical direct read.
# Returns { file, converted }: file = path to analyze, converted = true if a
# conversion happened (otherwise it was already parquet or a reused cache).
export def ensure_parquet [
    file: string
    infer_schema: int
    --force
    --cache-dir: string
] {
    # already parquet: nothing to convert.
    if ($file | path parse | get extension) == 'parquet' { return { file: $file, converted: false } }

    # Target parquet path: next to the source, or in --cache-dir.
    # Convergent rule (same as default_output of kunai_to_parquet.nu): we only
    # strip a possible `.gz`, keep the rest of the name then append `.parquet`,
    # so that a gz and its decompressed jsonl converge:
    #   test.jsonl    -> test.jsonl.parquet
    #   test.jsonl.gz -> test.jsonl.parquet
    let ext = ($file | path parse | get extension)
    let target = if $ext == 'gz' {
        let no_gz = ($file | str replace -r '\.gz$' '')
        if ($cache_dir | is-not-empty) { ($cache_dir | path join $"($no_gz | path basename).parquet") } else { $"($no_gz).parquet" }
    } else if ($cache_dir | is-not-empty) {
        ($cache_dir | path join $"($file | path basename).parquet")
    } else {
        $"($file).parquet"
    }

    # Reuses the cache if it exists and is up to date (newer than the source),
    # unless --force forces a reconversion.
    # NB: `metadata` does not return the file stats in this context (just
    # a span); so we read the real mtime via `ls`, whose `.modified?`
    # is a reliable datetime. We only compare freshness if both dates
    # are available.
    if (not $force) and ($target | path exists) {
        let src_m = ((ls $file).0.modified?)
        let dst_m = ((ls $target).0.modified?)
        if ($dst_m | is-not-empty) and ($src_m | is-not-empty) and ($dst_m >= $src_m) {
            return { file: $target, converted: false }
        }
    }

    # Converts via kunai_to_parquet.nu (nu subprocess), writing to the target.
    # lazy is the default mode (works everywhere, including large gz that
    # saturate RAM in eager). To force eager (6x faster but very
    # resource-hungry), one would need to add --eager to the call below.
    print $"(ansi yellow)⚙ conversion (($file)) → (($target))(ansi reset)"
    let script = ($env.FILE_PWD | path join "kunai_to_parquet.nu")
    let res = (^nu $script $file --output $target --infer-schema $infer_schema | complete)
    if $res.exit_code != 0 {
        error make { msg: $"conversion of ($file) failed: ($res.stderr)" }
    }
    { file: $target, converted: true }
}

# short filter on the kunai event name
export def ev [name: string] {
    polars filter ((polars col event_info_name) == $name)
}

# conditional unnest: only unfolds the column if it exists. The kunai formats
# vary (some versions do not have socket/src/dns_server/target/prog_type/mapped),
# and a polars unnest on an absent column fails plan resolution.
export def unnestif [base, col: string] {
    let cols = ($base | polars schema | columns)
    if $col in $cols { $base | polars unnest $col -s "_" } else { $base }
}

# normalizes the path column of an executed mmap according to the kunai format:
# mapped.path (renamed mapped_path by unnestif), or mapped_file (old format).
# Guarantees the presence of a mapped_path column usable by the mmap_exec family.
export def normalize_mapped [base] {
    let cols = ($base | polars schema | columns)
    if "mapped_path" in $cols {
        $base
    } else if "mapped_file" in $cols {
        $base | polars rename [mapped_file] [mapped_path]
    } else {
        $base
    }
}

# select from a list keeping only the columns really present in the
# lazy frame (the conditional unnests expose variable columns per format).
export def cols_keep [cols: list<string>] {
    let df = $in
    let present = ($df | polars schema | columns)
    let keep = ($cols | where {|c| $c in $present })
    if (($keep | length) == 0) {
        # none of the working columns exists in this format: nothing to detect.
        # We return an empty lazy frame (0 row, safe utc_time column only)
        # so as not to expose the Int128 columns and not crash the collect.
        $df
            | polars filter (polars lit false)
            | polars select [utc_time]
    } else {
        $df | polars select $keep
    }
}

# true if at least one event of the given name is present in the file.
# Costs a small collection but avoids referencing absent columns
# (e.g. kill without target_task_name) when the event does not exist at all.
# NB: we only select utc_time before collect, because some columns of the
# frame are typed Int128 by polars and unusable as nushell output.
export def has_events [base, evname: string] {
    let n = ($base
        | polars filter ((polars col event_info_name) == $evname)
        | polars select [utc_time]
        | polars collect | polars into-nu | length)
    $n > 0
}

# returns an empty lazy frame (0 row) without referencing a specific event column:
# used as the "no alert" output when the event is absent.
export def empty_like [base] {
    $base
    | polars filter ((polars col event_info_name) == 'kunai__no_such_event')
    | polars select [utc_time]
}

# =====================================================================
# ---- MISP IOC (misp-to-kunai) ---------------------------------------
# =====================================================================
# Loading of the indicators of compromise extracted from MISP, in JSONL format
# (one line = one IOC, e.g. misp.iocs):
#   {"type":"sha256","value":"<hash>","severity":<n>,"source":"CIRCL OSINT Feed",…}
# Exploited types: sha256 / sha1 / md5 (binary hash → execve/file_create/mmap_exec),
# ip-dst (→ connect/send_data) and domain (→ dns_query). The other types are ignored.
# Returns a record { sha256, sha1, md5, ip, domain } with the UNIQUE values.
export def load_misp_iocs [path: string] {
    if not ($path | path exists) {
        print $"(ansi yellow)⚠ no IOC file: ($path) — ioc family inactive(ansi reset)"
        return { sha256: [], sha1: [], md5: [], ip: [], domain: [] }
    }
    let rows = (open --raw $path | from json --objects)
    {
        sha256: ($rows | where type == 'sha256' | get value | into string | uniq)
        sha1:   ($rows | where type == 'sha1'   | get value | into string | uniq)
        md5:    ($rows | where type == 'md5'    | get value | into string | uniq)
        ip:     ($rows | where type == 'ip-dst' | get value | into string | uniq)
        domain: ($rows | where type == 'domain' | get value | into string | uniq)
    }
}

# polars table of ALL exploitable IOCs (for a JOIN that keeps, for each matched
# value, its type, severity and source). Read directly from the JSONL file ($path).
# Columns: ioc_type, ioc_value, ioc_severity, ioc_source. Empty (0 row) if the file
# is absent, empty or without any useful IOC.
export def misp_ioc_table [path: string] {
    if not ($path | path exists) { return ([[] [ioc_type ioc_value ioc_severity ioc_source]] | polars into-df | polars into-lazy) }
    let rows = (open --raw $path | from json --objects)
    let keep = ($rows
        | where {|r| $r.type in ['sha256', 'sha1', 'md5', 'ip-dst', 'domain'] }
        | each {|r|
            {
                ioc_type: $r.type
                ioc_value: ($r.value | into string)   # some MISP sources output value as int
                ioc_severity: $r.severity
                ioc_source: $r.source
            }
        })
    if ($keep | is-empty) {
        [[] [ioc_type ioc_value ioc_severity ioc_source]] | polars into-df | polars into-lazy
    } else {
        $keep | polars into-df | polars into-lazy
    }
}

# =====================================================================
# ---- DNS -> FQDN correlation (Option A, network flow analysis) ------
# =====================================================================
# Builds a map (task_pid, resolved_ip) -> fqdn from the dns_query events
# ("ip;ip;.." response exploded into one line per resolved IP), then
# annotates a network frame (connect/send_data) by LEFT join on
# (task_pid, dst_ip = resolved_ip). The flow analysis is done by
# reputable IP / port / FQDN, WITHOUT trusting the PROCESS NAME: the process
# name is a persistence vector easily masked (cron/systemd), and
# the internal DNS -> connect correlation (kunai alone) is enough to give the
# observed destination domain.
export def dns_fqdn_map [base] {
    # lazy frame of the DNS queries we are going to explode {task_pid, query, response}
    let cols = ($base | polars schema | columns)
    if 'response' not-in $cols { return ([[] [task_pid resolved_ip fqdn]] | polars into-df | polars into-lazy) }
    let dns = ($base
        | ev 'dns_query'
        | polars filter ((polars col response) | polars is-not-null)
        | polars select [task_pid query response])
    # "172.67.74.152;104.26.12.205;…" response exploded: one line per resolved IP.
    # DNS is rare (1-4/sample) so the eager collection is negligible.
    let flat = ($dns | polars collect | polars into-nu
        | each {|r|
            $r.response
            | split row ';'
            | each {|ip| $ip | str trim }
            | where {|ip| ($ip | is-not-empty) }
            | each {|ip| { task_pid: $r.task_pid, resolved_ip: $ip, fqdn: $r.query } }
        }
        | flatten)
    if (($flat | length) == 0) {
        # no resolved FQDN: empty lazy frame (same columns) for a safe join.
        [[] [task_pid resolved_ip fqdn]] | polars into-df | polars into-lazy
    } else {
        $flat | polars into-df | polars into-lazy
    }
}

# annotates a network frame (must already expose `task_pid` and `dst_ip`) with a
# `fqdn` column = resolved domain (DNS -> connect correlation), "" if unknown.
# Returns the annotated frame; the fqdn serves as REPORT CONTEXT (egress is
# decided by reputable IP/port, not by the process name).
export def annotate_fqdn [df, dmap] {
    ($df
     | polars join $dmap -l [task_pid dst_ip] [task_pid resolved_ip]
     | polars with-column (
        (polars when ((polars col fqdn) | polars is-null) (polars lit "")
         | polars otherwise (polars col fqdn))
        | polars as fqdn))
}

# =====================================================================
# ---- FAMILY execve: suspicious execution ----------------------------
# =====================================================================
export def detect_execve [base] {
    # GENERIC rules imported from the common pool kunai_rules.nu (reverse shell /
    # dropper / obfuscation / offensive tool phenotypes) — see also
    # the homonymous kunai .yaml files in kunai_rules/rules_v0.1/.
    let c_shell = (r_execve_shell_tool)
    let c_revsh = (r_execve_reverse_shell)
    let c_dl    = (r_execve_download_exec)
    let c_obf   = (r_execve_obfuscated)
    let c_tool  = (r_execve_offensive_tool)
    let c_tmp   = (r_execve_from_tmp)
    let is_build = (is_in_df 'task_name' (local_cfg allowlist_build_procs))
    # INITRAMFS build chain (mkinitramfs/dracut/update-initramfs, triggered by a
    # kernel/DKMS module update): rearrangement of utilities in the
    # /var/tmp/mkinitramfs_* staging zone then execution of many tasks (cp, ln,
    # mkdir, find, depmod, kmod) whose command references this staging.
    # Signal based ONLY on the staging path: /var/tmp/mkinitramfs_* is a
    # reserved directory created by mkinitramfs (random suffix, only exists for the
    # duration of the build), never used for malicious persistence. Same logic
    # as allowlist_build_paths (/tmp/cargo-*). This avoids adding banal utilities
    # (cp/ln/mkdir/find) to the proc allowlists, which would mask an offensive
    # use from /tmp.
    let is_initramfs = (contains_any 'command_line' (local_cfg allowlist_initramfs_paths))
    let c_build_ok = ($is_build or $is_initramfs)
    # A real "dropper": execution from /tmp BY a process that is NOT the build
    # chain (rustup/cargo/cc/rustc…) nor a legitimate initramfs (dracut/kmod/depmod…).
    # The execution of an "exec_from_tmp" binary by a bash/sh/language utility
    # remains detected.
    let c_tmp_real = ($c_tmp and ($c_build_ok | polars expr-not))
    if (not (has_events $base 'execve')) { return (empty_like $base) }

    $base
    | ev 'execve'
    # not_legit now also covers the legitimate children of the agents
    # (e.g. python3/perl/grep/sed from check_mk) without masking a docker/chmod/curl
    # launched by an attacker, nor the same binaries under a non-legitimate parent.
    | polars filter (not_legit)
    | polars select [utc_time task_name task_pid command_line]
    | polars with-column (
        (polars when $c_revsh (polars lit "reverse_shell")
         | polars when $c_shell (polars lit "reverse_shell")
         | polars when $c_dl (polars lit "download_and_execute")
         | polars when $c_obf (polars lit "obfuscated_exec")
         | polars when $c_tool (polars lit "offensive_tool")
         | polars when $c_tmp_real (polars lit "exec_from_tmp")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE file_create : persistance / drop -----------------------
# =====================================================================
export def detect_file_create [base] {
    let c_persist = ((polars col main_path) | polars contains "(?:/etc/cron\\.|/var/spool/cron|/etc/systemd/system/|/etc/rc\\.d/|/root/\\.|/home/[^/]+/\\.ssh/|/usr/local/bin/|/usr/bin/[^ ]+\\.old)")
    # Word boundary (\b) after the extension: avoids matching ".sh" at the start of
    # "shlex" or ".so" in "socks" (rustc codegen artifacts *.cgu.*.rcgu.o).
    let c_tmpdl   = ((polars col main_path) | polars contains "(?:/tmp/|/dev/shm/)[^ ]*(?:\\.sh|\\.py|\\.pl|\\.elf|\\.so|\\.bin|\\.out|\\.jar|\\.tar|\\.gz|\\.zip)\\b")
    let c_tmpdir  = ((polars col main_path) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/|/dev/mqueue/)")
    # LEGITIMATE build/toolchain scratch zone: under ~/.rustup, ~/.cargo, /tmp/cargo-*,
    # /tmp/cargo-install, /tmp/rustc*, /tmp/cc*, /tmp/tmp.*. The writes of the rustc
    # codegen threads ("opt cgu.*", "coordinator") and of the rustup extraction
    # ("tokio-runtime-w") fall there, as well as the GCC linker .res/.cdtor.*: these
    # are temporary compilation/extraction artifacts, NOT drops.
    # Internal LLVM codegen threads of rustc ("opt cgu.*", "lto cgu.*",
    # "coordinator"): write the *.cgu.*.rcgu.o in the build scratch zone.
    let is_build = ((is_in_df 'task_name' (local_cfg allowlist_build_procs))
        or (starts_with_any 'task_name' ['opt cgu','lto cgu','coordinator','rustc_backtrace']))
    let build_path = (starts_with_any 'main_path' (local_cfg allowlist_build_paths))
    # By default, any write in a build scratch zone is benign (scratch).
    let benign_zone = $build_path
    # tmp_dropper = write of an EXECUTABLE ARTIFACT (.sh/.so/.elf/.bin…) in a temporary
    # directory. Kept everywhere the writer is not the build chain: even in a scratch
    # zone, a binary dropped by a non-build shell/python/curl (downloaded script)
    # remains detected. tmp_write = any other write to /tmp.
    let c_tmpdl_real  = ($c_tmpdl and (($benign_zone and $is_build) | polars expr-not))
    let c_tmpdir_real = ($c_tmpdir and ($benign_zone | polars expr-not))
    # Webshell (common pool r_filecreate_webshell / fs_webshell_drop kunai,
    # T1505.003, sev 9): server script (php/asp/jsp/cgi/py/rb…) created IN a web
    # document-root. A legitimate server does not create a new executable script
    # in its docroot: a strong web compromise indicator.
    let is_webshell_file = (r_filecreate_webshell)
    # Temporary hidden file (common pool r_filecreate_hidden_tmp /
    # fs_hidden_dir_suspicious kunai, T1564.001, sev 7): hidden file ('.' prefix)
    # placed in /tmp|/var/tmp|/dev/shm. Only alerted if THIS file is really
    # executed (drop-and-run correlation $c_exec), eliminating legitimate hidden
    # artifacts never executed.
    let c_hidden_tmp = (r_filecreate_hidden_tmp)
    if (not (has_events $base 'file_create')) { return (empty_like $base) }

    # ---- drop-and-run correlation (replaces the volume-based filtering) -----
    # A file dropped in a temporary zone is only suspicious if it is really
    # executed: either mmapped for execution (mmap_exec.mapped_path), or launched as
    # a script/interpreter (first word of command_line under /tmp|/dev/shm|/var/tmp).
    # The LEGITIMATE temporary artifacts (e.g. the mkinitramfs initramfs generation
    # by dpkg/cp, never executed) are thus eliminated, while the real droppers
    # dropped then executed remain detected. This is strictly more restrictive: can
    # only create new FPs, only a FN risk if a dropped tmp artifact is
    # executed without being captured by mmap_exec nor execve.
    let exec_paths = (do {
        # The base frame may contain no `mapped` column (parquet flattened from a
        # source without mmap_exec events). The filter on mapped_path must then not
        # run, otherwise the plan resolution fails ("not found").
        let mapped_col = (($base | polars filter ((polars col event_info_name) == 'mmap_exec')
            | (unnestif $in mapped) | (normalize_mapped $in)
            | polars schema | columns) | where {|c| $c == 'mapped_path'} | length) > 0
        let mmap = (if $mapped_col {
            ($base
                | polars filter ((polars col event_info_name) == 'mmap_exec')
                | (unnestif $in mapped)
                | (normalize_mapped $in)
                | polars filter ((polars col mapped_path) | polars is-not-null)
                | (cols_keep [mapped_path])
                | polars unique | polars collect | polars into-nu | get mapped_path)
        } else { [] })
        let cfgs = ($base
            | polars filter ((polars col event_info_name) == 'execve')
            | (unnestif $in exe)
            | polars filter ((polars col command_line) | polars is-not-null)
            | (cols_keep [command_line])
            | polars collect | polars into-nu | get command_line)
        let tmp = ($cfgs
            | each {|x|
                let f = ($x | split row ' ' | first)
                (if (($f | str starts-with '/tmp/') or ($f | str starts-with '/dev/shm/') or ($f | str starts-with '/var/tmp/')) { $f } else { null })
            }
            | where {|x| $x != null})
        ($mmap | append $tmp | uniq)
    })
    let c_exec = (is_in_df 'main_path' $exec_paths)

    $base
    | ev 'file_create'
    | polars filter (not_legit)
    | (unnestif $in exe)
    | polars select [utc_time task_name task_pid main_path]
    | polars with-column (
        (polars when $is_webshell_file (polars lit "webshell_drop")
         | polars when $c_persist (polars lit "persistence_path")
         | polars when ($c_tmpdl_real and $c_exec) (polars lit "tmp_dropper")
         | polars when ($c_tmpdir_real and $c_exec) (polars lit "tmp_write")
         | polars when ($c_hidden_tmp and $c_exec) (polars lit "hidden_tmp")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILY connect: suspicious outbound network ---------------------
# =====================================================================
export def detect_connect [base] {
    # NETWORK FLOW REWORK (Option A): egress is judged on the REPUTATION of the
    # DESTINATION (reputable public IP CDN/mirror/repo + standard port), no more
    # trust in the PROCESS NAME/IDENTITY. `not_legit`, `allowlist_egress_procs`
    # and `allowlist_egress_paths` (process identity = persistence vector whose
    # name is trivially masked: cron/systemd/oom_reaper) disappear from the
    # network decision. The domain resolved by the DNS -> connect correlation
    # (`annotate_fqdn`) enriches the report as CONTEXT.
    let c_lbl_public = (r_connect_public_egress)
    # GENERIC FIX for the kunai bug: `dst_public` is wrongly rendered=true for the
    # IPv4-mapped RFC1918 `::ffff:` (internal docker/host traffic). A private/loopback
    # destination is never public egress -> we remove it from c_public.
    let c_private    = (is_private_dst)
    # the wildcard/loopback addresses (0.0.0.0, ::, ::1) are listening binds,
    # never exfiltration (FLOW criterion, not process identity).
    let c_wildcard   = (is_wildcard_dst)
    let c_public     = (($c_lbl_public) and (($c_private) | polars expr-not) and (($c_wildcard) | polars expr-not))
    # Unusual/suspicious ports (common pool r_connect_unusual_port, T1095/T1496):
    # C2/backdoor 4444/4445/31337/9001/8888/8443/1337/2222/9999 ; Tor 9050/9051/9150 ;
    # mining pools 5555/7777/14444/14433/45700/3256/20535/3333 ; nets 6667/161/137/445/49152.
    let c_port   = (r_connect_unusual_port)
    # FLOW reputation of the destination: reputable public IP (CDN/mirror/repo)
    # AND standard service port -> benign egress, whatever the process identity.
    let c_reput_net  = (starts_with_any 'dst_ip' (local_cfg allowlist_public_networks))
    let c_reput_port = ((polars col dst_port) | polars is-in (local_cfg allowlist_egress_ports | polars into-df))
    # SUSPECT public_egress = NON-reputable public destination (IP outside CDN/mirror
    # OR unusual port). Previously depended on not_legit(TASK) + process name.
    # Reputation: generally reputable IP AND standard port. NTP EXCEPTION (port 123,
    # UDP): the public NTP pool keeps changing IP (impossible to allowlist by
    # IP) and its system sync is benign background traffic, not an exploitable
    # exfil channel (small fixed packets; any anomaly still seen by send_data).
    # -> port 123 alone is enough to make the egress reputable **only in
    # --no-noise mode**. WITHOUT --no-noise (default), we want to SEE this system
    # noise (chronyd NTP shows up as public_egress), consistent with the analyst choice.
    let c_ntp_port = ((polars col dst_port) | polars is-in ([123] | polars into-df))
    # NTP only makes the egress reputable if noise SUPPRESSION is active.
    let c_ntp_benign = (($c_ntp_port) and (polars lit (noise_off)))
    let c_reput = ((($c_reput_net) and ($c_reput_port)) or ($c_ntp_benign))
    let c_egress_susp = ($c_public and ($c_reput | polars expr-not))
    if (not (has_events $base 'connect')) { return (empty_like $base) }

    # DNS -> FQDN correlation: map (task_pid, resolved_ip)->fqdn reused afterwards.
    let dmap = (dns_fqdn_map $base)

    $base
    | ev 'connect'
    | (unnestif $in exe) | (unnestif $in socket) | (unnestif $in src) | (unnestif $in dst)
    | polars filter ((polars col dst_ip) | polars is-not-null)
    # dst_port 0 = socket/RAW preparation events (resolution, bind via fd)
    # without a real outbound connection: systematic noise, not reported here.
    | polars filter ((polars col dst_port) != 0)
    # Note: NO not_legit / process identity filter — the flow is judged alone.
    | (annotate_fqdn $in $dmap)
    # dst_public + fqdn must be selected to be usable by with-column
    | (cols_keep [utc_time task_name task_pid command_line src_ip dst_ip dst_port dst_public fqdn])
    | polars with-column (
        (polars when $c_egress_susp (polars lit "public_egress")
         | polars when $c_port (polars lit "unusual_port")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILY send_data: exfiltration ---------------------------------
# =====================================================================
export def detect_send_data [base] {
    # NETWORK FLOW REWORK (Option A): same logic as connect — exfiltration is judged
    # on the DESTINATION (reputable public IP + standard port), WITHOUT trust in the
    # PROCESS NAME/IDENTITY (`not_legit`/`allowlist_egress_procs`/`allowlist_egress_paths` removed). A send to a NON-reputable target is
    # suspected (large_data/high_entropy → exfil); to a reputable target, silent.
    # The resolved FQDN (DNS -> connect correlation) enriches the report as CONTEXT.
    let c_lbl_public = (r_connect_public_egress)
    # GENERIC FIX for the kunai bug: `dst_public` is wrongly rendered=true for the
    # IPv4-mapped RFC1918 `::ffff:` (internal docker/host traffic). A private/loopback
    # destination is never public egress -> we remove it from c_public.
    let c_private    = (is_private_dst)
    # the wildcard/loopback addresses (0.0.0.0, ::, ::1) are listening binds,
    # never exfiltration (FLOW criterion, not process identity).
    let c_wildcard   = (is_wildcard_dst)
    let c_public     = (($c_lbl_public) and (($c_private) | polars expr-not) and (($c_wildcard) | polars expr-not))
    let c_big    = (r_senddata_large)
    let c_hi     = (r_senddata_high_entropy)
    # FLOW reputation of the destination: reputable public IP (CDN/mirror/repo)
    # AND standard service port -> benign egress, whatever the process identity.
    # NTP EXCEPTION (port 123): changing pool IP, benign system sync,
    # port 123 alone makes the egress reputable (see detect_connect for the reasoning).
    let c_reput_net  = (starts_with_any 'dst_ip' (local_cfg allowlist_public_networks))
    let c_reput_port = ((polars col dst_port) | polars is-in (local_cfg allowlist_egress_ports | polars into-df))
    let c_ntp_port   = ((polars col dst_port) | polars is-in ([123] | polars into-df))
    # NTP only makes the egress reputable if noise SUPPRESSION is active
    # (by default we SEE the chronyd NTP noise). See detect_connect.
    let c_ntp_benign = (($c_ntp_port) and (polars lit (noise_off)))
    let c_reput = ((($c_reput_net) and ($c_reput_port)) or ($c_ntp_benign))
    let c_egress_susp = ($c_public and ($c_reput | polars expr-not))
    # SUSPECT high_entropy / large_data = send to NON-reputable outside
    # (C2/exfil). Encryption to a reputable target (internal agent->manager TLS
    # or public CDN) is NOT exfiltration, whatever the process identity.
    let c_hi_susp  = ($c_hi  and $c_egress_susp)
    let c_big_susp = ($c_big and $c_egress_susp)
    if (not (has_events $base 'send_data')) { return (empty_like $base) }

    # DNS -> FQDN correlation: map (task_pid, resolved_ip)->fqdn reused afterwards.
    let dmap = (dns_fqdn_map $base)

    $base
    | ev 'send_data'
    | (unnestif $in exe) | (unnestif $in src) | (unnestif $in dst)
    | polars filter ((polars col dst_ip) | polars is-not-null)
    # Note: NO not_legit / process identity filter — the flow is judged alone.
    | (annotate_fqdn $in $dmap)
    # dst_public + fqdn must be selected to be usable by with-column
    | (cols_keep [utc_time task_name task_pid command_line src_ip dst_ip dst_port data_size data_entropy dst_public fqdn])
    | polars with-column (
        (polars when $c_egress_susp (polars lit "public_egress")
         | polars when $c_big_susp (polars lit "large_data")
         | polars when $c_hi_susp (polars lit "high_entropy")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILY dns_query: recon / tunneling -----------------------------
# =====================================================================
export def detect_dns_query [base] {
    # Generic common-pool rules (cf. dns_suspicious.dns_query.detection.yaml).
    let c_tld    = (r_dns_suspicious_tld)
    let c_long   = (r_dns_long_query)
    let c_nondns = ((((polars col dns_server_ip) | polars is-in (local_cfg dns_ips | polars into-df)) | polars expr-not))
    # Benign DNS queries (LOCAL context, profile): internal service names resolved
    # by Kibana/libuv (elasticsearch, epr.elastic.co…) via the Docker resolver 127.0.0.11.
    # Reverse-DNS (PTR) `*.in-addr.arpa`/`*.ip6.arpa`: IP->hostname form of the
    # administration tools (iptables -L, traceroute…), never tunneling (r_dns_reverse).
    # A benign/reverse query must trigger NO dns_query evidence (neither length, nor
    # non_standard_dns_server, nor tld) — BUT only **in --no-noise mode**. By default
    # (without --no-noise) we SEE the dns noise (pool.ntp.org chronyd, iptables
    # reverse-DNS): c_benign_nn is then FALSE, all the dns_query signals apply.
    # Consistent with the analyst choice (visible noise).
    let c_benign_nn = ((((contains_any 'query' (local_cfg allowlist_dns_queries)) or (r_dns_reverse)) and (polars lit (noise_off))))
    let c_tld_s    = (($c_tld)    and (($c_benign_nn) | polars expr-not))
    let c_long_s   = (($c_long)   and (($c_benign_nn) | polars expr-not))
    let c_nondns_s = (($c_nondns) and (($c_benign_nn) | polars expr-not))
    if (not (has_events $base 'dns_query')) { return (empty_like $base) }

    $base
    | ev 'dns_query'
    # Note: NO not_legit / process identity filter — the DNS query is judged on the
    # form (FQDN) and the resolver, whatever the emitting process.
    | (unnestif $in exe) | (unnestif $in src) | (unnestif $in dns_server)
    | polars filter ((polars col query) | polars is-not-null)
    | (cols_keep [utc_time task_name task_pid command_line query response dns_server_ip])
    | polars with-column (
        (polars when $c_long_s (polars lit "suspicious_length")
         | polars when $c_nondns_s (polars lit "non_standard_dns_server")
         | polars when $c_tld_s (polars lit "suspicious_tld")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILY kill: disruption / evasion -------------------------------
# =====================================================================
export def detect_kill [base] {
    # Generic common-pool rules (cf. kill_critical.kill.detection.yaml). The benign
    # signal list is a machine LOCAL context (local_cfg benign_signals).
    let c_target = (r_kill_critical_target)
    let c_hard   = (r_kill_hard_signal)
    let c_benign_sig = ((polars col signal) | polars is-in (local_cfg benign_signals | polars into-df))
    # SUSPECT kill: critical target (agent/daemon) killed by a TERMINATION signal
    # (not a benign runtime signal) -- the signal remains the real attack signal.
    let c_crit_susp = ($c_target and ($c_benign_sig | polars expr-not))
    # kill by a NON-legitimate killer (not not_legit) towards a critical target even
    # by SIGKILL; we keep the "SIGKILL hard" case outside the normal life cycle.
    let c_hard_susp = ($c_hard and ($c_benign_sig | polars expr-not))
    if (not (has_events $base 'kill')) { return (empty_like $base) }

    $base
    | ev 'kill'
    | (unnestif $in exe) | (unnestif $in target) | (unnestif $in target_exe) | (unnestif $in target_task)
    | polars filter ((polars col target_task_name) | polars is-not-null)
    # Only keeps the termination/destruction signals (not the benign runtime
    # signals like SIGURG/SIGCHLD): short-circuits most of the docker noise.
    | polars filter ($c_benign_sig | polars expr-not)
    # The killer process must be non-legitimate (not an agent nor a utility under a
    # legitimate parent): we do not alert docker which manages its own threads.
    | polars filter (not_legit)
    | (cols_keep [utc_time task_name task_pid command_line target_task_name target_task_pid signal])
    | polars with-column (
        (polars when $c_crit_susp (polars lit "kill_critical")
         | polars when $c_hard_susp (polars lit "SIGKILL_hard")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILY bpf_prog_load: rootkit / EDR bypass ----------------------
# =====================================================================
export def detect_bpf [base] {
    if (not (has_events $base 'bpf_prog_load')) { return (empty_like $base) }
    $base
    | ev 'bpf_prog_load'
    | polars filter (not_legit)
    | (unnestif $in exe) | (unnestif $in prog_type)
    | (cols_keep [utc_time task_name task_pid command_line ksym prog_type_name tag])
    | polars with-column ((polars lit "bpf_by_non_system") | polars as evidence)
}

# =====================================================================
# ---- FAMILY mmap_exec: injection / drop-and-run ----------------------
# =====================================================================
export def detect_mmap_exec [base] {
    # Generic common-pool pattern (cf. mmap_exec_tmp.mmap_exec.detection.yaml).
    let c_mapped = (r_mmapexec_from_tmp)
    # rustc/cc… legitimately load their proc-macro .so from /tmp/cargo-* during
    # compilation: this is not an injection (drop-and-run) but a build artifact.
    # We only report the mmap from /tmp for a non-build process.
    let is_build = (is_in_df 'task_name' (local_cfg allowlist_build_procs))
    let build_path = (starts_with_any 'mapped_path' (local_cfg allowlist_build_paths))
    let benign_mmap = ($is_build and $build_path)
    let c_susp = ($c_mapped and ($benign_mmap | polars expr-not))
    if (not (has_events $base 'mmap_exec')) { return (empty_like $base) }

    $base
    | ev 'mmap_exec'
    | (unnestif $in exe) | (unnestif $in mapped)
    # depending on the kunai format the path is exposed `mapped.path` (→ mapped_path)
    # or, older, directly `mapped_file`. We normalize towards mapped_path.
    | (normalize_mapped $in)
    | polars filter ((polars col mapped_path) | polars is-not-null)
    | (cols_keep [utc_time task_name task_pid command_line mapped_path])
    | polars with-column (
        (polars when $c_susp (polars lit "mmap_exec_suspicious")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILY prctl: evasion (dumpable / seccomp) ----------------------
# =====================================================================
export def detect_prctl [base] {
    # Generic common-pool rule (cf. set_dumpable.prctl.detection.yaml, T1622).
    let c_dump = (r_prctl_dumpable)
    if (not (has_events $base 'prctl')) { return (empty_like $base) }

    $base
    | ev 'prctl'
    | polars filter (not_legit)
    | polars select [utc_time task_name task_pid command_line option arg2 arg3 arg4 arg5]
    | polars with-column (
        (polars when $c_dump (polars lit "dumpable_change")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILY ioc: MISP match (hash / IP / domain) --------------------
# =====================================================================
# 10th family, fed by a file of indicators of compromise extracted from
# MISP (JSONL, e.g. misp.iocs). Unlike the other families (phenotypes from the
# rules), here it is an EXACT MATCH against an external feed:
#   - sha256 / sha1 / md5  -> executed binary (execve) or dropped (file_create)
#                            or mapped (mmap_exec) whose fingerprint matches ;
#   - ip-dst               -> dst.ip of a corresponding connect / send_data ;
#   - domain               -> query of a corresponding dns_query.
# The IOC file path is read via $env.KUNAI_IOC (set by --ioc of main), default
# "misp.iocs". The other MISP types are ignored (unsuitable for kunai).
# Each detection carries evidence="misp_ioc" + the matched ioc_value column.
export def detect_ioc [base] {
    let ioc_path = ($env.KUNAI_IOC? | default 'misp.iocs')
    let itab = (misp_ioc_table $ioc_path)

    # Common columns carried by each IOC detection (the specific columns — exe_path /
    # dst_ip / query — are deliberately excluded from the union: they vary per group
    # and would fail the concat; the matched value is carried by ioc_value).
    let common = [utc_time task_name task_pid command_line ioc_type ioc_value ioc_severity ioc_source]

    mut all = []   # list of lazy frames, each already reduced to the $common schema

    # ---- HASH: execve + file_create (exe_*) and mmap_exec (mapped_*) ----
    let exec_frame = (
        $base
        | polars filter ((polars col event_info_name) | polars is-in (['execve', 'file_create'] | polars into-df))
        | (unnestif $in exe)
        | cols_keep [utc_time task_name task_pid command_line exe_sha256 exe_sha1 exe_md5]
    )
    let mmap_frame = (
        $base
        | polars filter ((polars col event_info_name) == 'mmap_exec')
        | (unnestif $in exe) | (unnestif $in mapped)
        | (normalize_mapped $in)
        | cols_keep [utc_time task_name task_pid command_line mapped_sha256 mapped_sha1 mapped_md5]
    )
    # For each group (frame + sha256/sha1/md5 hash columns) we join the IOC table of
    # the corresponding type; an absent column is simply skipped.
    for g in [[$exec_frame exe_sha256 exe_sha1 exe_md5] [$mmap_frame mapped_sha256 mapped_sha1 mapped_md5]] {
        let frame = $g.0
        mut parts = []
        for t in ['sha256' 'sha1' 'md5'] {
            let col = (match $t { 'sha256' => $g.1, 'sha1' => $g.2, _ => $g.3 })
            let has = ($frame | polars schema | columns | where {|c| $c == $col } | length) > 0
            if not $has { continue }
            let lookup = ($itab | polars filter ((polars col ioc_type) == $t))
            $parts = ($parts | append (
                $frame
                | polars join $lookup -i [$col] [ioc_value]
                # the inner join eliminates the right key ioc_value; since it equals
                # the left key (exact match), we rebuild it from $col.
                | polars with-column ((polars col $col) | polars as ioc_value)
                | cols_keep $common
            ))
        }
        let np = ($parts | length)
        if $np == 1 { $all = ($all | append $parts.0) }
        if $np > 1 { $all = ($all | append (polars concat ...$parts)) }
    }

    # ---- IP: dst.ip of a connect / send_data (type ip-dst) ----
    let ip_frame = (
        $base
        | polars filter ((polars col event_info_name) | polars is-in (['connect', 'send_data'] | polars into-df))
        | (unnestif $in dst)
        | cols_keep [utc_time task_name task_pid command_line dst_ip]
    )
    let ip_has_col = ('dst_ip' in ($ip_frame | polars schema | columns))
    let ip_types = ($itab | polars select [ioc_type] | polars collect | polars into-nu | get ioc_type | uniq)
    if ($ip_has_col and ('ip-dst' in $ip_types)) {
        let lookup = ($itab | polars filter ((polars col ioc_type) == 'ip-dst'))
        $all = ($all | append (
            $ip_frame
            | polars join $lookup -i [dst_ip] [ioc_value]
            | polars with-column ((polars col dst_ip) | polars as ioc_value)
            | cols_keep $common
        ))
    }

    # ---- DOMAIN: query of a dns_query (type domain) ----
    let dns_frame = (
        $base
        | polars filter ((polars col event_info_name) == 'dns_query')
        | cols_keep [utc_time task_name task_pid command_line query]
    )
    let dns_has_col = ('query' in ($dns_frame | polars schema | columns))
    let dns_types = ($itab | polars select [ioc_type] | polars collect | polars into-nu | get ioc_type | uniq)
    if ($dns_has_col and ('domain' in $dns_types)) {
        let lookup = ($itab | polars filter ((polars col ioc_type) == 'domain'))
        $all = ($all | append (
            $dns_frame
            | polars join $lookup -i [query] [ioc_value]
            | polars with-column ((polars col query) | polars as ioc_value)
            | cols_keep $common
        ))
    }

    if ($all | is-empty) { return (empty_like $base) }

    # Union — each branch is already at the $common schema; a single branch = direct,
    # several = concat. We add the evidence column and that's it.
    let union = if ($all | length) == 1 { $all.0 } else { (polars concat ...$all) }
    $union
    | polars with-column ((polars lit "misp_ioc") | polars as evidence)
}



export def show_family [base, family: string, num: int, explore: bool] {
    let l2 = (match $family {
        'execve'        => (detect_execve $base)
        'file_create'   => (detect_file_create $base)
        'connect'       => (detect_connect $base)
        'send_data'     => (detect_send_data $base)
        'dns_query'     => (detect_dns_query $base)
        'kill'          => (detect_kill $base)
        'bpf_prog_load' => (detect_bpf $base)
        'mmap_exec'     => (detect_mmap_exec $base)
        'prctl'         => (detect_prctl $base)
        'ioc'           => (detect_ioc $base)
        _ => { error make { msg: $"unknown family: ($family)" } }
    })
    let rows = ($l2 | polars collect | polars into-nu)
    let n = ($rows | length)
    print $"(ansi green)≡≡  Family: ($family)   —  ($n) alerts(ansi reset)"
    if (($rows | length) > 0) {
        if $explore {
            $rows | polars into-df | explore
        } else {
            $rows | first $num | table -e
        }
    }
    print ""
}

# =====================================================================
# ---- Summary: IP / ports / download URLs -----------------------------
# =====================================================================
# Builds a consolidated summary from the already detected network and file
# families. Returns a list of markdown lines to integrate into the report:
#   - destination IPs (public / egress) and contacted ports, deduplicated,
#     with the number of connections ;
#   - malware download URLs and commands (curl/wget … piped) and dropped files
#     (binaries in /tmp, /dev/shm, …).
# Works standalone from the raw base (re-detects the useful families).
export def report_bilan [base, num: int = 50] {
    let connects = (detect_connect $base | polars collect | polars into-nu)
    let sends    = (detect_send_data $base | polars collect | polars into-nu)
    let execves  = (detect_execve $base | polars collect | polars into-nu)
    let files    = (detect_file_create $base | polars collect | polars into-nu)
    bilan_sections $connects $sends $execves $files $num
}

# "Internal" version of report_bilan: takes directly the nushell dataframes
# already detected (connect, send_data, execve, file_create) to avoid
# re-detecting the families when they have already been counted (e.g. report render).
def bilan_sections [
    connects: list
    sends: list
    execves: list
    files: list
    num: int
] {
    let data = (bilan_data $connects $sends $execves $files $num)
    mut lines = [ "## Summary — IP, ports and downloads" "" ]

    $lines = ($lines | append "### Detected IPs / ports")
    if ($data.ipports | is-empty) {
        $lines = ($lines | append "_No suspicious outbound connection detected._")
    } else {
        $lines = ($lines | append [ "| IP | Port | Connections |" "|---|---|---|" ])
        for r in ($data.ipports | first $num) {
            $lines = ($lines | append $"| ($r.ip) | ($r.port) | ($r.count) |")
        }
        if ($data.ipports | length) > $num {
            $lines = ($lines | append $"… and (($data.ipports | length) - $num) other IP:port pairs")
        }
    }
    $lines = ($lines | append "")

    $lines = ($lines | append "### Download URLs")
    if ($data.urls | is-empty) {
        $lines = ($lines | append "_No download URL detected in the commands._")
    } else {
        for u in ($data.urls | first $num) { $lines = ($lines | append $"- `($u)`") }
    }
    $lines = ($lines | append "")

    $lines = ($lines | append "### Dropped / persistent files")
    if ($data.files | is-empty) {
        $lines = ($lines | append "_No suspicious dropped file._")
    } else {
        for p in ($data.files | first $num) { $lines = ($lines | append $"- `($p)`") }
    }
    $lines = ($lines | append "")

    $lines
}

# Structured version (for the JSON report) of the summary: from the already
# detected connect / send_data / execve / file_create dataframes, computes:
#   - ipports : contacted ip:port pairs with their connection count (sorted) ;
#   - urls    : http(s) URLs extracted from the command lines (downloads) ;
#   - files   : main_path (dropped / persistent files).
def bilan_data [
    connects: list
    sends: list
    execves: list
    files: list
    num: int
] {
    # ---- 1. Contacted IP/port (public egress + unusual ports) ----
    let ipports = ((
            $connects | select dst_ip dst_port
        ) | append ($sends | select dst_ip dst_port))
    let ip_by_port = ($ipports
        | group-by {|r| $"(($r.dst_ip | into string)):($r.dst_port)" }
        | items {|k, rows|
            { ip: ($rows.0.dst_ip | into string)
              port: ($rows.0.dst_port | into int)
              count: ($rows | length) } }
        | sort-by count --reverse
        | first $num)

    # ---- 2. Malware download URLs ----
    # Extraction of http(s) URLs in the execve, connect, send_data alert command
    # lines (e.g. "curl http://evil/p.sh | sh").
    let urls = (
        ($execves | get command_line)
        | append ($connects | get command_line)
        | append ($sends | get command_line)
        | each {|c|
            ($c | split row ' '
                | where {|w| ($w | str starts-with 'http') }
                | each {|w| $w | str trim -c '"' | str trim -c "'" })
          }
        | flatten
        | where {|w| not ($w | is-empty) }
        | uniq
    )

    # ---- 3. Dropped files (drop-and-run, persistence) ----
    let file_cols = ($files | columns)
    let files_deposes = if ("main_path" not-in $file_cols) or ($files | is-empty) {
        []
    } else {
        ($files | get main_path | uniq | first $num)
    }

    { ipports: $ip_by_port, urls: $urls, files: $files_deposes }
}

# =====================================================================
# ---- Full report rendering (families + summary) ---------------------
# =====================================================================
# Detects ALL families ONCE, counts the alerts per family and builds the whole
# report: the markdown AND the structured data (useful for the JSON render).
# Returns { md, fam_counts, data, bilan }:
#   - md         : markdown lines to write into the file,
#   - fam_counts : nb of alerts per family (for the descriptive name),
#   - data       : record fam -> COMPLETE list of detections (each family
#                  materialized only once),
#   - bilan      : { ipports, urls, files } structure for the JSON report.
# max_rows bounds the number of DISPLAYED lines in the markdown (beyond, only
# counted) ; the JSON, on the other hand, embeds all detections.
export def render_report [base, max_rows: int = 25] {
    # Materializes each family ONCE in memory (nu list); then derives the count,
    # the markdown max_rows and the full JSON list from it.
    let data = {
        execve:        (detect_execve $base | polars collect | polars into-nu)
        file_create:   (detect_file_create $base | polars collect | polars into-nu)
        connect:       (detect_connect $base | polars collect | polars into-nu)
        send_data:     (detect_send_data $base | polars collect | polars into-nu)
        dns_query:     (detect_dns_query $base | polars collect | polars into-nu)
        kill:          (detect_kill $base | polars collect | polars into-nu)
        bpf_prog_load: (detect_bpf $base | polars collect | polars into-nu)
        mmap_exec:     (detect_mmap_exec $base | polars collect | polars into-nu)
        prctl:         (detect_prctl $base | polars collect | polars into-nu)
        ioc:           (detect_ioc $base | polars collect | polars into-nu)
    }

    let fam_counts = ($data | items {|fam, rows| { $fam: ($rows | length) } }
        | reduce -f {} {|it, acc| $acc | merge $it })

    mut md = []
    for fam in (report_families) {
        let n = ($fam_counts | get $fam)
        $md = ($md | append $"## ($fam) — ($n) alerts" "")
        if $n > 0 {
            $md = ($md | append (($data | get $fam | first $max_rows | to md) | split row "\n"))
            let more = $n - $max_rows
            if $more > 0 { $md = ($md | append $"… and ($more) more alerts not shown") }
        }
        $md = ($md | append "")
    }

    # Summary (IP/ports, URLs, files) reuses the already detected dataframes.
    let bilan = (bilan_data ($data.connect) ($data.send_data) ($data.execve) ($data.file_create) 50)
    $md = ($md | append (bilan_sections ($data.connect) ($data.send_data) ($data.execve) ($data.file_create) 50))

    { md: $md, fam_counts: $fam_counts, data: $data, bilan: $bilan }
}

# Writes the report of a sample into logs/ngsoti/scanresult<TS>/<name>.md AND
# <name>.json (JSON enabled by default; --no-json to write only the markdown),
# next to the input file.
# The TS is passed by the wrapper (unique for a whole session); otherwise generated.
# The file name is delegated to report_basename (descriptive from the families).
export def write_report [base, file: string, ts?: string, --no-json] {
    let ts = ($ts | default (date now | format date "%Y%m%d_%H%M%S"))
    let rep = (render_report $base 25)
    let json = (not $no_json)

    let log_dir = ($file | path dirname | path dirname)
    let out_dir = ($log_dir | path join $"scanresult($ts)")
    mkdir $out_dir
    let hash = ($file | path dirname | path basename)
    let base_name = (report_basename $hash $rep.fam_counts)

    let out_md = ($out_dir | path join $"($base_name).md")
    $rep.md | str join "\n" | save --force $out_md

    # Structured JSON report (full detections + summary + metadata).
    if $json {
        let out_json = ($out_dir | path join $"($base_name).json")
        (report_json $file $ts $rep) | to json | save --force $out_json
        # PARQUET save: one detection (non-empty) per family, directly usable by
        # nushell (`polars open <file>.parquet | polars into-nu`).
        # Each dataframe is already materialized as a nu list (rep.data); we
        # reserialize it to parquet via polars. An empty family writes nothing.
        let parquets = ($rep.data
            | items {|fam, rows| if ($rows | length) > 0 { { fam: $fam, rows: $rows } } else { null } }
            | where {|it| $it != null }
            | each {|it|
                let out_pq = ($out_dir | path join $"detections_($it.fam).parquet")
                do { $it.rows | polars into-df | polars into-lazy | polars save $out_pq }
                $out_pq
            })
        { md: $out_md, json: $out_json, parquets: $parquets, fam_counts: $rep.fam_counts }
    } else {
        { md: $out_md, fam_counts: $rep.fam_counts }
    }
}

# Assembles the structured JSON report content: metadata (source, short hash,
# timestamp), count per family, full detection lists per family and summary
# (IP/ports, URLs, dropped files).
export def report_json [file: string, ts: string, rep: record] {
    let hash = ($file | path dirname | path basename)
    {
        schema_version: 1,
        generated_at: (date now | format date "%Y-%m-%dT%H:%M:%S%z"),
        scan_ts: $ts,
        sample: {
            file: ($file | path expand),
            hash: $hash,
            short_hash: ($hash | str substring 0..7),
        },
        fam_counts: $rep.fam_counts,
        families: $rep.data,
        bilan: $rep.bilan,
    }
}

# =====================================================================
# ---- Report naming --------------------------------------------------
# =====================================================================
# Canonical (sorted) list of detection families: used to build the descriptive
# name of a report. Every detection family lives here, at the detection
# procedure level, not in the wrappers.
export def report_families [] {
    ['execve','file_create','connect','send_data','dns_query','kill',
     'bpf_prog_load','mmap_exec','prctl','ioc']
}

# Descriptive name of a report, built from the DETECTED detection families
# (family+alert count), preceded by the short hash of the sample.
# Receives the source hash and the alert count per family (record fam -> n).
# E.g. 15e67237_execve1_connect35_send_data9286_dns_query2_prctl3.md
export def report_basename [hash: string, fam_counts: record] {
    let short = ($hash | str substring 0..7)
    let sig = ($fam_counts
        | items {|fam, n| if ($n | into int) > 0 { $"($fam)($n)" } else { null } }
        | where {|v| $v != null }
        | str join "_")
    let label = if ($sig | is-empty) { "no_alert" } else { $sig }
    $"($short)_($label)"
}

# =====================================================================
# ---- MAIN ------------------------------------------------------------
# =====================================================================
def main [
    ...files: string
    --infer-schema: int = 200000
    --num (-n): int = 20                 # rows displayed per family
    --family (-f): string = "all"        # all or a single family
    --explore (-x)                       # interactive dataframe display
    --no-json                            # write only the markdown (no .json)
    --no-convert                         # disable automatic gz/jsonl->parquet conversion
    --force-convert                      # reconvert even if the parquet cache is up to date
    --cache-dir: string                  # directory for conversion parquets (default: next to the source)
    --profile (-p): string               # active local_cfg functions: e.g. "dns,network" (default: "all" = all)
    --no-noise (-N)                      # SUPPRESS benign system noise (NTP chronyd, reverse-DNS, dns allowlist)
    --ioc (-I): string = "misp.iocs"     # MISP IOC file (JSONL) for the ioc family (hash/IP/domain)
] {
    # Selection of LOCAL CONFIG FUNCTIONS: explicit --profile (CSV list of
    # functions) > $env.KUNAI_PROFILE > "all" (all functions — cf. kunai_local_cfg.nu).
    # We propagate --profile to the imported module via the process env var.
    if ($profile | is-not-empty) { $env.KUNAI_PROFILE = $profile }
    # NOISE SUPPRESSION mode: by default the engine SEES the benign system noise
    # (chronyd NTP port 123, iptables reverse-DNS, dns allowlist). With --no-noise,
    # these known benign flows are filtered (noise_off -> $env.KUNAI_NOISE).
    if $no_noise { $env.KUNAI_NOISE = "true" }
    # Path of the MISP IOC file for the ioc family (set for detect_ioc).
    if ($ioc | is-not-empty) { $env.KUNAI_IOC = $ioc }

    # default files: the 2 most recent .gz of the registry.
    # We always exclude the live `events.log` file (non-compressed log being
    # written), in addition to the .gz extension filter.
    let registry_dir = "/run/media/pouchou/SSD2T/ips-ids-siem-pcaps/kunai/kunai_registry/kunai"
    let file_list = if (($files | is-empty)) {
        (ls $registry_dir
            | where { |e|
                let n = ($e.name | path basename)
                $n != 'events.log' and (($n | str ends-with '.gz') or ($n | str ends-with '.parquet'))
            }
            | sort-by modified
            | last 2
            | get name
            | path expand)
    } else { $files }

    let nf = ($file_list | length)
    print $"(ansi cyan)🔍 Compromise detection — ($nf) files(ansi reset)"

    # Unique session timestamp: all samples share the same
    # scanresult<TS> folder (like the ngsoti_all/ngsoti_detail wrappers).
    let scan_ts = (date now | format date "%Y%m%d_%H%M%S")

    let families = if $family == 'all' {
        ['execve','file_create','connect','send_data','dns_query','kill','bpf_prog_load','mmap_exec','prctl','ioc']
    } else { [$family] }

    for file in $file_list {
        if not (($file | path exists)) {
            print $"(ansi yellow)⚠ file not found: ($file)(ansi reset)"
            continue
        }
        # .gz integrity (file being written / truncated) — only
        # for compressed files, not for a raw .jsonl (test case).
        if ($file | path parse | get extension) == 'gz' {
            let res = (^gzip -t $file | complete)
            if $res.exit_code != 0 {
                print $"(ansi yellow)⚠ truncated/incomplete file, ignored: ($file)(ansi reset)"
                continue
            }
        }
        # Automatic gz/jsonl -> parquet conversion (by default) before analysis,
        # unless --no-convert. build_base reads the resulting parquet, far faster
        # than reparsing the intact ndjson at each run.
        let eff = if $no_convert {
            { file: $file, converted: false }
        } else {
            ensure_parquet $file $infer_schema --force=$force_convert --cache-dir=$cache_dir
        }
        let base = (build_base $eff.file $infer_schema)
        let badge = if $eff.converted {
            $"(ansi yellow) [converted → ($eff.file)](ansi reset)"
        } else if not $no_convert and ($eff.file != $file) {
            $"(ansi yellow) [parquet cache: ($eff.file)](ansi reset)"
        } else { "" }
        print $"(ansi cyan)┌─ File: ($file)($badge) ───────────────────────┐(ansi reset)"
        for fam in $families {
            show_family $base $fam $num $explore
        }
        # Also writes the markdown report (+ JSON by default) into scanresult<TS>/
        # (same mechanism as ngsoti_detail.nu) so that the direct invocation
        # produces output files, not only a console display.
        # We pass the SOURCE ($file) for the hash/folder, regardless of the
        # conversion (the cache parquet may live elsewhere with --cache-dir).
        let wrote = (write_report $base $file $scan_ts --no-json=$no_json)
        if $no_json {
            print $"(ansi cyan)Report written: ($wrote.md)(ansi reset)"
        } else {
            let pq = ($wrote.parquets | str join "\n")
            print $"(ansi cyan)Report written: ($wrote.md)\nJSON: ($wrote.json)\nParquet:\n($pq)(ansi reset)"
        }
    }
}
