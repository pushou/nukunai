#!/usr/bin/env nu
# kunai_rules.nu
#
# COMMON POOL of generic rules — EXECUTABLE version in nushell.
#
# Each generic rule here corresponds to the homonymous kunai `.yaml` file located
# in `kunai_rules/rules_v0.1/` (the interoperable common pool, reusable by the
# whole kunai ecosystem). This module re-exposes the SAME indicators in the form
# of lazy polars expressions ready to be consumed by the analysis engine
# (kunai_detect_compromise.nu). The rules are generic: they depend on NO machine
# context. The local context (agents/network/path allowlists) lives in
# kunai_rules_local.nu and it is the ENGINE that composes the two.
#
# Convention: a rule `r_<family>_<name>` returns a polars Expr (the detection
# condition); the displayed evidence is given by `r_ev_<family>_<name>`.
# Families having only a fixed evidence carry their label directly in the engine.

# =====================================================================
# execve — suspicious execution
# =====================================================================

# T1059.004 — native BASH/SH reverse shell: opening /dev/tcp or /dev/udp
# by a shell interpreter, without requiring -i. No legitimate command-line
# usage: the most reliable indicator of active compromise.
export def r_execve_reverse_shell [] {
    (polars col command_line) | polars contains "(?:\\b(bash|sh|zsh|ksh|dash)\\b[^|]*?\\b(?:/dev/tcp/|/dev/udp/))"
}
export def r_ev_execve_reverse_shell [] { "reverse_shell" }

# T1059.004 — interactive shell (-i) with a network tool (nc/ncat/socat) or redirection.
export def r_execve_shell_tool [] {
    (polars col command_line) | polars contains "(?:/bin/(?:bash|sh|zsh|ksh) -[ic].*(?:nc |ncat|/dev/tcp/|/dev/udp/|socat)|(?:bash|sh|zsh) -[ic].*(?:/dev/tcp/|/dev/udp/))"
}
export def r_ev_execve_shell_tool [] { "reverse_shell" }

# T1105 / T1059.004 — download then execute on one line (curl|wget | sh).
export def r_execve_download_exec [] {
    (polars col command_line) | polars contains "(?:curl|wget) .*(?:\\|| \\-o| \\-O).*(?:sh|bash|python3?|perl)"
}
export def r_ev_execve_download_exec [] { "download_and_execute" }

# T1027 / T1140 — obfuscated execution / inline decoding (base64, perl -e, py -c).
export def r_execve_obfuscated [] {
    (polars col command_line) | polars contains "(?:base64 -d|base64 -D|xxd -r|openssl enc|perl -e|python3? -c|php -r)"
}
export def r_ev_execve_obfuscated [] { "obfuscated_exec" }

# T1588.002 — known offensive tool (scanners, crackers, droppers, implant).
export def r_execve_offensive_tool [] {
    (polars col command_line) | polars contains "(?:nmap|masscan|hydra|medusa|john|hashcat|mimikatz|msfconsole|meterpreter|procdump|sqlmap|nikto|dirb|gobuster)"
}
export def r_ev_execve_offensive_tool [] { "offensive_tool" }

# T1204 / T1059 — execution from a temporary directory (/tmp, /dev/shm…).
# Generic atomic pattern; the actual raising requires non-membership in the build
# chain (local context): the engine composes (see detect_execve).
export def r_execve_from_tmp [] {
    (polars col command_line) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/)[^ ]*(?:\\.sh|\\.py|\\.pl|\\.elf|\\.bin|\\.out| )"
}
export def r_ev_execve_from_tmp [] { "exec_from_tmp" }

# =====================================================================
# file_create — drop / persistence
# =====================================================================

# T1505.003 — webshell: creation of a server-side script (php/asp/jsp/cgi/py/rb/pl)
# in a web document-root. A legitimate server does not create brand-new executable
# scripts there: a strong web-compromise indicator.
export def r_filecreate_webshell [] {
    let docroot = ((polars col main_path) | polars contains "^/var/www(/|$)|^/srv/www(/|$)|^/usr/share/nginx/html(/|$)|^/usr/share/apache2(/|$)|^/opt/[^/]*/(htdocs|www|public_html|webroot)(/|$)")
    let ext = ((polars col main_path) | polars contains "\\.(php|php5|php7|phtml|asp|aspx|jsp|cgi|py|rb|pl)$")
    $docroot and $ext
}
export def r_ev_filecreate_webshell [] { "webshell_drop" }

# T1564.001 — hidden file ('.' prefix) dropped in /tmp|/dev/shm|/var/tmp.
# Atomic pattern; on the engine side it is correlated with actual execution
# (drop-and-run) to avoid flagging legitimate hidden artifacts never executed.
export def r_filecreate_hidden_tmp [] {
    (polars col main_path) | polars contains "^/tmp/\\.[^/]+|^/dev/shm/\\.[^/]+|^/var/tmp/\\.[^/]+"
}
export def r_ev_filecreate_hidden_tmp [] { "hidden_tmp" }

# =====================================================================
# connect — suspicious outgoing network
# =====================================================================

# T1095 / T1496 — connection to a C2 / backdoor / Tor / mining pool port,
# to a PUBLIC destination. The `dst_public` test is applied by the engine.
export def r_connect_unusual_port [] {
    ((polars col dst_port)
     | polars is-in ([4444,4445,6667,31337,9001,8888,8443,1337,2222,161,137,445,9999,49152,9050,9051,9150,5555,7777,14444,14433,45700,3256,20535,3333] | polars into-df))
}
export def r_ev_connect_unusual_port [] { "unusual_port" }

# T1041 — exfiltration: sending large amounts of data to a public destination
# not allowlisted (engine composition with the local context).
export def r_connect_public_egress [] { ((polars col dst_public) == true) }
export def r_ev_connect_public_egress [] { "public_egress" }

# =====================================================================
# send_data — exfiltration
# =====================================================================

# T1041 — abnormally high send volume (> 1 MB) to a public destination.
export def r_senddata_large [] { ((polars col data_size) > 1000000) }
export def r_ev_senddata_large [] { "large_data" }

# T1030 / T1001 — high-entropy send (encrypted/randomized content) to public.
export def r_senddata_high_entropy [] { ((polars col data_entropy) > 7.5) }
export def r_ev_senddata_high_entropy [] { "high_entropy" }

# =====================================================================
# dns_query — recon / tunneling
# =====================================================================

# T1568.002 — dynamic / suspicious TLDs (.tk, .ml, .ga, …, .onion, .i2p).
export def r_dns_suspicious_tld [] {
    (polars col query) | polars contains "(?:\\.tk$|\\.ml$|\\.ga$|\\.cf$|\\.gq$|\\.top$|\\.xyz$|\\.pw$|\\.onion$|\\.i2p$)"
}
export def r_ev_dns_suspicious_tld [] { "suspicious_tld" }

# T1071.004 — abnormally long DNS query (> 60): candidate for DNS tunneling.
export def r_dns_long_query [] { (((polars col query) | polars str-lengths) > 60) }
export def r_ev_dns_long_query [] { "suspicious_length" }

# Reverse-DNS (PTR): `*.in-addr.arpa` (IPv4) / `*.ip6.arpa` (IPv6) query, an
# IP->hostname form used by administration tools (iptables -L, traceroute,
# whois…). This is NEVER tunneling: no data is exfiltrated in a reverse query,
# and the query type is a FLOW criterion (FQDN shape), not a process-identity
# one. Such a query must NOT trigger ANY dns_query evidence (neither
# non_standard_dns_server, nor length, nor tld).
export def r_dns_reverse [] {
    (polars col query) | polars contains "(?:\\.in-addr\\.arpa$|\\.ip6\\.arpa$)"
}
export def r_ev_dns_reverse [] { "dns_reverse" }

# =====================================================================
# kill — disruption / evasion
# =====================================================================

# T1489 — stopping a critical service (agents, security daemon, systemd…).
export def r_kill_critical_target [] {
    (polars col target_task_name) | polars contains "(?:docker|containerd|sshd|systemd|wazuh|crowdsec|splunk|check_mk|auditd|cron|agent)"
}
export def r_ev_kill_critical_target [] { "kill_critical" }

# T1489 — forced SIGKILL termination outside normal lifecycle.
export def r_kill_hard_signal [] { ((polars col signal) == 'SIGKILL') }
export def r_ev_kill_hard_signal [] { "SIGKILL_hard" }

# =====================================================================
# mmap_exec — injection / drop-and-run
# =====================================================================

# T1055.001 — execution (mmap exec) from a temporary / fd / memfd area.
# Atomic pattern; the actual raising excludes the build chain (local context).
export def r_mmapexec_from_tmp [] {
    (polars col mapped_path) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/|/proc/self/fd/|memfd:)"
}
export def r_ev_mmapexec_from_tmp [] { "mmap_exec_suspicious" }

# =====================================================================
# prctl — evasion
# =====================================================================

# T1622 — PR_SET_DUMPABLE modification (evasion / anti-core-dump capture).
export def r_prctl_dumpable [] { ((polars col option) == 'PR_SET_DUMPABLE') }
export def r_ev_prctl_dumpable [] { "dumpable_change" }
