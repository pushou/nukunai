#!/usr/bin/env nu
# kunai_local_cfg.nu
#
# PARAMETRIZABLE LOCAL CONFIG — middleware for allowlist selection.
#
# This is THE data file that makes `kunai_rules_local.nu` parametrizable.
# It contains:
#   1. a generic `default` BASE (common to all platforms, strict);
#   2. `function_profiles` PER FUNCTION (allowlist themes), combinable.
#
# The profiles are NOT per machine: they are per FUNCTION (theme) —
#   dns / network / docker / elk / kube — and can be activated independently.
# The `all` profile takes them ALL and is the default choice. You can restrict
# by passing a LIST of functions, e.g. --profile "dns,network" to detect
# only with the DNS + network allowlists (without docker/elk/kube).
#
# The engine (kunai_detect_compromise.nu) / interface (kunai_rules_local.nu)
# keep calling `local_cfg <key_name>`. Function selection and merging with the
# base happen here, transparently for the caller.
#
# ── PROFILE SELECTION (decreasing priority) ────────────────────────
#   1. `local_cfg <key> --profile "a,b"`   (CSV list of functions / test)
#   2. `$env.KUNAI_PROFILE`                (set by the engine from --profile)
#   3. otherwise → `all`                        (ALL functions, by default)
# A value 'all' (or empty) = base + all functions, in canonical
# order [dns, network, docker, elk, kube]. A value a,b = base + a + b.
#
# ── BASE + FUNCTIONS MERGING ─────────────────────────────────────────────
# Each function touches ONLY the keys of its theme (dns_ips / DNS queries
# for `dns`, docker networks for `docker`, ELK paths for `elk`…). For a key
# shared between several functions, use the '__append__' marker to
# CONCAT (deduplicated) in the order of the active functions; without the
# marker, the list REPLACES the base. All values are LISTS (the smallest is
# allowlist_egress_ports which stays an INT list: [443, 80, 53]).
#
# ── IP PREFIX CONVENTION ───────────────────────────────────────────
# `allowlist_public_networks` is compared to the EXACT dst_ip PREFIX (via
# the engine's starts_with_any). But kunai renders depending on context:
#   * "bare" IPs        :  34.120.127.130   (Internet, IPv4)
#   * IPv4-mapped IPs  :  ::ffff:172.19.0.8  (docker/private networks resolved
#                         over IPv6-mapping by the container stack)
# So to cover a private network seen as ::ffff:, the prefix MUST include the
# `::ffff:` prefix (e.g. '::ffff:172.16.'), otherwise the match fails silently.
# This is the main cause of the tpot report noise: dst_public=true is set
# by kunai without knowing that ::ffff:172.19.x is a private docker network.
#
# ── ADDING A MACHINE ──────────────────────────────────────────────────
# Copy a `"<hostname>": { ... }` block under `profiles`, put ONLY the
# keys that change versus the base (the others inherit from `default`).
# Use '__append__' to extend an allowlist instead of rewriting it.

# Generic base — common to all analyzed machines. STRICT: it contains
# ONLY what is true on all platforms (repos/CDN/public mirrors, agents and
# universally used utilities). Everything else (docker networks, lab IPs,
# ELK services, embedded DNS resolver…) is LOCAL CONTEXT
# => put it in a dedicated `profile`, never here.
def default_defs [] {
    {
        # real agents / platform services to permanently remove from the noise.
        # We do NOT put the hijackable tools (docker, chmod, chown, tar, cp...),
        # which must only be benign when they come from a legitimate chain
        # (see benign_utilities + not_legit in the engine).
        legit_agents: ['dpkg','dpkg-deb','dpkg-db-backup','dpkg-trigger','apt','apt-get','apt-cache','apt-key',
                       'gitlab-runner','gitlab-workhorse','gitlab-shell','git','dockerd',
                       'containerd','containerd-shim','containerd-shim-runc-v2','runc',
                       'runc:[0:PARENT]','runc:[1:CHILD]','runc:[2:INIT]','docker-init','6',
                       'systemd','systemctl','journalctl',
                       'wazuh-agent','wazuh-check-mk','wazuh-agentd','wazuh-modulesd','wazuh-syscheckd',
                       'wazuh-logcollec','wazuh-analysisd','wazuh-execd','wazuh-db','wazuh-authd',
                       'crowdsec','cscli','splunk','splunkforwarder',
                       'check_mk_agent','cmk-agent-ctl','sqv','nginx','nginx-worker','sshd','sshd-auth',
                       'cron','crond','dbus-daemon','polkitd','NetworkManager','unattended-upgrade',
                       'chronyd','chrony','systemd-timesyn','systemd-timesyncd',
                       'auditd','auditctl','auditd-manager','augenrules','aureport','ausearch'],
        # mundane utilities / standard system commands. Benign ONLY when they
        # come from a legitimate chain (see not_legit).
        benign_utilities: ['docker','grep','sed','cat','tar','gzip','bzip2','xz','gpg','gpgv','gpgconf',
                           'sh','bash','curl','wget','chmod','chown','install','cp','mv','rm',
                           'cmp','mktemp','rmdir','touch','mkdir','ln','readlink','basename',
                           'dirname','date','sleep','printf','echo','nproc','lsblk','df','free',
                           'pgrep','ps','ss','ip','hostname','uname',
                           'stat','awk','python3','python','perl','timedatectl','tr','cut',
                           'postconf','find','wc','du','ls','locale','paste','head','sort',
                           'timeout','dmsetup','nohup','ipmitool','netstat','last','iptables',
                           'ip6tables','nft','findmnt','sysctl','unix_chkpwd','crontab','login',
                           'systemd-timesyn','sshd-session','sftp-server','su','gpg-agent','loginctl',
                           'udevadm','vim','view','sed','nano','fuser','sudo'],
        # LEGITIMATE DNS resolvers: the `non_standard_dns_server` noise drops if
        # the queried DNS server is here. (the embedded Docker resolver 127.0.0.11
        # and lab IPs are local context => profile)
        dns_ips: ['10.6.255.106'],
        # EGRESS network ALLOWLIST: known/reputed BENIGN PUBLIC destinations.
        # An outgoing connection to one of these IPs (prefix) is NOT a
        # suspicious "public_egress": legitimate repos/CDN/mirrors of the server.
        allowlist_public_networks: [
            '151.101.', '151.101',             # crates.io / static.rust-lang.org (Fastly)
            '140.82.', '185.199.',             # GitHub
            '172.65.', '104.18.', '172.64.',   # GitLab (Cloudflare)
            '104.16.', '104.17.', '104.19.',   # Cloudflare public
            '2a04:4e42',                       # Cloudflare IPv6 (crates.io egress)
            'registry.iutbeziers.fr',          # Docker Hub / docker.io registry mirror
            '52.85.', '52.84.', '52.222.',     # aws CloudFront (apt Debian)
            '3.174.', '18.161.', '18.160.',    # aws global accelerator (apt Debian)
            '13.248.', '13.224.', '13.35.',    # aws CloudFront (apt Debian)
            '99.86.', '3.162.',                # aws CloudFront (apt Debian mirrors)
            '143.204.', '99.84.', '99.83.',    # aws CloudFront (apt Debian)
            '76.223.', '15.197.', '3.33.',     # aws Global Accelerator/CloudFront (apt Debian)
            '172.67.', '104.21.',              # Cloudflare (apt Debian deb.debian.org)
            '10.6.',                           # NTP/paquets/miroirs locaux
        ],
        # Service ports towards which a "public_egress"/"send_data" is NOT
        # suspicious when the destination is already allowlisted (443 https, 80 http,
        # 53 dns, 123 ntp). 123 = system NTP sync (chronyd/ntpd) to the public
        # pool: standard protocol, silent under IP/port flow analysis.
        allowlist_egress_ports: [443, 80, 53, 123],
        # Processes (task_name) whose outgoing network activity is legitimate.
        # NB: only filters by exact task_name. For traffic whose task_name
        # is unstable or too generic (ELK threads "elastic..][T#3]",
        # "[http_output]>w", node/libuv-worker"), you must correlate the
        # command_line PATH => use `allowlist_egress_paths` (local context).
        allowlist_egress_procs: ['rustup','rustup-init','cargo','git','git-remote-http',
                                 'git-remote-https','docker','docker-buildx','dockerd',
                                 'containerd','apt','apt-get','dpkg','wget','curl',
                                 'ssl_client','wazuh-agentd',
                                 'https'],   # apt TLS transport (/usr/lib/apt/methods/https)
        # NEW — command_line paths (substring match via contains_any)
        # whose outgoing network activity is legitimate. Allows allowlisting
        # binaries with an unstable task_name (service threads, runtimes) by their
        # REAL path on disk. Empty by default (unchanged behavior) => to fill
        # in the profiles (e.g. /usr/share/elasticsearch, /usr/share/kibana, logstash).
        allowlist_egress_paths: [],
        # NEW — benign DNS queries (query, substring match) not to report
        # as `non_standard_dns_server`/`suspicious_*`. Empty by default => to fill
        # in the profiles (e.g. epr.elastic.co, infra-cdn.elastic.co, K8s/docker
        # service name "elasticsearch" resolved by libuv).
        # `pool.ntp.org` = resolution of the system NTP pool (chronyd/ntpd): benign
        # time sync (same logic as port 123 at egress) — the local resolver can
        # vary (10.0.2.3 QEMU/VBox, 10.6.x, Docker loopback), so we neutralize by
        # the QUERY SHAPE, not by the resolver IP.
        allowlist_dns_queries: ['pool.ntp.org'],
        # Rust build chain (heavy client ~/.cargo, ~/.rustup): execution from
        # /tmp/cargo-*, /tmp/rustc* or ~/.cargo is BENIGN.
        allowlist_build_procs: ['rustup','rustup-init','cargo','rustc','rustdoc',
                                'cc','cc1','cc1plus','cc1obj','cc1objplus','as','as1',
                                'ar','ranlib','llvm-ar','llvm-ranlib','nm',
                                'collect2','ld.lld','rust-lld','cargo-git-checkout',
                                'clippy-driver','cargo-clippy','cargo-build',
                                'CloseHandle'],
        # legitimate compilation / cache paths of the build chain.
        allowlist_build_paths: [
            '/tmp/cargo-', '/tmp/rustc', '/tmp/tmp.', '/tmp/cargo-install',
            '/tmp/cc', '/tmp/as',
            '/home/nushell/.cargo/', '/home/nushell/.rustup/',
            '/root/.cargo/', '/root/.rustup/',
        ],
        # BENIGN process/runtime management signals (never a suspicious kill).
        benign_signals: ['SIGURG','SIGCHLD','SIGCONT','SIGWINCH','SIGIO','SIGPIPE'],
        # INITRAMFS build chain (mkinitramfs/dracut/update-initramfs): staging
        # temporary /var/tmp/mkinitramfs_* / /usr/lib/dracut reserved for mkinitramfs.
        allowlist_initramfs_paths: ['/var/tmp/mkinitramfs','/usr/lib/dracut'],
    }
}

# PER-FUNCTION profiles (allowlist themes), combinable and INDEPENDENT.
# Each function touches ONLY the keys of its theme. A key shared between
# several functions uses '__append__' at the top = concat (deduplicated) in
# the order of the active functions; without the marker, the list replaces the base.
#
# The `all` profile (DEFAULT) = base + ALL the functions below, in canonical
# order: dns, network, docker, elk, kube. To activate only some
# functions: --profile "dns,network" (the base is always applied).
#
# ── Adding a function ──────────────────────────────────────────────────
# Add a `"<name>": { ... }` block here then insert it in the canonical order
# `fn_order` (see below). Explicitly put '__append__' to extend a
# key already provided by another function, otherwise it is replaced.
def function_profiles [] {
    {
        # ── dns: legitimate DNS resolvers and DNS queries ──────────────────
        # Neutralizes `non_standard_dns_server` when the queried server is the
        # embedded Docker resolver (127.0.0.11) or the local resolver (127.0.0.1),
        # and neutralizes the DNS signals of internal queries (container services,
        # epr.elastic.co / infra-cdn.elastic.co of the ELK stack).
        dns: {
            dns_ips: ['__append__', '127.0.0.11', '127.0.0.1'],
            allowlist_dns_queries: ['__append__',
                                    'epr.elastic.co', 'infra-cdn.elastic.co',
                                    'elasticsearch'],
        },
        # ── network: universal legitimate network egress ──────────────────────
        # Service ports and apt TLS transport (already in the base, explicit reminder
        # for those who disable the implicit base — redundant but harmless).
        network: {
            allowlist_egress_ports: [443, 80, 53, 123],
            allowlist_egress_procs: ['https'],
        },
        # ── docker: internal docker/host networks seen as ::ffff: ────────────
        # ELK docker networks (172.16-31./12), lab (10.6.), mapped local loopback
        # (127.0.0.), OVH tpot host (51.89.) — INTERNAL traffic, not a public_egress.
        # Includes the Docker resolver 127.0.0.11 (see also the dns function).
        docker: {
            allowlist_public_networks: ['__append__',
                                        '::ffff:172.16.', '::ffff:172.17.', '::ffff:172.18.',
                                        '::ffff:172.19.', '::ffff:172.20.', '::ffff:172.21.',
                                        '::ffff:172.22.', '::ffff:172.23.', '::ffff:172.24.',
                                        '::ffff:172.25.', '::ffff:172.26.', '::ffff:172.27.',
                                        '::ffff:172.28.', '::ffff:172.29.', '::ffff:172.30.',
                                        '::ffff:172.31.',
                                        '::ffff:10.6.', '::ffff:127.0.0.',
                                        '::ffff:51.89.'],   # OVH tpot host
            dns_ips: ['__append__', '127.0.0.11'],
        },
        # ── elk: Elasticsearch/Logstash/Kibana stack ───────────────────────
        # Paths of the ELK binaries (unstable task_name: Elasticsearch threads
        # "elastic..][T#3]", Logstash "[http_output]>w", node/libuv-worker for
        # Kibana) correlated on the command_line, + Elastic infra CDN (34.120.).
        elk: {
            allowlist_egress_paths: ['/usr/share/elasticsearch/',
                                     '/usr/share/logstash/',
                                     '/usr/share/kibana/'],
            allowlist_public_networks: ['__append__', '34.120.'],   # infra-cdn.elastic.co
            allowlist_dns_queries: ['__append__',
                                    'epr.elastic.co', 'infra-cdn.elastic.co',
                                    'elasticsearch'],
        },
        # ── kube: (reserved) Kubernetes networks/services ────────────────────
        # E.g. 10.96.0.0/12 (ClusterIP), 10.244.0.0/16 (CNI Flannel), service names
        # '<svc>.<ns>.svc.*'. Empty by default; to fill according to the platform.
        kube: {
        },
    }
}

# Ordre canonique des fonctions pour le profil `all` (et l'affichage).
def fn_order [] { ['dns', 'network', 'docker', 'elk', 'kube'] }

# =====================================================================
# Merging and selection logic — DO NOT MODIFY
# =====================================================================

# concat $base ∘ $extra if extra starts with '__append__', otherwise replace.
def merge_lists [base: list<any>, extra: list<any>] {
    if (not ($extra | is-empty)) and (($extra | first) == '__append__') {
        ($base | append ($extra | skip 1)) | uniq
    } else {
        $extra
    }
}

# loads the config resolved for the ACTIVE FUNCTIONS (merged base + functions).
# Returns a flat record containing ALL the keys expected by the engine.
# Internal usage: `kunai_rules_local.nu` calls it then does `.<key_name>`.
export def load_cfg [--profile: string] {
    let base = (default_defs)
    let fns  = (current_profile --profile=$profile)

    # Start from the base, then merge each active function in order.
    # merge_lists applies the '__append__' semantics (concat dedup) otherwise replaces.
    $fns | reduce -f $base {|fn, acc|
        let prof = (function_profiles | get --optional $fn | default {})
        ($prof | columns) | reduce -f $acc {|k, a|
            let base_v = ($a | get --optional $k | default [])
            let fn_v   = ($prof | get $k)
            let v      = (if ($fn_v == null) { $base_v } else { (merge_lists $base_v $fn_v) })
            $a | upsert $k $v
        }
    }
}

# List of the ACTIVE FUNCTIONS (list of names), for display / debug.
# Priority: explicit --profile > $env.KUNAI_PROFILE (set by the engine) > `all`.
# 'all' (or empty / unknown value) = all functions in canonical order.
# A value "a,b" = base + a + b (in the filtered canonical order).
export def current_profile [--profile: string] {
    let env_prof = ($env | get --optional KUNAI_PROFILE | default '')
    let sel = (if ($profile | is-not-empty) { $profile } else if (($env_prof | str trim) != '') { $env_prof } else { 'all' })
    let order = (fn_order)
    if (($sel | str trim) == 'all') {
        $order
    } else {
        # filters the canonical order on the CSV selection (deduplicates and orders).
        ($sel | split row ',' | each {|s| $s | str trim } | where {|s| $s != '' })
        | reduce -f [] {|s, acc| if ($order | any {|o| $o == $s }) { $acc | append $s } else { $acc } }
    }
}
