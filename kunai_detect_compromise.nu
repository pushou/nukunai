#!/usr/bin/env nu
# kunai_detect_compromise.nu
#
# Détection de compromission sur les logs kunai de la machine "registry".
# Analyse directement les fichiers d'événements kunai compressés (.gz), en mode
# lazy polars, sans passer par le format Parquet.
#
# Usage:
#   nu kunai_detect_compromise.nu                                # les 2 derniers fichiers du registry
#   nu kunai_detect_compromise.nu fichier1.gz fichier2.gz        # fichiers explicites
#   nu kunai_detect_compromise.nu -n 1                           # 1 seul fichier (le plus récent)
#   nu kunai_detect_compromise.nu -f execve                      # ne lancer qu'une famille
#   nu kunai_detect_compromise.nu --explore                      # affichage dataframe interactif
#
# Familles (--family): all, execve, file_create, connect, send_data,
#                      dns_query, kill, bpf_prog_load, mmap_exec, prctl

# =====================================================================
# ---- Contexte machine "registry" (agents/services légitimes) --------
# =====================================================================
def cfg [name: string] {
    match $name {
        # vrais agents / services de la plateforme à retirer du bruit définitivement.
        # On N'y met PAS les outils détournables (docker, chmod, chown, tar, cp, bash...),
        # qui ne doivent être bénins QUE quand ils proviennent d'une chaîne de processus
        # légitime (voir benign_utilities + not_legit), sinon un attaquant qui les lance
        # directement serait masqué.
        "legit_agents" => ['dpkg','dpkg-deb','dpkg-db-backup','dpkg-trigger','apt','apt-get','apt-cache','apt-key',
                           'gitlab-runner','gitlab-workhorse','gitlab-shell','git','dockerd',
                           'containerd','containerd-shim','runc','systemd','systemctl','journalctl',
                           'wazuh-agent','wazuh-check-mk','crowdsec','cscli','splunk','splunkforwarder',
                           'check_mk_agent','cmk-agent-ctl','sqv','nginx','nginx-worker','sshd',
                           'cron','crond','dbus-daemon','polkitd','NetworkManager','unattended-upgrade',
                           'auditd','auditctl','auditd-manager','augenrules','aureport','ausearch']
        # utilitaires banals / commandes système standard. Bénins UNIQUEMENT quand ils
        # proviennent d'une chaîne légitime (voir not_legit) : un docker/chmod/curl/perl/
        # python3 lancé par un attaquant (ou un task inconnu sous systemd) reste détecté.
        # Liste élargie avec les commandes standard observées comme enfants légitimes de
        # check_mk_agent / wazuh / ssh, afin de ne pas générer de faux positifs sur le trafic
        # normal quand on retire la règle "parent agent => bénin" (voir not_legit).
        "benign_utilities" => ['docker','grep','sed','cat','tar','gzip','bzip2','xz','gpg','gpgv',
                               'sh','bash','curl','wget','chmod','chown','install','cp','mv','rm',
                               'cmp','mktemp','rmdir','touch','mkdir','ln','readlink','basename',
                               'dirname','date','sleep','printf','echo','nproc','lsblk','df','free',
                               'pgrep','ps','ss','ip','hostname','uname',
                               'stat','awk','python3','python','perl','timedatectl','tr','cut',
                               'postconf','find','wc','du','ls','locale','paste','head','sort',
                               'timeout','dmsetup','nohup','ipmitool','netstat','last','iptables',
                               'ip6tables','nft','findmnt','sysctl','unix_chkpwd','crontab','login',
                               'systemd-timesyn','sshd-session','su','gpg-agent','loginctl',
                               'udevadm','sudo']
        # IP locales / plateforme (bruit réseau légitime)
        "dns_ips" => ['10.6.255.106']
        _ => { error make { msg: $"config inconnue: ($name)" } }
    }
}

# expression (expr polars) : la tâche n'appartient PAS au bruit légitime.
# Une ligne est BÉNIGNE (exclue du bruit) si :
#   - la tâche est elle-même un agent légitime (legit_agents), OU
#   - la tâche est un utilitaire bénin (benign_utilities, commande standard) et son
#     PARENT est légitime (agent OU utilitaire, chaîne de parents).
# NB : le PARENT seul ne suffit plus à rendre une ligne bénigne. Sinon un rootkit qui
# daemonise son binaire sous systemd (ex. perfctl → "oom_reaper", parent systemd) serait
# masqué : il n'est ni agent, ni utilitaire, donc il reste DÉTECTÉ quel que soit son parent.
# Un docker/chmod/curl/perl/python3 lancé par un task inconnu reste donc détecté.
def not_legit [] {
    let legit = ((cfg legit_agents) | polars into-df)
    let utils = ((cfg benign_utilities) | polars into-df)
    let task_legit   = ((polars col task_name) | polars is-in $legit)
    let task_utils   = ((polars col task_name) | polars is-in $utils)
    let parent_legit = ((polars col parent_task_name) | polars is-in $legit)
    let parent_utils = ((polars col parent_task_name) | polars is-in $utils)
    # parent "propre" = agent légitime OU utilitaire bénin
    let parent_ok  = ($parent_legit or $parent_utils)
    let benign     = ($task_legit or ($task_utils and $parent_ok))
    $benign | polars expr-not
}

# =====================================================================
# ---- Base lazy commune (unnests de base) -----------------------------
# =====================================================================
def build_base [file: string, infer_schema: int] {
    let b = (polars open $file -t ndjson --infer-schema $infer_schema
        | polars unnest data info
        # rename event -> event_info AVANT l'unnest pour éviter la collision
        # entre data.name/data.id (bpf) et event.name/event.id
        | polars rename event event_info
        | polars unnest event_info -s "_"
        | polars unnest host -s "_"
        | polars unnest task -s "_"
        | polars unnest parent_task -s "_")
    # path -> main_path (chemin du fichier / cible principale).
    # Ce champ n'existe que si au moins un évènement du lot expose un `path`
    # (ex. file_create) : le rename conditionnel évite un crash quand le
    # fichier n'en contient aucun (ex. un lot 100% connect / execve).
    let cols = ($b | polars schema | columns)
    if "path" in $cols { $b | polars rename [path] [main_path] } else { $b }
}

# filtre court sur le nom d'événement kunai
def ev [name: string] {
    polars filter ((polars col event_info_name) == $name)
}

# =====================================================================
# ---- FAMILLE execve : exécution suspecte -----------------------------
# =====================================================================
def detect_execve [base] {
    let c_shell = ((polars col command_line) | polars contains "(?:/bin/(?:bash|sh|zsh|ksh) -[ic].*(?:nc |ncat|/dev/tcp/|/dev/udp/|socat)|(?:bash|sh|zsh) -[ic].*(?:/dev/tcp/|/dev/udp/))")
    let c_dl    = ((polars col command_line) | polars contains "(?:curl|wget) .*(?:\\|| \\-o| \\-O).*(?:sh|bash|python3?|perl)")
    let c_obf   = ((polars col command_line) | polars contains "(?:base64 -d|base64 -D|xxd -r|openssl enc|perl -e|python3? -c|php -r)")
    let c_tool  = ((polars col command_line) | polars contains "(?:nmap|masscan|hydra|medusa|john|hashcat|mimikatz|msfconsole|meterpreter|procdump|sqlmap|nikto|dirb|gobuster)")
    let c_tmp   = ((polars col command_line) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/)[^ ]*(?:\\.sh|\\.py|\\.pl|\\.elf|\\.bin|\\.out| )")

    $base
    | ev 'execve'
    # not_legit couvre désormais aussi les enfants légitimes des agents
    # (ex. python3/perl/grep/sed de check_mk) sans masquer un docker/chmod/curl
    # lancé par un attaquant, ni les mêmes binaires sous un parent non légitime.
    | polars filter (not_legit)
    | polars select [utc_time task_name task_pid command_line]
    | polars with-column (
        (polars when $c_shell (polars lit "reverse_shell")
         | polars when $c_dl (polars lit "download_and_execute")
         | polars when $c_obf (polars lit "obfuscated_exec")
         | polars when $c_tool (polars lit "offensive_tool")
         | polars when $c_tmp (polars lit "exec_from_tmp")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE file_create : persistance / drop -----------------------
# =====================================================================
def detect_file_create [base] {
    let c_persist = ((polars col main_path) | polars contains "(?:/etc/cron\\.|/var/spool/cron|/etc/systemd/system/|/etc/rc\\.d/|/root/\\.|/home/[^/]+/\\.ssh/|/usr/local/bin/|/usr/bin/[^ ]+\\.old)")
    let c_tmpdl   = ((polars col main_path) | polars contains "(?:/tmp/|/dev/shm/)[^ ]*(?:\\.sh|\\.py|\\.pl|\\.elf|\\.so|\\.bin|\\.out|\\.jar|\\.tar|\\.gz|\\.zip)")
    let c_tmpdir  = ((polars col main_path) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/|/dev/mqueue/)")

    $base
    | ev 'file_create'
    | polars filter (not_legit)
    | polars unnest exe -s "_"
    | polars select [utc_time task_name task_pid main_path]
    | polars with-column (
        (polars when $c_persist (polars lit "persistence_path")
         | polars when $c_tmpdl (polars lit "tmp_dropper")
         | polars when $c_tmpdir (polars lit "tmp_write")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE connect : réseau sortant suspect ------------------------
# =====================================================================
def detect_connect [base] {
    let unusual = [4444,4445,6667,31337,9001,8888,8443,1337,2222,161,137,445,9999,49152]
    let unusual_s = ($unusual | polars into-df)
    let c_public = ((polars col dst_public) == true)
    let c_port   = ((polars col dst_port) | polars is-in $unusual_s)

    $base
    | ev 'connect'
    | polars filter (not_legit)
    | polars unnest exe -s "_" | polars unnest socket -s "_" | polars unnest src -s "_" | polars unnest dst -s "_"
    | polars filter ((polars col dst_ip) | polars is-not-null)
    # dst_public doit être sélectionné pour être utilisable par with-column
    | polars select [utc_time task_name task_pid command_line src_ip dst_ip dst_port dst_public]
    | polars with-column (
        (polars when ($c_public) (polars lit "public_egress")
         | polars when $c_port (polars lit "unusual_port")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE send_data : exfiltration --------------------------------
# =====================================================================
def detect_send_data [base] {
    let c_public = ((polars col dst_public) == true)
    let c_big    = ((polars col data_size) > 1000000)
    let c_hi     = ((polars col data_entropy) > 7.5)

    $base
    | ev 'send_data'
    | polars filter (not_legit)
    | polars unnest exe -s "_" | polars unnest src -s "_" | polars unnest dst -s "_"
    | polars filter ((polars col dst_ip) | polars is-not-null)
    # dst_public doit être sélectionné pour être utilisable par with-column
    | polars select [utc_time task_name task_pid command_line src_ip dst_ip dst_port data_size data_entropy dst_public]
    | polars with-column (
        (polars when $c_public (polars lit "public_egress")
         | polars when $c_big (polars lit "large_data")
         | polars when $c_hi (polars lit "high_entropy")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE dns_query : recon / tunneling ---------------------------
# =====================================================================
def detect_dns_query [base] {
    let c_tld    = ((polars col query) | polars contains "(?:\\.tk$|\\.ml$|\\.ga$|\\.cf$|\\.gq$|\\.top$|\\.xyz$|\\.pw$|\\.onion$|\\.i2p$)")
    let c_long   = (((polars col query) | polars str-lengths) > 60)
    let c_nondns = ((((polars col dns_server_ip) | polars is-in (cfg dns_ips | polars into-df)) | polars expr-not))

    $base
    | ev 'dns_query'
    | polars filter (not_legit)
    | polars unnest exe -s "_" | polars unnest src -s "_" | polars unnest dns_server -s "_"
    | polars filter ((polars col query) | polars is-not-null)
    | polars select [utc_time task_name task_pid command_line query response dns_server_ip]
    | polars with-column (
        (polars when $c_long (polars lit "suspicious_length")
         | polars when $c_nondns (polars lit "non_standard_dns_server")
         | polars when $c_tld (polars lit "suspicious_tld")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE kill : perturbation / évasion ---------------------------
# =====================================================================
def detect_kill [base] {
    let c_target = ((polars col target_task_name) | polars contains "(?:docker|containerd|sshd|systemd|wazuh|crowdsec|splunk|check_mk|auditd|cron|agent)")
    let c_hard   = ((polars col signal) == 'SIGKILL')

    $base
    | ev 'kill'
    | polars unnest exe -s "_" | polars unnest target -s "_" | polars unnest target_exe -s "_" | polars unnest target_task -s "_"
    | polars filter ((polars col target_task_name) | polars is-not-null)
    | polars select [utc_time task_name task_pid command_line target_task_name target_task_pid signal]
    | polars with-column (
        (polars when $c_target (polars lit "kill_critical")
         | polars when $c_hard (polars lit "SIGKILL_hard")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE bpf_prog_load : rootkit / EDR bypass --------------------
# =====================================================================
def detect_bpf [base] {
    $base
    | ev 'bpf_prog_load'
    | polars filter (not_legit)
    | polars unnest exe -s "_" | polars unnest prog_type -s "_"
    | polars select [utc_time task_name task_pid command_line ksym prog_type_name tag]
    | polars with-column ((polars lit "bpf_by_non_system") | polars as evidence)
}

# =====================================================================
# ---- FAMILLE mmap_exec : injection / drop-and-run --------------------
# =====================================================================
def detect_mmap_exec [base] {
    let c_mapped = ((polars col mapped_path) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/|/proc/self/fd/|memfd:)")

    $base
    | ev 'mmap_exec'
    | polars unnest exe -s "_" | polars unnest mapped -s "_"
    | polars filter ((polars col mapped_path) | polars is-not-null)
    | polars select [utc_time task_name task_pid command_line mapped_path]
    | polars with-column (
        (polars when $c_mapped (polars lit "mmap_exec_suspicious")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE prctl : évasion (dumpable / seccomp) --------------------
# =====================================================================
def detect_prctl [base] {
    let c_dump = ((polars col option) == 'PR_SET_DUMPABLE')
    let c_sec  = ((polars col option) == 'PR_SET_SECCOMP')

    $base
    | ev 'prctl'
    | polars select [utc_time task_name task_pid command_line option arg2 arg3 arg4 arg5]
    | polars with-column (
        (polars when $c_sec (polars lit "seccomp_change")
         | polars when $c_dump (polars lit "dumpable_change")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- Affichage d'une famille : dataframe nu --------------------------
# =====================================================================
def show_family [base, family: string, num: int, explore: bool] {
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
        _ => { error make { msg: $"famille inconnue: ($family)" } }
    })
    let rows = ($l2 | polars collect | polars into-nu)
    let n = ($rows | length)
    print $"(ansi green)≡≡  Famille: ($family)   —  ($n) alertes(ansi reset)"
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
# ---- MAIN ------------------------------------------------------------
# =====================================================================
def main [
    ...files: string
    --infer-schema: int = 200000
    --num (-n): int = 20                 # nb de lignes affichées par famille
    --family (-f): string = "all"        # all ou une famille
    --explore (-x)                       # affichage dataframe interactif
] {
    # fichiers par défaut : les 2 .gz les plus récents du registry
    let registry_dir = "/run/media/pouchou/SSD2T/ips-ids-siem-pcaps/kunai/kunai_registry/kunai"
    let file_list = if (($files | is-empty)) {
        (ls $registry_dir
            | where name ends-with '.gz'
            | sort-by modified
            | last 2
            | get name
            | path expand)
    } else { $files }

    let nf = ($file_list | length)
    print $"(ansi cyan)🔍 Détection de compromission — ($nf) fichiers(ansi reset)"

    let families = if $family == 'all' {
        ['execve','file_create','connect','send_data','dns_query','kill','bpf_prog_load','mmap_exec','prctl']
    } else { [$family] }

    for file in $file_list {
        if not (($file | path exists)) {
            print $"(ansi yellow)⚠ fichier introuvable: ($file)(ansi reset)"
            continue
        }
        # intégrité du .gz (fichier en cours d'écriture / tronqué) — uniquement
        # pour les fichiers compressés, pas pour un .jsonl brut (cas de test).
        if ($file | path parse | get extension) == 'gz' {
            let res = (^gzip -t $file | complete)
            if $res.exit_code != 0 {
                print $"(ansi yellow)⚠ fichier tronqué/incomplet, ignoré: ($file)(ansi reset)"
                continue
            }
        }
        let base = (build_base $file $infer_schema)
        print $"(ansi cyan)┌─ Fichier: ($file) ─────────────────────────────┐(ansi reset)"
        for fam in $families {
            show_family $base $fam $num $explore
        }
    }
}
