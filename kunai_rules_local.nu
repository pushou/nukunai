#!/usr/bin/env nu
# kunai_rules_local.nu
#
# RULES LOCALES — contexte machine "registry" (spécifique à CET hôte / à CETTE
# plateforme, PAS réutilisable ailleurs). Contrairement aux règles génériques
# migrées dans le pot commun `kunai_rules/`, ces listes décrivent les agents,
# services, réseaux et chemins LÉGITIMES de la machine, nécessaires pour filtrer
# le bruit sans masquer un compromission. Elles doivent être ajustées par machine.
#
# Usage (importé par kunai_detect_compromise.nu) :
#   use kunai_rules_local.nu local_cfg
#   local_cfg legit_agents

# Retourne la configuration locale de la machine (allowlists / contexte).
# Les valeurs alimentent le moteur d'analyse via des appels typed.
export def local_cfg [name: string] {
    match $name {
        # vrais agents / services de la plateforme à retirer du bruit définitivement.
        # On N'y met PAS les outils détournables (docker, chmod, chown, tar, cp, bash...),
        # qui ne doivent être bénins QUE quand ils proviennent d'une chaîne de processus
        # légitime (voir local_benign_utilities + not_legit dans le moteur).
        "legit_agents" => ['dpkg','dpkg-deb','dpkg-db-backup','dpkg-trigger','apt','apt-get','apt-cache','apt-key',
                           'gitlab-runner','gitlab-workhorse','gitlab-shell','git','dockerd',
                           'containerd','containerd-shim','containerd-shim-runc-v2','runc',
                           'runc:[0:PARENT]','runc:[1:CHILD]','runc:[2:INIT]','docker-init','6',
                           # "6" = comm du processus runc init (runc:[...]) tronqué à un chiffre ;
                           # n'apparaît que derrière runc dans la chaîne containerd (cf. command_line
                           # "runc init"), jamais hors conteneur.
                           'systemd','systemctl','journalctl',
                           'wazuh-agent','wazuh-check-mk','wazuh-agentd','wazuh-modulesd','wazuh-syscheckd',
                           'wazuh-logcollec','wazuh-analysisd','wazuh-execd','wazuh-db','wazuh-authd',
                           'crowdsec','cscli','splunk','splunkforwarder',
                           'check_mk_agent','cmk-agent-ctl','sqv','nginx','nginx-worker','sshd','sshd-auth',
                           'cron','crond','dbus-daemon','polkitd','NetworkManager','unattended-upgrade',
                           'chronyd','chrony','systemd-timesyn','systemd-timesyncd',
                           'auditd','auditctl','auditd-manager','augenrules','aureport','ausearch']
        # utilitaires banals / commandes système standard. Bénins UNIQUEMENT quand ils
        # proviennent d'une chaîne légitime (voir not_legit) : un docker/chmod/curl/perl/
        # python3 lancé par un attaquant (ou un task inconnu sous systemd) reste détecté.
        "benign_utilities" => ['docker','grep','sed','cat','tar','gzip','bzip2','xz','gpg','gpgv','gpgconf',
                               'sh','bash','curl','wget','chmod','chown','install','cp','mv','rm',
                               'cmp','mktemp','rmdir','touch','mkdir','ln','readlink','basename',
                               'dirname','date','sleep','printf','echo','nproc','lsblk','df','free',
                               'pgrep','ps','ss','ip','hostname','uname',
                               'stat','awk','python3','python','perl','timedatectl','tr','cut',
                               'postconf','find','wc','du','ls','locale','paste','head','sort',
                               'timeout','dmsetup','nohup','ipmitool','netstat','last','iptables',
                               'ip6tables','nft','findmnt','sysctl','unix_chkpwd','crontab','login',
                               'systemd-timesyn','sshd-session','sftp-server','su','gpg-agent','loginctl',
                               'udevadm','vim','view','sed','nano','fuser','sudo']
        # IP locales / plateforme (bruit réseau légitime)
        "dns_ips" => ['10.6.255.106']
        # ALLOWLIST réseau : destinations PUBLIQUES réputées connues/bénignes.
        # Une connexion sortante vers l'une de ces IP (préfixe /24, /16 …) N'est
        # PAS un "public_egress" suspect : ces hôtes sont les dépôts/CDN/miroirs
        # légitimes du serveur (téléchargement de paquets, git, docker hub…).
        "allowlist_public_networks" => [
            '151.101.', '151.101',            # crates.io / static.rust-lang.org (Fastly)
            '140.82.', '185.199.',            # GitHub
            '172.65.', '104.18.', '172.64.',  # GitLab (Cloudflare)
            '104.16.', '104.17.', '104.19.',  # Cloudflare public
            '2a04:4e42',                      # Cloudflare IPv6 (crates.io egress)
            'registry.iutbeziers.fr',         # Docker Hub / docker.io registry mirror
            '52.85.', '52.84.', '52.222.',   # aws CloudFront (apt Debian deb.debian.org)
            '3.174.', '18.161.', '18.160.',  # aws global accelerator (apt Debian)
            '13.248.', '13.224.', '13.35.',  # aws CloudFront (apt Debian)
            '99.86.', '3.162.',              # aws CloudFront (apt Debian mirrors: 99.86.0.0/16 AMAZO-CF, 3.160.0.0/14 cloudfront)
            '143.204.', '99.84.', '99.83.',  # aws CloudFront (apt Debian)
            '76.223.', '15.197.', '3.33.',   # aws Global Accelerator/CloudFront (apt Debian)
            '172.67.', '104.21.',            # Cloudflare (apt Debian deb.debian.org)
            '10.6.',                          # NTP/paquets/miroirs locaux
        ]
        # Ports de service vers lesquels un "public_egress" N'est PAS suspect quand
        # la destination est déjà allowlistée (443 https, 80 http, 53 dns). Les ports
        # d'exfiltration classiques restent signalés.
        "allowlist_egress_ports" => [443, 80, 53]
        # Processus dont l'activité réseau sortante vers l'extérieur est légitime
        # (téléchargement de dépendances / dépôts) : rustup, cargo, git, apt, etc.
        "allowlist_egress_procs" => ['rustup','rustup-init','cargo','git','git-remote-http',
                                     'git-remote-https','docker','docker-buildx','dockerd',
                                     'containerd','apt','apt-get','dpkg','wget','curl',
                                     'ssl_client','wazuh-agentd',
                                     # 'https' = transport TLS d'apt (/usr/lib/apt/methods/https),
                                     # process worker qu'apt lance pour télécharger les paquets.
                                     'https']
        # Utilitaires / tâches de la chaîne build Rust (client lourd ~/.cargo, ~/.rustup)
        # dont l'exécution depuis /tmp/cargo-*, /tmp/rustc* ou ~/.cargo est BÉNIGNE.
        "allowlist_build_procs" => ['rustup','rustup-init','cargo','rustc','rustdoc',
                                    'cc','cc1','cc1plus','cc1obj','cc1objplus','as','as1',
                                    'ar','ranlib','llvm-ar','llvm-ranlib','nm',
                                    'collect2','ld.lld','rust-lld','cargo-git-checkout',
                                    'clippy-driver','cargo-clippy','cargo-build',
                                    'CloseHandle']
        # Chemins de compilation / cache légitimes : l'exécution (mmap/exec) ou la
        # création de fichier dans ces préfixes par un process de la chaîne build
        # ci-dessus n'est PAS suspecte.
        "allowlist_build_paths" => [
            '/tmp/cargo-', '/tmp/rustc', '/tmp/tmp.', '/tmp/cargo-install',
            '/tmp/cc', '/tmp/as',
            '/home/nushell/.cargo/', '/home/nushell/.rustup/',
            '/root/.cargo/', '/root/.rustup/',
        ]
        # Signaux BÉNINS de gestion de processus / runtime (ne signalent jamais une
        # compromission) : SIGURG = préemption goroutine Go des dockerd/containerd
        # (alimentait 3151 faux positifs), SIGCHLD = fin de fils, etc.
        "benign_signals" => ['SIGURG','SIGCHLD','SIGCONT','SIGWINCH','SIGIO','SIGPIPE']
        # CHAÎNE BUILD INITRAMFS (mkinitramfs / dracut / update-initramfs / installkernel)
        # déclenchée par une mise à jour noyau ou un install de module DKMS. Ces outils
        # ré-agencent les utilitaires dans un répertoire de staging temporaire
        # /var/tmp/mkinitramfs_* puis y exécutent toute une série de tâches (cp, ln,
        # mkdir, find, depmod, kmod) : l'exec_from_tmp résultant est bénin (même logique
        # que allowlist_build_paths /tmp/cargo-*). La liste porte UNIQUEMENT sur les
        # chemins de staging (réservés à mkinitramfs, jamais de la persistance), pas sur
        # les procs génériques (cp/ln/mkdir/find) qui resteraient alors détectés en cas
        # d'usage offensif depuis /tmp.
        "allowlist_initramfs_paths" => ['/var/tmp/mkinitramfs','/usr/lib/dracut']
        _ => { error make { msg: $"règle locale inconnue: ($name)" } }
    }
}
