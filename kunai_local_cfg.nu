#!/usr/bin/env nu
# kunai_local_cfg.nu
#
# CONFIG LOCALE PARAMÉTRABLE — middleware de sélection des allowlists.
#
# C'est LE fichier de données qui rend `kunai_rules_local.nu` paramétrable.
# Il contient :
#   1. un socle `default` GÉNÉRIQUE (commun à toutes les plateformes, strict) ;
#   2. des `function_profiles` PAR FONCTION (thématiques d'allowlist), combina­bles.
#
# Les profils ne sont PAS par machine : ils sont par FONCTION (thém­atique) —
#   dns / network / docker / elk / kube — et peuvent être activés indépendamment.
# Le profil `all` les prend TOUS et est le choix par défaut. On peut restreindre
# en passant une LISTE de fonctions, ex. --profile "dns,network" pour ne détecter
# qu'avec les allowlists DNS + réseau (sans docker/elk/kube).
#
# Le moteur (kunai_detect_compromise.nu) / l'interface (kunai_rules_local.nu)
# continuent d'appeler `local_cfg <nom_de_clé>`. La sélection des fonctions et la
# fusion avec le socle se font ici, de façon transparente pour l'appelant.
#
# ── SÉLECTION DES PROFILS (priorité décroissante) ────────────────────────
#   1. `local_cfg <clé> --profile "a,b"`   (liste CSV de fonctions / test)
#   2. `$env.KUNAI_PROFILE`                (posé par le moteur depuis --profile)
#   3. sinon → `all`                        (TOUTES les fonctions, par défaut)
# Une valeur 'all' (ou vide) = socle + toutes les fonctions, dans l'ordre
# canonique [dns, network, docker, elk, kube]. Une valeur a,b = socle + a + b.
#
# ── FUSION SOCLE + FONCTIONS ─────────────────────────────────────────────
# Chaque fonction ne touche QUE les clés de sa thém­atique (dns_ips / requêtes DNS
# pour `dns`, réseaux docker pour `docker`, chemins ELK pour `elk`…). Pour une clé
# partagée entre plusieurs fonctions, on utilise le marqueur '__append__' pour
# CONCAT (dédupliqué) dans l'ordre des fonctions actives ; sans marqueur, la liste
# REMPLACE le socle. Toutes les valeurs sont des LISTES (la plus petite est
# allowlist_egress_ports qui reste une liste d'INT : [443, 80, 53]).
#
# ── CONVENTION des préfixes IP ───────────────────────────────────────────
# `allowlist_public_networks` est comparé au PRÉFIXE EXACT de dst_ip (via
# starts_with_any du moteur). Or kunai rend selon le contexte :
#   * IP "nues"        :  34.120.127.130   (Internet, IPv4)
#   * IP IPv4-mappées  :  ::ffff:172.19.0.8  (réseaux docker/privés résolus en
#                         IPv6-mapping par la pile du conteneur)
# Donc pour couvrir un réseau privé vu en ::ffff:, le préfixe DOIT inclure le
# préfixe `::ffff:` (ex. '::ffff:172.16.'), sinon le match échoue silencieusement.
# C'est la principale cause du bruit du rapport tpot : dst_public=true est posé
# par kunai sans savoir que ::ffff:172.19.x est un réseau docker privé.
#
# ── AJOUTER UNE MACHINE ──────────────────────────────────────────────────
# Copier un bloc `"<hostname>": { ... }` sous `profiles`, y mettre SEULEMENT les
# clés qui changent par rapport au socle (les autres héritent de `default`).
# Utiliser '__append__' pour étendre une allowlist au lieu de la réécrire.

# Socle générique — commun à toutes les machines analysées. STRICT : il ne
# contient QUE ce qui est vrai sur toutes les plateformes (dépôts/CDN/miroirs
# publics, agents et utilitaires d'usage universel). Tout le reste (réseaux
# docker, IP de labo, services ELK, résolveur DNS embarqué…) est du CONTEXTE
# LOCAL => à placer dans un `profile` dédié, jamais ici.
def default_defs [] {
    {
        # vrais agents / services de la plateforme à retirer du bruit définitivement.
        # On N'y met PAS les outils détournables (docker, chmod, chown, tar, cp...),
        # qui ne doivent être bénins QUE quand ils proviennent d'une chaîne légitime
        # (voir benign_utilities + not_legit dans le moteur).
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
        # utilitaires banals / commandes système standard. Bénins UNIQUEMENT quand ils
        # proviennent d'une chaîne légitime (voir not_legit).
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
        # résolveurs DNS LÉGITIMES : le bruit `non_standard_dns_server` diminue si
        # le serveur DNS interrogé est ici. (le résolveur Docker embarqué 127.0.0.11
        # et les IP de labo sont du contexte local => profile)
        dns_ips: ['10.6.255.106'],
        # ALLOWLIST réseau EGRESS : destinations PUBLIQUES réputées connues/bénignes.
        # Une connexion sortante vers l'une de ces IP (préfixe) N'est PAS un
        # "public_egress" suspect : dépôts/CDN/miroirs légitimes du serveur.
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
        # Ports de service vers lesquels un "public_egress"/"send_data" N'est PAS
        # suspect quand la destination est déjà allowlistée (443 https, 80 http, 53 dns).
        allowlist_egress_ports: [443, 80, 53],
        # Processus (task_name) dont l'activité réseau sortante est légitime.
        # NB : ne filtre QUE par task_name exact. Pour du trafic dont le task_name
        # est instable ou trop générique (threads ELK "elastic..][T#3]",
        # "[http_output]>w", node/libuv-worker"), il faut corréler le CHEMIN de la
        # command_line => utiliser `allowlist_egress_paths` (contexte local).
        allowlist_egress_procs: ['rustup','rustup-init','cargo','git','git-remote-http',
                                 'git-remote-https','docker','docker-buildx','dockerd',
                                 'containerd','apt','apt-get','dpkg','wget','curl',
                                 'ssl_client','wazuh-agentd',
                                 'https'],   # transport TLS d'apt (/usr/lib/apt/methods/https)
        # NOUVEAU — chemins de la command_line (match sous-chaîne via contains_any)
        # dont l'activité réseau sortante est légitime. Permet d'allowlister des
        # binaires à task_name instable (threads de services, runtimes) par leur VRAI
        # chemin sur disque. Vide par défaut (comportement inchangé) => à remplir
        # dans les profils (ex. /usr/share/elasticsearch, /usr/share/kibana, logstash).
        allowlist_egress_paths: [],
        # NOUVEAU — requêtes DNS bénignes (query, match sous-chaîne) à ne pas signaler
        # comme `non_standard_dns_server`/`suspicious_*`. Vide par défaut => à remplir
        # dans les profils (ex. epr.elastic.co, infra-cdn.elastic.co, nom de service
        # K8s/docker "elasticsearch" résolu par libuv).
        allowlist_dns_queries: [],
        # chaîne build Rust (client lourd ~/.cargo, ~/.rustup) : exécution depuis
        # /tmp/cargo-*, /tmp/rustc* ou ~/.cargo est BÉNIGNE.
        allowlist_build_procs: ['rustup','rustup-init','cargo','rustc','rustdoc',
                                'cc','cc1','cc1plus','cc1obj','cc1objplus','as','as1',
                                'ar','ranlib','llvm-ar','llvm-ranlib','nm',
                                'collect2','ld.lld','rust-lld','cargo-git-checkout',
                                'clippy-driver','cargo-clippy','cargo-build',
                                'CloseHandle'],
        # chemins de compilation / cache légitimes de la chaîne build.
        allowlist_build_paths: [
            '/tmp/cargo-', '/tmp/rustc', '/tmp/tmp.', '/tmp/cargo-install',
            '/tmp/cc', '/tmp/as',
            '/home/nushell/.cargo/', '/home/nushell/.rustup/',
            '/root/.cargo/', '/root/.rustup/',
        ],
        # signaux BÉNINS de gestion de processus / runtime (jamais un kill suspect).
        benign_signals: ['SIGURG','SIGCHLD','SIGCONT','SIGWINCH','SIGIO','SIGPIPE'],
        # chaîne build INITRAMFS (mkinitramfs/dracut/update-initramfs) : staging
        # temporaire /var/tmp/mkinitramfs_* / /usr/lib/dracut réservé à mkinitramfs.
        allowlist_initramfs_paths: ['/var/tmp/mkinitramfs','/usr/lib/dracut'],
    }
}

# Profils PAR FONCTION (thématiques d'allowlist), combina­bles et INDÉPENDANTS.
# Chaque fonction ne touche QUE les clés de sa thém­atique. Une clé partagée entre
# plusieurs fonctions utilise '__append__' en tête = concat (dédupliqué) dans
# l'ordre des fonctions actives ; sans marqueur, la liste remplace le socle.
#
# Le profil `all` (DÉFAUT) = socle + TOUTES les fonctions ci-dessous, dans l'ordre
# canonique : dns, network, docker, elk, kube. Pour n'activer que certaines
# fonctions : --profile "dns,network" (le socle reste toujours appliqué).
#
# ── Ajouter une fonction ──────────────────────────────────────────────────
# Ajouter un bloc `"<nom>": { ... }` ici puis l'insérer dans l'ordre canonique
# `fn_order` (voir plus bas). Mettre expressément '__append__' pour étendre une
# clé déjà fournie par une autre fonction, sinon elle est remplacée.
def function_profiles [] {
    {
        # ── dns : résolveurs DNS et requêtes DNS légitimes ──────────────────
        # Neutralise `non_standard_dns_server` quand le serveur interrogé est le
        # résolveur Docker embarqué (127.0.0.11) ou le résolveur local (127.0.0.1),
        # et neutralise les signaux DNS des requêtes internes (services container,
        # epr.elastic.co / infra-cdn.elastic.co de la stack ELK).
        dns: {
            dns_ips: ['__append__', '127.0.0.11', '127.0.0.1'],
            allowlist_dns_queries: ['__append__',
                                    'epr.elastic.co', 'infra-cdn.elastic.co',
                                    'elasticsearch'],
        },
        # ── network : egress réseau légitime universel ──────────────────────
        # Ports de service et transport TLS d'apt (déjà au socle, rappel explicite
        # pour qui désactive le socle implicite — redondant mais inoffensif).
        network: {
            allowlist_egress_ports: [443, 80, 53],
            allowlist_egress_procs: ['https'],
        },
        # ── docker : réseaux docker/hôte internes vus en ::ffff: ────────────
        # Réseaux docker ELK (172.16-31./12), labo (10.6.), loopback local mappé
        # (127.0.0.), OVH hôte tpot (51.89.) — trafic INTERNE, pas un public_egress.
        # Inclut le résolveur Docker 127.0.0.11 (c.f. fonction dns aussi).
        docker: {
            allowlist_public_networks: ['__append__',
                                        '::ffff:172.16.', '::ffff:172.17.', '::ffff:172.18.',
                                        '::ffff:172.19.', '::ffff:172.20.', '::ffff:172.21.',
                                        '::ffff:172.22.', '::ffff:172.23.', '::ffff:172.24.',
                                        '::ffff:172.25.', '::ffff:172.26.', '::ffff:172.27.',
                                        '::ffff:172.28.', '::ffff:172.29.', '::ffff:172.30.',
                                        '::ffff:172.31.',
                                        '::ffff:10.6.', '::ffff:127.0.0.',
                                        '::ffff:51.89.'],   # OVH hôte tpot
            dns_ips: ['__append__', '127.0.0.11'],
        },
        # ── elk : stack Elasticsearch/Logstash/Kibana ───────────────────────
        # Chemins des binaires ELK (task_name instable : threads Elasticsearch
        # "elastic..][T#3]", Logstash "[http_output]>w", node/libuv-worker pour
        # Kibana) corrélés sur la command_line, + CDN d'infra Elastic (34.120.).
        elk: {
            allowlist_egress_paths: ['/usr/share/elasticsearch/',
                                     '/usr/share/logstash/',
                                     '/usr/share/kibana/'],
            allowlist_public_networks: ['__append__', '34.120.'],   # infra-cdn.elastic.co
            allowlist_dns_queries: ['__append__',
                                    'epr.elastic.co', 'infra-cdn.elastic.co',
                                    'elasticsearch'],
        },
        # ── kube : (réservé) réseaux/services Kubernetes ────────────────────
        # Ex. 10.96.0.0/12 (ClusterIP), 10.244.0.0/16 (CNI Flannel), noms de service
        # '<svc>.<ns>.svc.*'. Vide par défaut ; à remplir selon la plateforme.
        kube: {
        },
    }
}

# Ordre canonique des fonctions pour le profil `all` (et l'affichage).
def fn_order [] { ['dns', 'network', 'docker', 'elk', 'kube'] }

# =====================================================================
# Logique de fusion et de sélection — NE PAS MODIFIER
# =====================================================================

# concat $base ∘ $extra si l'extra commence par '__append__', sinon remplace.
def merge_lists [base: list<any>, extra: list<any>] {
    if (not ($extra | is-empty)) and (($extra | first) == '__append__') {
        ($base | append ($extra | skip 1)) | uniq
    } else {
        $extra
    }
}

# charge la config résolue pour les FONCTIONS actives (socle + fonctions fusionnés).
# Retourne une record plate contenant TOUTES les clés attendues par le moteur.
# Usage interne : `kunai_rules_local.nu` l'appelle puis fait `.<nom_de_clé>`.
export def load_cfg [--profile: string] {
    let base = (default_defs)
    let fns  = (current_profile --profile=$profile)

    # On part du socle, puis on CONCATÈNE chaque fonction active dans l'ordre.
    # merge_lists applique la sémantique '__append__' (concat dedup) sinon remplace.
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

# Liste des FONCTIONS actives (liste de noms), pour affichage / debug.
# Priorité : --profile explicite > $env.KUNAI_PROFILE (posé par le moteur) > `all`.
# 'all' (ou valeur vide / inconnue) = toutes les fonctions dans l'ordre canonique.
# Une valeur "a,b" = socle + a + b (dans l'ordre canonique filtré).
export def current_profile [--profile: string] {
    let env_prof = ($env | get --optional KUNAI_PROFILE | default '')
    let sel = (if ($profile | is-not-empty) { $profile } else if (($env_prof | str trim) != '') { $env_prof } else { 'all' })
    let order = (fn_order)
    if (($sel | str trim) == 'all') {
        $order
    } else {
        # filtre l'ordre canonique sur la sélection CSV (déduplique et ordonne).
        ($sel | split row ',' | each {|s| $s | str trim } | where {|s| $s != '' })
        | reduce -f [] {|s, acc| if ($order | any {|o| $o == $s }) { $acc | append $s } else { $acc } }
    }
}
