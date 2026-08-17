#!/usr/bin/env nu
# kunai_local_cfg.nu
#
# CONFIG LOCALE PARAMÉTRABLE — contexte machine "registry" / plateforme.
#
# C'est LE fichier de données qui rend `kunai_rules_local.nu` paramétrable.
# Il contient :
#   1. un socle `default` GÉNÉRIQUE (commun à toutes les machines, strict) ;
#   2. des `profiles` PAR MACHINE / PAR REGISTRE, identifiés par le hostname de
#      l'hôte analysé OU par un `--profile` explicite passé à la ligne de commande.
#
# Le moteur (kunai_detect_compromise.nu) / l'interface (kunai_rules_local.nu)
# continuent d'appeler `local_cfg <nom_de_clé>`. La sélection du profil et la
# fusion avec le socle se font ici, de façon transparente pour l'appelant.
#
# ── SÉLECTION DU PROFIL (priorité décroissante) ──────────────────────────
#   1. `local_cfg <clé> --profile <nom>`   (appel direct / test)
#   2. `$env.KUNAI_PROFILE`                (posé par le moteur depuis --profile)
#   3. `hostname` de la machine analysée   (auto-détection)
#   4. sinon → `default` (socle générique)
#
# ── FUSION SOCLE + PROFIL ────────────────────────────────────────────────
# Chaque clé du profil peut :
#   * être ABSENTE  → hérite telle quelle de `default` ;
#   * fournir une LISTE SANS marqueur → REMPLACE la liste de `default` ;
#   * fournir ['__append__', ...]     → CONCATE `default` ∘ profil (dédupliqué).
# Toutes les valeurs sont des LISTES (la plus petite est allowlist_egress_ports
# qui reste une liste d'INT : [443, 80, 53]).
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

# Profils PAR MACHINE / PAR REGISTRE. Chaque clé absente hérite du socle ;
# '__append__' en tête d'une liste = concat avec le socle (dédupliqué).
def profile_defs [] {
    {
        # ────────────────────────────────────────────────────────────────
        # Machine tpot / ELK (analyse anti-FP du rapport
        #   scanresult20260817_194806/kunai_tp_connect386_send_data24311_dns_query28.md).
        # Bruit constaté sur cette machine :
        #   * connect/send_data 386 + 24311 : trafic INTERNE de la stack ELK
        #       (Elasticsearch <-> Logstash <-> Kibana) sur le réseau docker
        #       ::ffff:172.19.0.x, vu par kunai comme "public" (dst_public=true
        #       car IPv4-mappée ::ffff: mal résolue RFC1918) ;
        #   * dns_query 28 : Kibana(node/libuv-worker) qui résout ses services
        #       (elasticsearch, epr.elastic.co, infra-cdn.elastic.co) via le
        #       résolveur Docker embarqué 127.0.0.11 => non_standard_dns_server.
        # Ce profil permet de ZÉRO-FP sur cette machine sans toucher au socle.
        # ── Attention (2 points) ────────────────────────────────────────
        # 1) Le réseau docker entier est ici considéré interne/légitime pour la
        #    stack ELK. Ce choix est LOCAL à la machine honeypot ; sur une autre
        #    machine, ne PAS copier ce bloc.
        # 2) 'elasticsearch' en requête DNS matcherait un domaine contenant ce
        #    mot ; ici le service interne K8s/docker domine largement le bruit.
        "elastic": {
            # Réseaux docker ELK vus en ::ffff: (IPv4-mappée) + labo + loopback mappé.
            # Table déduite du Bilan du rapport : 172.19.0.x (ELK), 10.6.255.x (labo),
            # 127.0.0.x (loopback local), 51.89.x (OVH hôte tpot), 34.120.x (elastic CDN).
            allowlist_public_networks: ['__append__',
                                        '::ffff:172.16.', '::ffff:172.17.', '::ffff:172.18.',
                                        '::ffff:172.19.', '::ffff:172.20.', '::ffff:172.21.',
                                        '::ffff:172.22.', '::ffff:172.23.', '::ffff:172.24.',
                                        '::ffff:172.25.', '::ffff:172.26.', '::ffff:172.27.',
                                        '::ffff:172.28.', '::ffff:172.29.', '::ffff:172.30.',
                                        '::ffff:172.31.',
                                        '::ffff:10.6.', '::ffff:127.0.0.',
                                        '::ffff:51.89.',                      # OVH hôte tpot
                                        '34.120.'],                          # infra-cdn.elastic.co
            # Chemins des binaires de la stack ELK : le task_name y est instable
            # (threads Elasticsearch "elastic..][T#3]", Logstash "[http_output]>w",
            # node/libuv-worker pour Kibana). On corrèle donc le CHEMIN command_line.
            allowlist_egress_paths: ['/usr/share/elasticsearch/',
                                     '/usr/share/logstash/',
                                     '/usr/share/kibana/'],
            # Résolveur DNS Docker embarqué (127.0.0.11) + résolveur local loopback :
            # Kibana/les conteneurs y résolvent leurs services -> plus de FP
            # `non_standard_dns_server` quand le serveur DNS est celui-ci.
            dns_ips: ['__append__', '127.0.0.11', '127.0.0.1'],
            # Requêtes DNS internes de la stack ELK (Kibana -> services container).
            allowlist_dns_queries: ['epr.elastic.co', 'infra-cdn.elastic.co',
                                    'elasticsearch'],
        },
        # (exemple : la machine "registry" de développement est le socle strict,
        #  aucun profil nécessaire — le fichier précédent kunai_rules_local.nu
        #  constitue exactement le `default` ci-dessus. Ajouter ici un profil si
        #  cette machine a des spécificités supplémentaires.)
    }
}

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

# Charge la config résolue pour le profil sélectionné (socle + profil fusionnés).
# Retourne une record plate contenant TOUTES les clés attendues par le moteur.
# Usage interne : `kunai_rules_local.nu` l'appelle puis fait `.<nom_de_clé>`.
export def load_cfg [--profile: string] {
    let host = (current_profile --profile=$profile)
    let cfg = { default: (default_defs), profiles: (profile_defs) }
    let prof = ($cfg.profiles | get --optional $host | default {})

    # clés = union(keys default, keys profil) — on garde l'ordre du default d'abord.
    let keys = (($cfg.default | columns) ++ ($prof | columns) | uniq)
    $keys | reduce -f {} {|k, acc|
        let base_v = ($cfg.default | get --optional $k | default [])
        let prof_v = ($prof | get --optional $k)
        let v = (if ($prof_v == null) { $base_v } else { (merge_lists $base_v $prof_v) })
        $acc | upsert $k $v
    }
}

# Nom du profil résolu (pour affichage / debug).
# Priorité : --profile explicite > $env.KUNAI_PROFILE (posé par le moteur) > hostname.
export def current_profile [--profile: string] {
    let env_prof = ($env | get --optional KUNAI_PROFILE | default '')
    if ($profile | is-not-empty) {
        $profile
    } else if (($env_prof | str trim) != '') {
        $env_prof
    } else {
        (hostname | str trim)
    }
}
