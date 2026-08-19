#!/usr/bin/env nu
# kunai_queries.nu — requêtes ad-hoc structurées sur les logs kunai (EDR événementiel).
#
# Centralise et structure (en sous-commandes avec options) les oneliners dispersés
# de `kunai_requests.nu`, sur le modèle des exemples/scripts officiels kunai
# (filter_connect/exec/kill/send, count_event_types, view_network_events,
# view_command_lines). Chaque sous-commande est une requête = filtrage sur le nom
# d'événement + agrégation (count / top-N) + tri.
#
# Usage :
#   nu kunai_queries.nu <requête> <file> [--top N] [--filter RE] [--all] [--infer-schema N]
#   nu kunai_queries.nu help
#   nu kunai_queries.nu <requête> --help   (help détaillé de la sous-commande)
#
# `<file>` peut être un `.parquet` (déjà aplati) ou un `.gz`/`.jsonl`/`.log`
# (ndjson kunai, décompressé nativement par polars).
#
# Requêtes :
#   events           count d'événements par nom            (count_event_types)
#   dns              requêtes DNS groupées par query       (requête dns)
#   command-lines    command_line execve les plus fréquentes (view_command_lines)
#   exes             premier mot des command_line execve    (palette d'exe)
#   connect-ips      top dst_ip des connexions             (filter_connect)
#   connect-ports    top dst_port des connexions
#   network          vue réseau : command_line + dst (flatten)
#   dst-ports        ports + ancestors uniques groupés par IP de destination (dst nested ou aplati)
#   file-extensions  extensions des chemins créés          (filter_write)
#   file-creates     fichiers créés : chemin + binaire écrivain (--skip-benign pour masquer le bruit système)
#   kill-targets     cibles tuées (kill)                   (filter_kill)
#   prctl-options    options prctl par processus
#   file-renames     renommages old->new (file_rename)
#   file-unlinks     fichiers supprimés (file_unlink)
#   mmap-execs       fichiers mappés RX, drop-and-run (--mprotect -s)
#   bpf-progs        programmes eBPF chargés (bpf_prog_load)
#   send-ports       top ports du send_data                (filter_send)
#   send-ips         top IPs du send_data
#
# NB conventions & pièges nushell/polars documentés dans MEMORIES.md :
#  - les Expr polars `or`/`and` s'écrivent entre parenthèses, jamais `polars or`;
#  - les colonnes Int128 (error_code…) ne sont JAMAIS sélectionnées avant un
#    `collect`/`into-nu` : on ne garde que des colonnes sûres (helper cols_keep);
#  - les formats kunai varient : helpers `unnestif` / `cols_keep` (voir le moteur
#    kunai_detect_compromise.nu) pour les colonnes conditionnellement présentes.

# ---------------------------------------------------------------------------
# Conversion gz/jsonl -> parquet en cache (même logique que le moteur).
# Un .gz/.jsonl est d'abord converti en .parquet (à côté de la source), puis
# lu sur le parquet : bien plus rapide que parser l'ndjson à chaque exécution.
# Réutilise le cache s'il est plus récent que la source. Retourne le chemin.
def ensure_parquet [file: string, infer_schema: int] {
    # déjà du parquet : rien à convertir.
    if ($file | path parse | get extension) == 'parquet' { return $file }

    # Chemin cible convergent (même règle que le moteur / kunai_to_parquet.nu) :
    #   test.jsonl    -> test.jsonl.parquet
    #   test.jsonl.gz -> test.jsonl.parquet   (le `.gz` seul est retiré)
    let ext = ($file | path parse | get extension)
    let target = if $ext == 'gz' {
        let no_gz = ($file | str replace -r '\.gz$' '')
        $"($no_gz).parquet"
    } else {
        $"($file).parquet"
    }

    # Réutilise le cache s'il existe, est sain (footer PAR1) et plus récent
    # que la source. Un parquet tronqué/corrompu (footer absent) est supprimé
    # et régénéré : il arrive qu'une conversion laisse un fichier incomplet.
    if ($target | path exists) {
        let sane = ((^tail -c 4 $target) == 'PAR1')
        if $sane {
            let src_m = ((ls $file).0.modified?)
            let dst_m = ((ls $target).0.modified?)
            if ($dst_m | is-not-empty) and ($src_m | is-not-empty) and ($dst_m >= $src_m) {
                return $target
            }
        }
        # parquet absent côté existence non levée, corrompu, ou obsolète : on le régénère.
        rm -f $target
    }

    print $"(ansi yellow)⚙ conversion (($file)) → (($target))(ansi reset)"
    let script = ($env.FILE_PWD | path join "kunai_to_parquet.nu")
    let res = (^nu $script $file --output $target --infer-schema $infer_schema | complete)
    if $res.exit_code != 0 {
        error make { msg: $"échec de conversion de ($file) : ($res.stderr)" }
    }
    $target
}

# Ouvre la source en lazy frame APLATI et SANS collision de colonnes :
#  - .parquet sous-entendu après conversion éventuelle de .gz / .jsonl / .log,
#  - `event` est renommé `event_info` AVANT l'unnest pour ne pas entrer en
#    collision avec `data.name` / `data.id` (certaines versions kunai ont un
#    `name` à la racine et dans `event` -> une seule colonne `name` doit rester).
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

# Retourne la colonne de nom d'événement à utiliser (unique) :
# `event_info_name` (de l'event renommé) si présente, sinon `name` (data).
def event_name_col [base] {
    let cols = ($base | polars schema | columns)
    if 'event_info_name' in $cols { 'event_info_name' } else if 'name' in $cols { 'name' } else { '' }
}

# unnest conditionnel : ne déroule la colonne que si elle existe (formats kunai variables).
def unnestif [base, col: string] {
    let cols = ($base | polars schema | columns)
    if $col in $cols { $base | polars unnest $col -s "_" } else { $base }
}

# select parmi une liste en ne gardant que les colonnes réellement présentes (évite
# de référencer une colonne Int128 / absente avant collect). Retourne un lazy frame.
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

# attend True/False sur stdin et print un `✗ aucune donnée` sinon (évite une table vide muette).
def maybe_note [s: bool] { if $s { } else { print $"(ansi yellow)✗ aucune ligne ne matche ce filtre sur ce fichier(ansi reset)" } }

# valide la présence et l'existence du fichier source ; print l'erreur sinon. Retourne un bool.
def require_file [file: string] {
    if ($file | is-empty) {
        print $"(ansi red)✗ fichier manquant : fournir un chemin kunai .parquet / .gz / .jsonl(ansi reset)"
        false
    } else if not ($file | path exists) {
        print $"(ansi red)✗ fichier introuvable : ($file)(ansi reset)"
        false
    } else { true }
}

# base commune : frame aplati (data/info/event_info) filtré sur le nom d'événement.
# Retourne le lazy frame filtré (encore non collecté).
def events_frame [file: string, infer_schema: int, evname: string] {
    let base = (open_source $file $infer_schema)
    let ncol = (event_name_col $base)
    if $ncol != '' {
        $base | polars filter ((polars col $ncol) == $evname)
    } else {
        # colonne du nom absente (rare) : rien à filtrer, on renvoie un frame vide sûr.
        $base | polars filter (polars lit false)
    }
}

# Trie une frame value-counts (count) par count décroissant puis par valeur.
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

# Vrai si une IP (texte) est réellement publique : écarte les plages RFC1918
# (10/8, 172.16/12, 192.168/16), loopback (127.), link-local (169.254.),
# multicast/broadcast et les IPv4-mappées des réseaux Docker internes
# (::ffff:10. / ::ffff:192.168. / ::ffff:172.16..31.).
# Le texte suffit ici (dst_public est peu fiable en IPv4-mappée).
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

# ------------------------------------------------ requêtes (sous-commandes)

# Lien type d'événement kunai -> sous-requête de traitement (affiché par events).
const EVENT_QUERY = {
    execve:        'command-lines / exes'
    execve_script: 'command-lines / exes'
    connect:       'connect-ips / connect-ports / network / dst-ports'
    send_data:     'send-ips / send-ports'
    dns_query:     'dns'
    file_create:   'file-extensions / file-creates'
    kill:          'kill-targets'
    prctl:         'prctl-options'
    file_rename:   'file-renames'
    file_unlink:   'file-unlinks'
    mmap_exec:     'mmap-execs (--mprotect)'
    mprotect_exec: 'mmap-execs (--mprotect)'
    bpf_prog_load: 'bpf-progs'
}

# Compte d'événements par nom (count_event_types).
def "main events" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (open_source $file $infer_schema)
    let ncol = (event_name_col $base)
    if $ncol == '' {
        print $"(ansi yellow)✗ colonne de nom d'événement absente(ansi reset)"
        return
    }
    # unifie la colonne sous `name` (event_info_name du parquet converti, ou name).
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
        print $"(ansi yellow)✗ aucun événement dans ce fichier(ansi reset)"
    } else {
        # Annexe le lien type d'événement -> sous-requête de traitement (visible).
        $rows | each {|r| $r | merge {requête: ($EVENT_QUERY | get -o $r.name | default '—')} }
    }
}

# Requêtes DNS groupées par requête (événement dns_query), top-N ou toutes.
def "main dns" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'dns_query')
    let cols = ($base | polars schema | columns)
    if 'query' not-in $cols {
        print $"(ansi yellow)✗ colonne 'query' absente pour dns_query sur ce format(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucune requête DNS dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# command_line des execve les plus fréquentes (view_command_lines).
def "main command-lines" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'execve')
    let cols = ($base | polars schema | columns)
    if 'command_line' not-in $cols {
        print $"(ansi yellow)✗ colonne 'command_line' absente pour execve(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun execve dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Première colonne de chaque command_line execve (palette d'exécutables invoqués).
def "main exes" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'execve')
    let cols = ($base | polars schema | columns)
    if 'command_line' not-in $cols {
        print $"(ansi yellow)✗ colonne 'command_line' absente pour execve(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun execve dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Top dst_ip des connexions (filter_connect).
def "main connect-ips" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'connect')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    if 'dst_ip' not-in $cols {
        print $"(ansi yellow)✗ aucune colonne dst_ip pour connect sur ce format(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select dst_ip
        | polars drop-nulls
        | polars get dst_ip
        | polars value-counts
        | polars sort-by [count dst_ip] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucune connexion dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Top dst_port des connexions.
def "main connect-ports" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'connect')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    if 'dst_port' not-in $cols {
        print $"(ansi yellow)✗ aucune colonne dst_port pour connect sur ce format(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucune connexion dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Ports par IP de destination : groupes les adresses dst et agrège leurs ports uniques.
# Accepte un parquet avec `dst` NESTÉ (format source non aplati, ex. eventsreg.log.parquet)
# comme un parquet déjà aplati (`dst_ip` / `dst_port` au niveau racine). La forme NESTÉE
# reproduit le pipeline polars voulu :
#   polars get dst | drop-nulls | unnest dst | select ip port | collect
#   | group-by ip | agg (unique port) | collect
def "main dst-ports" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 20       # nombre de lignes à afficher
    --all (-a)            # tout afficher (au lieu des --top premières)
    --infer-schema: int = 200000
] {
    if not (require_file $file) { return }
    let base = (open_source $file $infer_schema)
    let cols = ($base | polars schema | columns)
    if 'dst' in $cols {
        # format source avec `dst` imbriqué : unnest -> ip / port, puis group-by ip.
        # `ancestors` (chaîne pipe-séparée) est agrégé en uniques comme le port.
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
            print $"(ansi yellow)✗ aucune ip de destination dans ce fichier(ansi reset)"
        } else if $all { $all_rows } else { $all_rows | first $top }
    } else if 'dst_ip' in $cols {
        # parquet déjà aplati flat=flat : dst_ip / dst_port au niveau racine,
        # `ancestors` (chaîne pipe-séparée) reste une colonne racine.
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
            print $"(ansi yellow)✗ aucune ip de destination dans ce fichier(ansi reset)"
        } else if $all { $all_rows } else { $all_rows | first $top }
    } else {
        print $"(ansi red)✗ ni colonne `dst` imbriquée ni `dst_ip`/`dst_port` dans ce fichier(ansi reset)"
    }
}

# Vue réseau : command_line + dst (connect), groupée par command_line
# (uniques des dst_ip / dst_port / dst_hostname / dst_public), filtrable sur command_line.
def "main network" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --filter: string = ''  # regex sur command_line
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'connect')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    let keep = ($cols | where {|c| $c in ['command_line' 'dst_ip' 'dst_port' 'dst_hostname' 'dst_public'] })
    if ($keep | length) == 0 {
        print $"(ansi yellow)✗ aucune colonne d'affichage disponible(ansi reset)"
        return
    }
    # les colonnes à agréger en uniques = toutes sauf la clé command_line
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

# Liste des fichiers créés (file_create) : chemin créé + binaire écrivain.
# `path` = chemin créé (ex. /root/bin), `exe_path` = binaire qui écrit (ex. /tmp/sample.bin).
# `--skip-benign` masque les écritures des agents système / utilitaires bénins connus
# (sftp-server/restic, systemd-journald, osqueryd, bash, cp, tar...) pour ne garder
# que les créations de fichiers qui ne tombent pas dans le bruit attendu.
def "main file-creates" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 20       # nombre de lignes
    --all (-a)            # toutes les lignes
    --skip-benign (-b)    # masque les écrivains système/utilitaires bénins connus
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'file_create')
    let cols = ($base | polars schema | columns)
    if 'path' not-in $cols {
        print $"(ansi yellow)✗ colonne path absente pour file_create(ansi reset)"
        return
    }
    # le chemin créé (path) est renommé pour ne pas entrer en collision avec le
    # chemin du binaire écrivain lors de l'unnest de la struct `exe` (même logique
    # que event -> event_info). Le champ interne de `exe` diffère selon le format :
    # `path` (registre/parquet) ou `file` (ngsoti/jsonl) -> on renomme le bon en
    # writer_path.
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
        print $"(ansi yellow)✗ aucun file_create dans ce fichier(ansi reset)"
        return
    }
    if $skip_benign {
        # équivalent local des legit_agents + benign_utilities du moteur, noms de
        # binaire de l'écrivain : sftp-server/restic, journald, systemd, osquery,
        # bascules shell et utilitaires banals de la chaîne système/backup.
        let benign = ['sftp-server','systemd-journald','systemd-logind','systemd',
                      'systemd-xdg-autostart-generator','osqueryd','wazuh-agentd',
                      'cmk-agent-ctl','kunai','dash','bash','sh','cp','tar','mktemp',
                      'gzip','xz','mv','rm','touch','mkdir']
        let kept = ($df | where {|r|
            let w = ($r.writer_path | path basename)
            $w not-in $benign and (not ($r.created_path =~ 'restic-temp'))
        })
        if (($kept | length) == 0) {
            print $"(ansi yellow)✗ aucun file_create non-bénin (--skip-benign) dans ce fichier(ansi reset)"
            return
        }
        if $all { $kept } else { $kept | first $top }
        return
    }
    if $all { $df } else { $df | first $top }
}

# Extensions des chemins créés (file_create / filter_write).
def "main file-extensions" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'file_create')
    let cols = ($base | polars schema | columns)
    if 'path' not-in $cols {
        print $"(ansi yellow)✗ colonne path absente pour file_create(ansi reset)"
        return
    }
    let all_rows = ($base
        | polars select path
        | polars drop-nulls
        | polars get path
        | polars collect
        | polars into-nu
        | each {|r| $r.path | path parse | get extension | str trim | if ($in == '') { '(sans ext)' } else { $in } }
        | group-by { $in }
        | transpose key extension
        | each {|r| { extension: $r.key, count: ($r.extension | length) } }
        | sort-by count -r)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun file_create dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Cibles tuées (kill / filter_kill) : colonne target* une fois déroulée par unnestif.
def "main kill-targets" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'kill')
    let base = (unnestif $base 'target')
    let cols = ($base | polars schema | columns)
    let tcol = (if 'target_executable' in $cols { 'target_executable' } else if 'target_exe' in $cols { 'target_exe' } else if 'target' in $cols { 'target' } else { '' })
    if $tcol == '' {
        print $"(ansi yellow)✗ colonne cible \(target*\) absente pour kill sur ce format(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun kill dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Options prctl les plus fréquentes, regroupées par (task_name, option) : montre
# QUI appelle QUEL prctl (durcissement PR_SET_*, lecture PR_CAPBSET_READ…).
def "main prctl-options" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'prctl')
    let cols = ($base | polars schema | columns)
    if 'option' not-in $cols {
        print $"(ansi yellow)✗ colonne 'option' absente pour prctl sur ce format(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun prctl dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Renommages (file_rename) : paires (task_name, old, new) les plus fréquentes.
def "main file-renames" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'file_rename')
    let cols = ($base | polars schema | columns)
    if 'old' not-in $cols or 'new' not-in $cols {
        print $"(ansi yellow)✗ colonnes 'old'/'new' absentes pour file_rename sur ce format(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun file_rename dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Suppressions de fichiers (file_unlink) : chemins les plus supprimés.
def "main file-unlinks" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'file_unlink')
    let cols = ($base | polars schema | columns)
    if 'path' not-in $cols {
        print $"(ansi yellow)✗ colonne 'path' absente pour file_unlink sur ce format(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun file_unlink dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Mapping en mémoire exécutable (mmap_exec / mprotect_exec) : fichiers mappés RX.
# REGROUPÉ SUR mapped_path (le fichier mappé), PAS exe_path : ce dernier n'est que
# le binaire exécutant (sh/who/grep...) qui mappe ses libs normalement -> bruit.
# Le signal utile est le fichier MAPPÉ, surtout depuis un chemin temporaire/fd/memfd
# (drop-and-run, cf. r_mmapexec_from_tmp du moteur). --suspicious filtre dessus.
def "main mmap-execs" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --mprotect            # inclure aussi mprotect_exec
    --suspicious (-s)     # ne garder que mapped_path temporaire (tmp/fd/memfd)
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'mmap_exec')
    let cols = ($base | polars schema | columns)
    if 'mapped' not-in $cols and 'mapped_file' not-in $cols {
        print $"(ansi yellow)✗ colonnes 'mapped'/'mapped_file' absentes pour mmap_exec sur ce format(ansi reset)"
        return
    }
    # mprotect_exec : mêmes colonnes, concaténées à mmap_exec si demandé.
    let base = if $mprotect {
        let mp = (events_frame $file $infer_schema 'mprotect_exec')
        if (($mp | polars schema | columns | length) > 0) { $base | polars concat $mp } else { $base }
    } else { $base }
    # déroule le struct mapped -> mapped_path (ancien format : mapped_file direct).
    let base = ($base
        | unnestif $in 'mapped'
        | if ('mapped_file' in ($in | polars schema | columns)) { $in | polars rename mapped_file mapped_path } else { $in })
    # filtre drop-and-run : chemin mappé temporaire (même motif que le moteur).
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun mmap_exec dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Programmes eBPF chargés (bpf_prog_load) : produits par type + nom + processus.
# Utile pour repérer des BPF malveillants / rootkit (kprobe, tracepoint, cgroup).
# La colonne nested prog_type est déroulée par unnest (prog_type_name).
def "main bpf-progs" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'bpf_prog_load')
    let cols = ($base | polars schema | columns)
    if 'prog_type' not-in $cols {
        print $"(ansi yellow)✗ colonne 'prog_type' absente pour bpf_prog_load sur ce format(ansi reset)"
        return
    }
    let has_task = ('task_name' in $cols)
    # colonnes group : prog_type_name et name / les indexants du group-by.
    let all_rows = ($base
        | polars unnest prog_type -s "_"
        | polars select (if $has_task { [task_name prog_type_name name] } else { [prog_type_name name] })
        | polars drop-nulls
        | polars group-by (if $has_task { [task_name prog_type_name name] } else { [prog_type_name name] })
        | polars agg [(polars col name | polars count | polars as count)]
        | polars sort-by [count name] -r [true false]
        | polars collect
        | polars into-nu)
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun bpf_prog_load dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Top ports du send_data (filter_send).
def "main send-ports" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'send_data')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    if 'dst_port' not-in $cols {
        print $"(ansi yellow)✗ aucune colonne dst_port pour send_data sur ce format(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun send_data dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
}

# Extraction consolidée des IOC (indicateurs de compromission), équivalent nushell
# de `view_iocs` des scripts kunai officiels. Regroupe en une seule vue les
# indicateurs portés par les événements à fort signal :
#   connect / send_data -> IP et port de destination (egress)
#   dns_query            -> domaine interrogé + IPs résolues (réponse)
#   execve / mmap_exec   -> exécutable exotique (hors chemins système)
# Chaque ligne indique le TYPE d'ioc, la valeur, le binaire responsable et sa
# chaîne d'ancêtres. `--public` ne garde que les IP réellement publiques (pas les
# plages privées/mappées, cf. dst_public peu fiable en IPv4-mappée).
def "main iocs" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --public (-p)         # ne garder que les IP réellement publiques (egress)
    --top: int = 30       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (open_source $file $infer_schema)

    # Binaire responsable : colonne issue de l'unnest de `exe` (ngsoti `exe.file`
    # -> exe_file, registre `exe.path` -> exe_path). Absente -> '' (on saute execve).
    let allcols = ($base | polars schema | columns)
    let bin_col = (if 'exe' in $allcols {
        let e = ($base | polars unnest exe -s "_" | polars schema | columns)
        if 'exe_path' in $e { 'exe_path' } else if 'exe_file' in $e { 'exe_file' } else { '' }
    } else { '' })
    let base = (if 'exe' in $allcols { $base | polars unnest exe -s "_" } else { $base })
    let evcol = (event_name_col $base)

    # colonnes à sélectionner par branche (évite de référencer une colonne absente).
    let net_keep = (['ancestors' 'dst'] | append (if $bin_col != '' { [$bin_col] } else { [] }))
    let dns_keep = (['query' 'response' 'ancestors'] | append (if $bin_col != '' { [$bin_col] } else { [] }))
    let exe_keep = (['ancestors' $evcol] | append (if $bin_col != '' { [$bin_col] } else { [] }))

    # ---- connect & send_data : egress IP + port ----
    let netsrc = ($base
        | polars filter (((polars col $evcol) == 'connect') or ((polars col $evcol) == 'send_data'))
        | polars select $net_keep
        | polars unnest dst -s "_")
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
                    port: ($r.dst_port | default '' | into string)
                    binary: (if $bin_col != '' { $r | get $bin_col | default '' } else { '' })
                    ancestors: ($r.ancestors | default '' | into string)
                }
            })
    } else { [] })
    # `--public` : ne garder que du trafic sortant réellement public (à destination
    # d'une IP non privée/liée locale). Le texte suffit ici : on écarte les plages
    # RFC1918 / loopback / réseaux Docker internes (y compris les IPv4-mappées ::ffff:).
    let net = ($net | where {|r| if not $public { true } else { (is_public_ip $r.indicator) } })
    let net = ($net | where {|r| $r.indicator != 'null' and ($r.indicator | is-not-empty) })

    # ---- dns_query : domaine interrogé + IPs de la réponse ----
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
                    port: ($r.response | default '' | into string)
                    binary: (if $bin_col != '' { $r | get $bin_col | default '' } else { '' })
                    ancestors: ($r.ancestors | default '' | into string)
                }
            })
    } else { [] })
    let dns = ($dns | where {|r| $r.indicator != 'null' and ($r.indicator | is-not-empty) })

    # ---- execve / mmap_exec : exécutable (IOC fichier) ----
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
                    port: ''
                    binary: ($r | get $bin_col | default '' | into string)
                    ancestors: ($r.ancestors | default '' | into string)
                }
            })
    } else { [] })
    let exes = ($exes | where {|r| $r.indicator != 'null' and ($r.indicator | is-not-empty) })

    let rows = ($net | append $dns | append $exes)

    if (($rows | length) == 0) {
        if $public { print $"(ansi yellow)✗ aucun IOC public dans ce fichier(ansi reset)" } else { print $"(ansi yellow)✗ aucun IOC dans ce fichier(ansi reset)" }
        return
    }

    # Agrège les lignes par (type, indicateur) pour sortir des uniques : ports et
    # binaires responsables concaténés, ancêtres les plus fréquents en tête.
    let agg = ($rows
        | group-by {|r| $"($r.type)|($r.indicator)" }
        | items {|k, v|
            let parts = ($k | split row '|')
            let ports = ($v.port | where {|p| ($p | is-not-empty) and $p != '' } | uniq | str join '; ')
            let bins  = ($v.binary | where {|b| ($b | is-not-empty) and $b != '' } | uniq | str join '; ')
            {
                type: ($parts.0)
                indicator: ($parts.1)
                count: ($v | length)
                ports: $ports
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


# ---------------------------------------------------------------- CLI

const REQUESTS = {
    events:            { desc: 'count d évènements par nom',            arg: '' }
    dns:               { desc: 'requêtes DNS groupées par query',       arg: '--top/--all' }
    'command-lines':   { desc: 'command_line execve les plus fréquentes', arg: '--top/--all' }
    exes:              { desc: 'palette dexecutables (1er mot command_line)', arg: '--top/--all' }
    'connect-ips':     { desc: 'top dst_ip des connexions',             arg: '--top/--all' }
    'connect-ports':   { desc: 'top dst_port des connexions',           arg: '--top/--all' }
    network:           { desc: 'vue réseau command_line + dst connect', arg: '--filter' }
    'dst-ports':       { desc: 'ports + ancestors uniques groupés par IP de destination', arg: '--top/--all' }
    'file-extensions': { desc: 'extensions des fichiers créés',         arg: '--top/--all' }
    'file-creates':    { desc: 'fichiers créés : chemin + binaire écrivain', arg: '--top/--all' }
    'kill-targets':    { desc: 'cibles tuées (kill)',                   arg: '--top/--all' }
    'prctl-options':   { desc: 'options prctl par processus',           arg: '--top/--all' }
    'file-renames':    { desc: 'renommages old->new (file_rename)',     arg: '--top/--all' }
    'file-unlinks':    { desc: 'fichiers supprimés (file_unlink)',      arg: '--top/--all' }
    'mmap-execs':      { desc: 'fichiers mappés RX / drop-and-run',   arg: '--top/--all/--mprotect/-s' }
    'bpf-progs':       { desc: 'programmes eBPF chargés (rootkit)',     arg: '--top/--all' }
    'send-ports':      { desc: 'top ports du send_data',                arg: '--top/--all' }
    'send-ips':        { desc: 'top IPs du send_data',                  arg: '--top/--all' }
    iocs:              { desc: 'vue consolidée des IOC (egress / dns / execve)', arg: '--public/--top/--all' }
}

def print_help [] {
    print $"(ansi cyan)kunai_queries.nu — requêtes ad-hoc sur logs kunai(ansi reset)"
    print $""
    print $"Chaque requête est une sous-commande avec son propre help :"
    print $"  nu kunai_queries.nu <requête> --help"
    print $""
    print $"(ansi green)Requêtes:(ansi reset)"
    for k in ($REQUESTS | columns) {
        let info = $REQUESTS | get $k
        print $"  (ansi yellow)($k)(ansi reset)  ($info.desc)"
        if $info.arg != '' { print $"         options: ($info.arg)" }
    }
    print $""
    print $"Fichier source accepté : .parquet déjà aplati, ou .gz / .jsonl / .log ndjson"
    print $"kunai. Loption --infer-schema, défaut 200000, règle lingérence de schéma du"
    print $"lecteur ndjson."
}

# Affiche le récapitulatif des requêtes disponibles (nu kunai_queries.nu help).
def "main help" [] {
    print_help
}

# Point dentrée : les requêtes réelles sont des sous-commandes (main <requête>).
# Ce `main` générique ne gère que le cas où aucun argument nest une sous-commande connue :
#  - `-h`/`--help` -> récapitulatif,
#  - un chemin de fichier passé seul -> requête par défaut `events`,
#  - sinon -> requête inconnue.
def main [
    query: string        # type de requête : events | dns | command-lines | exes | connect-ips | connect-ports | network | dst-ports | file-extensions | file-creates | kill-targets | prctl-options | file-renames | file-unlinks | mmap-execs | bpf-progs | send-ports | send-ips | help
] {
    if $query == '-h' or $query == '--help' {
        print_help
    } else if ($query | path exists) {
        main events $query
    } else {
        print $"(ansi red)✗ requête inconnue : '($query)'(ansi reset)"
        print_help
    }
}
