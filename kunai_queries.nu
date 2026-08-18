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
#   file-extensions  extensions des chemins créés          (filter_write)
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

# ------------------------------------------------ requêtes (sous-commandes)

# Lien type d'événement kunai -> sous-requête de traitement (affiché par events).
const EVENT_QUERY = {
    execve:        'command-lines / exes'
    execve_script: 'command-lines / exes'
    connect:       'connect-ips / connect-ports / network'
    send_data:     'send-ips / send-ports'
    dns_query:     'dns'
    file_create:   'file-extensions'
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

# Vue réseau : command_line + dst (connect), éventuellement filtrée sur command_line.
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
    let rows = ($base
        | cols_keep $keep
        | polars collect
        | polars into-nu)
    if ($filter | is-empty) { $rows } else if 'command_line' in $cols { $rows | where {|r| $r.command_line =~ $filter} } else { print $"(ansi yellow)✗ colonne command_line absente, --filter ignoré(ansi reset)"; $rows }
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
        | each {|r| $r.path | path parse | get extension | default '(sans ext)' }
        | uniq -c
        | sort-by count -r
        | rename count extension)
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

# Top IPs du send_data.
def "main send-ips" [
    file: string = ''     # fichier kunai .parquet / .gz / .jsonl
    --top: int = 10       # nombre de lignes
    --all (-a)            # toutes les lignes
    --infer-schema: int = 200000  # lignes d'inférence de schéma ndjson
] {
    if not (require_file $file) { return }
    let base = (events_frame $file $infer_schema 'send_data')
    let base = (unnestif $base 'dst')
    let cols = ($base | polars schema | columns)
    if 'dst_ip' not-in $cols {
        print $"(ansi yellow)✗ aucune colonne dst_ip pour send_data sur ce format(ansi reset)"
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
    if (($all_rows | length) == 0) { print $"(ansi yellow)✗ aucun send_data dans ce fichier(ansi reset)" } else if $all { $all_rows } else { $all_rows | first $top }
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
    'file-extensions': { desc: 'extensions des fichiers créés',         arg: '--top/--all' }
    'kill-targets':    { desc: 'cibles tuées (kill)',                   arg: '--top/--all' }
    'prctl-options':   { desc: 'options prctl par processus',           arg: '--top/--all' }
    'file-renames':    { desc: 'renommages old->new (file_rename)',     arg: '--top/--all' }
    'file-unlinks':    { desc: 'fichiers supprimés (file_unlink)',      arg: '--top/--all' }
    'mmap-execs':      { desc: 'fichiers mappés RX / drop-and-run',   arg: '--top/--all/--mprotect/-s' }
    'bpf-progs':       { desc: 'programmes eBPF chargés (rootkit)',     arg: '--top/--all' }
    'send-ports':      { desc: 'top ports du send_data',                arg: '--top/--all' }
    'send-ips':        { desc: 'top IPs du send_data',                  arg: '--top/--all' }
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
    query: string        # type de requête : events | dns | command-lines | exes | connect-ips | connect-ports | network | file-extensions | kill-targets | prctl-options | file-renames | file-unlinks | mmap-execs | bpf-progs | send-ports | send-ips | help
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
