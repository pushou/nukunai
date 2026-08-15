# nukunai
nushell polars scripts to analyse/filter kunai logs (jsonl to parquet file).

<img src="images/explore.gif" width="150%" >

## requirements 
Nushell and its blazzing fast polars plugins, kunai logs (install kunai (https://github.com/kunai-project/) or see ngsoti malware dataset)
(it offers satisfactory performance with 500MB Kunai log files.) 


## transform kunai jsonl files log file to parquet file 
```
nu kunai_to_parquet.nu events.log.1408.gz
╭────┬──────────────────────────────────────────────────────────────────────────────────────────────╮
│ 39 │ unzipped file  from events.log.1408.gz to events.log.1408                                    │
│    │ converting  events.log.1408 to events.log.1408.parquet --eager infer-schema=200000 flat=flat │
╰────┴──────────────────────────────────────────────────────────────────────────────────────────────
ls |get name|each {nu kunai_to_parquet.nu $in}
```

## explore dataset interactively
```
polars open  events.log.1408.parquet
     | polars collect
     | polars into-nu 
     | flatten --all | flatten --all
     | explore
```

## remember kunai events code:
```
❯ nu kunai_print_events_table.nu
╭────┬───────────────────┬───────────╮
│  # │    events_name    │ events_id │
├────┼───────────────────┼───────────┤
│  0 │ execve            │ 1         │
│  1 │ execve_script     │ 2         │
│  2 │ exit              │ 4         │
│  3 │ exit_group        │ 5         │
│  4 │ clone             │ 6         │
│  5 │ prctl             │ 7         │
│  6 │ kill              │ 8         │
│  7 │ ptrace            │ 9         │
│  8 │ init_module       │ 20        │
│  9 │ bpf_prog_load     │ 21        │
│ 10 │ bpf_socket_filter │ 22        │
│ 11 │ mprotect_exec     │ 40        │
│ 12 │ mmap_exec         │ 41        │
│ 13 │ connect           │ 60        │
│ 14 │ dns_query         │ 61        │
│ 15 │ send_data         │ 62        │
│ 16 │ read              │ 81        │
│ 17 │ read_config       │ 82        │
│ 18 │ write             │ 83        │
│ 19 │ write_config      │ 84        │
│ 20 │ file_rename       │ 85        │
│ 21 │ file_unlink       │ 86        │
│ 22 │ write_close       │ 87        │
│ 23 │ file_create       │ 88        │
│ 24 │ io_uring_sqe      │ 100       │
│ 25 │ file_scan         │ 500       │
├────┼───────────────────┼───────────┤
│  # │    events_name    │ events_id │
╰────┴───────────────────┴───────────╯
```

## filter kunai events 

### explore
```
nu filter_events.nu kunai.jsonl.parquet ./events.log.1521.gz -e 1,2,61
main_file_extension = gz
parquet_file_main events.log.1521.parquet
kunai_events_log_file file_extension parquet_file infer_schema events_id: ./events.log.1521.gz gz events.log.1521.parquet 200000 1,2,61
unzipped file  from ./events.log.1521.gz to ./events.log.1521
converting  ./events.log.1521 to ./events.log.1521.parquet --eager infer-schema=200000 flat=flat
parquet_file: events.log.1521.parquet
filter event_id: 1,2,61
```

### -s save filters events to parquet file 
```
nu filter_events.nu kunai.jsonl.parquet ./events.log.1521.gz -e 1,2,61 -s
main_file_extension = parquet
parquet_file_main ./events.log.1384.parquet
kunai_events_log_file unzipped_file file_extension parquet_file infer_schema events_id: ./events.log.1384.parquet ./events.log.1384 parquet ./events.log.1384.parquet 200000 1,2,61
save filtered_events in: ./events.log.1384_1_2_61.parquet
parquet_file: ./events.log.1384.parquet
parquet_file: ./events.log.1384.parquet
filter event_id: 1,2,61

polars open  ./events.log.1384.parquet | polars collect | polars into-nu |explore
```

## display events count from file

```
nu kunai_events_analysis.nu ./events.log.1503.gz
unzipped file  from ./events.log.1503.gz to ./events.log.1503
converting  ./events.log.1503 to ./events.log.1503.parquet --eager infer-schema=200000 flat=flat
╭────┬───────────────┬────────╮
│  # │     name      │ count  │
├────┼───────────────┼────────┤
│  0 │ prctl         │ 102434 │
│  1 │ send_data     │  90791 │
│  2 │ mmap_exec     │  79647 │
│  3 │ clone         │  61173 │
│  4 │ exit_group    │  52130 │
│  5 │ kill          │  50042 │
│  6 │ execve        │  15732 │
│  7 │ read_config   │  13552 │
│  8 │ connect       │   4479 │
│  9 │ dns_query     │   3118 │
│ 10 │ file_unlink   │   3041 │
│ 11 │ file_create   │   2422 │
│ 12 │ file_rename   │    791 │
│ 13 │ exit          │    316 │
│ 14 │ bpf_prog_load │    117 │
│ 15 │ execve_script │     80 │
╰────┴───────────────┴────────╯
```



## usefull oneliners

```
polars open events.log.5319.parquet | polars shape
╭───┬────────┬─────────╮
│ # │  rows  │ columns │
├───┼────────┼─────────┤
│ 0 │ 412240 │      44 │
╰───┴────────┴─────────╯
```

```
 polars open kunai.jsonl.parquet    | polars schema
╭──────────────┬─────────────────────────────────╮
│ ancestors    │ str                             │
│ command_line │ str                             │
│              │ ╭────────┬─────╮                │
│ exe          │ │ file   │ str │                │
│              │ │ md5    │ str │                │
│              │ │ sha1   │ str │                │
│              │ │ sha256 │ str │                │
│              │ │ sha512 │ str │                │
│              │ │ size   │ i64 │                │
│              │ │ error  │ str │                │
│              │ ╰────────┴─────╯                │
│              │ ╭──────────┬──────╮             │
│ dst          │ │ hostname │ str  │             │
│              │ │ ip       │ str  │             │
│              │ │ port     │ i64  │             │
│              │ │ public   │ bool │             │
│              │ │ is_v6    │ bool │             │
│              │ ╰──────────┴──────╯             
```

### flatten host_container 
```
❯ polars open events.log.1411.parquet
   | polars rename [name] [main_name]
   | polars unnest host
   | polars rename [uuid name container] [host_uuid host_name host_container]
   | polars collect

```
### right way to select containers events
```
timeit {polars open events.log.1380.parquet 
        | polars rename [name] [main_name]
        | polars unnest host
        | polars rename [uuid name container] [host_uuid host_name host_container]
        | polars unnest host_container| polars rename [name type] [container_name container_type]
        | polars filter ((polars col container_type) == "docker")| polars collect|polars into-nu}
16sec 154ms 189µs 325ns
```
### select src dst address dst port
```
 polars open events.log.1375.parquet
     | polars select src dst
     | polars unnest src
     | polars rename [ip port ] [src_ip src_port] 
     | polars unnest dst
     | polars rename [hostname ip port public is_v6]  [dst_hostname dst_ip dst_port dst_public dst_is_v6]
     | polars group-by (polars col dst_ip)
     | polars agg [(polars col dst_port|polars unique)]
     | polars into-nu
╭────┬─────────────────────────────────────────┬────────────────╮
│  # │                 dst_ip                  │    dst_port    │
├────┼─────────────────────────────────────────┼────────────────┤
│  0 │ 2a05:d018:19bb:ea02:8107:67e6:6dd9:97da │ ╭───┬────╮     │
│    │                                         │ │ 0 │ 53 │     │
│    │                                         │ ╰───┴────╯     │
│  1 │ 10.255.255.135                          │ ╭───┬─────╮    │
│    │                                         │ │ 0 │ 443 │    │
│    │                                         │ ╰───┴─────╯    │
│  2 │ 52.212.210.86                           │ ╭───┬─────╮    │
│    │                                         │ │ 0 │  53 │    │
│    │                                         │ │ 1 │ 443 │    │
│    │                                         │ ╰───┴─────╯    │
│  3 │ 54.220.9.34                             │ ╭───┬────╮     │
│    │                                         │ │ 0 │ 53 │     │
│    │                                         │ ╰───┴────╯
```

### dns

```
polars open events.log.1373.parquet 
    | polars  select query response dns_server_ip 
    | polars drop-nulls 
    | polars collect
    | polars into-nu

polars open events.log.1373.parquet
    | polars  select query response dns_server_ip
    | polars drop-nulls
    | polars group-by query
    | polars agg [(polars col response|polars count) (polars col dns_server_ip|polars unique)]
    | polars collect
╭───┬────────────────────────┬──────────┬──────────────────────╮
│ # │         query          │ response │    dns_server_ip     │
├───┼────────────────────────┼──────────┼──────────────────────┤
│ 0 │ api.crowdsec.net       │        4 │ ╭───┬──────────────╮ │
│   │                        │          │ │ 0 │ 10.6.255.106 │ │
│   │                        │          │ ╰───┴──────────────╯ │
│ 1 │ registry.local         │     2086 │ ╭───┬──────────────╮ │
│   │                        │          │ │ 0 │ 127.0.0.11   │ │
│   │                        │          │ │ 1 │ 10.6.255.106 │ │
│   │                        │          │ ╰───┴──────────────╯ │
│ 2 │ gitlab.com             │        2 │ ╭───┬──────────────╮ │
│   │                        │          │ │ 0 │ 10.6.255.106 │ │
│   │                        │          │ ╰───┴──────────────╯ │

polars open kunai.jsonl_61.parquet
    | polars  select query response dns_server
    | polars drop-duplicates
    | polars group-by query
    | polars agg [(polars col response|polars count) (polars col dns_server|polars unique)]
    | polars collect
╭───┬───────────────┬──────────┬──────────────────────────────────────────╮
│ # │     query     │ response │                dns_server                │
├───┼───────────────┼──────────┼──────────────────────────────────────────┤
│ 0 │ api.ipify.org │        2 │ ╭───┬──────────┬──────┬────────┬───────╮ │
│   │               │          │ │ # │    ip    │ port │ public │ is_v6 │ │
│   │               │          │ ├───┼──────────┼──────┼────────┼───────┤ │
│   │               │          │ │ 0 │ 10.0.2.3 │   53 │ false  │ false │ │
│   │               │          │ ╰───┴──────────┴──────┴────────┴───────╯ │
│ 1 │ kunai-sandbox │        1 │ ╭───┬──────────┬──────┬────────┬───────╮ │
│   │               │          │ │ # │    ip    │ port │ public │ is_v6 │ │
│   │               │          │ ├───┼──────────┼──────┼────────┼───────┤ │
│   │               │          │ │ 0 │ 10.0.2.3 │   53 │ false  │ false │ │
│   │               │          │ ╰───┴──────────┴──────┴────────┴───────╯ │
╰───┴───────────────┴──────────┴──────────────────────────────────────────╯
```

## filter command lines

```
polars open events.log.5319.parquet 
    | polars filter (polars col command_line|polars contains "curl|wget")
    | polars get command_line
    | polars collect
╭───────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│     # │                                                                command_line                                                                │
├───────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│     0 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     1 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     2 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     3 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     4 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     5 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     6 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     7 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     8 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│     9 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│   ... │ ...                                                                                                                                        │
│ 23716 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23717 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23718 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23719 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23720 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23721 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23722 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23723 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23724 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
│ 23725 │ curl --max-time 30 --no-buffer -s --unix-socket /var/run/docker.sock http://localhost/containers/json?filters=\{"health":\["unhealthy"\]\} │
╰───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## filter zombie processes
```
polars open  events.log.1502.parquet 
     | polars rename [command_line task flags path exe name] [main_command_line main_task main_flags main_path main_exe main_name] 
     | polars unnest main_task
     | polars rename [name pid tgid guuid uid  gid  namespaces flags] [t_name t_pid t_tgid t_guuid t_uid  t_gid  t_namespaces t_flags] 
     | polars filter ((polars col zombie) == 'true')
     | polars into-nu 
     | flatten --all
     | explore
```

---

## Détection de compromission sur le dataset NGSOTI

Les scripts ci-dessous analysent les logs kunai du dataset malware
[ngsoti](https://github.com/cert-orangecyberdefense/ngsoti) pour détecter des
comportements suspects. Contrairement aux outils précédents, ils lisent
directement les fichiers `.jsonl.gz` en lazy polars, **sans passer par le
format Parquet**, et produisent des rapports lisibles.

### Fichiers

| Fichier | Rôle |
|---------|------|
| `kunai_detect_compromise.nu` | **Moteur** : détection (9 familles), bilan IP/ports & URLs, rendu markdown + JSON, procédures réutilisables |
| `ngsoti_detail.nu` | Rapport détaillé d'un échantillon (un fichier via `FILE`) → `.md` + `.json` |
| `ngsoti_all.nu` | Traite séquentiellement tous les échantillons de `logs/ngsoti/*/kunai.jsonl.gz` |
| `ngsoti_report.nu` | Résumé des alertes par famille pour **tous** les échantillons (parallèle, 4 cœurs) |

### Les 9 familles de détection

`execve`, `file_create`, `connect`, `send_data`, `dns_query`, `kill`,
`bpf_prog_load`, `mmap_exec`, `prctl`.

Chaque ligne est classée **bénigne** (bruit légitime de la plateforme :
agents, utilitaires standards, IP locales) ou **suspecte** selon la tâche
et la chaîne de processus. Les utilitaires détournables (`docker`, `chmod`,
`curl`, `bash`…) ne sont bénins que s'ils proviennent d'une chaîne légitime.

### Usage du moteur (`kunai_detect_compromise.nu`)

```
# les 2 fichiers .gz les plus récents du registry
nu kunai_detect_compromise.nu

# fichiers explicites
nu kunai_detect_compromise.nu fichier1.gz fichier2.gz

# n'afficher que 1 ligne par famille
nu kunai_detect_compromise.nu -n 1

# ne lancer qu'une famille
nu kunai_detect_compromise.nu -f execve

# affichage dataframe interactif
nu kunai_detect_compromise.nu --explore
```

Options : `--infer-schema <n>` (défaut 200000), `-n/--num <n>` (lignes par
famille, défaut 20), `-f/--family <fam>`, `-x/--explore`, `--no-json`
(n'écrit que le markdown).

### Rapports produits

Chaque invocation écrit ses rapports dans `scanresult<TS>/` (dossier commun à
toute une session), avec un nom descriptif basé sur les familles détectées et
le hash court de l'échantillon :

```
scanresult20260816_000812/15e67237_execve1_connect35_send_data9286_dns_query2_prctl3.md
scanresult20260816_000812/15e67237_execve1_connect35_send_data9286_dns_query2_prctl3.json
```

- **`.md`** : détail des alertes par famille (25 premières lignes max) + bilan
  consolidé (IP/ports les plus actifs, URLs de téléchargement, fichiers déposés).
- **`.json`** (par défaut, `--no-json` pour désactiver) : structure complète et
  exploitable par programme :

```json
{
  "schema_version": 1,
  "scan_ts": "20260816_000812",
  "sample": { "file": "logs/ngsoti/<hash>/kunai.jsonl.gz", "hash": "<sha256>", "short_hash": "15e67237" },
  "fam_counts": { "execve": 1, "connect": 35, "send_data": 9286, "dns_query": 2, "prctl": 3 },
  "families": {
    "execve": [ "..." ],
    "send_data": [ "..." ]
  },
  "bilan": { "ipports": [ "..." ], "urls": [ "..." ], "files": [ "..." ] }
}
```

### Analyses en masse

**Rapport détaillé d'un échantillon** (via la variable d'env `FILE`) :
```
FILE=logs/ngsoti/<hash>/kunai.jsonl.gz nu ngsoti_detail.nu
```

**Tous les échantillons, séquentiellement** (mêmes dossiers `scanresult<TS>`) :
```
nu ngsoti_all.nu
```

**Résumé rapide pour tous les échantillons** (parallèle, affiche familles + compteurs) :
```
nu ngsoti_report.nu                    # sortie dans $JCODE_SCRATCH_DIR/ngsoti_out (sinon ./ngsoti_out)
nu ngsoti_report.nu mon_dossier        # sortie dans mon_dossier
```


