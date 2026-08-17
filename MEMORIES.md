# Mémoires — Projet nukunai

Mémoires de travail (scope projet) reconciliées avec l'état actuel du dépôt.
Dernière mise à jour : 2026-08-17 (après refactor pot commun + intégration des dépôts de règles kunai en submodules).

Rappel : le projet a été **refactorisé** dans le dernier commit (`05bbbe2`). Les règles
génériques vivent dans le pot commun (`kunai_rules.nu` + fichiers `.kun` `kunai_rules/rules_v0.1/`),
le contexte machine (allowlists agents/réseaux/chemins) dans `kunai_rules_local.nu`,
et le moteur d'analyse (`kunai_detect_compromise.nu`) compose les deux.

---

## 1. Refactor bruit — legit_agents / benign_utilities

**Type :** fact

Le bruit est maîtrisé par 2 listes du contexte LOCAL (`kunai_rules_local.nu`, fonction
`local_cfg`):
- **legit_agents** — vrais agents/services : dpkg (dont dpkg-db-backup), gitlab, dockerd,
  containerd, systemd, wazuh, crowdsec, splunk, check_mk, nginx, sshd, cron, dbus, polkit,
  NetworkManager, unattended-upgrade, audit.
- **benign_utilities** — outils détournables : docker, grep, sed, cat, tar, gzip, bzip2,
  xz, gpg, gpgv, sh, bash, curl, wget, chmod, chown, install, cp, mv, rm, cmp, mktemp,
  rmdir, touch, mkdir, ln, readlink, basename, dirname, date, sleep, printf, echo, nproc,
  lsblk, df, free, pgrep, ps, ss, ip, hostname, uname (+ python3/python/perl, awk, etc.).

Fonction `not_legit` du moteur (`kunai_detect_compromise.nu:50`) : une ligne est bénigne si
`task ∈ legit` OU `(task ∈ utils ET parent ∈ (legit ∪ utils))`. Résultat : un
docker/chmod/chown/curl/tar lancé par un process NON légitime est DÉTECTÉ ; bénin uniquement
sous une chaîne légitime. Le parent seul ne suffit plus (sinon un rootkit daemonisé sous
systemd comme perfctl→oom_reaper serait masqué).

**SYNTAXE EXPR POLARS :** pas de `polars or` / `polars and` — utiliser les opérateurs nushell
`or` / `and` entre parenthèses, ex. `(($a) or ($b))`.

---

## 2. Affinage bruit execve — filtre parent

**Type :** fact (historique, intégré)

L'ancien `not_legit_parent` a été **absorbé dans `not_legit`** : les enfants d'un agent
(ex. python3 -c / perl -e / grep / sed / date / ps de check_mk) ne sont plus signalés.
L'oute d'une commande est DETECTÉ est l'outil lancé par un process non-légitime — on filtre
donc par la CHAÎNE (task+parent), pas par le seul nom de task, pour ne pas masquer un vrai
python3/perl lancé hors chaîne légitime. Limite documentée : un attaquant compromettant un
agent légitime verrait ses commandes invisibilisées.

---

## 3. Second affinage bruit execve — audit et cmp

**Type :** fact (historique, intégré)

`auditd`, `auditctl`, `auditd-manager`, `augenrules`, `aureport`, `ausearch` ont été ajoutés
à `legit_agents`, et `cmp` à `benign_utilities` (provoquait le faux positif légitime
`cmp -s /tmp/aurules.* /etc/audit/audit.rules`). Élimine le bruit auditd sans perte de signal.

**PIÈGE nushell :** dans les chaînes `$"…"` ne JAMAIS mettre de parenthèses littérales
non-échappées (ex. `'(ignoré)'`) sinon nushell les interprète comme commande → plantage.

---

## 4. Les 9 familles de détection

**Type :** fact

Familles implémentées dans le moteur : **execve, file_create, connect, send_data, dns_query,
kill, bpf_prog_load, mmap_exec, prctl**.

Points clés (validés sur échantillons .jsonl/.gz du dataset ngsoti et registre réel) :
- **kill** : `signal` est un STRING → `== 'SIGKILL'` (pas `== 9`).
- **prctl** : `option` est un STRING → `== 'PR_SET_DUMPABLE'` / `'PR_SET_SECCOMP'` (pas 4/22).
- **send_data** : le `high_entropy` seul ne suffit pas (du TLS interne/agent ou buildx vers le
  registry est normalement à entropie élevée) → corrélé à une destination **publique non
  allowlistée** (`dst_public` + egress).
- **mmap_exec / file_create / execve from_tmp** : corrélation **drop-and-run** — un artefact
  temporaire n'est suspect QUE s'il est réellement exécuté (mmap_exec.mapped_path ou 1er mot
  de command_line sous /tmp|/dev/shm) et que l'écrivain n'est pas la chaîne build.

---

## 5. Emplacement des données

**Type :** fact

- Dataset malware NGSOTI : `logs/ngsoti/*/kunai.jsonl.gz` (les logs analysés).
- Registre kunai réel (registry de la machine) : `/run/media/pouchou/SSD2T/ips-ids-siem-pcaps/kunai/kunai_registry/kunai` (fichiers `.gz`/`.parquet`, le moteur prend par défaut les 2 plus récents).
- Rapports de scan : `logs/ngsoti/scanresult<TS>/<hash>_<familles>.md` + `.json`.

---

## 6. Colonnes Int128 non convertibles

**Type :** fact

Les colonnes **Int128** typées par polars sont inutilisables en sortie nushell et cassent
le `collect` / `into-nu`. On ne sélectionne donc que des colonnes sûres (utc_time) avant
collect dans les helpers ; si aucune colonne de travail n'existe, `cols_keep`
(`kunai_detect_compromise.nu:203`) renvoie un lazy frame vide (0 ligne, utc_time seul).

---

## 7. Performances

**Type :** fact

- Conversion gz/jsonl → parquet par défaut (`ensure_parquet`) : ~**14x** plus rapide que la
  re-lecture ndjson (cache réutilisé si plus récent que la source, `--force-convert` pour
  forcer). `--no-convert` revient à la lecture directe.
- Registre réel .gz (45 Mo) : ~5 min par fichier ; kill/mmap/prctl rapides (ordre 4/3/4 alertes).

---

## 8. Résultats validés (référence)

**Type :** fact (référence historique)

Sur le petit fichier `kunai.jsonl-1.gz` : execve 3, connect 14, send_data 6, dns_query 2,
prctl 3. Ces valeurs varient avec le refactor des règles ; elles ont surtout valeur de
non-régression.

---

## 9. perfctl — signature d'infection

**Type :** fact

perfctl — malware dont l'infection est détectée par nukunai : drop `/tmp/sample.bin`,
installation de binaire rootkit (bsd-port/knerl), `prctl` dumpable. C'est le cas de test
canonique de non-masquage d'un daemon non-légitime sous systemd.

---

## 10. Formats kunai variables — pipeline_mismatch (corrigé)

**Type :** fact (corrigé)

Les formats kunai varient : certaines colonnes sont absentes selon les événements/versions
(ex. kill sans target_task_name). Cela faisait échouer le plan à `cols_keep`
(**pipeline_mismatch**). Corrigé par des **unnests conditionnels** (`unnestif`
`kunai_detect_compromise.nu:182`) et des guards `has_events` / `cols_keep` / `empty_like`
qui court-circuitent les événements ou colonnes absents sans casser le collect.

---

## 11. Helpers du moteur

**Type :** fact

Dans `kunai_detect_compromise.nu` :
- **has_events** : vérifie par collecte ciblée (select utc_time) si l'événement est présent ;
- **empty_like** : renvoie un lazy frame 0 alerte (événement sentinelle impossible) ;
- **unnestif / cols_keep / normalize_mapped / starts_with_any** : robustesse aux formats variés.

---

## 12. Le logiciel nukunai

**Type :** fact

nukunai est un ensemble de scripts **nushell + polars** analysant des logs kunai. Il détecte
des compromissions par malware sur : le dataset ngsoti (`logs/ngsoti`), l'ancien format
`kunai.jsonl-1.gz`, et le registre réel `.gz`/`.parquet`. Le moteur central est
`kunai_detect_compromise.nu` ; les utilities de conversion/filtrage sont `kunai_to_parquet.nu`,
`kunai_to_flatten_parquet.nu`, `kunai_filter_events.nu`, `kunai_events_analysis.nu`.

---

## 13. Non-régression — suppression de la clause parent_legit

**Type :** fact

Validé sur registre réel : l'ancienne logique (avec parent_legit global) et la nouvelle
(sans, compensée par benign_utilities étendus) donnent des résultats identiques sur trafic
réel sans faux positif. Concerne le traitement des utilitaires détournables sous chaînes
légitimes.

---

## 14. Support des .jsonl bruts

**Type :** fact (intégré)

Le moteur supporte `.jsonl`, `.jsonl.gz` et `.parquet`. Le contrôle d'intégrité `gzip -t`
n'est appliqué qu'aux fichiers `.gz` (pas aux `.jsonl` bruts — cas de test, fichier de 28 801
lignes). Voir `main` de `kunai_detect_compromise.nu` (test sur l'extension).

---

## 15. Dépôt — branches

**Type :** fact

Branches **main** et **deepseek**, les deux poussées sur origin. `deepseek` est la branche
active de travail (actuellement sur `05bbbe2`, 1 commit devant `origin/deepseek`).

---

## 16. Refactor pot commun — architecture actuelle (POST-export)

**Type :** fact (ajout)

Refactor `05bbbe2` : séparation générique / local.
- **Générique (réutilisable, sans contexte machine)** : `kunai_rules.nu` (module exécutable
  exposant `r_<famille>_<nom>` → Expr polars, et `r_ev_<famille>_<nom>` → evidence) et le pot
  commun interopérable gene/kunai dans `kunai_rules/rules_v0.1/*.kun` (mêmes indicateurs : reverse_shell,
  download_and_execute, obfuscated_exec, offensive_tool, webshell_drop, hidden_tmp, unusual_port,
  public_egress, large_data, high_entropy, suspicious_tld, kill_critical, SIGKILL_hard,
  mmap_exec_suspicious, dumpable_change).
- **Local (machine « registry »)** : `kunai_rules_local.nu` → `local_cfg` (legit_agents,
  benign_utilities, dns_ips, allowlist_public_networks, allowlist_egress_ports,
  allowlist_egress_procs, allowlist_build_procs, allowlist_build_paths, benign_signals).
- Le **moteur** (`kunai_detect_compromise.nu`) consomme les deux modules (`use … local_cfg`,
  `use … *`) et compose générique + local (ex. `exec_from_tmp` seulement si pas chaîne build ;
  `public_egress` seulement si destination publique non allowlistée ; `SIGKILL_hard` hors
  signaux bénins SIGURG/SIGCHLD/SIGCONT/SIGWINCH/SIGIO/SIGPIPE).

---

## 17. Conventions d'usage du moteur

**Type :** fact (ajout)

Options `main` : `-f/--family` (all ou une famille), `-n/--num` (lignes/famille, défaut 20),
`-x/--explore`, `--no-json`, `--no-convert`, `--force-convert`, `--cache-dir`,
`--infer-schema` (défaut 200000). Écrit toujours un rapport markdown (+ JSON par défaut) dans
`scanresult<TS>/`. Wrappers : `ngsoti_detail.nu` (via `FILE`), `ngsoti_all.nu` (séquentiel),
`ngsoti_report.nu` (parallèle, ≤4 cœurs) → répertoire sortie par défaut `$JCODE_SCRATCH_DIR/ngsoti_out`.

---

## 18. Format des règles `.kun` interopérables (gene/kunai)

**Type :** fact (ajout 2026-08-17)

Les fichiers `kunai_rules/rules_v0.1/*.kun` visent l'interopérabilité avec le moteur kunai,
qui charge ses règles en DSL **gene** (crate `gene` 0.7.2, via `gene::Rule` — champ `deny_unknown_fields`).
Pièges vérifiés dans les sources gene/kunai :
- **Extension** : kunai charge en règles de détection les fichiers `.kun`/`.kunai`/`.gen`/`.gene` ;
  `.yaml`/`.yml` sont réservés aux TEMPLATES (chargés via `load_templates_from_reader`). → utiliser `.kun`.
- **Opérateurs match** (grammaire `match.pest` de gene) : seulement `==`/`is`, `<`, `<=`, `>`, `>=`,
  `~=` (regex), `&=`. **Ni `in […]`, ni littéral de liste, ni fonction `len()`**. Une « liste » de ports
  se traduit en N opérandes `==` combinés par `or` dans `condition`.
- **Valeurs regex en quotes SIMPLES YAML** sinon serde_yaml interprète `\b`→backspace et rejette `\.`
  (échappement inconnu) → regex cassée ou erreur de parse.
- **Chemins de champs réels kunai** (vérifiés sur `.gz` du registre/ngsoti) : kill cible = `.data.target.task.name`
  (PAS `.data.target.name`), `signal` = `.data.signal` ; mmap_exec = `.data.mapped.file` (PAS `.mapped.path`) ;
  connect/send_data = `.data.dst.port`, `.data.dst.public`, `.data.data_size`, `.data.data_entropy` ;
  file_create = `.data.path` ; prctl = `.data.option` (chaîne) ; dns = `.data.query`.
- **Noms d'événements** `match-on.events.kunai` = noms `#[str(...)]` du `enum Type` kunai (execve,
  execve_script, kill, prctl, connect, dns_query, send_data, mmap_exec, file_create…) — convertis en ids par le loader.
Les 13 `.kun` ont été convertis/corrigés depuis les `.yaml` (2026-08-17) ; validation : parse YAML + grammaire
match gene + compilation regex + cohérence des vars de `condition`.

**PIÈGE kunai 0.4.0 (gene) — entiers nus en RHS de `==` :** `kunai replay -r <regle>` rejette
parfois un littéral entier (`.data.dst.port == 31337`, `== 4444`, `== 161`, `== 443`) par
`expected value or indirect_field_path`. Reproductible sur les règles officielles digisquad
(`net_c2_port.connect.detection.yaml` échoue sur `31337`) : ce n'est DONC pas notre format, c'est
un quirk du parser gene embarqué. Comportement contextuel (un matcher UNIQUE échoue systématiquement,
une règle multi-matchers `any of $port_…` passe partiellement). Ne PAS utiliser `kunai replay` comme
gate de validation : notre moteur nushell/polars reste la vérité pour l'analyse.

---

## 19. Dépôts de règles officiels en submodules

**Type :** fact (ajout 2026-08-17)

Deux dépôts upstream de règles kunai sont intégrés en **submodules** (lecture seule, pour
recoupement / réutilisation des phénotypes dans `kunai_rules.nu`) :
- `kunai_rules/vendored/community-rules` → https://github.com/kunai-project/community-rules
    (règles communautaires, gene `.kun`).
- `kunai_rules/vendored/kunai-rules` → https://github.com/digisquad-repo/kunai-rules
  (306 règles `.yaml` de détection/dépendances, même DSL gene).

`git submodule update --init --recursive` pour les cloner. Notre pot commun local
(`kunai_rules/rules_v0.1/*.detection.kun`, syntaxe gene identique aux upstreams) et le moteur
restent autonomes : l'analyse ne dépend jamais du réseau ni de ces submodules. Le format `.kun`
a été validé conforme au format kunai officiel (même syntaxe que les 306 règles digisquad).
