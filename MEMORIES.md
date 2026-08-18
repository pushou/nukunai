# Mémoires — Projet nukunai

Mémoires de travail (scope projet) reconciliées avec l'état actuel du dépôt.
Dernière mise à jour : 2026-08-17 (après refactor pot commun + intégration des dépôts de règles kunai en submodules).

Rappel : le projet a été **refactorisé** dans le dernier commit (`05bbbe2`). Les règles
génériques vivent dans le pot commun (`kunai_rules.nu` + fichiers `.kun` `kunai_rules/rules_v0.1/`),
le contexte machine (allowlists agents/réseaux/chemins) dans `kunai_local_cfg.nu` (socle
`default` toujours appliqué + profils par FONCTION `dns`/`network`/`docker`/`elk`/`kube`,
`all` par défaut), exposé au moteur via l'interface `kunai_rules_local.nu`
(`local_cfg <clé> --profile "a,b"`), et le moteur d'analyse (`kunai_detect_compromise.nu`)
compose les deux.

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
active de travail (actuellement sur `d3f8426`, quelques commits locaux devant `origin/deepseek`,
non poussés tant que l'utilisateur ne le demande pas).

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

**PIÈGE gene (TOUTES versions testées, y compris 0.6.2) — littéraux numériques nus en RHS NON
supportés :** le moteur `replay` (gene, `expected value` / `expected value or indirect_field_path`)
rejette TOUT littéral numérique nu en RHS d'une comparaison — entiers `== 31337`, `> 1000000` ET
floats `> 7.5` (vérifié sur le binaire local 0.6.2, 2026-08-17). La règle officielle vendored
`net_c2_port.connect.detection.yaml` (digisquad) échoue IDENTIQUEMENT → ce n'est pas notre
format, c'est un défaut du gene embarqué dans kunai (0.4.0 ET 0.6.2).
**FIX VALIDÉ (0.6.2) : QUOTER le littéral numérique** `== '31337'`, `> '1000000'`, `> '7.5'`.
Gene parse alors un atome numérique et compare numériquement (un port u16 `31337` matche
`'31337'` ; zéro faux positif sur 443/8080/53/22). Corrections appliquées (2026-08-17) :
`c2_unusual_port.connect.detection.kun` (N ports quotés) et `exfil_public_send.send_data.detection.kun`
(`$big > '1000000'`, `$hi > '7.5'`). Les 13 `.kun` chargent sans erreur dans un replay commun.

**PIÈGE replay synthétique — `event.id` DOIT être l'id numérique réel du `enum Type` :**
le gate `match-on.events.kunai` de gene compare à l'id numérique de l'événement, PAS au nom.
Un `connect` synthétique avec `info.event.id = 8` (id de Kill) fait ÉCHOUE silencieusement
TOUTE règle connect (`condition` jamais vraie), même une règle `== 'connect'` toujours-true.
Il faut utiliser l'id réel du `enum Type` kunai (bpf_events.rs) : `Connect = 60`. Un événement
valide `connect` : `info.event.id = 60`, `info.event.name = "connect"`, schéma TaskSection
(`user`/`group`/`flags` hex). (Vérifié 2026-08-17 : avec `id=60` la règle mini `== '31337'` et la
règle complète `c2_unusual_port` détectent ; `id=8` aucune détection.)
**Action :** pour valider une règle `.kun`, utiliser `kunai replay` 0.6.2 + événements synthétiques
avec les bons ids ; notre moteur nushell/polars reste la vérité pour l'analyse réelle.

**Ids numériques du `enum Type` kunai (bpf_events.rs, discriminants réels pour `event.id` des
événements synthétiques) :** execve=1, execve_script=2, prctl=7, kill=8, ptrace=9, mmap_exec=41,
connect=60, dns_query=61, send_data=62, file_create=88, read=81. (InitModule=20, MprotectExec=40,
Read=81 sont des resets explicites : les variantes qui suivent incrémentent de 1.)

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

---

## 20. Fixes d'allowlist 2026-08-17 (bruit local, non-régression)

**Type :** fact (ajout 2026-08-17)

Trois affinages du contexte local (`kunai_rules_local.nu`) qui retirent du bruit LÉGITIME
sans toucher aux règles génériques. Validés : zéro FP sur 5 logs registry sains
(20334/20335/20338/20372/20374) + 16 échantillons ngsoti, détections attaques préservées
(C2 beacon 66.23.233.179:9375 / 15e67237, drop-and-run /tmp/sample.bin, synflood 22e4a57a,
Telnet port 23).

- **NTP (chronyd) — commit `df27987`** : le trafic de synchro NTP de `chronyd`/`chrony`/
  `systemd-timesyn*` (port 123, pool Debian NTP, paquets 48 o) était signalé en
  `public_egress` (connect/send_data/dns 5+3+2 → 0 après fix). Ajoutés à `legit_agents` →
  filtrés par `not_legit` ; les règles egress génériques restent intactes.
  > **SUPERSEDÉ (2026-08-18, refactor flux)** : plus ajout à `legit_agents` pour l'egress.
  > La synchro NTP est désormais tue par le CRITÈRE DE FLUX port 123 seul (« port NTP réputé »,
  > indépendant de l'IP, qui passe — cf. détection DNS 8.8.8.8 gardée). Voir §25.
- **Initramfs — commit `ec0b66e`** : la génération d'initramfs (mkinitramfs/dracut/
  update-initramfs, `/var/tmp/mkinitramfs_*`, `/usr/lib/dracut`) écrit/exécute des artefacts
  scratch jamais utilisés par de la persistance malveillante. `allowlist_initramfs_paths`
  + chaîne moteur (même logique que `allowlist_build`).
- **apt/CloudFront — commit `d3f8426`** : `apt-get update` vers les miroirs Debian sertis par
  AWS CloudFront/Global Accelerator/Cloudflare déclenchait des connect/send_data. Ajout des
  plages `99.86.` (AMAZO-CF /16) et `3.162.` (CloudFront /14) à `allowlist_public_networks`,
  et clarification du process worker `https` (`/usr/lib/apt/methods/https`, transport TLS
  d'apt) dans `allowlist_egress_procs`.

---

## 21. `kunai_rules_local.nu` n'est PAS transformable en règles kunai `.kun` (décision)

**Type :** décision (2026-08-17)

Question posée : « `kunai_rules_local.nu` est-il transformable en règles kunai `.kun` ? »
Réponse : **non**, et c'est une propriété voulue de l'architecture, pas une limite à lever.
C'est le pendant exact de la séparation pot commun / contexte local documentée en §16.

**Rappel du rôle des deux fichiers :**
- `kunai_rules.nu` + `kunai_rules/rules_v0.1/*.kun` : **générique** = phénotypes d'ATTAQUE
  réutilisables (« si X alors ALERTE »). Transparent en gene : une règle `.kun` = 1 alerte.
- `kunai_rules_local.nu` : **contexte machine** = allowlists de l'hôte (« si X alors NE PAS
  alerter ») : agents, utilitaires bénins, IP/ports/chemins/signaux légitimes.

**Pourquoi la conversion échoue (4 raisons) :**

1. **Nature du DSL gene = détection, pas exclusion.** Une règle `.kun` décrit `match-on`
   (quels événements) + `condition` → produit un événement détecté. Il n'existe pas de mécanisme
   natif « allowlist de process/chemin » ni d'exclusion : chaque règle sort une alerte qu'il
   faudrait ENSUITE retirer par le contexte local. gene mélangerait détection et contexte dans
   une même règle, l'opposé de l'architecture choisie.

2. **Logique relationnelle (`not_legit`).** Le moteur (`kunai_detect_compromise.nu:50`) classe
   un process bénin si `task ∈ legit` OU (`task ∈ benign_utilities` ET `parent ∈ legit ∪ utils`).
   Cette dépendance à la **chaîne de processus (parent)** est relationnelle : gene n'a pas de
   notion de parentage pour cela. Un `.kun` ne peut pas exprimer « bénin si le parent est légitime ».

3. **Pas de littéral de liste en gene.** gene n'a **ni `in [...]`, ni opérateur de liste**
   (cf. §18). `legit_agents` (≈62 entrées) ou `benign_utilities` (≈60) deviendraient chacun une
   condition `or` gigantesque, fragile et illisible.

4. **Contre la portabilité du pot commun.** Les IP/chemins/agents de `local_cfg` sont propres à
   CET hôte/registre (ex. `10.6.255.106`, `registry.iutbeziers.fr`, `~/.cargo` de l'utilisateur
   `nushell`). Les glisser dans les `.kun` casserait la réutilisabilité du pot commun générique.

**Ce qui EST (déjà) exprimable en `.kun`** : uniquement les plages réseau via regex `~=`
(`starts_with_any` du moteur en est exactement la transposition), ex. un `exfil_public_egress`
avec `allowlist_public_networks` en négation regex. Mais même ces plages restent du contexte
local et NE doivent PAS être dans les `.kun` génériques.

**Conclusion immuable :** le contexte machine vit dans `kunai_local_cfg.nu` (consommé par le
moteur nushell/polars, via l'interface `kunai_rules_local.nu` : `not_legit`, `expr-not egress
allowlist`, `expr-not build/initramfs`). Ne pas tenter de convertir `local_cfg` en `.kun`. La
seule zone de traduction légitime est le pot commun `kunai_rules.nu -> .kun` (déjà fait, §16),
jamais l'allowlist locale.

---

## 22. Paramétrisation par FONCTION — `kunai_local_cfg.nu` (décision 2026-08-17)

**Type :** décision

`kunai_rules_local.nu` est devenu une INTERFACE MINCE : la source de vérité des allowlists
machine vit dans **`kunai_local_cfg.nu`**, un fichier de DONNÉES à deux étages :

- **socle `default`** — générique, strict, identique à l'ancien contenu de `kunai_rules_local.nu`
  (62 `legit_agents`, 32 `allowlist_public_networks`, `dns_ips` = `10.6.255.106`, …) ;
- **`function_profiles`** — des profils **par FONCTION** (thématiques d'allowlist) combina­bles :
  `dns` / `network` / `docker` / `elk` / `kube`. Chaque fonction ne touche QUE les clés de sa
  thématique ; le profil `all` (DÉFAUT) les prend TOUS.

Cette refonte (commit `2078edd`, 2026-08-17) remplace l'ancien modèle **par machine** (profil
`elastic` clavé sur le hostname tpot) : l'auto-détection par `hostname` échouait car la machine
d'analyse (PC-JMP) n'avait pas de clé → retombait sur `default` et le bruit `127.0.0.11` du
registre tpot n'était pas filtrable sans `--profile elastic` explicite. Désormais la sélection
par défaut est `all` (toutes fonctions), sans rien configurer.

**Répartition des fonctions (ordre canonique, cf. `fn_order`) :**
- `dns` — résolveurs DNS légitimes (`dns_ips` += `127.0.0.11`, `127.0.0.1`) + requêtes DNS
  internes (`allowlist_dns_queries` += `epr.elastic.co`, `infra-cdn.elastic.co`, `elasticsearch`).
- `network` — egress réseau légitime universel (`allowlist_egress_ports` `[443,80,53,123]`,
  `allowlist_egress_procs` `['https']`, redondant avec le socle).
- `docker` — réseaux docker/hôte internes vus en `::ffff:` (`allowlist_public_networks` +=
  `::ffff:172.16.–172.31.`, `::ffff:10.6.`, `::ffff:127.0.0.`, `::ffff:51.89.`) + `127.0.0.11`.
- `elk` — stack Elasticsearch/Logstash/Kibana (`allowlist_egress_paths` = chemins
  `/usr/share/{elasticsearch,logstash,kibana}/`, `allowlist_public_networks` += `34.120.`,
  `allowlist_dns_queries` += requêtes ELK).
- `kube` — (réservé, vide) réseaux/services Kubernetes.

Le **socle `default`** (mécanismes du socle : `legit_agents`, `benign_utilities`, chaîne build,
initramfs, `benign_signals`) est TOUJOURS appliqué quel que soit le choix de fonctions.

**Sélection des FONCTIONS** (priorité décroissante, dans `current_profile`/`load_cfg`) :
1. `local_cfg <clé> --profile "a,b"` explicite (liste CSV de fonctions) ;
2. `$env.KUNAI_PROFILE` — **posé par le moteur** depuis son flag `--profile`/`-p` dans `main` ;
3. sinon `all` (toutes les fonctions, dans l'ordre canonique ci-dessus).

`current_profile` retourne la LISTE des fonctions actives ; `load_cfg` part du socle puis
CONCATÈNE chaque fonction active dans l'ordre (fusion `merge_lists`, sémantique `__append__`).

**Fusion socle+fonctions** (dans `load_cfg`) : le socle est la base ; chaque fonction n'étend QUE
les clés de sa thématique ; liste SANS marqueur → REMPLACE celle du socle ; liste portant
`['__append__', …]` → CONCATÈNE (dédupliqué) dans l'ordre des fonctions actives. `load_cfg`
retourne une record plate avec TOUTES les clés attendues par le moteur.

**Nouvelles clés profilables (vides par défaut)** pour le bruit de la stack ELK/tpot (386 connect
+ 24 311 send_data + 28 dns_query sur le rapport de scan initial) :
- `allowlist_egress_paths` — MATCH `command_line` (`contains_any`), corrélé dans `detect_connect`
  et `detect_send_data` en plus de `allowlist_egress_procs` (`task_name` instable pour les
  threads ELK : `elastic..][T#3]`, `node/libuv-worker`). Fonction `elk` : `/usr/share/
  elasticsearch/`, `/usr/share/logstash/`, `/usr/share/kibana/`.
- `allowlist_dns_queries` — MATCH `query`, corrélé dans `detect_dns_query` : neutralise les 3
  signaux (length / non_standard_dns_server / tld) d'une requête bénigne. Fonctions `dns`/`elk` :
  `epr.elastic.co`, `infra-cdn.elastic.co`, `elasticsearch` (résolus via le résolveur Docker
  `127.0.0.11`, ajouté à `dns_ips`).

> **SUPERSEDÉ (2026-08-18, refactor flux)** : `allowlist_egress_paths` (MATCH `command_line`) et
> `allowlist_egress_procs` (MATCH `task_name`) ne sont **plus consommées par les familles réseau**
> (connect/send_data/dns_query). La décision egress est désormais de FLUX pur (IP + port réputés,
> + corrélation DNS→FQDN en contexte). Les clés restent définies dans `kunai_local_cfg.nu` mais sont
> mortes côté moteur réseau. La corrélation DNS→connect/FQDN ne se supprime plus par chemin de
> command_line : le FQDN est une colonne de CONTEXTE du rapport. Voir §25.

**Préfixes IP** : les réseaux docker/privés doivent être au format IPv4-mappé (`::ffff:172.19.`…)
car kunai rend les adresses ainsi ; `allowlist_public_networks` matche un préfixe exact via
`starts_with_any`.

**Robustesse liste vide** : `starts_with_any`/`contains_any` retournent `polars lit false` sur une
liste VIDE, sinon une regex `^()`/`(?:)` matcherait la chaîne vide (donc chaque ligne) et
annulerait le filtre. Correctif générique sain pour toutes les familles.

**Pièges nushell corrigés pendant la réécriture :**
- `get -i` déprécié (0.114) → `--optional`.
- `is-null` n'existe pas → comparaison `== null`.
- `list ++ list` pour concaténer (PAS `append`, qui imbrique un élément au lieu d'aplatir),
  puis `uniq` pour l'union des clés socle+fonctions.

Validé (profil par défaut `all` = socle + toutes fonctions) : 
- tpot `kunai_tpot/events.log.3923.gz` → **connect 0, send_data 54, dns_query 0** (zéro FP du
  réseau docker ELK), sans aucun `--profile` explicite ;
- non-régression ngsoti `15e67237…` → connect 18, send_data 9280 (identique avant/après).
- Sélection restreinte vérifiée : `--profile "dns"` garde le socle + dns (dns_ips =
  10.6.255.106 + 127.0.0.11 + 127.0.0.1) sans les chemins ELK ; `--profile "docker"` donne
  dns_ips = 10.6.255.106 + 127.0.0.11 (sans 127.0.0.1).

## 23. Bug kunai `dst_public` sur RFC1918 IPv4-mappées — correctif GÉNÉRIQUE (2026-08-17)

**Constat** : sur un registre tpot (`kunai_tpot/events.log.3923.gz`), le bruit n'était PAS réglé
par les allowlists ELK seules (connect 386→384, send_data 24311→24307) : il subsistait car la
cause racine était ailleurs que dans les `command_line` ELK.

**Diagnostic** : kunai rend `dst_public=true` pour des adresses **IPv4-mappées `::ffff:` privées
RFC1918** du réseau docker interne (`::ffff:10.6.255.134`, `::ffff:172.19.0.8`, `::ffff:127.0.0.1`).
La colonne source `dst_public` est donc **erronée** pour ces plages : le trafic hôte/docker
interne (logstash `[http_output]>w` vers logstash/ES, elasticsearch `elastic..][T#N]` entre nœuds)
est qualifié à tort de `public_egress`.

**Correctif GÉNÉRIQUE (dans le moteur, pas dans le local)** : nouveau helper
`is_private_dst` (`kunai_detect_compromise.nu`) qui renvoie `true` quand `dst_ip` (brute OU
`::ffff:`) commence par une plage privée : RFC1918 `10.`, `172.16.–172.31.`, `192.168.`, bouclage
`127.`, link-local `169.254.`. Dans `detect_connect` et `detect_send_data` :
`c_public = (dst_public == true) AND (NOT is_private_dst)`. Un RFC1918 mappé n'est JAMAIS un
C2 externe → ce n'est pas de l'egress public.

**Résultat sur le registre tpot (profil par défaut `all`)** :
- connect 384 → **0** (tout `::ffff:10.6.255.134` interne).
- send_data 24307 → **54** : restent UNIQUEMENT les destinations réellement PUBLIQUES
  (`51.89.235.201` OVH ×51, `16.176.125.169` CloudFront ×2, `52.180.136.250` Azure ×1) = le vrai
  signal d'exfiltration à investiguer. Aucun RFC1918 survivant.
- Non-régression : ngsoti `15e67237…` identique avant/après (connect 18, send_data 9280, tous
  publics, 0 RFC1918 résiduel), en défaut ET avec fonctions restreintes.

**À retenir** : ne JAMAIS se fier à la colonne `dst_public` seule pour qualifier un egress
public ; filtrer d'abord les plages privées/mappées (correction générique, pas une allowlist
locale). Cela ne masque aucune compromission : le trafic privé/docker n'est pas une exfiltration
externe, et les destinations publiques réelles restent signalées.

---

## 24. Refactor EGRESS : suppression de la confiance process → analyse de FLUX IP/port + FQDN (2026-08-18)

**Décision utilisateur (profondément lié au travail de this session)** : remplacer la confiance
par **nom de processus** (allowlists `legit_agents`/`benign_utilities`/`allowlist_egress_procs`/
`allowlist_egress_paths`) par une analyse de FLUX réseau (IP réputée, port standard, corrélation
DNS→FQDN). **L'exigence « zéro FP » est ABANDONNÉE** ; Option A = corrélation DNS→connect **interne**
(sans source externe). Objectif : faire disparaître `kunai_local_cfg`/`kunai_rules_local.nu` de la
**décision egress**.

### Ce qui a changé dans le moteur (`kunai_detect_compromise.nu`)

- **`not_legit` retiré des familles réseau** (connect/send_data/dns_query). Il reste pour les
  familles **non-réseau** : execve (:375), file_create (:464), kill (:657), bpf_prog_load (:674),
  prctl (:720). `legit_agents`/`benign_utilities` ne sont plus consultés par l'egress.
- **`detect_connect` / `detect_send_data`** : suppression de `c_allow_proc` (task_name),
  `c_allow_path` (command_line), `c_allow_proc_or_path`. La réputation devient :
  `c_reput_net` (IP ∈ `allowlist_public_networks`) `and` `c_reput_port` (port ∈ `allowlist_egress_ports`)
  **ou** `c_ntp_port` (port == 123 seul). `c_egress_susp = c_public and (not c_reput)`.
- **`detect_dns_query`** : plus de filtre `not_legit` ; la requête est jugée sur sa forme (FQDN)
  et son résolveur, quel que soit le process.

### Corrélations & contexte

- **Helpers nouveaux** : `dns_fqdn_map [base]` (extrait les dns_query, explode la réponse `;`,
  table plate `{task_pid, resolved_ip, fqdn}`, lazy) et `annotate_fqdn [df, dmap]`
  (`polars join` left + coalesce `fqdn` null→`""`). **Appel impératif dans un pipeline** :
  `| (annotate_fqdn $in $dmap)` (comme `unnestif`), sinon erreur du parser `missing dmap`.
- Le FQDN d'une corrélation DNS→connect est **une colonne de CONTEXTE du rapport** (`fqdn`),
  jamais une suppression automatique. Ajouté à `cols_keep` → rendu par `render_report` sans modif.
- **`is_wildcard_dst []`** : `0.0.0.0`/`::`/`::1` = binds d'écoute, jamais une exfiltration.
  Intégré dans `c_public` (`... and (not c_wildcard)`).

### Critère de FLUX NTP (port 123)

Le pool NTP public change d'IP en continu → **impossible à allowlister par IP**. La synchro système
(chronyd/ntpd) port 123 est du trafic de fond bénin, pas un canal d'exfil exploitable (petits
paquets fixes ; toute anomalie reste vue par send_data). => **le port 123 seul suffit à rendre
l'egress réputé** (sans identité process). Port 80/443/53 restent gated sur la réputation IP
(C2 peut les abuser : DNS vers 8.8.8.8 reste signalé).

### Mécanique de dépréciation

- `allowlist_egress_ports` (socle `[443,80,53,123]` + profil `network`) et
  `allowlist_public_networks` : toujours actifs.
- `allowlist_egress_procs` / `allowlist_egress_paths` : **plus consommées par l'egress**. Clés
  conservées dans `kunai_local_cfg.nu` (extensibilité / docs), mortes côté moteur réseau.

### Non-régression validée (2026-08-18)

| échantillon | avant → après (connect) | remarque |
| --- | --- | --- |
| `22e4a57a` (référence) | 10 → **10** | fqdn=api.ipify.org sur oom_reaper→Cloudflare, dns_query 4 |
| `15e67237` | 28 → **18** | 10 chronyd NTP (port 123) enfin silencieux ; reste le C2 réel `sample.bin`→66.23.233.179:9375 + DNS 8.8.8.8 |
| send_data `22e4a57a` | 807 → **806** | 1 beacon NTP en moins, C2 présent |
| send_data `15e67237` | 9286 → **9280** | idem |

**Piège redis couvert** : ne JAMAIS se fier à `dst_public` seul (RFC1918 `::ffff:` mappées, §23) ;
appliquer `c_public = (dst_public==true) and (not is_private_dst) and (not is_wildcard_dst)`.
