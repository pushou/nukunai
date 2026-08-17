# AGENTS.md — nukunai

Guide de travail pour les agents/éditeurs sur le dépôt **nukunai** : scripts nushell + polars
pour analyser, filtrer et détecter des compromissions dans des logs **kunai** (EDR événementiel).

## Qu'est-ce que c'est

- `kunai_detect_compromise.nu` — **moteur de détection** de compromission (9 familles).
  C'est le fichier central. Lit `.gz` / `.jsonl` / `.parquet`, produit des rapports `.md` + `.json`.
- Savoir-vivre du dépôt : **une grande partie de la logique métier vit dans les RÈGLES**, pas
  dans le moteur. Toujours penser en « pot commun générique » vs « contexte local machine ».
- Lire `README.md` et `MEMORIES.md` avant de modifier — les pièges nushell/polars y sont documentés.

## Architecture (refactor `05bbbe2`)

```
kunai_detect_compromise.nu   moteur : compose générique + local, 9 familles, rapports
kunai_rules.nu               POT COMMUN exécutable : r_<fam>_<nom> -> Expr polars (générique, sans contexte machine)
kunai_rules_local.nu         CONTEXTE LOCAL machine "registry" : local_cfg (allowlists agents/réseaux/chemins/build)
kunai_rules/rules_v0.1/*.kun   pot commun interopérable kunai/gene (mêmes indicateurs que kunai_rules.nu)
kunai_to_parquet.nu          conversion gz/jsonl -> parquet (lazy par défaut)
kunai_to_flatten_parquet.nu  conversion + flatten complet
kunai_filter_events.nu       filtre + explore / sauvegarde parquet
kunai_events_analysis.nu     compteurs d'événements
kunai_print_events_table.nu  table code->nom d'événement
ngsoti_detail.nu / ngsoti_all.nu / ngsoti_report.nu   wrappers sur le dataset ngsoti
```

## Architecture des règles — OÙ METTRE QUOI

- Une règle **générique** (phénotype d'attaque réutilisable) → `kunai_rules.nu` ET son pendant
  `kunai_rules/rules_v0.1/<nom>.<famille>.detection.kun`. Convention : `r_<famille>_<nom>`
  retourne une Expr polars, `r_ev_<famille>_<nom>` retourne le libellé d'evidence.
- Une **allowlist / contexte machine** (agents de l'hôte, IP locales, ports/chemins bénins,
  chaîne build rustup/cargo/rustc/cc…) → `kunai_rules_local.nu`, seeds de `local_cfg`.
- Le **moteur** consomme `use kunai_rules_local.nu local_cfg` et `use kunai_rules.nu *`, puis
  **compose** en gardant les règles génériques MAIS en retirant le bruit local (build, egress
  allowlisté, signaux bénins).

## Règles métier / contexte local (résumé)

- `legit_agents` : vrais agents/services (dpkg, gitlab, dockerd, containerd, systemd, wazuh,
  crowdsec, splunk, check_mk, nginx, sshd, cron, dbus, polkit, NetworkManager, unattended-upgrade,
  chronyd/chrony/systemd-timesyn*, audit*). Les services de synchro NTP (chronyd) sont des
  agents système légitimes : leur trafic port 123 vers le pool Debian NTP n'est PAS une
  exfiltration (rejoint not_legit). Ne pas les retirer sous peine de FP public_egress.
- `benign_utilities` : outils DÉTOURNABLES (docker, curl, wget, chmod, chown, tar, cp, bash…).
  Ils ne sont bénins QUE s'ils viennent d'une chaîne légitime (voir `not_legit`).
- `allowlist_build_procs` / `allowlist_build_paths` : chaîne de build Rust (rustup, cargo,
  rustc, cc, as, /tmp/cargo-*, ~/.cargo, ~/.rustup…) dont l'activité scratch est bénigne.
- `allowlist_initramfs_paths` / chaîne initramfs : écriture/exécution de la génération
  initramfs (mkinitramfs/dracut/update-initramfs sous /var/tmp/mkinitramfs_*, /usr/lib/dracut)
  jamais utilisée par de la persistance malveillante — même logique que allowlist_build.
- `allowlist_egress_procs/ports/networks` : egress réseau légitime (cargo/git/apt/docker →
  CDN Fastly, GitHub, GitLab/Cloudflare, registry docker local, ports 443/80/53).
  Inclut les miroirs apt Debian desservis par AWS CloudFront/Global Accelerator/Cloudflare
  (dont les plages 99.86./3.162. AMAZO-CF) et le process worker `https` — transport TLS
  d'apt (/usr/lib/apt/methods/https) qui télécharge les paquets.
- `benign_signals` : SIGURG/SIGCHLD/SIGCONT/SIGWINCH/SIGIO/SIGPIPE (régulation docker/Go, jamais un kill suspect).

## Concepts de détection (à préserver)

- **not_legit** (`kunai_detect_compromise.nu:50`) : bénin si task∈legit OU (task∈utils ET parent∈legit∪utils).
  Le parent SEUL ne suffit PAS (sinon un rootkit daemonisé sous systemd, ex. perfctl→oom_reaper,
  serait masqué). Ne pas re-supprimer cette nuance.
- **drop-and-run** (file_create / mmap_exec / exec_from_tmp) : un artefact tmp n'est suspect QUE
  s'il est réellement exécuté ET que l'écrivain n'est pas la chaîne build.
- **send_data/connect public_egress** : le trafic chiffré/entropie élevée vers une cible
  allowlistée (TLS agent/client interne, buildx→registry) n'est PAS une exfiltration. Corréler à
  `dst_public` + `expr-not egress_allowlist`.
- **9 familles** : execve, file_create, connect, send_data, dns_query, kill, bpf_prog_load,
  mmap_exec, prctl.

## Pièges nushell / polars (IMPORTANT)

- **Expr polars** : PAS de `polars or`/`polars and` → utiliser `or`/`and` nushell entre parenthèses : `(($a) or ($b))`.
- **Chaînes `$"…"`** : ne jamais y mettre de parenthèses littérales non-échappées (`'(x)'`) sinon execution → plantage.
- **signal / option (kill, prctl)** sont des STRINGS : `== 'SIGKILL'`, `== 'PR_SET_DUMPABLE'`.
- **Colonnes Int128** non convertibles en nu → ne sélectionner que des colonnes sûres avant
  `collect` (helpers `has_events`/`cols_keep`/`empty_like`).
- **Formats kunai variables** : colonnes absentes selon événements → utiliser `unnestif`,
  `cols_keep`, `normalize_mapped` (ne PAS faire un `polars unnest`/`select` sur une colonne absente).
- `starts_with_any` échappe les préfixes en regex ancrée `^(p1|p2|…)` (match de plage CIDR / chemin).
- **Règles gene `.kun`** : TOUT littéral numérique NU en RHS de comparaison est rejeté au load par
  `kunai replay` (même 0.6.2) → **toujours QUOTER les nombres** : `== '31337'`, `> '1000000'`, `> '7.5'`.
- **Replay synthétique** : le gate `match-on` de gene compare à l'id NUMÉRIQUE de l'événement, pas au nom.
  Un `connect` synthétique doit avoir `info.event.id = 60` (`Type::Connect`); `id=8` (=Kill) fait échouer
  silencieusement toute règle connect. Détails dans MEMORIES.md §18.

## Lancer / vérifier

- `nu kunai_detect_compromise.nu -f <famille> <fichier.gz|parquet>` — test d'une famille.
- `FILE=logs/ngsoti/<hash>/kunai.jsonl.gz nu ngsoti_detail.nu` — rapport détaillé d'un échantillon.
- `nu kunai_detect_compromise.nu` — les 2 fichiers `.gz`/`.parquet` les plus récents du registre.
- Registre réel : `/run/media/pouchou/SSD2T/ips-ids-siem-pcaps/kunai/kunai_registry/kunai`.
- Après une modif, se valider sur un échantillon ngsoti ET de préférence un .gz du registre (non-régression).
- Pas de linter dédié : l'exécution nushell (`nu <script>` sans erreur) est le test de syntaxe/pipeline.

## Git

- Branche de travail courante : **deepseek** (avec `main`). Vérifier `git status` avant tout commit.
- Ne committer/pousser que si explicitement demandé.
- `logs/`, `*.parquet`, `*.gz`, `*.log`, `sessions/`, `themes/` sont gitignorés (données volumineuses/artefacts).
