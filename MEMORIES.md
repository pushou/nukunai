# Mémoires Jcode — Projet nukunai

Sauvegarde des 15 mémoires jcode (scope projet) liées à nukunai.
Date d'export : 2026-08-15

---

## 1. Refactor bruit — legit_agents / benign_utilities

**Type :** fact
**id :** mem_1786816350141_6214528666275903692

REFACTOR bruit — docker/chmod/chown/etc ne sont PLUS légitimes par défaut. Le cfg a maintenant 2 listes:
- **legit_agents** (vrais agents: dpkg, y compris dpkg-db-backup ajouté, gitlab, dockerd, containerd, systemd, wazuh, crowdsec, splunk, check_mk, nginx, sshd, cron, dbus, polkit, NetworkManager, unattended-upgrade, audit)
- **benign_utilities** (docker, grep, sed, cat, tar, gzip, bzip2, xz, gpg, gpgv, sh, bash, curl, wget, chmod, chown, install, cp, mv, rm, cmp, mktemp, rmdir, touch, mkdir, ln, readlink, basename, dirname, date, sleep, printf, echo, nproc, lsblk, df, free, pgrep, ps, ss, ip, hostname, uname)

Fonction `not_legit` unifiée (remplace les anciennes `not_legit` + `not_legit_parent`): ligne bénigne si task∈legit OU parent∈legit OU (task∈utils ET parent∈(legit∪utils)). Résultat: un docker/chmod/chown/curl/tar lancé par un process NON légitime est DÉTECTÉ, bénin que sous chaîne légitime.

Validé: 0 alerte execve sur échantillon 20558 (bruit neutralisé), kill=495 OK, et 9/9 cas de test contrôlés corrects.

**SYNTAXE EXPR POLARS :** pas de `polars or`/`polars and` — utiliser les opérateurs nushell `or`/`and` entre parenthèses, ex: `(($a) or ($b))`. Le MCP nushell a été temporairement indisponible, contourné via nu local + source.

---

## 2. Affinage bruit execve — not_legit_parent

**Type :** fact
**id :** mem_1786813922033_14951042412233515830

Affinage bruit execve fait. Nouvelle fonction `not_legit_parent` (exclut quand le processus PARENT est dans cfg legit_agents), appliquée dans detect_execve après not_legit. Résultat sur fichier 20550: execve passe de 1389 -> 0 alertes (tout était du bruit check_mk_agent: python3 -c, perl -e, date, ps, df... dont le parent est check_mk_agent).

Choix du filtre parent (pas task_name) pour ne pas masquer un vrai python3/perl lancé par un process non-légitime. Limite documentée: un attaquant compromettant check_mk_agent verrait ses commandes invisibilisées. Les autres familles ne sont pas impactées. Le script charge sans erreur.

---

## 3. Second affinage bruit execve — audit et cmp

**Type :** fact
**id :** mem_1786815536683_1184307451535198937

Second affinage bruit execve. Ajouté audid, auditctl, auditd-manager, augenrules, aureport, ausearch et cmp au cfg legit_agents (bloc utilitaires). Élimine le faux positif `cmp -s /tmp/aurules.* /etc/audit/audit.rules` (activité légitime auditd via augenrules, parent).

Validation sur tout 20558 (35956 execve): execve passe de 1 -> 0 alertes après ajout audit/cmp. Série complète execve: 1389 (avant) -> 1 (filtre parent check_mk) -> 0 (audit/cmp). Aucun vrai signal perdu. Les autres familles (send_data 78, file_create 22, kill 665, prctl 131 sur 20558) inchangées et correctes.

**PIÈGE nushell :** dans les chaînes `$"…"` il ne faut JAMAIS mettre de parenthèses littérales non-échappées (ex `'(ignoré)'`) sinon nushell les interprète comme commande -> plantage. Corrigé dans main (fichier tronqué) et dans les tests MCP.

---

## 4. Familles testées via MCP nushell

**Type :** fact
**id :** mem_1786811859408_5284148426722818906

Script `kunai_detect_compromise.nu`. Toutes les 9 familles testées via MCP nushell sur échantillons .jsonl extraits du .gz 20550. Execve/file_create/connect validés avant. Aujourd'hui:
- **send_data** OK
- **dns_query** OK (0 alerte)
- **bpf_prog_load** OK
- **mmap_exec** OK (0 alerte)
- **kill CORRIGÉ** (signal est un string: `== 'SIGKILL'` au lieu de `== 9`)
- **prctl CORRIGÉ** (option est un string: `== 'PR_SET_DUMPABLE'`/`'PR_SET_SECCOMP'` au lieu de 4/22)

Echantillons dans /tmp/s_*.jsonl. Prochaine étape (à l'époque): relancer le script main complet sur le fichier 20550 via bash pour valider l'exécution bout-en-bout.

---

## 5. Emplacement des logs analysés

**Type :** fact
**id :** mem_1786825771762_14303493545520322128

Les logs analysés par nukunai sont stockés dans `~/Nextcloud/dev/dev_nushell/nukunai/logs/ngsoti`, contenant des fichiers jsonl issus de compromissions par malware.

---

## 6. Colonnes Int128 non convertibles

**Type :** fact
**id :** mem_1786825778878_1901008726826200310

Les colonnes **Int128** ne sont pas convertibles en nu et cassent le collect ; le fix renvoie un lazy frame vide (0 ligne, utc_time seul) quand aucune colonne de travail n'existe.

---

## 7. Performances sur registre réel

**Type :** fact
**id :** mem_1786825777526_9641153352617010625

Sur kunai.jsonl-1.gz, kill/mmap/prctl tournent désormais (4/3/4) ; le registre réel .gz (45 Mo) prend ~5 min par fichier avec send_data 44 de bruit.

---

## 8. Résultats validés sur gros fichier

**Type :** fact
**id :** mem_1786825776174_3406323120323324496

Résultats validés sur le gros fichier kunai.jsonl : execve 14, connect 28, send_data 960, dns_query 2 ; le petit fichier kunai.jsonl-1.gz donne execve 3, connect 14, send_data 6, dns_query 2, prctl 3.

---

## 9. perfctl — signature d'infection

**Type :** fact
**id :** mem_1786825775076_17483826071847926676

perfctl — malware dont l'infection est détectée par nukunai (drop /tmp/sample.bin, installation bsd-port/knerl, prctl dumpable).

---

## 10. Pipeline_mismatch — ancien format kunai

**Type :** fact
**id :** mem_1786825773283_16707552533687807444

Le fichier kunai.jsonl-1.gz (ancien format) fait échouer le pipeline à cols_keep avec **pipeline_mismatch** ; le bug venait de formats kunai variables où certaines colonnes (ex. kill sans target_task_name) sont absentes.

---

## 11. Helpers has_events et empty_like

**Type :** fact
**id :** mem_1786825772535_11139053058202369502

Deux helpers ont été ajoutés au script `kunai_detect_compromise.nu` :
- **has_events** : vérifie par collecte ciblée si l'événement est présent
- **empty_like** : retourne 0 alerte

---

## 12. Le logiciel nukunai

**Type :** fact
**id :** mem_1786825771014_12347550826301139653

Le logiciel nukunai (script `kunai_detect_compromise.nu`) analyse des logs kunai au format jsonl.gz et détecte des compromissions par malware, testé sur des fichiers comme kunai.jsonl, kunai.jsonl-1.gz (ancien format), et des registres réels .gz.

---

## 13. Non-régression — OLD vs NEW (parent_legit)

**Type :** fact
**id :** mem_1786818458476_2441449967797627689

Non-regression validated on real .gz 20335: OLD logic (with parent_legit) and NEW logic (without parent_legit) both yield identical results (execve 0, connect 0, send_data 123) — the removal of parent_legit clause compensated by expanded benign utilities causes zero false positives on real traffic.

---

## 14. Support des .jsonl bruts

**Type :** fact
**id :** mem_1786818457865_7972681223104821925

Detection script `kunai_detect_compromise.nu`: infection test file logs/kunai.jsonl (28,801 lines) was successfully run via main after adapting gzip -t integrity check to only apply to .gz files, enabling raw .jsonl support.

---

## 15. Branches du dépôt nukunai

**Type :** fact
**id :** mem_1786809686661_4698222355088632512

Repository nukunai has branches **main** and **deepseek**, both pushed to origin at commit cd43a60.
