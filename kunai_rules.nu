#!/usr/bin/env nu
# kunai_rules.nu
#
# POT COMMUN de règles génériques — version EXÉCUTABLE en nushell.
#
# Chaque règle générique ici correspond au fichier `.yaml` kunai homonyme placé
# dans `kunai_rules/rules_v0.1/` (le pot commun interopérable, réutilisable par
# tout l'écosystème kunai). Ce module ré-expose les MÊMES indicateurs sous forme
# d'expressions polars lazy prêtes à être consommées par le moteur d'analyse
# (kunai_detect_compromise.nu). Les règles sont génériques : elles ne dépendent
# d'AUCUN contexte machine. Le contexte local (allowlists agents/réseaux/chemins)
# vit dans kunai_rules_local.nu et c'est le MOTEUR qui compose les deux.
#
# Convention : une règle `r_<famille>_<nom>` retourne une Expr polars (la
# condition de détection) ; l'evidence affichée est donnée par
# `r_ev_<famille>_<nom>`. Les familles n'ayant qu'une évidence fixe portent
# directement leur libellé dans le moteur.

# =====================================================================
# execve — exécution suspecte
# =====================================================================

# T1059.004 — reverse shell natif BASH/SH : ouverture /dev/tcp ou /dev/udp
# par un interpréteur shell, sans exiger -i. Aucun usage légitime en ligne de
# commande : indicateur de compromission active le plus fiable.
export def r_execve_reverse_shell [] {
    (polars col command_line) | polars contains "(?:\\b(bash|sh|zsh|ksh|dash)\\b[^|]*?\\b(?:/dev/tcp/|/dev/udp/))"
}
export def r_ev_execve_reverse_shell [] { "reverse_shell" }

# T1059.004 — shell interactif (-i) avec outil réseau (nc/ncat/socat) ou redirection.
export def r_execve_shell_tool [] {
    (polars col command_line) | polars contains "(?:/bin/(?:bash|sh|zsh|ksh) -[ic].*(?:nc |ncat|/dev/tcp/|/dev/udp/|socat)|(?:bash|sh|zsh) -[ic].*(?:/dev/tcp/|/dev/udp/))"
}
export def r_ev_execve_shell_tool [] { "reverse_shell" }

# T1105 / T1059.004 — téléchargement puis exécution en une ligne (curl|wget | sh).
export def r_execve_download_exec [] {
    (polars col command_line) | polars contains "(?:curl|wget) .*(?:\\|| \\-o| \\-O).*(?:sh|bash|python3?|perl)"
}
export def r_ev_execve_download_exec [] { "download_and_execute" }

# T1027 / T1140 — exécution obfusquée / décodage inline (base64, perl -e, py -c).
export def r_execve_obfuscated [] {
    (polars col command_line) | polars contains "(?:base64 -d|base64 -D|xxd -r|openssl enc|perl -e|python3? -c|php -r)"
}
export def r_ev_execve_obfuscated [] { "obfuscated_exec" }

# T1588.002 — outil offensif connu (scanners, forceurs, droppers, implant).
export def r_execve_offensive_tool [] {
    (polars col command_line) | polars contains "(?:nmap|masscan|hydra|medusa|john|hashcat|mimikatz|msfconsole|meterpreter|procdump|sqlmap|nikto|dirb|gobuster)"
}
export def r_ev_execve_offensive_tool [] { "offensive_tool" }

# T1204 / T1059 — exécution depuis un répertoire temporaire (/tmp, /dev/shm…).
# Motif atomique générique ; la levée réelle exige la non-appartenance à la
# chaîne build (contexte local) : le moteur compose (voir detect_execve).
export def r_execve_from_tmp [] {
    (polars col command_line) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/)[^ ]*(?:\\.sh|\\.py|\\.pl|\\.elf|\\.bin|\\.out| )"
}
export def r_ev_execve_from_tmp [] { "exec_from_tmp" }

# =====================================================================
# file_create — drop / persistance
# =====================================================================

# T1505.003 — webshell : création d'un script serveur (php/asp/jsp/cgi/py/rb/pl)
# dans un document-root web. Un serveur légitime n'y crée pas de script exécutable
# neuf : indicateur fort de compromission web.
export def r_filecreate_webshell [] {
    let docroot = ((polars col main_path) | polars contains "^/var/www(/|$)|^/srv/www(/|$)|^/usr/share/nginx/html(/|$)|^/usr/share/apache2(/|$)|^/opt/[^/]*/(htdocs|www|public_html|webroot)(/|$)")
    let ext = ((polars col main_path) | polars contains "\\.(php|php5|php7|phtml|asp|aspx|jsp|cgi|py|rb|pl)$")
    $docroot and $ext
}
export def r_ev_filecreate_webshell [] { "webshell_drop" }

# T1564.001 — fichier caché (préfixe '.') déposé dans /tmp|/dev/shm|/var/tmp.
# Motif atomique ; côté moteur il est corrélé à l'exécution effective (drop-and-run)
# pour ne pas flagger les artefacts cachés légitimes jamais exécutés.
export def r_filecreate_hidden_tmp [] {
    (polars col main_path) | polars contains "^/tmp/\\.[^/]+|^/dev/shm/\\.[^/]+|^/var/tmp/\\.[^/]+"
}
export def r_ev_filecreate_hidden_tmp [] { "hidden_tmp" }

# =====================================================================
# connect — réseau sortant suspect
# =====================================================================

# T1095 / T1496 — connexion vers un port C2 / backdoor / Tor / pool de minage,
# sur une destination PUBLIQUE. Le test `dst_public` est appliqué par le moteur.
export def r_connect_unusual_port [] {
    ((polars col dst_port)
     | polars is-in ([4444,4445,6667,31337,9001,8888,8443,1337,2222,161,137,445,9999,49152,9050,9051,9150,5555,7777,14444,14433,45700,3256,20535,3333] | polars into-df))
}
export def r_ev_connect_unusual_port [] { "unusual_port" }

# T1041 — exfiltration : envoi de données volumineuses vers une destination
# publique non allowlistée (composition moteur avec le contexte local).
export def r_connect_public_egress [] { ((polars col dst_public) == true) }
export def r_ev_connect_public_egress [] { "public_egress" }

# =====================================================================
# send_data — exfiltration
# =====================================================================

# T1041 — volume d'envoi anormalement élevé (> 1 Mo) vers destination publique.
export def r_senddata_large [] { ((polars col data_size) > 1000000) }
export def r_ev_senddata_large [] { "large_data" }

# T1030 / T1001 — envoi à entropie élevée (contenu chiffré/randomisé) vers public.
export def r_senddata_high_entropy [] { ((polars col data_entropy) > 7.5) }
export def r_ev_senddata_high_entropy [] { "high_entropy" }

# =====================================================================
# dns_query — recon / tunneling
# =====================================================================

# T1568.002 — TLD dynamique / suspects (.tk, .ml, .ga, …, .onion, .i2p).
export def r_dns_suspicious_tld [] {
    (polars col query) | polars contains "(?:\\.tk$|\\.ml$|\\.ga$|\\.cf$|\\.gq$|\\.top$|\\.xyz$|\\.pw$|\\.onion$|\\.i2p$)"
}
export def r_ev_dns_suspicious_tld [] { "suspicious_tld" }

# T1071.004 — requête DNS anormalement longue (> 60) : candidat au tunneling DNS.
export def r_dns_long_query [] { (((polars col query) | polars str-lengths) > 60) }
export def r_ev_dns_long_query [] { "suspicious_length" }

# =====================================================================
# kill — perturbation / évasion
# =====================================================================

# T1489 — arrêt d'un service critique (agents, daemon de sécurité, systemd…).
export def r_kill_critical_target [] {
    (polars col target_task_name) | polars contains "(?:docker|containerd|sshd|systemd|wazuh|crowdsec|splunk|check_mk|auditd|cron|agent)"
}
export def r_ev_kill_critical_target [] { "kill_critical" }

# T1489 — terminaison forcée SIGKILL hors cycle de vie normal.
export def r_kill_hard_signal [] { ((polars col signal) == 'SIGKILL') }
export def r_ev_kill_hard_signal [] { "SIGKILL_hard" }

# =====================================================================
# mmap_exec — injection / drop-and-run
# =====================================================================

# T1055.001 — exécution (mmap exec) depuis une zone temporaire / fd / memfd.
# Motif atomique ; la levée réelle exclut la chaîne build (contexte local).
export def r_mmapexec_from_tmp [] {
    (polars col mapped_path) | polars contains "(?:/tmp/|/var/tmp/|/dev/shm/|/run/shm/|/proc/self/fd/|memfd:)"
}
export def r_ev_mmapexec_from_tmp [] { "mmap_exec_suspicious" }

# =====================================================================
# prctl — évasion
# =====================================================================

# T1622 — modification PR_SET_DUMPABLE (évasion / anti-capture de mémoire).
export def r_prctl_dumpable [] { ((polars col option) == 'PR_SET_DUMPABLE') }
export def r_ev_prctl_dumpable [] { "dumpable_change" }
