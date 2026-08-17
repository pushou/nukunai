#!/usr/bin/env nu
# kunai_rules_local.nu
#
# RULES LOCALES — INTERFACE d'accès au contexte machine PARAMÉTRABLE.
#
# Ce fichier n'est PLUS la source de vérité des allowlists : c'est une INTERFACE
# (contrat) vers `kunai_local_cfg.nu`, le fichier de DONNÉES où vivent désormais :
#   * le SOCLE générique `default` (commun à toutes les machines, strict) ;
#   * les PROFILS par machine / par registre (ex. "elastic" pour la stack ELK/tpot),
#     déduits de l'analyse anti-faux-positifs des rapports de scan.
#
# Le moteur (kunai_detect_compromise.nu) continue d'appeler :
#   use kunai_rules_local.nu local_cfg
#   local_cfg legit_agents            # profil auto-détecté (hostname)
#   local_cfg allowlist_egress_paths --profile elastic   # profil explicite
#
# La sélection du profil et la fusion socle+profil sont gérées dans
# kunai_local_cfg.nu (load_cfg), pas ici — ce fichier reste volontairement mince.
#
# ── Sélection du profil (dans load_cfg, priorité décroissante) ─────────
#   1. `--profile` explicite sur un appel `local_cfg <clé> --profile <nom>`
#   2. `$env.KUNAI_PROFILE` (posé par le moteur depuis son flag --profile)
#   3. `hostname` de la machine analysée
#   4. sinon → `default` (socle générique strict, comportement hérité de l'ancien
#      fichier : c'est exactement l'ex-content du match ci-dessous).

use kunai_local_cfg.nu [load_cfg, current_profile]

# Retourne la valeur d'une clé de config locale, résolue pour le profil courant.
# Accepte désormais toutes les clés du socle ET les clés propres aux profils
# (allowlist_egress_paths, allowlist_dns_queries…).
export def local_cfg [name: string, --profile: string] {
    let cfg = (load_cfg --profile=$profile)
    if (($cfg | get --optional $name) == null) {
        error make { msg: $"règle locale inconnue: ($name) — clés dispo: (($cfg | columns | str join ', '))" }
    }
    $cfg | get $name
}

# Propriété de debug : résout et affiche le profil effectivement utilisé.
#   nu kunai_rules_local.nu --  (non ; lancer :  nu -c 'use kunai_rules_local.nu *; profile')
export def profile [--profile: string] {
    current_profile --profile=$profile
}
