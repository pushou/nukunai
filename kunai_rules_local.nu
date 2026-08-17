#!/usr/bin/env nu
# kunai_rules_local.nu
#
# RULES LOCALES — INTERFACE d'accès au contexte machine PARAMÉTRABLE.
#
# Ce fichier n'est PLUS la source de vérité des allowlists : c'est une INTERFACE
# (contrat) vers `kunai_local_cfg.nu`, le fichier de DONNÉES où vivent désormais :
#   * le SOCLE générique `default` (commun à toutes les machines, strict) ;
#   * des `function_profiles` PAR FONCTION (thématiques d'allowlist) combina­bles :
#     dns / network / docker / elk / kube. Le profil `all` (DÉFAUT) les prend TOUS.
#
# Le moteur (kunai_detect_compromise.nu) continue d'appeler :
#   use kunai_rules_local.nu local_cfg
#   local_cfg legit_agents                  # profil `all` par défaut (toutes fonctions)
#   local_cfg allowlist_egress_paths --profile "docker,elk"   # fonctions restreintes
#
# La sélection des fonctions et la fusion socle+fonctions sont gérées dans
# kunai_local_cfg.nu (load_cfg), pas ici — ce fichier reste volontairement mince.
#
# ── Sélection des fonctions (dans load_cfg, priorité décroissante) ──────
#   1. `--profile` explicite (liste CSV de fonctions, ex. "dns,network") sur un
#      appel `local_cfg <clé> --profile "a,b"` ;
#   2. `$env.KUNAI_PROFILE` (posé par le moteur depuis son flag --profile) ;
#   3. sinon → `all` (toutes les fonctions dans l'ordre canonique).
# Dans tous les cas, le SOCLE `default` est toujours appliqué en base.

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
