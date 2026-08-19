#!/usr/bin/env nu
# kunai_rules_local.nu
#
# LOCAL RULES — INTERFACE to access the PARAMETRIZABLE machine context.
#
# This file is NO LONGER the source of truth for the allowlists: it is an
# INTERFACE (contract) to `kunai_local_cfg.nu`, the DATA file where now live:
#   * the generic `default` BASE (common to all machines, strict);
#   * `function_profiles` PER FUNCTION (allowlist themes) combinable:
#     dns / network / docker / elk / kube. The `all` profile (DEFAULT) takes them ALL.
#
# The engine (kunai_detect_compromise.nu) keeps calling:
#   use kunai_rules_local.nu local_cfg
#   local_cfg legit_agents                  # `all` profile by default (all functions)
#   local_cfg allowlist_egress_paths --profile "docker,elk"   # restricted functions
#
# Function selection and base+functions merging are handled in
# kunai_local_cfg.nu (load_cfg), not here — this file stays intentionally thin.
#
# ── Function selection (in load_cfg, decreasing priority) ──────
#   1. explicit `--profile` (CSV list of functions, e.g. "dns,network") on a
#      `local_cfg <key> --profile "a,b"` call;
#   2. `$env.KUNAI_PROFILE` (set by the engine from its --profile flag);
#   3. otherwise → `all` (all functions in canonical order).
# In every case, the `default` BASE is always applied as the foundation.

use kunai_local_cfg.nu [load_cfg, current_profile]

# Returns the value of a local config key, resolved for the current profile.
# Now accepts all the base keys AND the profile-specific keys
# (allowlist_egress_paths, allowlist_dns_queries…).
export def local_cfg [name: string, --profile: string] {
    let cfg = (load_cfg --profile=$profile)
    if (($cfg | get --optional $name) == null) {
        error make { msg: $"unknown local rule: ($name) — available keys: (($cfg | columns | str join ', '))" }
    }
    $cfg | get $name
}

# Debug property: resolves and prints the profile actually in use.
#   nu kunai_rules_local.nu --  (no; run:  nu -c 'use kunai_rules_local.nu *; profile')
export def profile [--profile: string] {
    current_profile --profile=$profile
}
