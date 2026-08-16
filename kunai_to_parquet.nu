
# kunai_to_parquet.nu
#
# Convertit un fichier d'événements kunai (ndjson brut .jsonl ou compressé .gz)
# vers le format Parquet (.parquet), pour un traitement lazy polars beaucoup plus
# rapide (le parsing ndjson + l'inférence de schéma ne sont faits qu'une fois).
#
# Usage:
#   nu kunai_to_parquet.nu kunai.jsonl
#   nu kunai_to_parquet.nu kunai.jsonl.gz
#   nu kunai_to_parquet.nu kunai.jsonl --output out.parquet
#   nu kunai_to_parquet.nu kunai.jsonl --lazy
#   nu kunai_to_parquet.nu kunai.jsonl --noflat
#
# Options:
#   --infer-schema N   nb de lignes pour inférer le schéma (défaut 200000 ;
#                      sous 200000 l'inférence peut échouer sur les gros lots)
#   --lazy             conversion en mode lazy (moins de RAM mais plus lent ;
#                      eager est ~6x plus rapide mais très gourmand en RAM)
#   --noflat           ne pas aplatir data/info (conserver la structure brute)
#   --output FILE      nom exact du parquet de sortie (défaut : à côté de la
#                      source, nom non ambigu, cf. default_output)
#
# NOM DE SORTIE : un .gz et son .jsonl décompressé convergent vers le SAME parquet :
#   kunai.jsonl    -> kunai.jsonl.parquet
#   kunai.jsonl.gz -> kunai.jsonl.parquet
#
# NON DESTRUCTIF : la source .gz/.jsonl n'est JAMAIS supprimée ni écrasée. En mode
# eager, le .gz est décompressé vers un fichier temporaire (polars ne lit pas le
# gz compressé en eager), converti, puis le temporaire est supprimé.

# Nom de sortie par défaut : pour qu'un .gz et son .jsonl décompressé convergent
# vers le MÊME parquet, on retire seulement un éventuel suffixe `.gz`, puis on
# conserve le reste du nom et on ajoute `.parquet` :
#   test.jsonl.gz -> test.jsonl.parquet
#   test.jsonl    -> test.jsonl.parquet
def default_output [eventslog: string] {
    let ext = ($eventslog | path parse | get extension)
    if $ext == 'gz' {
        let no_gz = ($eventslog | str replace -r '\.gz$' '')
        $"($no_gz).parquet"
    } else if $ext == 'parquet' {
        $eventslog
    } else {
        $"($eventslog).parquet"
    }
}

export def save_into_parquet [
    eventslog: string
    eager_param: string
    infer_schema_num: int
    noflat_param: string
    output: string
] {
    let parquetfile = $output
    print $"converting  ($eventslog) to ($parquetfile) ($eager_param) infer-schema=($infer_schema_num) flat=($noflat_param)"

    let frame = if $eager_param == "--lazy" {
        polars open --infer-schema $infer_schema_num -t ndjson $eventslog
    } else {
        polars open --infer-schema $infer_schema_num -t ndjson $eventslog --eager
    }
    let flat = if $noflat_param == 'flat' { $frame | polars unnest data info } else { $frame }
    $flat | polars save -t parquet $parquetfile
}

export def main [
    kunai_events_log_file: string
    --infer-schema: int = 200000
    --lazy                       # lazy = moins de RAM, plus lent ; eager par défaut
    --noflat                     # ne pas aplatir data/info (conserver le brut)
    --output: string             # nom exact du parquet de sortie (défaut : par défaut)
] {
    let eager_param = if $lazy { "--lazy" } else { "--eager" }
    let noflat_param = if $noflat { "noflat" } else { "flat" }

    # le fichier source doit exister
    if not ($kunai_events_log_file | path exists) {
        return $"file ($kunai_events_log_file) not found"
    }

    let file_extension = ($kunai_events_log_file | path parse | get extension)
    if $file_extension == 'parquet' {
        return $"skipping parquet file ($kunai_events_log_file) already converted"
    }

    let out = ($output | default (default_output $kunai_events_log_file))

    # En mode eager, un .gz compressé ne peut pas être lu directement par polars.
    # On le décompresse vers un fichier TEMPORAIRE (la source .gz reste intacte),
    # on convertit le temp vers le parquet final, puis on supprime le temp.
    # Le temp est supprimé dans TOUS les cas (même si la conversion échoue, par
    # ex. épuisement mémoire du plugin polars) : on capture le statut avec try/
    # catch, on nettoie, puis on réémett l'erreur éventuelle.
    if $file_extension == 'gz' and $eager_param == "--eager" {
        let ori_dir = ($kunai_events_log_file | path dirname)
        let temp_unzipped = ($ori_dir | path join ($kunai_events_log_file | path basename | path parse | get stem)) + ".unzip.tmp"
        ^gzip -dc $kunai_events_log_file o> $temp_unzipped
        print $"unzipped - non destructif - depuis ($kunai_events_log_file) vers ($temp_unzipped)"
        let status = (try {
            save_into_parquet $temp_unzipped $eager_param $infer_schema $noflat_param $out
            'ok'
        } catch { |err| $err.msg })
        rm -f $temp_unzipped
        if $status != 'ok' {
            error make { msg: $"conversion failed: ($status)", label: { text: "conversion", span: (metadata $kunai_events_log_file).span } }
        }
    } else {
        save_into_parquet $kunai_events_log_file $eager_param $infer_schema $noflat_param $out
    }
    print $"parquet saved: ($out)"
}
