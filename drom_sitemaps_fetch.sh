#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./drom_sitemaps_fetch.sh https://www.drom.ru/sitemaps/catalog/generated/catalog_models_index.xml output_urls.txt
# Env:
#   PARALLEL=6     # по умолчанию 4

INDEX_URL="${1:-}"
OUT_FILE="${2:-urls.txt}"

if [[ -z "$INDEX_URL" ]]; then
  echo "Usage: $0 <sitemap_index_url> [output_file]" >&2
  exit 2
fi

PARALLEL="${PARALLEL:-4}"
TMPDIR="$(mktemp -d)"
INDEX_FILE="$TMPDIR/index.xml"
SITEMAPS_LIST="$TMPDIR/sitemaps.lst"

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

echo "Index: $INDEX_URL"
echo "Output: $OUT_FILE"
echo "Parallel downloads: $PARALLEL"
echo

# -------- fetch helper (with retries if нужно можно добавить) ----------
fetch() {
  local url="$1" out="$2"
  curl -sS --fail --location --compressed --max-time 60 "$url" -o "$out"
}

echo "Downloading sitemap index..."
fetch "$INDEX_URL" "$INDEX_FILE"

# -------- extract <loc> from xml (namespace-agnostic) ------------------
extract_locs() {
  # stdin -> stdout (one URL per line)
  if command -v xmllint >/dev/null 2>&1; then
    # Строим список строк через XPath, игнорируя namespace:
    # string-join(//*[local-name()="loc"]/text(), "\n")
    xmllint --xpath 'string-join(//*[local-name()="loc"]/text(), "\n")' - 2>/dev/null || true
  else
    # Fallback на grep/sed (на обычных sitemap работает)
    grep -oP '(?<=<loc>)[^<]+' || true
  fi
}

echo "Parsing sitemap index for sitemap URLs..."
<"$INDEX_FILE" extract_locs | sed '/^[[:space:]]*$/d' > "$SITEMAPS_LIST"

if [[ ! -s "$SITEMAPS_LIST" ]]; then
  echo "ERROR: не удалось извлечь sitemap URLs из index." >&2
  sed -n '1,120p' "$INDEX_FILE" >&2
  exit 3
fi

echo "Found $(wc -l < "$SITEMAPS_LIST") sitemap(s)."

# -------- process one sitemap ------------------------------------------
process_sitemap() {
  local sitemap_url="$1"
  local tmpdir="$2"  # per-job temp dir
  mkdir -p "$tmpdir"
  local localfile="$tmpdir/sitemap.xml"

  # скачать
  if ! fetch "$sitemap_url" "$localfile"; then
    echo "WARN: failed to download $sitemap_url" >&2
    return 0
  fi

  # Если gzip: определяем по расширению либо по попытке распаковки
  if [[ "$sitemap_url" =~ \.gz($|\?) ]]; then
    if ! gzip -dc "$localfile" > "$tmpdir/sitemap.xml.unz" 2>/dev/null; then
      echo "WARN: failed to gunzip $sitemap_url" >&2
      # пробуем как обычный XML
    else
      mv "$tmpdir/sitemap.xml.unz" "$localfile"
    fi
  else
    # Бывает, что контент gzip, но без .gz — аккуратно тестируем
    if gzip -t "$localfile" 2>/dev/null; then
      gzip -dc "$localfile" > "$tmpdir/sitemap.xml.unz" || true
      if [[ -s "$tmpdir/sitemap.xml.unz" ]]; then
        mv "$tmpdir/sitemap.xml.unz" "$localfile"
      fi
    fi
  fi

  # извлечь все loc в отдельный файл
  local out_urls="$tmpdir/urls.tmp"
  if command -v xmllint >/dev/null 2>&1; then
    xmllint --xpath 'string-join(//*[local-name()="loc"]/text(), "\n")' "$localfile" 2>/dev/null \
      | sed '/^[[:space:]]*$/d' > "$out_urls" || true
  else
    grep -oP '(?<=<loc>)[^<]+' "$localfile" > "$out_urls" || true
  fi

  # прогресс
  local count=0
  if [[ -f "$out_urls" ]]; then count=$(wc -l < "$out_urls"); fi
  printf '.' >&2
  echo "$count $sitemap_url" >> "$TMPDIR/progress.log"
}

export -f fetch process_sitemap extract_locs
export TMPDIR

echo "Processing sitemaps in parallel..."
# Пер-задачные временные каталоги для избежания гонок записи
i=0
while IFS= read -r url; do
  i=$((i+1))
  echo -e "$url\t$TMPDIR/job_$i"
done < "$SITEMAPS_LIST" \
| xargs -P "$PARALLEL" -n1 -I{} bash -c '
  line="{}"
  url="${line%%[[:space:]]*}"
  dir="${line##*[[:space:]]}"
  process_sitemap "$url" "$dir"
'

echo -e "\nMerging and deduplicating..."
# Собираем все per-job файлы и делаем уникализацию
find "$TMPDIR" -maxdepth 1 -type d -name "job_*" -print0 \
  | xargs -0 -I{} sh -c "test -f '{}/urls.tmp' && cat '{}/urls.tmp'" \
  | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' \
  | grep -E 'https?://.+' \
  | sort -u > "$OUT_FILE"

echo "Done. URLs saved to: $OUT_FILE"
echo "Total URLs: $(wc -l < "$OUT_FILE")"
