#%%
import os
import time
import csv
import shutil
from datetime import datetime
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
import requests

BASE = "https://www.metacritic.com"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "Referer": "https://www.google.com/",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# patrón robusto: /game/<platform>/<slug> (ej: /game/switch/the-legend-of-zelda-tears-of-the-kingdom)
GAME_HREF_RE = re.compile(r"^/game/[^/]+/[^/?#]+$")

# ---------- utilidades CSV / paths ----------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def csv_path(data_dir: str, filename: str) -> str:
    ensure_dir(data_dir)
    return os.path.join(data_dir, filename)


def read_last_page_and_trim(csv_file: str) -> int:
    """
    Si el CSV existe, devuelve la última página scrapeada (max num_pag),
    y elimina todas las filas de esa última página para poder reintentarla.
    Si no existe, devuelve 0.
    """
    if not os.path.exists(csv_file):
        return 0

    rows = []
    last_page = 0
    with open(csv_file, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="|")
        for row in r:
            try:
                p = int(row.get("num_pag", 0))
                rows.append(row)
                if p > last_page:
                    last_page = p
            except Exception:
                pass

    if last_page == 0:
        return 0

    # nos quedamos con todas las filas que NO sean de la última página
    rows = [r for r in rows if int(r.get("num_pag", 0)) != last_page]

    # reescribimos el archivo con las filas filtradas
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["fch_scrap", "num_pag", "url"], delimiter="|")
        w.writeheader()
        w.writerows(rows)

    return last_page

def append_rows(csv_file: str, rows: list[tuple[str, int, str]]):
    """
    rows: lista de tuplas (fch_scrap, num_pag, url).
    Crea el archivo con encabezado si no existe y luego agrega filas.
    """
    file_exists = os.path.exists(csv_file)
    with open(csv_file, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="|")
        if not file_exists:
            w.writerow(["fch_scrap", "num_pag", "url"])
        w.writerows(rows)

def is_game_href(href: str) -> bool:
    if not href or not href.startswith("/game/"):
        return False
    if href.startswith("/game/browse"):
        return False
    path = urlparse(href).path.rstrip("/")
    parts = path.strip("/").split("/")  # ["game", "<slug>"] o ["game", "<platform>", "<slug>"]
    return parts[0] == "game" and len(parts) in (2, 3)

def normalize_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path.rstrip('/')}"

def extract_game_urls_from_page(html: str) -> set:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if is_game_href(href):
            urls.add(normalize_url(urljoin(BASE, href)))
    return urls

def build_page_url(base_url: str, page: int) -> str:
    parts = list(urlparse(base_url))
    qs = parse_qs(parts[4])
    qs["page"] = [str(page)]
    parts[4] = urlencode(qs, doseq=True)
    return urlunparse(parts)

def scrape_best_all_time(
    start_url: str,
    max_pages: int = 10,
    sleep_secs: float = 1.5,
    data_dir: str = "data",
    out_csv: str = "urls_metacritic.csv",
    resume: bool = True,
    retries: int = 2,
    timeout: int = 30,
):
    """
    Scrapea las páginas ?page=1..max_pages, guarda URLs en CSV con separador '|'.
    Si resume=True:
      - lee la última página registrada en el CSV,
      - elimina sus filas,
      - y la vuelve a scrapear para evitar duplicados.
    """
    sess = requests.Session()
    all_urls_run = set()

    csv_file = csv_path(data_dir, out_csv)

    start_page = 1
    if resume:
        last_done = read_last_page_and_trim(csv_file)
        start_page = min(max(last_done, 1), max_pages)

    if start_page > max_pages:
        print(f"✅ Nada para hacer: ya llegaste a page={max_pages} en {csv_file}")
        return sorted(all_urls_run)

    print(f"▶️ Empezando en page={start_page} hasta page={max_pages} (resume={resume})")

    for page in range(start_page, max_pages + 1):
        url = build_page_url(start_url, page)

        # reintentos
        ok = False
        for attempt in range(1, retries + 1):
            try:
                resp = sess.get(url, headers=headers, timeout=timeout)
                if resp.status_code == 200:
                    ok = True
                    break
                else:
                    print(f"⚠️ {resp.status_code} en {url} (intento {attempt}/{retries})")
                    time.sleep(1.0 * attempt)
            except requests.RequestException as e:
                print(f"⚠️ Error en request ({e}) {url} (intento {attempt}/{retries})")
                time.sleep(1.0 * attempt)

        if not ok:
            print(f"⛔ No se pudo descargar {url}. Corto el scraping.")
            break

        # extraer URLs de juegos de esta página
        page_urls = sorted(extract_game_urls_from_page(resp.text))
        all_urls_run.update(page_urls)

        # guardar en CSV
        now = datetime.now().isoformat(timespec="seconds")
        rows = [(now, page, u) for u in page_urls]
        append_rows(csv_file, rows)

        print(f"✅ Página {page}: {len(page_urls)} juegos (acum corrida: {len(all_urls_run)})")
        time.sleep(sleep_secs)

    return sorted(all_urls_run)


def _normalize_url_for_dedupe(url: str) -> str:
    """
    Normaliza URLs para deduplicar mejor:
      - quita fragmentos y querystring
      - quita barra final
      - normaliza a minúsculas el hostname
    """
    url = url.strip()
    p = urlparse(url)
    # normalizamos netloc (host) a minúsculas, sin puerto extra si no corresponde
    netloc = p.netloc.lower()
    path = p.path.rstrip("/")  # evito que /game/foo y /game/foo/ cuenten distinto
    return urlunparse((p.scheme, netloc, path, "", "", ""))

def dedupe_csv_by_url(
    csv_file: str,
    delimiter: str = "|",
    backup: bool = True,
    keep: str = "last",
) -> dict:
    """
    Abre csv_file (con columnas: fch_scrap | num_pag | url), elimina duplicados por 'url' y sobrescribe el archivo.
    - delimiter: separador del CSV (por defecto '|')
    - backup: si True, crea un .bak antes de sobrescribir
    - keep: 'first' conserva la primera aparición, 'last' conserva la última

    Devuelve estadísticas: {'total_rows', 'unique_urls', 'removed', 'written'}.
    """
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"No existe el archivo: {csv_file}")

    # leer
    rows = []
    with open(csv_file, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter=delimiter)
        # validación mínima de encabezados
        required = {"fch_scrap", "num_pag", "url"}
        if not required.issubset(set(r.fieldnames or [])):
            raise ValueError(f"El CSV debe tener encabezados {required}. Encontrado: {r.fieldnames}")
        for row in r:
            rows.append(row)

    total_rows = len(rows)
    if total_rows == 0:
        return {"total_rows": 0, "unique_urls": 0, "removed": 0, "written": 0}

    # dedupe
    by_url = {}
    order = range(total_rows) if keep == "first" else range(total_rows)  # igual, pero la lógica cambia abajo
    for idx in order:
        row = rows[idx]
        norm = _normalize_url_for_dedupe(row["url"])
        # si keep == 'first', solo guardo si no existe; si 'last', sobrescribo
        if keep == "first":
            if norm not in by_url:
                by_url[norm] = row
        else:  # keep == 'last'
            by_url[norm] = row

    unique_rows = list(by_url.values())
    removed = total_rows - len(unique_rows)

    # respaldo
    if backup:
        shutil.copy2(csv_file, csv_file + ".bak")

    # sobrescribir con deduplicado (conservo encabezados y orden de columnas)
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["fch_scrap", "num_pag", "url"], delimiter=delimiter)
        w.writeheader()
        w.writerows(unique_rows)

    return {
        "total_rows": total_rows,
        "unique_urls": len(unique_rows),
        "removed": removed,
        "written": len(unique_rows),
    }

# ------------------ EJEMPLO DE USO ------------------
if __name__ == "__main__":
    START_URL = "https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2025&page=1"
    urls = scrape_best_all_time(
        start_url=START_URL,
        max_pages=577,
        sleep_secs=1.5,
        data_dir="data",
        out_csv="urls_metacritic.csv",
        resume=True
    )
    print(f"Total recopilado en la corrida: {len(urls)}")

    stats = dedupe_csv_by_url("data/urls_metacritic.csv", delimiter="|", backup=True, keep="last")
    print(stats)  # {'total_rows': 1234, 'unique_urls': 987, 'removed': 247, 'written': 987}

