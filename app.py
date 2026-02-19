import os
import re
import datetime as dt
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Optional, Tuple

import requests
import pymysql
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

APP_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(APP_DIR, "templates"))

# ===== Apilayer config =====
APILAYER_KEY = os.getenv("APILAYER_KEY", "")
APILAYER_BASE_URL = os.getenv("APILAYER_BASE_URL", "https://api.apilayer.com/exchangerates_data")

# Base de conversion fixée côté serveur (non modifiable dans l'UI)
BASE_ISO = os.getenv("BASE_ISO", "EUR").strip().upper()

app = FastAPI(title="Parités Jour", version="1.1")

# Static (optionnel) — ne plante pas si absent
STATIC_DIR = os.path.join(APP_DIR, "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ===== Dictionnaire ISO -> (Libellé, Code 1 caractère) =====
PARITES_DICT: Dict[str, Tuple[str, str]] = {
    "USD": ("Dollar américain", "$"),
    "GBP": ("Livre sterling", "L"),
    "JPY": ("Yen japonais", "J"),
    "CHF": ("Franc suisse", "0"),
    "CAD": ("Dollar canadien", "C"),
    "AUD": ("Dollar australien", "A"),
    "BGN": ("Lev bulgare", "B"),
    "DKK": ("Couronne danoise", "D"),
    "HUF": ("Forint hongrois", "H"),
    "ILS": ("Nouveau shekel israélien", "I"),
    "CZK": ("Couronne tchèque", "K"),
    "NOK": ("Couronne norvégienne", "N"),
    "RON": ("Leu roumain", "R"),
    "SEK": ("Couronne suédoise", "S"),
    "TRY": ("Livre turque", "T"),
    "CNY": ("Yuan chinois", "Y"),
    "PLN": ("Zloty polonais", "Z"),
    "ISK": ("Couronne islandaise", "1"),
    "BRL": ("Réal brésilien", "2"),
    "HKD": ("Dollar de Hong Kong", "3"),
    "INR": ("Roupie indienne", "4"),
    "KRW": ("Won sud-coréen", "5"),
    "MXN": ("Peso mexicain", "6"),
    "MYR": ("Ringgit malaisien", "7"),
    "PHP": ("Peso philippin", "9"),
    "SGD": ("Dollar de Singapour", "W"),
    "THB": ("Baht thaïlandais", "X"),
    "ZAR": ("Rand sud-africain", "P"),
    "IDR": ("Roupie indonésienne", "Q"),
}

# =========================
# Helpers
# =========================
def _must_have_apilayer():
    if not APILAYER_KEY:
        raise RuntimeError("APILAYER_KEY manquant (variable d'environnement).")

def _safe_iso(code: str) -> str:
    code = (code or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", code):
        raise HTTPException(status_code=400, detail="Code ISO devise invalide (ex: USD).")
    return code

def _parse_date(s: str) -> dt.date:
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        raise HTTPException(status_code=400, detail="Date invalide (format attendu YYYY-MM-DD).")

def _to_decimal(x) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        raise HTTPException(status_code=502, detail="Taux Apilayer invalide.")

def _apilayer_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    _must_have_apilayer()
    url = f"{APILAYER_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, headers={"apikey": APILAYER_KEY}, params=params, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Apilayer {r.status_code}: {r.text[:180]}")
    try:
        return r.json()
    except Exception:
        raise HTTPException(status_code=502, detail="Réponse Apilayer non JSON.")

def _connect_mysql(cfg: Dict[str, Any]):
    host = (cfg.get("host") or "").strip()
    user = (cfg.get("user") or "").strip()
    password = cfg.get("password") or ""
    database = (cfg.get("database") or "").strip()
    port = int(cfg.get("port") or 3306)

    if not host or not user or not database:
        raise HTTPException(status_code=400, detail="Configuration MySQL incomplète (host/user/database).")

    try:
        return pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=10,
            read_timeout=25,
            write_timeout=25,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connexion MySQL impossible: {str(e)[:180]}")

# =========================
# DB schema
# =========================
def _ensure_tables(conn):
    ddl_parites = """
    CREATE TABLE IF NOT EXISTS parites (
      PARITES_CODE CHAR(1)      NOT NULL,
      PARITES_ISO  CHAR(3)      NOT NULL,
      PARITES_LIB  VARCHAR(128) NOT NULL,
      PRIMARY KEY (PARITES_CODE),
      UNIQUE KEY uq_parites_iso (PARITES_ISO)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    ddl_parites_jour = """
    CREATE TABLE IF NOT EXISTS parites_jour (
      PARITES_CODE          CHAR(1)       NOT NULL,
      PARITES_JOUR_DATE     DATE          NOT NULL,
      PARITES_JOUR_TAUX     DECIMAL(18,8)  NOT NULL,
      PARITES_JOUR_TAUX_DIV DECIMAL(18,8)  NOT NULL,
      PRIMARY KEY (PARITES_CODE, PARITES_JOUR_DATE),
      CONSTRAINT fk_parites_jour_parites
        FOREIGN KEY (PARITES_CODE) REFERENCES parites(PARITES_CODE)
          ON UPDATE RESTRICT ON DELETE RESTRICT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(ddl_parites)
        cur.execute(ddl_parites_jour)

def _ensure_parites_row_for_target(conn, target_iso: str) -> str:
    """
    Crée uniquement la devise sélectionnée dans PARITES (si absente).
    Ne rien faire si PARITES_CODE existe déjà.
    Retourne PARITES_CODE (1 caractère).
    """
    iso = _safe_iso(target_iso)
    if iso not in PARITES_DICT:
        raise HTTPException(status_code=400, detail=f"Devise {iso} non supportée (pas de mapping PARITES_CODE).")

    lib, code = PARITES_DICT[iso]

    with conn.cursor() as cur:
        cur.execute(
            "INSERT IGNORE INTO parites (PARITES_CODE, PARITES_ISO, PARITES_LIB) VALUES (%s, %s, %s);",
            (code, iso, lib),
        )
        cur.execute("SELECT PARITES_CODE FROM parites WHERE PARITES_CODE=%s LIMIT 1;", (code,))
        if not cur.fetchone():
            raise HTTPException(status_code=500, detail=f"PARITES_CODE {code} introuvable en base.")

    return code

def _upsert_parites_jour(conn, rows: List[Dict[str, Any]]):
    sql = """
    INSERT INTO parites_jour (PARITES_CODE, PARITES_JOUR_DATE, PARITES_JOUR_TAUX, PARITES_JOUR_TAUX_DIV)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      PARITES_JOUR_TAUX=VALUES(PARITES_JOUR_TAUX),
      PARITES_JOUR_TAUX_DIV=VALUES(PARITES_JOUR_TAUX_DIV);
    """
    with conn.cursor() as cur:
        cur.executemany(sql, [(r["code"], r["date"], r["rate"], r["rate_div"]) for r in rows])

# =========================
# Apilayer fetch
# =========================
def _get_supported_symbols() -> Dict[str, str]:
    # /symbols -> filtre selon PARITES_DICT
    data = _apilayer_get("symbols", {})
    symbols = data.get("symbols")
    if not isinstance(symbols, dict):
        raise HTTPException(status_code=502, detail="Apilayer symbols inattendu.")
    out: Dict[str, str] = {}
    for iso, label in symbols.items():
        iso_u = str(iso).upper()
        if iso_u in PARITES_DICT:
            out[iso_u] = str(label)
    return out

def _get_latest_rate(base: str, target: str, date_override: Optional[dt.date] = None) -> Dict[dt.date, Decimal]:
    if date_override:
        data = _apilayer_get(date_override.isoformat(), {"base": base, "symbols": target})
    else:
        data = _apilayer_get("latest", {"base": base, "symbols": target})

    rates = data.get("rates", {})
    if target not in rates:
        raise HTTPException(status_code=502, detail="Taux absent dans la réponse Apilayer.")
    d = data.get("date") or (date_override.isoformat() if date_override else None)
    if not d:
        raise HTTPException(status_code=502, detail="Date absente dans la réponse Apilayer.")
    return {dt.date.fromisoformat(d): _to_decimal(rates[target])}

def _get_timeseries_rates(base: str, target: str, start: dt.date, end: dt.date) -> Dict[dt.date, Decimal]:
    data = _apilayer_get("timeseries", {
        "base": base,
        "symbols": target,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    })
    rates_by_date = data.get("rates")
    if not isinstance(rates_by_date, dict):
        raise HTTPException(status_code=502, detail="Réponse timeseries inattendue.")
    out: Dict[dt.date, Decimal] = {}
    for d_str, rate_obj in rates_by_date.items():
        if isinstance(rate_obj, dict) and target in rate_obj:
            out[dt.date.fromisoformat(d_str)] = _to_decimal(rate_obj[target])
    if not out:
        raise HTTPException(status_code=502, detail="Aucune parité retournée sur la période.")
    return out

# =========================
# Routes
# =========================
@app.get("/", response_class=HTMLResponse)
def home(request: Request, admin: int = Query(0)):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "is_admin_ui": (admin == 1), "base_iso": BASE_ISO},
    )

@app.get("/api/meta")
def api_meta():
    return {"base_iso": BASE_ISO, "supported": sorted(list(PARITES_DICT.keys()))}

@app.get("/api/symbols")
def api_symbols():
    return _get_supported_symbols()

@app.post("/api/ensure_schema")
async def api_ensure_schema(payload: Dict[str, Any]):
    db = payload.get("db") or {}
    conn = _connect_mysql(db)
    try:
        _ensure_tables(conn)
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur MySQL: {str(e)[:180]}")
    finally:
        conn.close()
    return {"ok": True}

@app.post("/api/import_day")
async def api_import_day(payload: Dict[str, Any]):
    db = payload.get("db") or {}
    target = _safe_iso(payload.get("target"))
    date_s = (payload.get("date") or "").strip()
    date_override = _parse_date(date_s) if date_s else None

    rates = _get_latest_rate(BASE_ISO, target, date_override=date_override)

    conn = _connect_mysql(db)
    try:
        _ensure_tables(conn)
        parites_code = _ensure_parites_row_for_target(conn, target)

        rows: List[Dict[str, Any]] = []
        for d, rate in rates.items():
            if rate == 0:
                raise HTTPException(status_code=502, detail="Taux 0 (division impossible).")
            div = (Decimal("1") / rate).quantize(Decimal("0.00000001"))
            rows.append({"code": parites_code, "date": d.isoformat(), "rate": str(rate), "rate_div": str(div)})

        _upsert_parites_jour(conn, rows)
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur MySQL: {str(e)[:180]}")
    finally:
        conn.close()

    return {"ok": True, "base": BASE_ISO, "target": target, "parites_code": parites_code, "rows": len(rows)}

@app.post("/api/import_range")
async def api_import_range(payload: Dict[str, Any]):
    db = payload.get("db") or {}
    target = _safe_iso(payload.get("target"))
    start = _parse_date(payload.get("start"))
    end = _parse_date(payload.get("end"))
    if end < start:
        raise HTTPException(status_code=400, detail="La date de fin doit être >= date de début.")

    rates = _get_timeseries_rates(BASE_ISO, target, start, end)

    conn = _connect_mysql(db)
    try:
        _ensure_tables(conn)
        parites_code = _ensure_parites_row_for_target(conn, target)

        rows: List[Dict[str, Any]] = []
        for d in sorted(rates.keys()):
            rate = rates[d]
            if rate == 0:
                continue
            div = (Decimal("1") / rate).quantize(Decimal("0.00000001"))
            rows.append({"code": parites_code, "date": d.isoformat(), "rate": str(rate), "rate_div": str(div)})

        _upsert_parites_jour(conn, rows)
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur MySQL: {str(e)[:180]}")
    finally:
        conn.close()

    return {
        "ok": True,
        "base": BASE_ISO,
        "target": target,
        "parites_code": parites_code,
        "rows": len(rows),
        "from": start.isoformat(),
        "to": end.isoformat(),
    }
