import os, base64, time, requests, psycopg2
from psycopg2.extras import Json
from datetime import date, timedelta

BASE_URL = "https://api.alegra.com/api/v1"
EMAIL = os.environ["ALEGRA_EMAIL"]
TOKEN = os.environ["ALEGRA_TOKEN"]
AUTH = base64.b64encode(f"{EMAIL}:{TOKEN}".encode()).decode()
HEADERS = {"Authorization": f"Basic {AUTH}", "Content-Type": "application/json"}

PG_CONN = os.environ["SUPABASE_PG_CONN"] 

# Trae últimos N días para ser idempotentes (y evitar lag de emisión electrónica)
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "400"))

OVERLAP_DAYS = int(os.environ.get("OVERLAP_DAYS", "7"))

def _get_conn():
    return psycopg2.connect(PG_CONN)

def table_is_empty():
    conn = _get_conn(); cur = conn.cursor()
    cur.execute("select count(1) from alegra_sales_documents;")
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    return n == 0

def get_max_issue_date(doc_type):
    conn = _get_conn(); cur = conn.cursor()
    cur.execute("select max(issue_date) from alegra_sales_documents where doc_type = %s;", (doc_type,))
    val = cur.fetchone()[0]
    cur.close(); conn.close()
    return val

def fetch_collection(path, params):
    out = []
    start = 0
    while True:
        qp = dict(params or {})
        qp.update({"start": start, "limit": 30})  # patrón común en Alegra
        r = requests.get(f"{BASE_URL}{path}", headers=HEADERS, params=qp, timeout=60)
        r.raise_for_status()
        batch = r.json()
        if not isinstance(batch, list):
            batch = batch.get("data", batch) or []
        out.extend(batch)
        if len(batch) < 30:
            break
        start += 30
        time.sleep(0.2)  # malla por rate limit (150 rpm)
    return out

from datetime import datetime

def fetch_by_day(path, start_date, end_date):
    out = []
    d = start_date
    while d <= end_date:
        # La API de Alegra acepta `date=YYYY-MM-DD` (día exacto).
        batch = fetch_collection(path, {"date": d.isoformat()})
        if batch:
            out.extend(batch)
        d += timedelta(days=1)
        time.sleep(0.1)
    return out

def _to_num(v):
    try:
        return None if v is None else float(v)
    except Exception:
        return v
def coalesce_num(*vals):
    """
    Devuelve el primer valor no-None convertido a float, preservando ceros.
    """
    for v in vals:
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            try:
                return float(str(v).replace(",", "."))
            except Exception:
                continue
    return None

def compute_total(doc, subtotal, tax):
    # 1) total directo
    t = coalesce_num(doc.get("total"), doc.get("totalAmount"))
    if t is not None:
        return t
    # 2) subtotal + tax (preserva tax=0)
    if subtotal is not None and tax is not None:
        try:
            return round(float(subtotal) + float(tax), 2)
        except Exception:
            pass
    # 3) Reconstrucción por ítems
    items = doc.get("items") or []
    if isinstance(items, list) and items:
        tot = 0.0
        for it in items:
            qty = coalesce_num(it.get("quantity"), it.get("qty")) or 0.0
            price = coalesce_num(it.get("price"), it.get("unitPrice")) or 0.0
            line = qty * price

            line_tax = 0.0
            taxes = it.get("taxes") or it.get("tax") or []
            if isinstance(taxes, list):
                for t in taxes:
                    perc = coalesce_num((t or {}).get("percentage"))
                    if perc is not None:
                        line_tax += line * (perc / 100.0)
                    else:
                        amt = coalesce_num((t or {}).get("amount"))
                        if amt is not None:
                            line_tax += amt
            elif isinstance(taxes, dict):
                perc = coalesce_num(taxes.get("percentage"))
                if perc is not None:
                    line_tax += line * (perc / 100.0)

            tot += line + line_tax
        return round(tot, 2)

    # 4) fallback final
    return 0.0

def normalize(doc, doc_type):

    status = (doc.get("status") or doc.get("state") or "").lower()
    canceled = status in ("void", "anulada")


    issue_date = doc.get("date")
    if not issue_date:

        fallback_dt = doc.get("createdAt") or doc.get("updatedAt") or doc.get("lastUpdated")
        if isinstance(fallback_dt, str) and len(fallback_dt) >= 10:
            issue_date = fallback_dt[:10]
        else:
            issue_date = date.today().isoformat()

    subtotal = coalesce_num(doc.get("subtotal"), doc.get("subtotalAmount"))
    tax      = coalesce_num(doc.get("tax"),      doc.get("taxAmount"))
    total    = compute_total(doc, subtotal, tax)

    return {
        "alegra_id": int(doc["id"]),
        "doc_type": doc_type,
        "status": status,
        "number": doc.get("number"),
        "currency": (doc.get("currency") or {}).get("code") if isinstance(doc.get("currency"), dict) else doc.get("currency"),
        "issue_date": issue_date,
        "created_at": doc.get("createdAt"),
        "updated_at": doc.get("updatedAt") or doc.get("lastUpdated"),
        "client_id": (doc.get("client") or {}).get("id") if isinstance(doc.get("client"), dict) else None,
        "client_name": (doc.get("client") or {}).get("name") if isinstance(doc.get("client"), dict) else None,
        "subtotal": subtotal,
        "tax": tax,            
        "total": total,         
        "canceled": canceled,
        "raw": doc,
    }

def daterange_params():
    since = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    until = date.today().isoformat()
    # Algunos endpoints aceptan filtros de fecha de creación "date"
    return {"date": f"{since}..{until}"}

def upsert_rows(rows):
    if not rows: return
    conn = psycopg2.connect(PG_CONN)
    cur = conn.cursor()
    sql = """
    insert into alegra_sales_documents
    (alegra_id, doc_type, status, number, currency, issue_date, created_at, updated_at,
     client_id, client_name, subtotal, tax, total, canceled, raw)
    values
    (%(alegra_id)s, %(doc_type)s, %(status)s, %(number)s, %(currency)s, %(issue_date)s,
     %(created_at)s, %(updated_at)s, %(client_id)s, %(client_name)s, %(subtotal)s,
     %(tax)s, %(total)s, %(canceled)s, %(raw)s)
    on conflict (alegra_id) do update set
      doc_type = excluded.doc_type,
      status = excluded.status,
      number = excluded.number,
      currency = excluded.currency,
      issue_date = excluded.issue_date,
      created_at = excluded.created_at,
      updated_at = excluded.updated_at,
      client_id = excluded.client_id,
      client_name = excluded.client_name,
      subtotal = excluded.subtotal,
      tax = excluded.tax,
      total = excluded.total,
      canceled = excluded.canceled,
      raw = excluded.raw;
    """
    # Adapt JSON fields for psycopg2
    rows_prepped = []
    for r in rows:
        r2 = dict(r)
        if isinstance(r2.get("raw"), (dict, list)):
            r2["raw"] = Json(r2["raw"])  # ensure JSONB is adapted
        rows_prepped.append(r2)
    cur.executemany(sql, rows_prepped)
    conn.commit()
    cur.close(); conn.close()

def main():
    # Si la tabla está vacía: FULL LOAD (trae TODO sin filtro, paginando)
    if table_is_empty() or os.environ.get("FULL_LOAD", "false").lower() == "true":
        print("[Alegra] FULL LOAD…")
        invoices = fetch_collection("/invoices", None)
        remissions = fetch_collection("/remissions", None)
    else:
        # INCREMENTAL: desde la última fecha de emisión por tipo, con solapamiento
        today = date.today()

        max_inv = get_max_issue_date("invoice")
        since_inv = (max_inv or (today - timedelta(days=LOOKBACK_DAYS))) - timedelta(days=OVERLAP_DAYS)
        if since_inv < date(2000,1,1):
            since_inv = today - timedelta(days=LOOKBACK_DAYS)
        print(f"[Alegra] INCR invoices desde {since_inv} hasta {today}")
        invoices = fetch_by_day("/invoices", since_inv, today)

        max_rem = get_max_issue_date("remission")
        since_rem = (max_rem or (today - timedelta(days=LOOKBACK_DAYS))) - timedelta(days=OVERLAP_DAYS)
        if since_rem < date(2000,1,1):
            since_rem = today - timedelta(days=LOOKBACK_DAYS)
        print(f"[Alegra] INCR remissions desde {since_rem} hasta {today}")
        remissions = fetch_by_day("/remissions", since_rem, today)

    payload = [normalize(x, "invoice") for x in invoices] + [normalize(x, "remission") for x in remissions]
    print(f"[Alegra] upsert {len(payload)} registros (invoices={len(invoices)}, remissions={len(remissions)})")
    upsert_rows(payload)

if __name__ == "__main__":
    main()