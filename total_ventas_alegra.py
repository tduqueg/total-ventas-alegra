import os, base64, time, requests, psycopg2
from datetime import date, timedelta

BASE_URL = "https://api.alegra.com/api/v1"
EMAIL = os.environ["ALEGRA_EMAIL"]
TOKEN = os.environ["ALEGRA_TOKEN"]
AUTH = base64.b64encode(f"{EMAIL}:{TOKEN}".encode()).decode()
HEADERS = {"Authorization": f"Basic {AUTH}", "Content-Type": "application/json"}

PG_CONN = os.environ["SUPABASE_PG_CONN"]  # e.g. postgres://user:pass@host:6543/db?sslmode=require

# Trae últimos N días para ser idempotentes (y evitar lag de emisión electrónica)
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "400"))

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

def normalize(doc, doc_type):
    # Campos comunes estimados por la API (ajusta si en tu cuenta difieren)
    status = (doc.get("status") or doc.get("state") or "").lower()
    canceled = status in ("void","anulada")
    return {
        "alegra_id": int(doc["id"]),
        "doc_type": doc_type,
        "status": status,
        "number": doc.get("number"),
        "currency": (doc.get("currency") or {}).get("code") if isinstance(doc.get("currency"), dict) else doc.get("currency"),
        "issue_date": doc.get("date"),
        "created_at": doc.get("createdAt"),
        "updated_at": doc.get("updatedAt") or doc.get("lastUpdated"),
        "client_id": (doc.get("client") or {}).get("id") if isinstance(doc.get("client"), dict) else None,
        "client_name": (doc.get("client") or {}).get("name") if isinstance(doc.get("client"), dict) else None,
        "subtotal": doc.get("subtotal") or doc.get("subtotalAmount"),
        "tax": doc.get("tax") or doc.get("taxAmount"),
        "total": doc.get("total") or doc.get("totalAmount"),
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
    cur.executemany(sql, rows)
    conn.commit()
    cur.close(); conn.close()

def main():
    params = daterange_params()
    invoices = fetch_collection("/invoices", params)  # Facturas
    remissions = fetch_collection("/remissions", params)  # Remisiones
    payload = [ normalize(x,"invoice") for x in invoices ] + [ normalize(x,"remission") for x in remissions ]
    upsert_rows(payload)

if __name__ == "__main__":
    main()