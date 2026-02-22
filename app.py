from flask import Flask, redirect, url_for, session, request
import io
import sqlite3
import datetime as dt
import uuid
import hashlib
import pandas as pd

app = Flask(__name__)
app.secret_key = "dev-secret-change-later"

DB_PATH = "data.db"

LUCAS_SHARE = 0.40
RAFA_SHARE = 0.60

ALLOWED_PROFILES = {"Lucas", "Rafa"}
ALLOWED_TIPO = {"Saida", "Entrada"}
ALLOWED_DONO = {"Casa", "Lucas", "Rafa"}
ALLOWED_RATEIO = {"60_40", "50_50", "100_meu", "100_outro"}

REQUIRED_COLUMNS = [
    "Data",
    "Estabelecimento",
    "Categoria",
    "Valor",
    "Tipo",
    "Dono",
    "Rateio",
    "Observacao",
    "Parcela",
]

BASE_CSS = """
<style>
  body { font-family: Arial, sans-serif; margin: 0; background: #fafafa; }
  .topbar { background: #fff; border-bottom: 1px solid #eee; padding: 14px 18px; }
  .wrap { max-width: 1200px; margin: 0 auto; padding: 18px; }
  .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; justify-content: space-between; }
  .pill { display: inline-block; padding: 6px 12px; border-radius: 999px; background: #f2f2f2; font-size: 12px; }
  .btn { display: inline-flex; justify-content: center; align-items: center; padding: 10px 12px; border-radius: 12px;
         text-decoration: none; border: 1px solid #ddd; background: #fff; color: #111; font-weight: 700; cursor: pointer;}
  .btn:hover { border-color: #bbb; }
  .btnPrimary { background: #111; color: #fff; border-color: #111; }
  .btnPrimary:hover { opacity: .92; }
  .btnDanger { background: #b00020; color: #fff; border-color: #b00020; }
  .btnDanger:hover { opacity: .92; }
  .card { background: #fff; border: 1px solid #eee; border-radius: 16px; padding: 18px; margin-top: 14px; box-shadow: 0 8px 24px rgba(0,0,0,.05); }
  h1, h2, h3 { margin: 0 0 10px; }
  p { margin: 0 0 10px; color: #444; }
  label { font-weight: 700; display: block; margin-top: 10px; margin-bottom: 6px; }
  input[type="text"], input[type="number"], input[type="file"], select, textarea {
    width: 100%; padding: 10px 12px; border: 1px solid #ddd; border-radius: 12px; background: #fff;
  }
  textarea { min-height: 90px; resize: vertical; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }
  .grid4 { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px; }
  .errorBox { border: 1px solid #f3b6b6; background: #fff3f3; padding: 12px; border-radius: 12px; }
  .okBox { border: 1px solid #bfe6c8; background: #f3fff6; padding: 12px; border-radius: 12px; }
  table { width: 100%; border-collapse: collapse; margin-top: 10px; }
  th, td { border-bottom: 1px solid #eee; padding: 10px 8px; text-align: left; font-size: 13px; vertical-align: top;}
  th { background: #fafafa; }
  .muted { color: #777; font-size: 12px; }
  .nav { display: inline-flex; gap: 8px; flex-wrap: wrap; }
  .kpi { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px; }
  .kpi .box { background: #fff; border: 1px solid #eee; border-radius: 16px; padding: 14px; }
  .kpi .label { font-size: 12px; color: #666; margin-bottom: 6px; }
  .kpi .value { font-size: 20px; font-weight: 800; }
  .right { text-align: right; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
  .small { font-size: 12px; }
  .stickyBar { position: sticky; top: 0; background: rgba(250,250,250,.92); backdrop-filter: blur(6px); padding: 10px 0; border-bottom: 1px solid #eee; z-index: 2; }
  @media (max-width: 900px) { .grid4 { grid-template-columns: 1fr; } .grid3 { grid-template-columns: 1fr; } .kpi { grid-template-columns: 1fr; } .grid2 { grid-template-columns: 1fr; } }
</style>
"""

def brl(x: float) -> str:
    if x is None:
        x = 0.0
    s = f"{x:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def pct(x: float) -> str:
    try:
        return f"{x*100:.1f}%"
    except:
        return "0.0%"

def _normalize_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()

def current_year_month():
    today = dt.date.today()
    return today.year, today.month

def month_ref_from(year_str: str, month_str: str) -> str:
    return f"{year_str}{month_str}"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _col_exists(conn, table: str, col: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS imports (
      batch_id TEXT PRIMARY KEY,
      month_ref TEXT NOT NULL,
      uploaded_by TEXT NOT NULL,
      filename TEXT,
      row_count INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
    """)
    if not _col_exists(conn, "imports", "file_hash"):
        cur.execute("ALTER TABLE imports ADD COLUMN file_hash TEXT")
    if not _col_exists(conn, "imports", "source"):
        cur.execute("ALTER TABLE imports ADD COLUMN source TEXT")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id TEXT NOT NULL,
      month_ref TEXT NOT NULL,
      uploaded_by TEXT NOT NULL,
      dt_text TEXT,
      estabelecimento TEXT,
      categoria TEXT,
      valor REAL NOT NULL,
      tipo TEXT NOT NULL,
      dono TEXT NOT NULL,
      rateio TEXT NOT NULL,
      observacao TEXT,
      parcela TEXT,
      created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS incomes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      month_ref TEXT NOT NULL,
      profile TEXT NOT NULL,
      salario_1 REAL NOT NULL DEFAULT 0,
      salario_2 REAL NOT NULL DEFAULT 0,
      extras REAL NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(month_ref, profile)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS investments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      month_ref TEXT NOT NULL,
      profile TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      note TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(month_ref, profile)
    )
    """)

    conn.commit()
    conn.close()

init_db()

def topbar_html(profile: str):
    nav = ""
    if profile:
        nav = f"""
        <div class="nav">
          <a class="btn" href="{url_for('dashboard')}">Painel</a>
          <a class="btn" href="{url_for('gastos')}">Adicionar gasto</a>
          <a class="btn" href="{url_for('lancamentos')}">Lançamentos</a>
          <a class="btn" href="{url_for('casa')}">Casa</a>
          <a class="btn" href="{url_for('individual')}">Individual</a>
          <a class="btn" href="{url_for('renda')}">Renda</a>
          <a class="btn" href="{url_for('home')}">Trocar perfil</a>
        </div>
        """
    return f"""
    <div class="topbar">
      <div class="wrap">
        <div class="row">
          <div>
            <b>Finanças da Casa</b>
            {"<span class='pill'>Perfil: <b>"+profile+"</b></span>" if profile else ""}
          </div>
          {nav}
        </div>
      </div>
    </div>
    """

def signed_value(tipo: str, valor: float) -> float:
    if tipo == "Entrada":
        return -abs(valor)
    return abs(valor)

def share_for(profile: str, rateio: str) -> float:
    if rateio == "50_50":
        return 0.5
    if rateio == "60_40":
        return LUCAS_SHARE if profile == "Lucas" else RAFA_SHARE
    return 0.0

def year_month_select_html(selected_year: str, selected_month: str):
    year_options = "".join([
        f"<option value='{y}' {'selected' if str(y)==str(selected_year) else ''}>{y}</option>"
        for y in range(2024, 2031)
    ])
    month_options = "".join([
        f"<option value='{m:02d}' {'selected' if f'{m:02d}'==str(selected_month) else ''}>{m:02d}</option>"
        for m in range(1, 13)
    ])
    return year_options, month_options

def month_selector_block(selected_year: str, selected_month: str, action_url: str):
    year_options, month_options = year_month_select_html(selected_year, selected_month)
    month_ref = month_ref_from(selected_year, selected_month)
    return f"""
      <form method="get" action="{action_url}">
        <div class="grid2">
          <div>
            <label>Ano</label>
            <select name="Ano">{year_options}</select>
          </div>
          <div>
            <label>Mês</label>
            <select name="Mes">{month_options}</select>
          </div>
        </div>
        <p class="muted" style="margin-top:10px;">Mês de referência: <b>{month_ref}</b></p>
        <div class="row" style="justify-content:flex-start; margin-top:10px;">
          <button class="btn btnPrimary" type="submit">Atualizar</button>
        </div>
      </form>
    """

def compute_file_hash(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()

def read_excel_from_bytes(raw: bytes) -> pd.DataFrame:
    buf = io.BytesIO(raw)
    df = pd.read_excel(buf, engine="openpyxl")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError("Colunas faltando: " + ", ".join(missing))

    df = df.copy()
    for col in ["Estabelecimento", "Categoria", "Tipo", "Dono", "Rateio", "Observacao", "Parcela"]:
        df[col] = df[col].apply(_normalize_str)

    df["Data"] = df["Data"].apply(lambda v: "" if pd.isna(v) else str(v))
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")
    return df

def validate_transactions(df: pd.DataFrame):
    errors = []
    normalized_rows = []

    for idx, row in df.iterrows():
        line_number = idx + 2

        tipo = row["Tipo"]
        dono = row["Dono"]
        rateio = row["Rateio"]
        valor = row["Valor"]

        if pd.isna(valor) or float(valor) <= 0:
            errors.append(f"Linha {line_number}: Valor inválido, precisa ser maior que 0")

        if tipo not in ALLOWED_TIPO:
            errors.append(f"Linha {line_number}: Tipo inválido, use Saida ou Entrada")

        if dono not in ALLOWED_DONO:
            errors.append(f"Linha {line_number}: Dono inválido, use Casa, Lucas ou Rafa")

        if rateio not in ALLOWED_RATEIO:
            errors.append(f"Linha {line_number}: Rateio inválido, use 60_40, 50_50, 100_meu ou 100_outro")

        if rateio in {"60_40", "50_50"} and dono != "Casa":
            errors.append(f"Linha {line_number}: Rateio {rateio} exige Dono Casa")

        if rateio in {"100_meu", "100_outro"} and dono == "Casa":
            errors.append(f"Linha {line_number}: Rateio {rateio} nunca pode ter Dono Casa")

        normalized_rows.append(
            {
                "Data": row["Data"],
                "Estabelecimento": row["Estabelecimento"],
                "Categoria": row["Categoria"],
                "Valor": None if pd.isna(valor) else float(valor),
                "Tipo": tipo,
                "Dono": dono,
                "Rateio": rateio,
                "Observacao": row["Observacao"],
                "Parcela": row["Parcela"],
            }
        )

    return errors, normalized_rows

def _insert_import(conn, batch_id, month_ref, uploaded_by, filename, row_count, status, created_at, file_hash, source):
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO imports (batch_id, month_ref, uploaded_by, filename, row_count, status, created_at, file_hash, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (batch_id, month_ref, uploaded_by, filename, row_count, status, created_at, file_hash, source))

def _insert_transaction(conn, batch_id, month_ref, uploaded_by, r, created_at):
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO transactions
      (batch_id, month_ref, uploaded_by, dt_text, estabelecimento, categoria, valor, tipo, dono, rateio, observacao, parcela, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        batch_id,
        month_ref,
        uploaded_by,
        r.get("Data", ""),
        r.get("Estabelecimento", ""),
        r.get("Categoria", ""),
        float(r.get("Valor") or 0),
        r.get("Tipo", ""),
        r.get("Dono", ""),
        r.get("Rateio", ""),
        r.get("Observacao", ""),
        r.get("Parcela", ""),
        created_at
    ))

def is_duplicate_import(month_ref: str, uploaded_by: str, file_hash: str) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT 1
      FROM imports
      WHERE month_ref = ?
        AND uploaded_by = ?
        AND file_hash = ?
        AND status = 'imported'
      LIMIT 1
    """, (month_ref, uploaded_by, file_hash))
    hit = cur.fetchone() is not None
    conn.close()
    return hit

def create_preview_batch(month_ref: str, uploaded_by: str, filename: str, rows: list[dict], file_hash: str) -> str:
    batch_id = uuid.uuid4().hex
    now = dt.datetime.utcnow().isoformat(timespec="seconds")

    conn = get_db()
    _insert_import(conn, batch_id, month_ref, uploaded_by, filename, len(rows), "preview", now, file_hash, "excel")
    for r in rows:
        _insert_transaction(conn, batch_id, month_ref, uploaded_by, r, now)

    conn.commit()
    conn.close()
    return batch_id

def finalize_import(batch_id: str, profile: str) -> tuple[bool, str]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM imports WHERE batch_id = ?", (batch_id,))
    imp = cur.fetchone()
    if not imp:
        conn.close()
        return False, "Importação não encontrada"

    if imp["uploaded_by"] != profile:
        conn.close()
        return False, "Você só pode importar batches criados no seu perfil"

    if imp["status"] == "imported":
        conn.close()
        return False, "Esse batch já foi importado"

    cur.execute("UPDATE imports SET status = 'imported' WHERE batch_id = ?", (batch_id,))
    conn.commit()
    conn.close()
    return True, "Importação concluída"

def delete_batch(batch_id: str, profile: str) -> tuple[bool, str]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM imports WHERE batch_id = ?", (batch_id,))
    imp = cur.fetchone()
    if not imp:
        conn.close()
        return False, "Importação não encontrada"

    if imp["uploaded_by"] != profile:
        conn.close()
        return False, "Você só pode excluir imports feitos no seu perfil"

    cur.execute("DELETE FROM transactions WHERE batch_id = ?", (batch_id,))
    cur.execute("DELETE FROM imports WHERE batch_id = ?", (batch_id,))
    conn.commit()
    conn.close()
    return True, "Importação excluída"

def fetch_imported_transactions(month_ref: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT t.*, i.source, i.filename, i.status
      FROM transactions t
      JOIN imports i ON i.batch_id = t.batch_id
      WHERE t.month_ref = ?
        AND i.status = 'imported'
      ORDER BY t.id DESC
    """, (month_ref,))
    rows = cur.fetchall()
    conn.close()
    return rows

def fetch_house_transactions(month_ref: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT t.*
      FROM transactions t
      JOIN imports i ON i.batch_id = t.batch_id
      WHERE t.month_ref = ?
        AND i.status = 'imported'
        AND t.dono = 'Casa'
        AND t.rateio IN ('60_40','50_50')
      ORDER BY t.id ASC
    """, (month_ref,))
    rows = cur.fetchall()
    conn.close()
    return rows

def compute_casa(month_ref: str):
    rows = fetch_house_transactions(month_ref)

    total_casa = 0.0
    paid_lucas = 0.0
    paid_rafa = 0.0
    expected_lucas = 0.0
    expected_rafa = 0.0

    by_category = {}

    for r in rows:
        val = signed_value(r["tipo"], r["valor"])
        total_casa += val

        cat = r["categoria"] or "Sem categoria"
        if cat not in by_category:
            by_category[cat] = {"total": 0.0, "lucas": 0.0, "rafa": 0.0}
        by_category[cat]["total"] += val

        if r["uploaded_by"] == "Lucas":
            paid_lucas += val
            by_category[cat]["lucas"] += val
        elif r["uploaded_by"] == "Rafa":
            paid_rafa += val
            by_category[cat]["rafa"] += val

        if r["rateio"] == "60_40":
            expected_lucas += val * LUCAS_SHARE
            expected_rafa += val * RAFA_SHARE
        elif r["rateio"] == "50_50":
            expected_lucas += val * 0.5
            expected_rafa += val * 0.5

    lucas_diff = paid_lucas - expected_lucas
    rafa_diff = paid_rafa - expected_rafa

    settlement_text = "Sem acerto necessário"
    settlement_value = 0.0

    if lucas_diff > 0.01:
        settlement_text = "Rafa deve passar para Lucas"
        settlement_value = lucas_diff
    elif rafa_diff > 0.01:
        settlement_text = "Lucas deve passar para Rafa"
        settlement_value = rafa_diff

    cats_sorted = sorted(by_category.items(), key=lambda x: x[1]["total"], reverse=True)

    return {
        "total_casa": total_casa,
        "paid_lucas": paid_lucas,
        "paid_rafa": paid_rafa,
        "expected_lucas": expected_lucas,
        "expected_rafa": expected_rafa,
        "settlement_text": settlement_text,
        "settlement_value": settlement_value,
        "cats_sorted": cats_sorted,
    }

def get_income(month_ref: str, profile: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT salario_1, salario_2, extras
      FROM incomes
      WHERE month_ref = ? AND profile = ?
    """, (month_ref, profile))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"salario_1": 0.0, "salario_2": 0.0, "extras": 0.0, "total": 0.0}
    s1 = float(row["salario_1"] or 0)
    s2 = float(row["salario_2"] or 0)
    ex = float(row["extras"] or 0)
    return {"salario_1": s1, "salario_2": s2, "extras": ex, "total": s1 + s2 + ex}

def upsert_income(month_ref: str, profile: str, salario_1: float, salario_2: float, extras: float):
    now = dt.datetime.utcnow().isoformat(timespec="seconds")
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id FROM incomes WHERE month_ref = ? AND profile = ?", (month_ref, profile))
    exists = cur.fetchone() is not None

    if exists:
        cur.execute("""
          UPDATE incomes
          SET salario_1 = ?, salario_2 = ?, extras = ?, updated_at = ?
          WHERE month_ref = ? AND profile = ?
        """, (salario_1, salario_2, extras, now, month_ref, profile))
    else:
        cur.execute("""
          INSERT INTO incomes (month_ref, profile, salario_1, salario_2, extras, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (month_ref, profile, salario_1, salario_2, extras, now, now))

    conn.commit()
    conn.close()

def get_investment(month_ref: str, profile: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT amount, note
      FROM investments
      WHERE month_ref = ? AND profile = ?
    """, (month_ref, profile))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"amount": 0.0, "note": ""}
    return {"amount": float(row["amount"] or 0), "note": row["note"] or ""}

def upsert_investment(month_ref: str, profile: str, amount: float, note: str):
    now = dt.datetime.utcnow().isoformat(timespec="seconds")
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id FROM investments WHERE month_ref = ? AND profile = ?", (month_ref, profile))
    exists = cur.fetchone() is not None

    if exists:
        cur.execute("""
          UPDATE investments
          SET amount = ?, note = ?, updated_at = ?
          WHERE month_ref = ? AND profile = ?
        """, (amount, note, now, month_ref, profile))
    else:
        cur.execute("""
          INSERT INTO investments (month_ref, profile, amount, note, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?)
        """, (month_ref, profile, amount, note, now, now))

    conn.commit()
    conn.close()

def compute_individual(month_ref: str, profile: str):
    rows = fetch_imported_transactions(month_ref)

    house_by_cat = {}
    house_total = 0.0

    my_personal_by_cat = {}
    my_personal_total = 0.0

    receivable_total = 0.0
    payable_total = 0.0

    for r in rows:
        val = signed_value(r["tipo"], r["valor"])
        cat = r["categoria"] or "Sem categoria"

        if r["dono"] == "Casa" and r["rateio"] in ("60_40", "50_50"):
            sh = share_for(profile, r["rateio"])
            part = val * sh
            house_total += part
            house_by_cat[cat] = house_by_cat.get(cat, 0.0) + part
            continue

        if r["rateio"] == "100_meu" and r["uploaded_by"] == profile and r["dono"] == profile:
            my_personal_total += val
            my_personal_by_cat[cat] = my_personal_by_cat.get(cat, 0.0) + val
            continue

        if r["rateio"] == "100_outro" and r["uploaded_by"] == profile and r["dono"] != "Casa" and r["dono"] != profile:
            receivable_total += val
            continue

        if r["rateio"] == "100_outro" and r["uploaded_by"] != profile and r["dono"] == profile:
            payable_total += val
            continue

    income = get_income(month_ref, profile)
    inv = get_investment(month_ref, profile)
    invested = float(inv["amount"] or 0)

    expenses_effective = house_total + my_personal_total + payable_total
    saldo_pos_pagamentos = income["total"] - expenses_effective
    saldo_em_conta = saldo_pos_pagamentos - invested

    invested_pct = 0.0
    if income["total"] > 0:
        invested_pct = invested / income["total"]

    cats_house = sorted(house_by_cat.items(), key=lambda x: x[1], reverse=True)
    cats_personal = sorted(my_personal_by_cat.items(), key=lambda x: x[1], reverse=True)

    return {
        "income": income,
        "invested": invested,
        "invest_note": inv.get("note", ""),
        "invested_pct": invested_pct,
        "house_total": house_total,
        "my_personal_total": my_personal_total,
        "receivable_total": receivable_total,
        "payable_total": payable_total,
        "expenses_effective": expenses_effective,
        "saldo_pos_pagamentos": saldo_pos_pagamentos,
        "saldo_em_conta": saldo_em_conta,
        "cats_house": cats_house,
        "cats_personal": cats_personal,
    }

def create_manual_batch(month_ref: str, uploaded_by: str, row: dict) -> str:
    batch_id = uuid.uuid4().hex
    now = dt.datetime.utcnow().isoformat(timespec="seconds")

    conn = get_db()
    _insert_import(conn, batch_id, month_ref, uploaded_by, "manual_entry", 1, "imported", now, None, "manual")
    _insert_transaction(conn, batch_id, month_ref, uploaded_by, row, now)

    conn.commit()
    conn.close()
    return batch_id

def get_transaction(tx_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT t.*, i.source, i.status, i.filename
      FROM transactions t
      JOIN imports i ON i.batch_id = t.batch_id
      WHERE t.id = ?
      LIMIT 1
    """, (tx_id,))
    row = cur.fetchone()
    conn.close()
    return row

def delete_transaction_any(tx_id: int, profile: str) -> tuple[bool, str]:
    tx = get_transaction(tx_id)
    if not tx:
        return False, "Lançamento não encontrado"
    if tx["uploaded_by"] != profile:
        return False, "Você só pode excluir lançamentos do seu perfil"

    batch_id = tx["batch_id"]

    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))

    cur.execute("SELECT COUNT(*) as c FROM transactions WHERE batch_id = ?", (batch_id,))
    c = int(cur.fetchone()["c"])
    if c == 0:
        cur.execute("DELETE FROM imports WHERE batch_id = ?", (batch_id,))

    conn.commit()
    conn.close()
    return True, "Lançamento excluído"

def delete_transactions_bulk(ids: list[int], profile: str) -> tuple[int, list[str]]:
    deleted = 0
    errors = []
    for tx_id in ids:
        ok, msg = delete_transaction_any(tx_id, profile)
        if ok:
            deleted += 1
        else:
            errors.append(f"ID {tx_id}: {msg}")
    return deleted, errors

@app.route("/")
def home():
    active_profile = session.get("profile", "")
    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Finanças da Casa</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(active_profile)}
        <div class="wrap">
          <div class="card">
            <h1>Finanças da Casa</h1>
            <p>Escolha seu perfil para continuar.</p>
            <div class="row" style="justify-content:flex-start;">
              <a class="btn btnPrimary" href="{url_for('set_profile', profile='Lucas')}">Entrar como Lucas</a>
              <a class="btn btnPrimary" href="{url_for('set_profile', profile='Rafa')}">Entrar como Rafa</a>
            </div>
            <p class="muted" style="margin-top:12px;">Sem senha no MVP, só para evitar confusão de perfil.</p>
          </div>
        </div>
      </body>
    </html>
    """
    return html

@app.route("/set_profile/<profile>")
def set_profile(profile: str):
    profile = profile.strip()
    if profile not in ALLOWED_PROFILES:
        return "Perfil inválido", 400
    session["profile"] = profile
    return redirect(url_for("dashboard"))

@app.route("/dashboard")
def dashboard():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    selected_year = request.args.get("Ano") or "2026"
    selected_month = request.args.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    label_renda = "Renda do Lucas" if profile == "Lucas" else "Renda da Rafa"

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Painel</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>Painel do {profile}</h2>
            <p class="muted">Fluxo sugerido: preencher {label_renda}, adicionar gastos e depois ver Individual e Casa.</p>
            {month_selector_block(selected_year, selected_month, url_for('dashboard'))}
            <div class="row" style="justify-content:flex-start; margin-top:12px;">
              <a class="btn btnPrimary" href="{url_for('renda')}?Ano={selected_year}&Mes={selected_month}">{label_renda}</a>
              <a class="btn btnPrimary" href="{url_for('gastos')}?Ano={selected_year}&Mes={selected_month}">Adicionar gasto</a>
              <a class="btn btnPrimary" href="{url_for('individual')}?Ano={selected_year}&Mes={selected_month}">Individual</a>
              <a class="btn" href="{url_for('casa')}?month_ref={month_ref}">Casa</a>
              <a class="btn" href="{url_for('lancamentos')}?Ano={selected_year}&Mes={selected_month}">Lançamentos</a>
            </div>
          </div>
        </div>
      </body>
    </html>
    """
    return html

@app.route("/renda", methods=["GET", "POST"])
def renda():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    selected_year = request.values.get("Ano") or "2026"
    selected_month = request.values.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    msg = ""

    if request.method == "POST":
        def num(v):
            try:
                v = str(v).replace(".", "").replace(",", ".")
                return float(v) if v else 0.0
            except:
                return 0.0

        s1 = num(request.form.get("salario_1"))
        s2 = num(request.form.get("salario_2"))
        ex = num(request.form.get("extras"))
        upsert_income(month_ref, profile, s1, s2, ex)
        msg = "Renda salva"

    inc = get_income(month_ref, profile)
    label = "Renda do Lucas" if profile == "Lucas" else "Renda da Rafa"

    msg_block = ""
    if msg:
        msg_block = f"""
          <div class="card">
            <div class="okBox">
              <b>{msg}</b>
            </div>
          </div>
        """

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Renda</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>{label}</h2>
            {month_selector_block(selected_year, selected_month, url_for('renda'))}
          </div>

          <div class="card">
            <h3>Valores do mês</h3>
            <form method="post">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">

              <div class="grid3">
                <div>
                  <label>Salário 1</label>
                  <input type="text" name="salario_1" value="{inc['salario_1']:.2f}" />
                </div>
                <div>
                  <label>Salário 2</label>
                  <input type="text" name="salario_2" value="{inc['salario_2']:.2f}" />
                </div>
                <div>
                  <label>Extras</label>
                  <input type="text" name="extras" value="{inc['extras']:.2f}" />
                </div>
              </div>

              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Salvar</button>
                <a class="btn" href="{url_for('individual')}?Ano={selected_year}&Mes={selected_month}">Ver Individual</a>
              </div>

              <p class="muted" style="margin-top:10px;">Total do mês: <b>{brl(inc['total'])}</b></p>
            </form>
          </div>

          {msg_block}
        </div>
      </body>
    </html>
    """
    return html

@app.route("/individual", methods=["GET", "POST"])
def individual():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    selected_year = request.values.get("Ano") or "2026"
    selected_month = request.values.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    invest_msg = ""
    if request.method == "POST":
        def num(v):
            try:
                v = str(v).replace(".", "").replace(",", ".")
                return float(v) if v else 0.0
            except:
                return 0.0

        amount = num(request.form.get("invest_amount"))
        note = _normalize_str(request.form.get("invest_note"))
        if amount < 0:
            amount = 0.0
        upsert_investment(month_ref, profile, amount, note)
        invest_msg = "Investimento salvo"

    data = compute_individual(month_ref, profile)

    def rows_from(items):
        out = ""
        for cat, val in items:
            out += f"<tr><td>{cat}</td><td class='right'>{brl(val)}</td></tr>"
        if not out:
            out = "<tr><td colspan='2' class='muted'>Sem dados</td></tr>"
        return out

    invest_block = ""
    if invest_msg:
        invest_block = f"""
          <div class="card">
            <div class="okBox"><b>{invest_msg}</b></div>
          </div>
        """

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Individual</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>Individual do {profile}</h2>
            {month_selector_block(selected_year, selected_month, url_for('individual'))}
            <div class="row" style="justify-content:flex-start; margin-top:12px;">
              <a class="btn btnPrimary" href="{url_for('renda')}?Ano={selected_year}&Mes={selected_month}">Editar renda</a>
              <a class="btn btnPrimary" href="{url_for('gastos')}?Ano={selected_year}&Mes={selected_month}">Adicionar gasto</a>
              <a class="btn" href="{url_for('lancamentos')}?Ano={selected_year}&Mes={selected_month}">Ver lançamentos</a>
              <a class="btn" href="{url_for('casa')}?month_ref={month_ref}">Ver casa</a>
            </div>
          </div>

          <div class="card">
            <h3>Resumo</h3>
            <div class="kpi">
              <div class="box">
                <div class="label">Renda total</div>
                <div class="value">{brl(data["income"]["total"])}</div>
              </div>
              <div class="box">
                <div class="label">Minha parte da casa</div>
                <div class="value">{brl(data["house_total"])}</div>
              </div>
              <div class="box">
                <div class="label">Meu pessoal</div>
                <div class="value">{brl(data["my_personal_total"])}</div>
              </div>
              <div class="box">
                <div class="label">A pagar para o outro</div>
                <div class="value">{brl(data["payable_total"])}</div>
              </div>
            </div>

            <div class="kpi" style="margin-top:12px;">
              <div class="box">
                <div class="label">Gastos efetivos</div>
                <div class="value">{brl(data["expenses_effective"])}</div>
                <div class="muted">Casa + Pessoal + A pagar</div>
              </div>
              <div class="box">
                <div class="label">Saldo pós pagamentos</div>
                <div class="value">{brl(data["saldo_pos_pagamentos"])}</div>
                <div class="muted">Renda menos gastos efetivos</div>
              </div>
              <div class="box">
                <div class="label">Investir</div>
                <div class="value">{brl(data["invested"])}</div>
                <div class="muted">{pct(data["invested_pct"])} da renda do mês</div>
              </div>
              <div class="box">
                <div class="label">Saldo em conta corrente</div>
                <div class="value">{brl(data["saldo_em_conta"])}</div>
                <div class="muted">Saldo pós pagamentos menos investir</div>
              </div>
            </div>
          </div>

          <div class="card">
            <h3>Atualizar investir</h3>
            <p class="muted">Você pode ajustar isso quantas vezes quiser. O percentual atualiza na hora.</p>
            <form method="post">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">
              <div class="grid2">
                <div>
                  <label>Quanto investir</label>
                  <input type="text" name="invest_amount" value="{data["invested"]:.2f}" />
                </div>
                <div>
                  <label>Observação (opcional)</label>
                  <input type="text" name="invest_note" value="{_normalize_str(data["invest_note"])}" />
                </div>
              </div>
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Salvar investir</button>
              </div>
            </form>
          </div>

          {invest_block}

          <div class="card">
            <h3>Minha parte da casa por categoria</h3>
            <table>
              <thead><tr><th>Categoria</th><th class="right">Valor</th></tr></thead>
              <tbody>{rows_from(data["cats_house"])}</tbody>
            </table>
          </div>

          <div class="card">
            <h3>Meu pessoal por categoria</h3>
            <table>
              <thead><tr><th>Categoria</th><th class="right">Valor</th></tr></thead>
              <tbody>{rows_from(data["cats_personal"])}</tbody>
            </table>
          </div>

        </div>
      </body>
    </html>
    """
    return html

@app.route("/gastos", methods=["GET", "POST"])
def gastos():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    default_year = "2026"
    default_month = f"{now_m:02d}"

    selected_year = request.values.get("Ano") or default_year
    selected_month = request.values.get("Mes") or default_month
    month_ref = month_ref_from(selected_year, selected_month)

    action = request.form.get("action", "")

    errors = []
    info = ""
    info_ok = True

    preview_rows = []
    preview_batch_id = ""

    manual_defaults = {
        "Data": "",
        "Estabelecimento": "",
        "Categoria": "",
        "Valor": "",
        "Tipo": "Saida",
        "Dono": "Casa",
        "Rateio": "60_40",
        "Observacao": "",
        "Parcela": "",
    }

    if request.method == "POST":
        if action == "manual":
            form_data = dict(manual_defaults)
            for k in form_data.keys():
                form_data[k] = _normalize_str(request.form.get(k))

            try:
                v = str(form_data["Valor"]).replace(".", "").replace(",", ".")
                valor = float(v) if v else 0.0
            except:
                valor = 0.0

            tipo = form_data["Tipo"]
            dono = form_data["Dono"]
            rateio = form_data["Rateio"]

            if valor <= 0:
                errors.append("Valor precisa ser maior que 0")
            if tipo not in ALLOWED_TIPO:
                errors.append("Tipo inválido")
            if dono not in ALLOWED_DONO:
                errors.append("Dono inválido")
            if rateio not in ALLOWED_RATEIO:
                errors.append("Rateio inválido")
            if rateio in {"60_40", "50_50"} and dono != "Casa":
                errors.append(f"Rateio {rateio} exige Dono Casa")
            if rateio in {"100_meu", "100_outro"} and dono == "Casa":
                errors.append(f"Rateio {rateio} nunca pode ter Dono Casa")

            if not errors:
                row = dict(form_data)
                row["Valor"] = valor
                create_manual_batch(month_ref, profile, row)
                info = "Lançamento manual adicionado"
                info_ok = True

        elif action == "excel_preview":
            file = request.files.get("file")
            if not file or file.filename.strip() == "":
                errors.append("Arquivo obrigatório")

            if not errors:
                try:
                    raw = file.read()
                    file_hash = compute_file_hash(raw)

                    if is_duplicate_import(month_ref, profile, file_hash):
                        errors.append("Esse mesmo arquivo já foi importado neste mês para este perfil")
                    else:
                        df = read_excel_from_bytes(raw)
                        errors, preview_rows = validate_transactions(df)
                        if not errors:
                            preview_batch_id = create_preview_batch(month_ref, profile, file.filename, preview_rows, file_hash)
                            info = "Preview criado, confirme para importar"
                            info_ok = True

                except Exception as e:
                    errors.append(str(e))

        elif action == "excel_import":
            batch_id = _normalize_str(request.form.get("batch_id"))
            ok, msg = finalize_import(batch_id, profile)
            info = msg
            info_ok = ok

    tipo_opts = "".join([f"<option value='{t}'>{t}</option>" for t in ["Saida", "Entrada"]])
    dono_opts = "".join([f"<option value='{d}'>{d}</option>" for d in ["Casa", "Lucas", "Rafa"]])
    rateio_opts = "".join([f"<option value='{r}'>{r}</option>" for r in ["60_40", "50_50", "100_meu", "100_outro"]])

    err_block = ""
    if errors:
        items = "".join([f"<li>{e}</li>" for e in errors[:40]])
        err_block = f"""
          <div class="card">
            <h3>Erros</h3>
            <div class="errorBox"><ul>{items}</ul></div>
          </div>
        """

    info_block = ""
    if info:
        klass = "okBox" if info_ok else "errorBox"
        info_block = f"""
          <div class="card">
            <div class="{klass}">
              <b>{info}</b>
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <a class="btn btnPrimary" href="{url_for('lancamentos')}?Ano={selected_year}&Mes={selected_month}">Ver lançamentos</a>
                <a class="btn" href="{url_for('individual')}?Ano={selected_year}&Mes={selected_month}">Ver individual</a>
              </div>
            </div>
          </div>
        """

    preview_table = ""
    if preview_batch_id and preview_rows and not errors:
        head = "".join([f"<th>{c}</th>" for c in REQUIRED_COLUMNS])
        body_rows = ""
        for r in preview_rows[:20]:
            tds = "".join([f"<td>{'' if r.get(c) is None else r.get(c)}</td>" for c in REQUIRED_COLUMNS])
            body_rows += f"<tr>{tds}</tr>"

        preview_table = f"""
          <div class="card">
            <h3>Preview do Excel</h3>
            <p class="muted">Batch: <span class="mono">{preview_batch_id[:10]}...</span> mostrando 20 linhas</p>
            <div class="okBox">
              <form method="post">
                <input type="hidden" name="Ano" value="{selected_year}">
                <input type="hidden" name="Mes" value="{selected_month}">
                <input type="hidden" name="action" value="excel_import">
                <input type="hidden" name="batch_id" value="{preview_batch_id}">
                <div class="row" style="justify-content:flex-start;">
                  <button class="btn btnPrimary" type="submit">Importar</button>
                  <a class="btn" href="{url_for('lancamentos')}?Ano={selected_year}&Mes={selected_month}">Ir para lançamentos</a>
                </div>
              </form>
              <p class="muted" style="margin-top:10px;">Se você não importar, esse batch fica como preview e pode ser excluído em Lançamentos.</p>
            </div>
            <table>
              <thead><tr>{head}</tr></thead>
              <tbody>{body_rows}</tbody>
            </table>
          </div>
        """

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Adicionar gasto</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>Adicionar gasto</h2>
            <p class="muted">Pagador atual: <b>{profile}</b></p>
            {month_selector_block(selected_year, selected_month, url_for('gastos'))}
          </div>

          {err_block}
          {info_block}

          <div class="card">
            <h3>Lançamento manual</h3>
            <form method="post">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">
              <input type="hidden" name="action" value="manual">

              <div class="grid2">
                <div>
                  <label>Data</label>
                  <input type="text" name="Data" placeholder="Opcional" />
                </div>
                <div>
                  <label>Valor</label>
                  <input type="text" name="Valor" placeholder="ex: 120,50" />
                </div>
              </div>

              <div class="grid2">
                <div>
                  <label>Estabelecimento</label>
                  <input type="text" name="Estabelecimento" />
                </div>
                <div>
                  <label>Categoria</label>
                  <input type="text" name="Categoria" />
                </div>
              </div>

              <div class="grid3">
                <div>
                  <label>Tipo</label>
                  <select name="Tipo">{tipo_opts}</select>
                </div>
                <div>
                  <label>Dono</label>
                  <select name="Dono">{dono_opts}</select>
                </div>
                <div>
                  <label>Rateio</label>
                  <select name="Rateio">{rateio_opts}</select>
                </div>
              </div>

              <div class="grid2">
                <div>
                  <label>Parcela (opcional)</label>
                  <input type="text" name="Parcela" />
                </div>
                <div>
                  <label>Observação (opcional)</label>
                  <input type="text" name="Observacao" />
                </div>
              </div>

              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Salvar lançamento</button>
              </div>
            </form>
          </div>

          <div class="card">
            <h3>Importar arquivo</h3>
            <p class="muted">Ao escolher o arquivo, o preview abre automaticamente.</p>
            <form id="excelForm" method="post" enctype="multipart/form-data">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">
              <input type="hidden" name="action" value="excel_preview">
              <label>Arquivo Excel</label>
              <input id="fileInput" type="file" name="file" accept=".xlsx,.xls" />
              <p class="muted">Colunas obrigatórias: {", ".join(REQUIRED_COLUMNS)}</p>
            </form>
          </div>

          {preview_table}
        </div>

        <script>
          const fileInput = document.getElementById("fileInput");
          const form = document.getElementById("excelForm");
          if (fileInput && form) {{
            fileInput.addEventListener("change", () => {{
              if (fileInput.files && fileInput.files.length > 0) {{
                form.submit();
              }}
            }});
          }}
        </script>
      </body>
    </html>
    """
    return html

@app.route("/lancamentos", methods=["GET", "POST"])
def lancamentos():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    selected_year = request.values.get("Ano") or "2026"
    selected_month = request.values.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    filter_profile = request.values.get("filter_profile") or "Todos"

    msg = ""
    msg_ok = True
    errors = []

    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "delete_one":
            try:
                tx_id = int(request.form.get("tx_id"))
            except:
                tx_id = 0
            ok, m = delete_transaction_any(tx_id, profile)
            msg = m
            msg_ok = ok

        elif action == "delete_bulk":
            ids_raw = request.form.getlist("tx_ids")
            ids = []
            for v in ids_raw:
                try:
                    ids.append(int(v))
                except:
                    pass
            deleted, errs = delete_transactions_bulk(ids, profile)
            msg = f"{deleted} lançamentos excluídos"
            msg_ok = True
            if errs:
                errors.extend(errs[:30])

        elif action == "delete_batch":
            batch_id = _normalize_str(request.form.get("batch_id"))
            ok, m = delete_batch(batch_id, profile)
            msg = m
            msg_ok = ok

    rows = fetch_imported_transactions(month_ref)

    if filter_profile in ("Lucas", "Rafa"):
        rows = [r for r in rows if r["uploaded_by"] == filter_profile]

    row_html = ""
    for r in rows[:800]:
        val = signed_value(r["tipo"], r["valor"])
        can_delete = (r["uploaded_by"] == profile)

        delete_btn = ""
        if can_delete:
            delete_btn = f"""
              <form method="post" style="display:inline;">
                <input type="hidden" name="Ano" value="{selected_year}">
                <input type="hidden" name="Mes" value="{selected_month}">
                <input type="hidden" name="filter_profile" value="{filter_profile}">
                <input type="hidden" name="action" value="delete_one">
                <input type="hidden" name="tx_id" value="{r['id']}">
                <button class="btn btnDanger" type="submit">Excluir</button>
              </form>
            """

        checkbox = f"<input type='checkbox' name='tx_ids' value='{r['id']}' {'disabled' if not can_delete else ''} />"
        src = r["source"] or ""
        fname = _normalize_str(r["filename"])

        row_html += f"""
          <tr>
            <td>{checkbox}</td>
            <td class="mono">{r['id']}</td>
            <td>{r['uploaded_by']}</td>
            <td class="small">{src}</td>
            <td class="small">{fname}</td>
            <td class="small">{_normalize_str(r['dt_text'])}</td>
            <td class="small">{_normalize_str(r['estabelecimento'])}</td>
            <td class="small">{_normalize_str(r['categoria'])}</td>
            <td class="right">{brl(val)}</td>
            <td class="small">{r['tipo']}</td>
            <td class="small">{r['dono']}</td>
            <td class="small">{r['rateio']}</td>
            <td>{delete_btn}</td>
          </tr>
        """

    if not row_html:
        row_html = "<tr><td colspan='13' class='muted'>Sem lançamentos importados para esse mês</td></tr>"

    batches_html = ""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT * FROM imports
      WHERE month_ref = ?
      ORDER BY created_at DESC
    """, (month_ref,))
    batches = cur.fetchall()
    conn.close()

    for b in batches:
        can_del_batch = (b["uploaded_by"] == profile)
        btn = ""
        if can_del_batch:
            btn = f"""
              <form method="post" style="display:inline;">
                <input type="hidden" name="Ano" value="{selected_year}">
                <input type="hidden" name="Mes" value="{selected_month}">
                <input type="hidden" name="filter_profile" value="{filter_profile}">
                <input type="hidden" name="action" value="delete_batch">
                <input type="hidden" name="batch_id" value="{b['batch_id']}">
                <button class="btn btnDanger" type="submit">Excluir batch</button>
              </form>
            """
        batches_html += f"""
          <tr>
            <td class="small">{b['created_at']}</td>
            <td>{b['uploaded_by']}</td>
            <td class="small">{b['status']}</td>
            <td class="small">{_normalize_str(b['source'])}</td>
            <td class="small">{_normalize_str(b['filename'])}</td>
            <td class="right">{b['row_count']}</td>
            <td class="mono">{b['batch_id'][:10]}...</td>
            <td>{btn}</td>
          </tr>
        """

    if not batches_html:
        batches_html = "<tr><td colspan='8' class='muted'>Sem batches</td></tr>"

    msg_block = ""
    if msg:
        klass = "okBox" if msg_ok else "errorBox"
        msg_block = f"""
          <div class="card">
            <div class="{klass}">
              <b>{msg}</b>
            </div>
          </div>
        """

    err_block = ""
    if errors:
        items = "".join([f"<li>{e}</li>" for e in errors[:40]])
        err_block = f"""
          <div class="card">
            <h3>Erros</h3>
            <div class="errorBox"><ul>{items}</ul></div>
          </div>
        """

    filter_opts = ""
    for opt in ["Todos", "Lucas", "Rafa"]:
        sel = "selected" if opt == filter_profile else ""
        filter_opts += f"<option value='{opt}' {sel}>{opt}</option>"

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Lançamentos</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">

          <div class="card">
            <h2>Lançamentos</h2>
            <p class="muted">Você só consegue excluir lançamentos do seu perfil ({profile}).</p>
            {month_selector_block(selected_year, selected_month, url_for('lancamentos'))}

            <form method="get" style="margin-top:10px;">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">
              <label>Filtrar por pagador</label>
              <select name="filter_profile">{filter_opts}</select>
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Aplicar filtro</button>
                <a class="btn" href="{url_for('gastos')}?Ano={selected_year}&Mes={selected_month}">Adicionar gasto</a>
              </div>
            </form>
          </div>

          {msg_block}
          {err_block}

          <div class="card">
            <h3>Lista micro</h3>
            <div class="stickyBar">
              <form method="post" id="bulkForm">
                <input type="hidden" name="Ano" value="{selected_year}">
                <input type="hidden" name="Mes" value="{selected_month}">
                <input type="hidden" name="filter_profile" value="{filter_profile}">
                <input type="hidden" name="action" value="delete_bulk">
                <div class="row" style="justify-content:flex-start;">
                  <button class="btn btnDanger" type="submit">Excluir selecionados</button>
                  <button class="btn" type="button" onclick="selectAll(true)">Selecionar tudo (meus)</button>
                  <button class="btn" type="button" onclick="selectAll(false)">Limpar seleção</button>
                </div>

                <table>
                  <thead>
                    <tr>
                      <th></th>
                      <th>ID</th>
                      <th>Pagador</th>
                      <th>Fonte</th>
                      <th>Arquivo</th>
                      <th>Data</th>
                      <th>Estabelecimento</th>
                      <th>Categoria</th>
                      <th class="right">Valor</th>
                      <th>Tipo</th>
                      <th>Dono</th>
                      <th>Rateio</th>
                      <th>Ação</th>
                    </tr>
                  </thead>
                  <tbody>
                    {row_html}
                  </tbody>
                </table>
              </form>
            </div>
          </div>

          <div class="card">
            <h3>Batches do mês</h3>
            <p class="muted">Preview também aparece aqui. Você pode excluir batch do seu perfil.</p>
            <table>
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Pagador</th>
                  <th>Status</th>
                  <th>Fonte</th>
                  <th>Arquivo</th>
                  <th class="right">Linhas</th>
                  <th>Batch</th>
                  <th>Ação</th>
                </tr>
              </thead>
              <tbody>
                {batches_html}
              </tbody>
            </table>
          </div>

        </div>

        <script>
          function selectAll(flag) {{
            const boxes = document.querySelectorAll("input[type='checkbox'][name='tx_ids']");
            boxes.forEach(b => {{
              if (b.disabled) return;
              b.checked = flag;
            }});
          }}
        </script>
      </body>
    </html>
    """
    return html

@app.route("/casa", methods=["GET"])
def casa():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    default_year = "2026"
    default_month = f"{now_m:02d}"
    month_ref = request.args.get("month_ref") or f"{default_year}{default_month}"

    data = compute_casa(month_ref)
    settle_line = f"{data['settlement_text']}: {brl(data['settlement_value'])}"

    cats_rows = ""
    for cat, obj in data["cats_sorted"]:
        cats_rows += f"""
          <tr>
            <td>{cat}</td>
            <td class="right">{brl(obj["total"])}</td>
            <td class="right">{brl(obj["lucas"])}</td>
            <td class="right">{brl(obj["rafa"])}</td>
          </tr>
        """
    if not cats_rows:
        cats_rows = "<tr><td colspan='4' class='muted'>Sem lançamentos importados de Casa para esse mês</td></tr>"

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Casa</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">

          <div class="card">
            <h2>Casa</h2>
            <p class="muted">Somente Dono Casa com rateio 60_40 e 50_50, somente importados.</p>
            <form method="get">
              <label>Mês de referência</label>
              <input type="text" name="month_ref" value="{month_ref}" placeholder="202602" />
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Atualizar</button>
                <a class="btn" href="{url_for('lancamentos')}?Ano={month_ref[:4]}&Mes={month_ref[4:]}">Ver lançamentos</a>
              </div>
              <p class="muted" style="margin-top:10px;">Formato YYYYMM</p>
            </form>
          </div>

          <div class="card">
            <h3>Resumo</h3>
            <div class="kpi">
              <div class="box">
                <div class="label">Total Casa</div>
                <div class="value">{brl(data["total_casa"])}</div>
              </div>
              <div class="box">
                <div class="label">Pago Lucas</div>
                <div class="value">{brl(data["paid_lucas"])}</div>
                <div class="muted">Deveria: {brl(data["expected_lucas"])}</div>
              </div>
              <div class="box">
                <div class="label">Pago Rafa</div>
                <div class="value">{brl(data["paid_rafa"])}</div>
                <div class="muted">Deveria: {brl(data["expected_rafa"])}</div>
              </div>
              <div class="box">
                <div class="label">Acerto</div>
                <div class="value">{brl(data["settlement_value"])}</div>
                <div class="muted">{data["settlement_text"]}</div>
              </div>
            </div>

            <div style="margin-top:12px;" class="okBox">
              <b>Acerto do mês</b><br/>
              {settle_line}
            </div>
          </div>

          <div class="card">
            <h3>Casa por categoria</h3>
            <table>
              <thead>
                <tr>
                  <th>Categoria</th>
                  <th class="right">Total</th>
                  <th class="right">Pago Lucas</th>
                  <th class="right">Pago Rafa</th>
                </tr>
              </thead>
              <tbody>
                {cats_rows}
              </tbody>
            </table>
          </div>

        </div>
      </body>
    </html>
    """
    return html

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
