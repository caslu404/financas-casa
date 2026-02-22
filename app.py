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
  .btnGhost { background: #fff; color: #111; border-color: #ddd; }
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
  @media (max-width: 900px) { .grid4 { grid-template-columns: 1fr; } .grid3 { grid-template-columns: 1fr; } .kpi { grid-template-columns: 1fr; } .grid2 { grid-template-columns: 1fr; } }
</style>
"""

def brl(x: float) -> str:
    if x is None:
        x = 0.0
    s = f"{x:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

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

    # Imports
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

    # Add migrations to imports
    if not _col_exists(conn, "imports", "file_hash"):
        cur.execute("ALTER TABLE imports ADD COLUMN file_hash TEXT")
    if not _col_exists(conn, "imports", "source"):
        cur.execute("ALTER TABLE imports ADD COLUMN source TEXT")  # 'excel' ou 'manual'

    # Transactions
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

    # Incomes
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

    # Investments (tracking do quanto foi guardado/investido no mês)
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
          <a class="btn" href="{url_for('upload')}">Upload</a>
          <a class="btn" href="{url_for('manual_add')}">Adicionar</a>
          <a class="btn" href="{url_for('entries')}">Lançamentos</a>
          <a class="btn" href="{url_for('imports_list')}">Importações</a>
          <a class="btn" href="{url_for('casa')}">Casa</a>
          <a class="btn" href="{url_for('individual')}">Individual</a>
          <a class="btn" href="{url_for('renda')}">Renda</a>
          <a class="btn" href="{url_for('investimentos')}">Investimentos</a>
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
    # Saida soma, Entrada subtrai
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

        # Regras Dono x Rateio
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
      SELECT t.*
      FROM transactions t
      JOIN imports i ON i.batch_id = t.batch_id
      WHERE t.month_ref = ?
        AND i.status = 'imported'
      ORDER BY t.id ASC
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

    receivable_by_cat = {}
    receivable_total = 0.0

    payable_by_cat = {}
    payable_total = 0.0

    for r in rows:
        val = signed_value(r["tipo"], r["valor"])
        cat = r["categoria"] or "Sem categoria"

        # Minha parte da Casa (sempre pelo rateio, independente de quem pagou)
        if r["dono"] == "Casa" and r["rateio"] in ("60_40", "50_50"):
            sh = share_for(profile, r["rateio"])
            part = val * sh
            house_total += part
            house_by_cat[cat] = house_by_cat.get(cat, 0.0) + part
            continue

        # Meu pessoal (eu subi e é meu)
        if r["rateio"] == "100_meu" and r["uploaded_by"] == profile and r["dono"] == profile:
            my_personal_total += val
            my_personal_by_cat[cat] = my_personal_by_cat.get(cat, 0.0) + val
            continue

        # Eu paguei algo que é 100% do outro (a receber)
        if r["rateio"] == "100_outro" and r["uploaded_by"] == profile and r["dono"] != "Casa" and r["dono"] != profile:
            receivable_total += val
            receivable_by_cat[cat] = receivable_by_cat.get(cat, 0.0) + val
            continue

        # O outro pagou algo que é 100% meu (eu devo pagar)
        if r["rateio"] == "100_outro" and r["uploaded_by"] != profile and r["dono"] == profile:
            payable_total += val
            payable_by_cat[cat] = payable_by_cat.get(cat, 0.0) + val
            continue

    income = get_income(month_ref, profile)
    inv = get_investment(month_ref, profile)
    invested = float(inv["amount"] or 0)

    expenses_effective = house_total + my_personal_total + payable_total
    sobra = income["total"] - expenses_effective
    sobra_pos_invest = sobra - invested

    cats_house = sorted(house_by_cat.items(), key=lambda x: x[1], reverse=True)
    cats_personal = sorted(my_personal_by_cat.items(), key=lambda x: x[1], reverse=True)
    cats_recv = sorted(receivable_by_cat.items(), key=lambda x: x[1], reverse=True)
    cats_pay = sorted(payable_by_cat.items(), key=lambda x: x[1], reverse=True)

    return {
        "income": income,
        "invested": invested,
        "house_total": house_total,
        "my_personal_total": my_personal_total,
        "receivable_total": receivable_total,
        "payable_total": payable_total,
        "expenses_effective": expenses_effective,
        "sobra": sobra,
        "sobra_pos_invest": sobra_pos_invest,
        "cats_house": cats_house,
        "cats_personal": cats_personal,
        "cats_recv": cats_recv,
        "cats_pay": cats_pay,
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

def is_manual_transaction(tx_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT i.source
      FROM transactions t
      JOIN imports i ON i.batch_id = t.batch_id
      WHERE t.id = ?
      LIMIT 1
    """, (tx_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row and (row["source"] == "manual"))

def get_transaction(tx_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT t.*, i.source
      FROM transactions t
      JOIN imports i ON i.batch_id = t.batch_id
      WHERE t.id = ?
      LIMIT 1
    """, (tx_id,))
    row = cur.fetchone()
    conn.close()
    return row

def update_transaction(tx_id: int, profile: str, new_row: dict) -> tuple[bool, str]:
    tx = get_transaction(tx_id)
    if not tx:
        return False, "Lançamento não encontrado"
    if tx["uploaded_by"] != profile:
        return False, "Você só pode editar lançamentos do seu perfil"
    if tx["source"] != "manual":
        return False, "Somente lançamentos manuais podem ser editados aqui"

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      UPDATE transactions
      SET dt_text = ?, estabelecimento = ?, categoria = ?, valor = ?, tipo = ?, dono = ?, rateio = ?, observacao = ?, parcela = ?
      WHERE id = ?
    """, (
        new_row.get("Data", ""),
        new_row.get("Estabelecimento", ""),
        new_row.get("Categoria", ""),
        float(new_row.get("Valor") or 0),
        new_row.get("Tipo", ""),
        new_row.get("Dono", ""),
        new_row.get("Rateio", ""),
        new_row.get("Observacao", ""),
        new_row.get("Parcela", ""),
        tx_id
    ))
    conn.commit()
    conn.close()
    return True, "Lançamento atualizado"

def delete_transaction(tx_id: int, profile: str) -> tuple[bool, str]:
    tx = get_transaction(tx_id)
    if not tx:
        return False, "Lançamento não encontrado"
    if tx["uploaded_by"] != profile:
        return False, "Você só pode excluir lançamentos do seu perfil"
    if tx["source"] != "manual":
        return False, "Somente lançamentos manuais podem ser excluídos aqui"

    batch_id = tx["batch_id"]

    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))

    # Se o batch ficou vazio, remove o import também
    cur.execute("SELECT COUNT(*) as c FROM transactions WHERE batch_id = ?", (batch_id,))
    c = int(cur.fetchone()["c"])
    if c == 0:
        cur.execute("DELETE FROM imports WHERE batch_id = ?", (batch_id,))

    conn.commit()
    conn.close()
    return True, "Lançamento excluído"

def list_entries(month_ref: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT t.*, i.source, i.status
      FROM transactions t
      JOIN imports i ON i.batch_id = t.batch_id
      WHERE t.month_ref = ? AND i.status = 'imported'
      ORDER BY t.id DESC
      LIMIT 400
    """, (month_ref,))
    rows = cur.fetchall()
    conn.close()
    return rows

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
            <p class="muted">Fluxo sugerido: Renda e Investimentos e depois Individual.</p>
            {month_selector_block(selected_year, selected_month, url_for('dashboard'))}
            <div class="row" style="justify-content:flex-start; margin-top:12px;">
              <a class="btn btnPrimary" href="{url_for('renda')}?Ano={selected_year}&Mes={selected_month}">Renda</a>
              <a class="btn btnPrimary" href="{url_for('investimentos')}?Ano={selected_year}&Mes={selected_month}">Investimentos</a>
              <a class="btn btnPrimary" href="{url_for('individual')}?Ano={selected_year}&Mes={selected_month}">Individual</a>
              <a class="btn" href="{url_for('casa')}?month_ref={month_ref}">Casa</a>
              <a class="btn" href="{url_for('manual_add')}?Ano={selected_year}&Mes={selected_month}">Adicionar lançamento</a>
            </div>
          </div>
        </div>
      </body>
    </html>
    """
    return html

@app.route("/upload", methods=["GET", "POST"])
def upload():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    default_year = "2026"
    default_month = f"{now_m:02d}"

    selected_year = request.form.get("Ano") or request.args.get("Ano") or default_year
    selected_month = request.form.get("Mes") or request.args.get("Mes") or default_month
    month_ref = month_ref_from(selected_year, selected_month)

    errors = []
    preview_rows = []
    batch_id = ""
    imported_msg = ""
    imported_ok = False

    action = request.form.get("action", "")

    if request.method == "POST" and action == "import":
        batch_id = _normalize_str(request.form.get("batch_id"))
        ok, msg = finalize_import(batch_id, profile)
        imported_ok = ok
        imported_msg = msg

    elif request.method == "POST":
        if not (selected_year.isdigit() and len(selected_year) == 4):
            errors.append("Ano inválido")
        if not (selected_month.isdigit() and len(selected_month) == 2 and 1 <= int(selected_month) <= 12):
            errors.append("Mês inválido")

        file = request.files.get("file")
        if not file or file.filename.strip() == "":
            errors.append("Arquivo obrigatório")

        if not errors:
            try:
                raw = file.read()
                file_hash = compute_file_hash(raw)

                if is_duplicate_import(month_ref, profile, file_hash):
                    errors.append("Esse mesmo arquivo já foi importado neste mês para este perfil. Vá em Importações para ver e, se precisar, excluir.")
                else:
                    df = read_excel_from_bytes(raw)
                    errors, preview_rows = validate_transactions(df)
                    if not errors:
                        batch_id = create_preview_batch(month_ref, profile, file.filename, preview_rows, file_hash)

            except Exception as e:
                errors.append(str(e))

    year_options, month_options = year_month_select_html(selected_year, selected_month)

    error_block = ""
    ok_block = ""

    if errors:
        items = "".join([f"<li>{e}</li>" for e in errors[:60]])
        error_block = f"""
          <div class="card">
            <h3>Erros</h3>
            <div class="errorBox"><ul>{items}</ul></div>
          </div>
        """

    if imported_msg:
        klass = "okBox" if imported_ok else "errorBox"
        ok_block = f"""
          <div class="card">
            <h3>Resultado</h3>
            <div class="{klass}">
              <p><b>{imported_msg}</b></p>
              <div class="row" style="justify-content:flex-start; margin-top:10px;">
                <a class="btn btnPrimary" href="{url_for('casa')}?month_ref={month_ref}">Abrir Casa</a>
                <a class="btn" href="{url_for('imports_list')}?month_ref={month_ref}">Ver Importações</a>
                <a class="btn" href="{url_for('individual')}?Ano={selected_year}&Mes={selected_month}">Ver Individual</a>
              </div>
            </div>
          </div>
        """

    preview_table = ""
    if batch_id and preview_rows and not errors:
        head = "".join([f"<th>{c}</th>" for c in REQUIRED_COLUMNS])
        body_rows = ""
        for r in preview_rows[:20]:
            tds = "".join([f"<td>{'' if r.get(c) is None else r.get(c)}</td>" for c in REQUIRED_COLUMNS])
            body_rows += f"<tr>{tds}</tr>"

        preview_table = f"""
          <div class="card">
            <h3>Preview</h3>
            <p class="muted">Batch criado: <b>{batch_id[:10]}...</b> (mostrando 20 linhas)</p>
            <div class="okBox">
              <p>Preview válido. Clique em <b>Importar</b> para confirmar esse batch.</p>
              <form method="post">
                <input type="hidden" name="Ano" value="{selected_year}">
                <input type="hidden" name="Mes" value="{selected_month}">
                <input type="hidden" name="action" value="import">
                <input type="hidden" name="batch_id" value="{batch_id}">
                <div class="row" style="justify-content:flex-start; margin-top:10px;">
                  <button class="btn btnPrimary" type="submit">Importar</button>
                  <a class="btn" href="{url_for('imports_list')}?month_ref={month_ref}">Ver Importações</a>
                </div>
              </form>
              <p class="muted" style="margin-top:10px;">Se você não importar, o batch fica como preview e você pode excluir depois em Importações.</p>
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
        <title>Upload</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>Upload</h2>
            <p>Pagador do upload: <b>{profile}</b></p>

            <form id="uploadForm" method="post" enctype="multipart/form-data">
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

              <label>Arquivo Excel</label>
              <input id="fileInput" type="file" name="file" accept=".xlsx,.xls" />

              <p class="muted" style="margin-top:12px;">Ao escolher o arquivo, o preview abre automaticamente.</p>
              <p class="muted">Colunas obrigatórias: {", ".join(REQUIRED_COLUMNS)}</p>
            </form>

            <div class="row" style="justify-content:flex-start; margin-top:12px;">
              <a class="btn" href="{url_for('manual_add')}?Ano={selected_year}&Mes={selected_month}">Adicionar lançamento manual</a>
              <a class="btn" href="{url_for('entries')}?month_ref={month_ref}">Ver lançamentos do mês</a>
            </div>
          </div>

          {error_block}
          {ok_block}
          {preview_table}
        </div>

        <script>
          const fileInput = document.getElementById("fileInput");
          const form = document.getElementById("uploadForm");
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

@app.route("/imports", methods=["GET", "POST"])
def imports_list():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    default_year = "2026"
    default_month = f"{now_m:02d}"
    month_ref = request.values.get("month_ref") or f"{default_year}{default_month}"

    msg = ""
    msg_ok = True

    if request.method == "POST":
        action = request.form.get("action", "")
        batch_id = _normalize_str(request.form.get("batch_id"))
        if action == "delete":
            ok, m = delete_batch(batch_id, profile)
            msg = m
            msg_ok = ok

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
      SELECT * FROM imports
      WHERE month_ref = ?
      ORDER BY created_at DESC
    """, (month_ref,))
    imports = cur.fetchall()
    conn.close()

    rows_html = ""
    for imp in imports:
        can_delete = (imp["uploaded_by"] == profile)
        del_btn = ""
        if can_delete:
            del_btn = f"""
              <form method="post" style="display:inline;">
                <input type="hidden" name="month_ref" value="{month_ref}">
                <input type="hidden" name="action" value="delete">
                <input type="hidden" name="batch_id" value="{imp['batch_id']}">
                <button class="btn btnDanger" type="submit">Excluir</button>
              </form>
            """
        rows_html += f"""
          <tr>
            <td>{imp['created_at']}</td>
            <td>{imp['uploaded_by']}</td>
            <td>{imp['status']}</td>
            <td>{imp['source'] or ''}</td>
            <td>{imp['filename'] or ''}</td>
            <td class="right">{imp['row_count']}</td>
            <td class="mono">{(imp['batch_id'][:10] + '...')}</td>
            <td class="mono">{(imp['file_hash'][:10] + '...') if imp['file_hash'] else ''}</td>
            <td>{del_btn}</td>
          </tr>
        """

    if not rows_html:
        rows_html = "<tr><td colspan='9' class='muted'>Sem importações para esse mês</td></tr>"

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

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Importações</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>Importações</h2>
            <p class="muted">Você só consegue excluir imports feitos no seu perfil ({profile}).</p>

            <form method="get">
              <label>Mês de referência</label>
              <input type="text" name="month_ref" value="{month_ref}" placeholder="202602" />
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Atualizar</button>
                <a class="btn" href="{url_for('upload')}?Ano={month_ref[:4]}&Mes={month_ref[4:]}">Novo Upload</a>
              </div>
            </form>

            <table>
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Perfil</th>
                  <th>Status</th>
                  <th>Fonte</th>
                  <th>Arquivo</th>
                  <th class="right">Linhas</th>
                  <th>Batch</th>
                  <th>Hash</th>
                  <th>Ações</th>
                </tr>
              </thead>
              <tbody>
                {rows_html}
              </tbody>
            </table>
          </div>

          {msg_block}
        </div>
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
            <p>Somente Dono Casa com rateio 60_40 e 50_50, somente batches importados.</p>
            <form method="get">
              <label>Mês de referência</label>
              <input type="text" name="month_ref" value="{month_ref}" placeholder="202602" />
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Atualizar</button>
                <a class="btn" href="{url_for('imports_list')}?month_ref={month_ref}">Ver Importações do mês</a>
                <a class="btn" href="{url_for('entries')}?month_ref={month_ref}">Ver lançamentos</a>
              </div>
              <p class="muted" style="margin-top:10px;">Formato: YYYYMM</p>
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
            <p class="muted">Total por categoria e quanto cada um pagou dentro daquela categoria.</p>
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
    msg_ok = True

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
        msg_ok = True

    inc = get_income(month_ref, profile)

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
            <h2>Renda do {profile}</h2>
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

@app.route("/investimentos", methods=["GET", "POST"])
def investimentos():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    selected_year = request.values.get("Ano") or "2026"
    selected_month = request.values.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    msg = ""
    msg_ok = True

    if request.method == "POST":
        def num(v):
            try:
                v = str(v).replace(".", "").replace(",", ".")
                return float(v) if v else 0.0
            except:
                return 0.0

        amount = num(request.form.get("amount"))
        note = _normalize_str(request.form.get("note"))
        upsert_investment(month_ref, profile, amount, note)
        msg = "Investimento salvo"
        msg_ok = True

    inv = get_investment(month_ref, profile)

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

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Investimentos</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>Investimentos do {profile}</h2>
            <p class="muted">Tracking simples: quanto você guardou/investiu no mês, depois do fechamento.</p>
            {month_selector_block(selected_year, selected_month, url_for('investimentos'))}
          </div>

          <div class="card">
            <h3>Valor do mês</h3>
            <form method="post">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">

              <div class="grid2">
                <div>
                  <label>Quanto você guardou/investiu</label>
                  <input type="text" name="amount" value="{inv['amount']:.2f}" />
                </div>
                <div>
                  <label>Observação (opcional)</label>
                  <input type="text" name="note" value="{inv['note']}" />
                </div>
              </div>

              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Salvar</button>
                <a class="btn" href="{url_for('individual')}?Ano={selected_year}&Mes={selected_month}">Ver Individual</a>
              </div>
            </form>
          </div>

          {msg_block}
        </div>
      </body>
    </html>
    """
    return html

@app.route("/individual", methods=["GET"])
def individual():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    selected_year = request.args.get("Ano") or "2026"
    selected_month = request.args.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    data = compute_individual(month_ref, profile)

    def rows_from(items):
        out = ""
        for cat, val in items:
            out += f"<tr><td>{cat}</td><td class='right'>{brl(val)}</td></tr>"
        if not out:
            out = "<tr><td colspan='2' class='muted'>Sem dados</td></tr>"
        return out

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
              <a class="btn btnPrimary" href="{url_for('renda')}?Ano={selected_year}&Mes={selected_month}">Editar Renda</a>
              <a class="btn btnPrimary" href="{url_for('investimentos')}?Ano={selected_year}&Mes={selected_month}">Editar Investimentos</a>
              <a class="btn" href="{url_for('casa')}?month_ref={month_ref}">Ver Casa</a>
              <a class="btn" href="{url_for('entries')}?month_ref={month_ref}">Ver lançamentos</a>
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
                <div class="label">A receber do outro</div>
                <div class="value">{brl(data["receivable_total"])}</div>
              </div>
              <div class="box">
                <div class="label">Gastos efetivos</div>
                <div class="value">{brl(data["expenses_effective"])}</div>
                <div class="muted">Casa + Pessoal + A pagar</div>
              </div>
              <div class="box">
                <div class="label">Sobra do mês</div>
                <div class="value">{brl(data["sobra"])}</div>
                <div class="muted">Renda - Gastos efetivos</div>
              </div>
              <div class="box">
                <div class="label">Investido no mês</div>
                <div class="value">{brl(data["invested"])}</div>
                <div class="muted">Tracking manual</div>
              </div>
            </div>

            <div class="kpi" style="margin-top:12px;">
              <div class="box">
                <div class="label">Sobra após investir</div>
                <div class="value">{brl(data["sobra_pos_invest"])}</div>
                <div class="muted">Sobra - Investido</div>
              </div>
              <div class="box">
                <div class="label">Atalho</div>
                <div class="value">{month_ref}</div>
                <div class="muted">Mês de referência</div>
              </div>
              <div class="box">
                <div class="label">Ação rápida</div>
                <div class="value">+</div>
                <div class="muted"><a class="btn btnPrimary" href="{url_for('manual_add')}?Ano={selected_year}&Mes={selected_month}">Adicionar lançamento</a></div>
              </div>
              <div class="box">
                <div class="label">Nota</div>
                <div class="value">•</div>
                <div class="muted">A receber não entra na sobra até cair no saldo</div>
              </div>
            </div>
          </div>

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

          <div class="card">
            <h3>A receber do outro por categoria</h3>
            <table>
              <thead><tr><th>Categoria</th><th class="right">Valor</th></tr></thead>
              <tbody>{rows_from(data["cats_recv"])}</tbody>
            </table>
          </div>

          <div class="card">
            <h3>A pagar para o outro por categoria</h3>
            <table>
              <thead><tr><th>Categoria</th><th class="right">Valor</th></tr></thead>
              <tbody>{rows_from(data["cats_pay"])}</tbody>
            </table>
          </div>

        </div>
      </body>
    </html>
    """
    return html

@app.route("/entries", methods=["GET"])
def entries():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    default_year = "2026"
    default_month = f"{now_m:02d}"
    month_ref = request.args.get("month_ref") or f"{default_year}{default_month}"

    rows = list_entries(month_ref)

    body = ""
    for r in rows:
        val = signed_value(r["tipo"], r["valor"])
        src = r["source"] or ""
        can_edit = (src == "manual" and r["uploaded_by"] == profile)
        actions = ""
        if can_edit:
            actions = f"""
              <a class="btn" href="{url_for('manual_edit', tx_id=r['id'])}">Editar</a>
              <a class="btn btnDanger" href="{url_for('manual_delete', tx_id=r['id'])}">Excluir</a>
            """
        body += f"""
          <tr>
            <td class="mono">{r['id']}</td>
            <td>{r['uploaded_by']}</td>
            <td>{src}</td>
            <td>{r['dt_text'] or ''}</td>
            <td>{r['estabelecimento'] or ''}</td>
            <td>{r['categoria'] or ''}</td>
            <td class="right">{brl(val)}</td>
            <td>{r['tipo']}</td>
            <td>{r['dono']}</td>
            <td>{r['rateio']}</td>
            <td>{actions}</td>
          </tr>
        """

    if not body:
        body = "<tr><td colspan='11' class='muted'>Sem lançamentos importados para esse mês</td></tr>"

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
            <form method="get">
              <label>Mês de referência</label>
              <input type="text" name="month_ref" value="{month_ref}" placeholder="202602" />
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Atualizar</button>
                <a class="btn" href="{url_for('manual_add')}?Ano={month_ref[:4]}&Mes={month_ref[4:]}">Adicionar manual</a>
              </div>
              <p class="muted" style="margin-top:10px;">Você só consegue editar/excluir lançamentos manuais feitos no seu perfil.</p>
            </form>

            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Pagador</th>
                  <th>Fonte</th>
                  <th>Data</th>
                  <th>Estabelecimento</th>
                  <th>Categoria</th>
                  <th class="right">Valor</th>
                  <th>Tipo</th>
                  <th>Dono</th>
                  <th>Rateio</th>
                  <th>Ações</th>
                </tr>
              </thead>
              <tbody>
                {body}
              </tbody>
            </table>
          </div>
        </div>
      </body>
    </html>
    """
    return html

@app.route("/manual/add", methods=["GET", "POST"])
def manual_add():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    selected_year = request.values.get("Ano") or "2026"
    selected_month = request.values.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    msg = ""
    msg_ok = True
    errors = []

    # defaults
    form_data = {
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
        for k in form_data.keys():
            form_data[k] = _normalize_str(request.form.get(k))

        # parse valor
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
            msg = "Lançamento manual adicionado"
            msg_ok = True

            # limpa o form
            form_data["Data"] = ""
            form_data["Estabelecimento"] = ""
            form_data["Categoria"] = ""
            form_data["Valor"] = ""
            form_data["Observacao"] = ""
            form_data["Parcela"] = ""

    err_block = ""
    if errors:
        items = "".join([f"<li>{e}</li>" for e in errors[:30]])
        err_block = f"""
          <div class="card">
            <h3>Erros</h3>
            <div class="errorBox"><ul>{items}</ul></div>
          </div>
        """

    msg_block = ""
    if msg:
        klass = "okBox" if msg_ok else "errorBox"
        msg_block = f"""
          <div class="card">
            <div class="{klass}">
              <b>{msg}</b>
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <a class="btn btnPrimary" href="{url_for('entries')}?month_ref={month_ref}">Ver lançamentos</a>
                <a class="btn" href="{url_for('individual')}?Ano={selected_year}&Mes={selected_month}">Ver Individual</a>
              </div>
            </div>
          </div>
        """

    tipo_opts = "".join([f"<option value='{t}' {'selected' if form_data['Tipo']==t else ''}>{t}</option>" for t in sorted(ALLOWED_TIPO)])
    dono_opts = "".join([f"<option value='{d}' {'selected' if form_data['Dono']==d else ''}>{d}</option>" for d in ["Casa","Lucas","Rafa"]])
    rateio_opts = "".join([f"<option value='{r}' {'selected' if form_data['Rateio']==r else ''}>{r}</option>" for r in ["60_40","50_50","100_meu","100_outro"]])

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Adicionar lançamento</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>Adicionar lançamento manual</h2>
            <p class="muted">Pagador: <b>{profile}</b> • Isso entra automaticamente como importado (fonte manual).</p>
            {month_selector_block(selected_year, selected_month, url_for('manual_add'))}
          </div>

          <div class="card">
            <h3>Dados</h3>
            <form method="post">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">

              <div class="grid2">
                <div>
                  <label>Data</label>
                  <input type="text" name="Data" value="{form_data['Data']}" placeholder="Opcional" />
                </div>
                <div>
                  <label>Valor</label>
                  <input type="text" name="Valor" value="{form_data['Valor']}" placeholder="ex: 120,50" />
                </div>
              </div>

              <div class="grid2">
                <div>
                  <label>Estabelecimento</label>
                  <input type="text" name="Estabelecimento" value="{form_data['Estabelecimento']}" />
                </div>
                <div>
                  <label>Categoria</label>
                  <input type="text" name="Categoria" value="{form_data['Categoria']}" />
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
                  <input type="text" name="Parcela" value="{form_data['Parcela']}" />
                </div>
                <div>
                  <label>Observação (opcional)</label>
                  <input type="text" name="Observacao" value="{form_data['Observacao']}" />
                </div>
              </div>

              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Salvar lançamento</button>
                <a class="btn" href="{url_for('entries')}?month_ref={month_ref}">Ver lançamentos do mês</a>
              </div>
            </form>
          </div>

          {err_block}
          {msg_block}
        </div>
      </body>
    </html>
    """
    return html

@app.route("/manual/edit/<int:tx_id>", methods=["GET", "POST"])
def manual_edit(tx_id: int):
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    tx = get_transaction(tx_id)
    if not tx:
        return "Lançamento não encontrado", 404

    if tx["uploaded_by"] != profile:
        return "Você só pode editar lançamentos do seu perfil", 403

    if tx["source"] != "manual":
        return "Somente lançamentos manuais podem ser editados aqui", 400

    month_ref = tx["month_ref"]
    selected_year = month_ref[:4]
    selected_month = month_ref[4:]

    msg = ""
    msg_ok = True
    errors = []

    form_data = {
        "Data": tx["dt_text"] or "",
        "Estabelecimento": tx["estabelecimento"] or "",
        "Categoria": tx["categoria"] or "",
        "Valor": f"{float(tx['valor'] or 0):.2f}",
        "Tipo": tx["tipo"] or "Saida",
        "Dono": tx["dono"] or "Casa",
        "Rateio": tx["rateio"] or "60_40",
        "Observacao": tx["observacao"] or "",
        "Parcela": tx["parcela"] or "",
    }

    if request.method == "POST":
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
            ok, m = update_transaction(tx_id, profile, row)
            msg = m
            msg_ok = ok

    err_block = ""
    if errors:
        items = "".join([f"<li>{e}</li>" for e in errors[:30]])
        err_block = f"""
          <div class="card">
            <h3>Erros</h3>
            <div class="errorBox"><ul>{items}</ul></div>
          </div>
        """

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

    tipo_opts = "".join([f"<option value='{t}' {'selected' if form_data['Tipo']==t else ''}>{t}</option>" for t in sorted(ALLOWED_TIPO)])
    dono_opts = "".join([f"<option value='{d}' {'selected' if form_data['Dono']==d else ''}>{d}</option>" for d in ["Casa","Lucas","Rafa"]])
    rateio_opts = "".join([f"<option value='{r}' {'selected' if form_data['Rateio']==r else ''}>{r}</option>" for r in ["60_40","50_50","100_meu","100_outro"]])

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Editar lançamento</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          <div class="card">
            <h2>Editar lançamento manual</h2>
            <p class="muted">ID: <span class="mono">{tx_id}</span> • Mês: <b>{month_ref}</b></p>

            <form method="post">
              <div class="grid2">
                <div>
                  <label>Data</label>
                  <input type="text" name="Data" value="{form_data['Data']}" />
                </div>
                <div>
                  <label>Valor</label>
                  <input type="text" name="Valor" value="{form_data['Valor']}" />
                </div>
              </div>

              <div class="grid2">
                <div>
                  <label>Estabelecimento</label>
                  <input type="text" name="Estabelecimento" value="{form_data['Estabelecimento']}" />
                </div>
                <div>
                  <label>Categoria</label>
                  <input type="text" name="Categoria" value="{form_data['Categoria']}" />
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
                  <input type="text" name="Parcela" value="{form_data['Parcela']}" />
                </div>
                <div>
                  <label>Observação (opcional)</label>
                  <input type="text" name="Observacao" value="{form_data['Observacao']}" />
                </div>
              </div>

              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Salvar alterações</button>
                <a class="btn" href="{url_for('entries')}?month_ref={month_ref}">Voltar para lançamentos</a>
                <a class="btn btnDanger" href="{url_for('manual_delete', tx_id=tx_id)}">Excluir</a>
              </div>
            </form>
          </div>

          {err_block}
          {msg_block}
        </div>
      </body>
    </html>
    """
    return html

@app.route("/manual/delete/<int:tx_id>", methods=["GET"])
def manual_delete(tx_id: int):
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    tx = get_transaction(tx_id)
    if not tx:
        return "Lançamento não encontrado", 404

    month_ref = tx["month_ref"]
    ok, msg = delete_transaction(tx_id, profile)

    if not ok:
        return msg, 400

    return redirect(url_for("entries", month_ref=month_ref))

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
