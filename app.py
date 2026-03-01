from flask import Flask, redirect, url_for, session, request, send_file
import io
import os
import sqlite3
import datetime as dt
import uuid
import hashlib
import pandas as pd

app = Flask(__name__)
app.secret_key = "dev-secret-change-later"

DB_PATH = "data.db"

# Shares (sempre Rafa 60 / Lucas 40)
LUCAS_SHARE = 0.40
RAFA_SHARE = 0.60

ALLOWED_PROFILES = {"Lucas", "Rafa"}
ALLOWED_TIPO = {"Saida", "Entrada"}

# Template lists
ALLOWED_PAGADOR = {"Lucas", "Rafa", "Casa"}
ALLOWED_RATEIO_DISPLAY = {"60/40", "50/50", "100%_Meu", "100%_Outro"}

ALLOWED_CATEGORIAS = {
    "Alimentação",
    "Assinaturas e Serviços Digitais",
    "Carro",
    "Combustível",
    "Compras Online Diversas",
    "Compras Pessoais",
    "Contas da Casa",
    "Pets",
    "Presentes",
    "Saúde",
    "Supermercado e Itens Domésticos",
    "Transporte",
    "Viagens e Lazer",
    "Outros",
}

TEMPLATE_REQUIRED_COLUMNS = ["Data", "Pagador", "Categoria", "Descrição", "Valor", "Rateio"]

# Template download path inside repo
TEMPLATE_FILE_PATH = os.path.join("static", "templates_download", "Template__Finanças__Casella.xlsx")


# =========================
# UI (nicer + responsive)
# =========================
BASE_CSS = """
<style>
  :root{
    --bg: #0b1020;
    --card: rgba(255,255,255,.92);
    --card2: rgba(255,255,255,.86);
    --text: #101425;
    --muted: rgba(16,20,37,.70);
    --line: rgba(16,20,37,.10);
    --accent: #6d5efc;
    --accent2: #10b981;
    --danger: #ef4444;
    --shadow: 0 14px 38px rgba(0,0,0,.18);
    --radius: 18px;
  }

  *{ box-sizing:border-box; }
  body{
    font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    margin:0;
    color: var(--text);
    background:
      radial-gradient(1200px 600px at 15% 10%, rgba(109,94,252,.35), transparent 60%),
      radial-gradient(900px 500px at 85% 15%, rgba(16,185,129,.25), transparent 55%),
      radial-gradient(800px 500px at 50% 90%, rgba(239,68,68,.10), transparent 55%),
      #070b16;
    min-height:100vh;
  }

  a { color: inherit; }
  .wrap{ max-width: 1180px; margin: 0 auto; padding: 18px; }
  .topbar{
    position: sticky; top: 0; z-index: 3;
    background: rgba(7,11,22,.72);
    backdrop-filter: blur(10px);
    border-bottom: 1px solid rgba(255,255,255,.08);
  }
  .topbar .inner{ max-width: 1180px; margin:0 auto; padding: 14px 18px; display:flex; gap:12px; align-items:center; justify-content:space-between; }
  .brand{ display:flex; align-items:center; gap:10px; color: rgba(255,255,255,.92); }
  .brand .logo{
    width: 34px; height: 34px; border-radius: 12px;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    box-shadow: 0 12px 26px rgba(109,94,252,.25);
  }
  .brand b{ font-size: 14px; letter-spacing:.2px; }
  .pill{
    display:inline-flex; gap:8px; align-items:center;
    padding: 6px 10px; border-radius: 999px;
    background: rgba(255,255,255,.12);
    border: 1px solid rgba(255,255,255,.12);
    color: rgba(255,255,255,.88);
    font-size: 12px;
  }
  .nav{ display:flex; gap:8px; flex-wrap: wrap; justify-content:flex-end; }
  .btn{
    display:inline-flex; align-items:center; justify-content:center;
    padding: 10px 12px; border-radius: 14px;
    text-decoration:none; cursor:pointer; border: 1px solid rgba(255,255,255,.14);
    background: rgba(255,255,255,.08);
    color: rgba(255,255,255,.92);
    font-weight: 750;
  }
  .btn:hover{ transform: translateY(-1px); transition: .15s; border-color: rgba(255,255,255,.22); }
  .btnPrimary{
    background: linear-gradient(135deg, var(--accent), #8b7cff);
    border-color: rgba(255,255,255,.10);
  }
  .btnGreen{
    background: linear-gradient(135deg, var(--accent2), #34d399);
    border-color: rgba(255,255,255,.10);
  }
  .btnDanger{
    background: linear-gradient(135deg, var(--danger), #fb7185);
    border-color: rgba(255,255,255,.10);
  }

  .card{
    background: var(--card);
    border: 1px solid rgba(255,255,255,.30);
    border-radius: var(--radius);
    padding: 18px;
    margin-top: 14px;
    box-shadow: var(--shadow);
  }
  .card.soft{ background: var(--card2); }
  h1,h2,h3{ margin: 0 0 10px; }
  p{ margin: 0 0 10px; color: var(--muted); }
  .muted{ color: var(--muted); font-size: 12px; }

  label{ font-weight: 800; display:block; margin-top: 10px; margin-bottom: 6px; font-size: 13px; }
  input[type="text"], input[type="number"], input[type="file"], select, textarea{
    width: 100%;
    padding: 11px 12px;
    border: 1px solid rgba(16,20,37,.12);
    border-radius: 14px;
    background: rgba(255,255,255,.95);
    outline: none;
  }
  input:focus, select:focus, textarea:focus{
    border-color: rgba(109,94,252,.55);
    box-shadow: 0 0 0 4px rgba(109,94,252,.12);
  }

  .row{ display:flex; gap: 10px; flex-wrap: wrap; align-items:center; justify-content:space-between; }
  .grid2{ display:grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .grid3{ display:grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }
  .grid4{ display:grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px; }

  .kpi{ display:grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px; }
  .kpi .box{
    background: rgba(255,255,255,.92);
    border: 1px solid rgba(16,20,37,.08);
    border-radius: 16px;
    padding: 14px;
  }
  .kpi .label{ font-size: 12px; color: rgba(16,20,37,.65); margin-bottom: 6px; font-weight: 800; }
  .kpi .value{ font-size: 20px; font-weight: 900; letter-spacing: .2px; }

  .okBox{
    border: 1px solid rgba(16,185,129,.22);
    background: rgba(16,185,129,.10);
    padding: 12px;
    border-radius: 14px;
  }
  .errorBox{
    border: 1px solid rgba(239,68,68,.22);
    background: rgba(239,68,68,.10);
    padding: 12px;
    border-radius: 14px;
  }
  .warnBox{
    border: 1px solid rgba(245,158,11,.22);
    background: rgba(245,158,11,.10);
    padding: 12px;
    border-radius: 14px;
  }

  table{ width:100%; border-collapse: collapse; margin-top: 10px; overflow:hidden; border-radius: 14px; }
  th, td{
    border-bottom: 1px solid rgba(16,20,37,.08);
    padding: 10px 8px;
    text-align:left;
    font-size: 13px;
    vertical-align: top;
  }
  th{
    background: rgba(16,20,37,.05);
    font-weight: 900;
  }
  tr:hover td{ background: rgba(109,94,252,.04); }
  .right{ text-align:right; }
  .mono{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
  .small{ font-size: 12px; }

  .stickyBar{ position: sticky; top: 62px; background: rgba(255,255,255,.84); backdrop-filter: blur(10px); padding: 10px; border-radius: 16px; border: 1px solid rgba(16,20,37,.08); }

  .divider{ height:1px; background: rgba(16,20,37,.10); margin: 12px 0; }

  @media (max-width: 980px){
    .grid4,.grid3,.grid2,.kpi{ grid-template-columns: 1fr; }
    .topbar .inner{ flex-direction: column; align-items: flex-start; }
    .nav{ justify-content:flex-start; }
  }
</style>
"""


# =========================
# Helpers
# =========================
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


def compute_file_hash(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()


def signed_value(tipo: str, valor: float) -> float:
    # Mantém seu comportamento: Entrada entra negativo no somatório de gastos
    if tipo == "Entrada":
        return -abs(valor)
    return abs(valor)


def share_for_profile(profile: str, rateio_display: str) -> float:
    if rateio_display == "50/50":
        return 0.5
    if rateio_display == "60/40":
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


def topbar_html(profile: str):
    nav = ""
    if profile:
        nav = f"""
        <div class="nav">
          <a class="btn" href="{url_for('dashboard')}">Painel</a>
          <a class="btn" href="{url_for('gastos')}">Gastos</a>
          <a class="btn" href="{url_for('lancamentos')}">Lançamentos</a>
          <a class="btn" href="{url_for('casa')}">Casa</a>
          <a class="btn" href="{url_for('individual')}">Individual</a>
          <a class="btn" href="{url_for('renda')}">Renda</a>
          <a class="btn" href="{url_for('home')}">Trocar perfil</a>
        </div>
        """
    return f"""
    <div class="topbar">
      <div class="inner">
        <div class="brand">
          <div class="logo"></div>
          <div>
            <b>Finanças da Casa</b><br/>
            {f"<span class='pill'>Perfil: <b>{profile}</b></span>" if profile else ""}
          </div>
        </div>
        {nav}
      </div>
    </div>
    """


# =========================
# Month lock (por perfil)
# =========================
def is_month_locked(month_ref: str, profile: str) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT is_locked
      FROM month_locks
      WHERE month_ref = ? AND profile = ?
      LIMIT 1
    """, (month_ref, profile))
    row = cur.fetchone()
    conn.close()
    return bool(row["is_locked"]) if row else False


def set_month_lock(month_ref: str, profile: str, locked: bool):
    now = dt.datetime.utcnow().isoformat(timespec="seconds")
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM month_locks WHERE month_ref = ? AND profile = ? LIMIT 1", (month_ref, profile))
    exists = cur.fetchone() is not None
    if exists:
        cur.execute("""
          UPDATE month_locks
          SET is_locked = ?, updated_at = ?
          WHERE month_ref = ? AND profile = ?
        """, (1 if locked else 0, now, month_ref, profile))
    else:
        cur.execute("""
          INSERT INTO month_locks (month_ref, profile, is_locked, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?)
        """, (month_ref, profile, 1 if locked else 0, now, now))
    conn.commit()
    conn.close()


# =========================
# Fixos e pendentes
# =========================
FIXOS = [
    # description, valor, pagador_real, rateio
    ("Aluguel", 2541.00, "Rafa", "60/40"),
    ("Condominio", 1374.42, "Lucas", "60/40"),
    ("Internet", 115.00, "Lucas", "60/40"),
    ("Estacionamento Amazon", 75.00, "Rafa", "60/40"),
]

PENDENTES = [
    # description, categoria sugerida
    ("Luz", "Contas da Casa"),
    ("Gas", "Contas da Casa"),
    ("Empregada", "Contas da Casa"),
]


def ensure_fixed_rows(month_ref: str):
    """
    Garante que os fixos existam no mês, mas sem duplicar.
    Eles entram como Casa (rateio 60/40) e com pagador real definido.
    """
    conn = get_db()
    cur = conn.cursor()

    # batch fixo por mês
    cur.execute("""
      SELECT batch_id FROM imports
      WHERE month_ref = ? AND source = 'fixed' AND status = 'imported'
      LIMIT 1
    """, (month_ref,))
    existing_batch = cur.fetchone()
    batch_id = existing_batch["batch_id"] if existing_batch else None

    if not batch_id:
        batch_id = uuid.uuid4().hex
        now = dt.datetime.utcnow().isoformat(timespec="seconds")
        cur.execute("""
          INSERT INTO imports (batch_id, month_ref, uploaded_by, filename, row_count, status, created_at, file_hash, source)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (batch_id, month_ref, "system", "fixed", 0, "imported", now, None, "fixed"))
        conn.commit()

    # para cada fixo, se não existir uma linha igual naquele mês, criar
    created = 0
    for desc, valor, pagador_real, rateio in FIXOS:
        cur.execute("""
          SELECT 1 FROM transactions t
          JOIN imports i ON i.batch_id = t.batch_id
          WHERE t.month_ref = ?
            AND i.status = 'imported'
            AND i.source = 'fixed'
            AND t.descricao = ?
          LIMIT 1
        """, (month_ref, desc))
        if cur.fetchone():
            continue

        now = dt.datetime.utcnow().isoformat(timespec="seconds")
        cur.execute("""
          INSERT INTO transactions
          (batch_id, month_ref, uploaded_by, dt_text, descricao, categoria, valor, tipo,
           pagador_label, pagador_real, rateio_display, dono, observacao, parcela, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            batch_id,
            month_ref,
            "system",
            "",  # data opcional
            desc,
            "Contas da Casa",
            float(valor),
            "Saida",
            "Casa",          # label para mostrar como "Casa" (casa / casal)
            pagador_real,    # quem pagou de fato
            rateio,
            "Casa",
            "Fixo",
            "",
            now
        ))
        created += 1

    # atualiza row_count
    cur.execute("SELECT COUNT(*) as c FROM transactions WHERE batch_id = ?", (batch_id,))
    c = int(cur.fetchone()["c"])
    cur.execute("UPDATE imports SET row_count = ? WHERE batch_id = ?", (c, batch_id))

    conn.commit()
    conn.close()
    return created


def pendentes_status(month_ref: str):
    """
    Considera "preenchido" se existe pelo menos uma transação do mês
    com descricao contendo o termo (case-insensitive) e dono Casa.
    """
    conn = get_db()
    cur = conn.cursor()
    result = []
    for term, cat in PENDENTES:
        cur.execute("""
          SELECT 1 FROM transactions t
          JOIN imports i ON i.batch_id = t.batch_id
          WHERE t.month_ref = ?
            AND i.status = 'imported'
            AND t.dono = 'Casa'
            AND LOWER(t.descricao) LIKE ?
          LIMIT 1
        """, (month_ref, f"%{term.lower()}%"))
        filled = cur.fetchone() is not None
        result.append({"term": term, "categoria": cat, "filled": filled})
    conn.close()
    return result


def month_top_block(month_ref: str, profile: str):
    # garante fixos sempre existindo
    ensure_fixed_rows(month_ref)
    pend = pendentes_status(month_ref)
    pending_count = sum(1 for p in pend if not p["filled"])
    locked = is_month_locked(month_ref, profile)

    lock_btn = ""
    if locked:
        lock_btn = f"""
          <form method="post" action="{url_for('toggle_month_lock')}" style="display:inline;">
            <input type="hidden" name="month_ref" value="{month_ref}">
            <input type="hidden" name="action" value="unlock">
            <button class="btn btnGreen" type="submit">Editar mês</button>
          </form>
        """
    else:
        # aviso de pendentes no confirm
        warn = f"Existem {pending_count} pendentes" if pending_count > 0 else "Sem pendentes"
        lock_btn = f"""
          <form method="post" action="{url_for('toggle_month_lock')}" style="display:inline;"
                onsubmit="return confirm('Fechar mês {month_ref}? {warn}. Você pode reabrir depois em Editar mês.');">
            <input type="hidden" name="month_ref" value="{month_ref}">
            <input type="hidden" name="action" value="lock">
            <button class="btn btnPrimary" type="submit">Fechar mês</button>
          </form>
        """

    pend_html = ""
    for p in pend:
        badge = "<span class='pill' style='background:rgba(16,185,129,.20); border-color:rgba(16,185,129,.22)'>OK</span>" if p["filled"] else "<span class='pill' style='background:rgba(245,158,11,.22); border-color:rgba(245,158,11,.22)'>Pendente</span>"
        pend_html += f"<div class='row' style='justify-content:flex-start; gap:10px;'><b>{p['term']}</b> {badge}</div>"

    fixos_html = ""
    for desc, valor, pagador_real, rateio in FIXOS:
        fixos_html += f"<div class='row' style='justify-content:space-between;'><div><b>{desc}</b><div class='muted'>Pagador: {pagador_real} | Rateio: {rateio}</div></div><div><b>{brl(valor)}</b></div></div><div class='divider'></div>"

    lock_badge = "<span class='pill' style='background:rgba(239,68,68,.18)'>Mês fechado para você</span>" if locked else "<span class='pill' style='background:rgba(16,185,129,.14)'>Mês aberto</span>"

    return f"""
      <div class="card soft">
        <div class="row">
          <div>
            <h3 style="margin-bottom:4px;">Topo do mês {month_ref}</h3>
            <div class="muted">Fixos preenchidos automaticamente e lembretes de pendentes</div>
          </div>
          <div class="row" style="justify-content:flex-end;">
            {lock_badge}
            {lock_btn}
          </div>
        </div>

        <div class="grid2" style="margin-top:12px;">
          <div class="card" style="margin:0;">
            <h3>Fixos</h3>
            {fixos_html}
          </div>
          <div class="card" style="margin:0;">
            <h3>Pendentes</h3>
            <p class="muted">Eles podem ficar pendentes. Ao preencher, ficam OK.</p>
            {pend_html if pend_html else "<div class='muted'>Sem pendentes configurados</div>"}
          </div>
        </div>
      </div>
    """


# =========================
# DB init + migrations
# =========================
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
      descricao TEXT,
      categoria TEXT,
      valor REAL NOT NULL,
      tipo TEXT NOT NULL,

      pagador_label TEXT,
      pagador_real TEXT,
      rateio_display TEXT,

      dono TEXT,
      observacao TEXT,
      parcela TEXT,

      created_at TEXT NOT NULL
    )
    """)

    # migrate from older schema if needed (columns existence)
    # If old columns exist but new ones not, add them
    for col in ["descricao", "pagador_label", "pagador_real", "rateio_display", "dono", "observacao", "parcela"]:
        if not _col_exists(conn, "transactions", col):
            cur.execute(f"ALTER TABLE transactions ADD COLUMN {col} TEXT")

    # incomes table (keep)
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

    # investments table (keep)
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

    # month locks (new)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS month_locks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      month_ref TEXT NOT NULL,
      profile TEXT NOT NULL,
      is_locked INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(month_ref, profile)
    )
    """)

    conn.commit()
    conn.close()


init_db()


# =========================
# Income / investment (keep)
# =========================
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


# =========================
# Imports + transactions
# =========================
def _insert_import(conn, batch_id, month_ref, uploaded_by, filename, row_count, status, created_at, file_hash, source):
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO imports (batch_id, month_ref, uploaded_by, filename, row_count, status, created_at, file_hash, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (batch_id, month_ref, uploaded_by, filename, row_count, status, created_at, file_hash, source))


def _insert_transaction(conn, batch_id, month_ref, uploaded_by, row, created_at):
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO transactions
      (batch_id, month_ref, uploaded_by, dt_text, descricao, categoria, valor, tipo,
       pagador_label, pagador_real, rateio_display, dono, observacao, parcela, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        batch_id,
        month_ref,
        uploaded_by,
        row.get("Data", ""),
        row.get("Descrição", ""),
        row.get("Categoria", ""),
        float(row.get("Valor") or 0),
        row.get("Tipo", "Saida"),
        row.get("Pagador", ""),
        row.get("PagadorReal", ""),
        row.get("Rateio", ""),
        row.get("Dono", ""),
        row.get("Observacao", ""),
        row.get("Parcela", ""),
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


def read_template_xlsx_from_bytes(raw: bytes) -> pd.DataFrame:
    buf = io.BytesIO(raw)
    df = pd.read_excel(buf, engine="openpyxl", sheet_name="Template")

    missing = [c for c in TEMPLATE_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError("Colunas faltando na aba Template: " + ", ".join(missing))

    df = df.copy()

    for col in ["Data", "Pagador", "Categoria", "Descrição", "Rateio"]:
        df[col] = df[col].apply(_normalize_str)

    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")
    return df


def normalize_and_validate_template(df: pd.DataFrame, uploaded_by_profile: str):
    """
    Aplica regras:
    - Pagador ∈ {Lucas, Rafa, Casa}
    - Categoria ∈ lista
    - Rateio ∈ lista
    - Se Pagador=Casa => Rateio só {60/40, 50/50}
    - Se Pagador=Casa => PagadorReal = uploader (perfil)
    - Rateio 60/40 ou 50/50 => Dono Casa
    - Rateio 100%_Meu => Dono = pagador_real
    - Rateio 100%_Outro => Dono = outro (se pagador_real Lucas -> Rafa, vice-versa)
    """
    errors = []
    rows = []

    for idx, r in df.iterrows():
        line = idx + 2

        data = r.get("Data", "")
        pagador = r.get("Pagador", "")
        cat = r.get("Categoria", "")
        desc = r.get("Descrição", "")
        rateio = r.get("Rateio", "")
        valor = r.get("Valor", None)

        if not desc:
            errors.append(f"Linha {line}: Descrição vazia")
        if not cat or cat not in ALLOWED_CATEGORIAS:
            errors.append(f"Linha {line}: Categoria inválida")
        if pagador not in ALLOWED_PAGADOR:
            errors.append(f"Linha {line}: Pagador inválido")
        if rateio not in ALLOWED_RATEIO_DISPLAY:
            errors.append(f"Linha {line}: Rateio inválido")
        if pd.isna(valor) or float(valor) <= 0:
            errors.append(f"Linha {line}: Valor inválido, precisa ser maior que 0")

        # rule Casa
        if pagador == "Casa" and rateio not in {"60/40", "50/50"}:
            errors.append(f"Linha {line}: Pagador Casa só pode usar rateio 60/40 ou 50/50")

        # Pagador real
        pagador_real = pagador
        if pagador == "Casa":
            pagador_real = uploaded_by_profile

        # dono derivado do rateio
        dono = ""
        if rateio in {"60/40", "50/50"}:
            dono = "Casa"
        elif rateio == "100%_Meu":
            dono = pagador_real
        elif rateio == "100%_Outro":
            if pagador_real not in {"Lucas", "Rafa"}:
                errors.append(f"Linha {line}: Rateio 100%_Outro exige pagador Lucas ou Rafa")
            else:
                dono = "Rafa" if pagador_real == "Lucas" else "Lucas"

        rows.append({
            "Data": data,
            "Pagador": pagador,
            "PagadorReal": pagador_real,
            "Categoria": cat,
            "Descrição": desc,
            "Valor": None if pd.isna(valor) else float(valor),
            "Rateio": rateio,
            "Tipo": "Saida",
            "Dono": dono,
            "Observacao": "",
            "Parcela": "",
        })

    return errors, rows


def create_preview_batch(month_ref: str, uploaded_by: str, filename: str, rows: list[dict], file_hash: str) -> str:
    batch_id = uuid.uuid4().hex
    now = dt.datetime.utcnow().isoformat(timespec="seconds")
    conn = get_db()
    _insert_import(conn, batch_id, month_ref, uploaded_by, filename, len(rows), "preview", now, file_hash, "template_xlsx")
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

    if imp["uploaded_by"] != profile and imp["uploaded_by"] != "system":
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
        AND t.rateio_display IN ('60/40','50/50')
      ORDER BY t.id ASC
    """, (month_ref,))
    rows = cur.fetchall()
    conn.close()
    return rows


# =========================
# Computations
# =========================
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

        pagador_real = r["pagador_real"] or r["uploaded_by"]
        if pagador_real == "Lucas":
            paid_lucas += val
            by_category[cat]["lucas"] += val
        elif pagador_real == "Rafa":
            paid_rafa += val
            by_category[cat]["rafa"] += val

        if r["rateio_display"] == "60/40":
            expected_lucas += val * LUCAS_SHARE
            expected_rafa += val * RAFA_SHARE
        elif r["rateio_display"] == "50/50":
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

        dono = r["dono"] or ""
        rateio = r["rateio_display"] or ""

        # Casa share
        if dono == "Casa" and rateio in ("60/40", "50/50"):
            sh = share_for_profile(profile, rateio)
            part = val * sh
            house_total += part
            house_by_cat[cat] = house_by_cat.get(cat, 0.0) + part
            continue

        # Personal
        if dono == profile:
            my_personal_total += val
            my_personal_by_cat[cat] = my_personal_by_cat.get(cat, 0.0) + val
            continue

        # Debts/receivables: when dono is the other, it means "this was for the other"
        # We interpret:
        # - if I uploaded and dono is other => I paid for them => receivable
        # - if other uploaded and dono is me => they paid for me => payable
        if r["uploaded_by"] == profile and dono in {"Lucas", "Rafa"} and dono != profile:
            receivable_total += val
            continue

        if r["uploaded_by"] != profile and dono == profile:
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


# =========================
# Manual entries (Extras within app + repetition)
# =========================
def create_manual_rows(month_ref: str, uploaded_by: str, base_row: dict, repeat_months: int) -> str:
    """
    Cria um batch já importado, com repetição por N meses.
    repeat_months = 1 => só mês atual
    repeat_months = 3 => mês atual + 2 meses seguintes
    """
    batch_id = uuid.uuid4().hex
    now = dt.datetime.utcnow().isoformat(timespec="seconds")

    conn = get_db()
    _insert_import(conn, batch_id, month_ref, uploaded_by, "manual_entry", 0, "imported", now, None, "manual")

    def add_months(yyyymm: str, add: int) -> str:
        y = int(yyyymm[:4])
        m = int(yyyymm[4:])
        d = dt.date(y, m, 1)
        # add months
        mm = d.month - 1 + add
        yy = d.year + mm // 12
        mm = mm % 12 + 1
        return f"{yy}{mm:02d}"

    rows_created = 0
    for i in range(max(1, repeat_months)):
        mr = add_months(month_ref, i)
        r = dict(base_row)
        r["Observacao"] = _normalize_str(r.get("Observacao", ""))
        # Keep same row but in different month_ref
        _insert_transaction(conn, batch_id, mr, uploaded_by, r, now)
        rows_created += 1

    cur = conn.cursor()
    cur.execute("UPDATE imports SET row_count = ? WHERE batch_id = ?", (rows_created, batch_id))
    conn.commit()
    conn.close()
    return batch_id


# =========================
# Routes
# =========================
@app.route("/")
def home():
    active_profile = session.get("profile", "")
    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Finanças</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(active_profile)}
        <div class="wrap">
          <div class="card">
            <h1>Finanças</h1>
            <p>Escolha seu perfil para continuar.</p>
            <div class="row" style="justify-content:flex-start;">
              <a class="btn btnPrimary" href="{url_for('set_profile', profile='Lucas')}">Entrar como Lucas</a>
              <a class="btn btnPrimary" href="{url_for('set_profile', profile='Rafa')}">Entrar como Rafa</a>
            </div>
            <p class="muted" style="margin-top:12px;">MVP sem senha, só para evitar confusão de perfil.</p>
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


@app.route("/download-template")
def download_template():
    if not os.path.exists(TEMPLATE_FILE_PATH):
        return f"Template não encontrado em {TEMPLATE_FILE_PATH}", 404
    return send_file(
        TEMPLATE_FILE_PATH,
        as_attachment=True,
        download_name=os.path.basename(TEMPLATE_FILE_PATH),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.post("/toggle-month-lock")
def toggle_month_lock():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))
    month_ref = _normalize_str(request.form.get("month_ref"))
    action = _normalize_str(request.form.get("action"))
    if not month_ref or len(month_ref) != 6:
        return redirect(url_for("dashboard"))
    if action == "lock":
        set_month_lock(month_ref, profile, True)
    elif action == "unlock":
        if not request.form.get("confirm"):
            # confirm step fallback: do it anyway but you can add UI confirm
            set_month_lock(month_ref, profile, False)
        else:
            set_month_lock(month_ref, profile, False)
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    profile = session.get("profile", "")
    if not profile:
        return redirect(url_for("home"))

    now_y, now_m = current_year_month()
    selected_year = request.args.get("Ano") or str(now_y)
    selected_month = request.args.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    label_renda = "Renda do Lucas" if profile == "Lucas" else "Renda da Rafa"
    locked = is_month_locked(month_ref, profile)

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
          {month_top_block(month_ref, profile)}

          <div class="card">
            <h2>Painel do {profile}</h2>
            <p class="muted">Fluxo sugerido: preencher {label_renda}, importar fatura, lançar extras manuais, e conferir Casa e Individual.</p>
            {month_selector_block(selected_year, selected_month, url_for('dashboard'))}

            <div class="row" style="justify-content:flex-start; margin-top:12px;">
              <a class="btn btnPrimary" href="{url_for('renda')}?Ano={selected_year}&Mes={selected_month}">{label_renda}</a>
              <a class="btn btnPrimary" href="{url_for('gastos')}?Ano={selected_year}&Mes={selected_month}">Gastos</a>
              <a class="btn btnPrimary" href="{url_for('individual')}?Ano={selected_year}&Mes={selected_month}">Individual</a>
              <a class="btn" href="{url_for('casa')}?month_ref={month_ref}">Casa</a>
              <a class="btn" href="{url_for('lancamentos')}?Ano={selected_year}&Mes={selected_month}">Lançamentos</a>
              <a class="btn btnGreen" href="{url_for('download_template')}">Baixar template</a>
            </div>

            {("<div class='warnBox' style='margin-top:12px;'><b>Mês fechado para você.</b> Para editar, clique em Editar mês no topo do mês.</div>" if locked else "")}
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
    selected_year = request.values.get("Ano") or str(now_y)
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
            <div class="okBox"><b>{msg}</b></div>
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
          {month_top_block(month_ref, profile)}

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
    selected_year = request.values.get("Ano") or str(now_y)
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
          <div class="card"><div class="okBox"><b>{invest_msg}</b></div></div>
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
          {month_top_block(month_ref, profile)}

          <div class="card">
            <h2>Individual do {profile}</h2>
            {month_selector_block(selected_year, selected_month, url_for('individual'))}
            <div class="row" style="justify-content:flex-start; margin-top:12px;">
              <a class="btn btnPrimary" href="{url_for('renda')}?Ano={selected_year}&Mes={selected_month}">Editar renda</a>
              <a class="btn btnPrimary" href="{url_for('gastos')}?Ano={selected_year}&Mes={selected_month}">Gastos</a>
              <a class="btn" href="{url_for('lancamentos')}?Ano={selected_year}&Mes={selected_month}">Lançamentos</a>
              <a class="btn" href="{url_for('casa')}?month_ref={month_ref}">Casa</a>
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
    selected_year = request.values.get("Ano") or str(now_y)
    selected_month = request.values.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    # Ensure fixed always present
    ensure_fixed_rows(month_ref)

    locked = is_month_locked(month_ref, profile)

    action = request.form.get("action", "")

    errors = []
    info = ""
    info_ok = True

    preview_rows = []
    preview_batch_id = ""

    # dropdown options
    pagador_opts = "".join([f"<option value='{p}'>{p}</option>" for p in ["Lucas", "Rafa", "Casa"]])
    rateio_opts = "".join([f"<option value='{r}'>{r}</option>" for r in ["60/40", "50/50", "100%_Meu", "100%_Outro"]])
    categoria_opts = "".join([f"<option value='{c}'>{c}</option>" for c in sorted(ALLOWED_CATEGORIAS)])

    if request.method == "POST":
        if locked:
            errors.append("Mês fechado para você. Clique em Editar mês no topo para liberar edições.")
        else:
            if action == "manual_extra":
                # Extra manual dentro do app
                data = _normalize_str(request.form.get("Data"))
                pagador = _normalize_str(request.form.get("Pagador"))
                categoria = _normalize_str(request.form.get("Categoria"))
                desc = _normalize_str(request.form.get("Descrição"))
                rateio = _normalize_str(request.form.get("Rateio"))
                repetir = _normalize_str(request.form.get("Repetir"))

                try:
                    v = str(request.form.get("Valor", "")).replace(".", "").replace(",", ".")
                    valor = float(v) if v else 0.0
                except:
                    valor = 0.0

                try:
                    rep = int(repetir) if repetir else 1
                except:
                    rep = 1

                # Validate
                if valor <= 0:
                    errors.append("Valor precisa ser maior que 0")
                if pagador not in ALLOWED_PAGADOR:
                    errors.append("Pagador inválido")
                if categoria not in ALLOWED_CATEGORIAS:
                    errors.append("Categoria inválida")
                if not desc:
                    errors.append("Descrição obrigatória")
                if rateio not in ALLOWED_RATEIO_DISPLAY:
                    errors.append("Rateio inválido")
                if pagador == "Casa" and rateio not in {"60/40", "50/50"}:
                    errors.append("Pagador Casa só pode ser 60/40 ou 50/50")
                if rep < 1 or rep > 36:
                    errors.append("Repetir precisa estar entre 1 e 36")

                # build normalized row (same logic as template)
                pagador_real = pagador if pagador != "Casa" else profile
                dono = "Casa" if rateio in {"60/40", "50/50"} else (pagador_real if rateio == "100%_Meu" else ("Rafa" if pagador_real == "Lucas" else "Lucas"))

                if not errors:
                    row = {
                        "Data": data,
                        "Pagador": pagador,
                        "PagadorReal": pagador_real,
                        "Categoria": categoria,
                        "Descrição": desc,
                        "Valor": valor,
                        "Rateio": rateio,
                        "Tipo": "Saida",
                        "Dono": dono,
                        "Observacao": "Extra manual",
                        "Parcela": "",
                    }
                    create_manual_rows(month_ref, profile, row, rep)
                    info = "Extra manual adicionado"
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
                            df = read_template_xlsx_from_bytes(raw)
                            errors, preview_rows = normalize_and_validate_template(df, profile)
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

    err_block = ""
    if errors:
        items = "".join([f"<li>{e}</li>" for e in errors[:50]])
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
        head = "".join([f"<th>{c}</th>" for c in ["Data","Pagador","Categoria","Descrição","Valor","Rateio"]])
        body_rows = ""
        for r in preview_rows[:25]:
            body_rows += f"""
              <tr>
                <td class="small">{_normalize_str(r.get("Data"))}</td>
                <td class="small">{_normalize_str(r.get("Pagador"))}</td>
                <td class="small">{_normalize_str(r.get("Categoria"))}</td>
                <td class="small">{_normalize_str(r.get("Descrição"))}</td>
                <td class="right">{brl(float(r.get("Valor") or 0))}</td>
                <td class="small">{_normalize_str(r.get("Rateio"))}</td>
              </tr>
            """

        preview_table = f"""
          <div class="card">
            <h3>Preview do Template</h3>
            <p class="muted">Batch: <span class="mono">{preview_batch_id[:10]}...</span> mostrando 25 linhas</p>
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

    locked_block = ""
    if locked:
        locked_block = """
          <div class="card">
            <div class="warnBox">
              <b>Mês fechado para você.</b> Clique em <b>Editar mês</b> no topo do mês para liberar edições.
            </div>
          </div>
        """

    html = f"""
    <!doctype html>
    <html lang="pt-br">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Gastos</title>
        {BASE_CSS}
      </head>
      <body>
        {topbar_html(profile)}
        <div class="wrap">
          {month_top_block(month_ref, profile)}

          <div class="card">
            <h2>Gastos</h2>
            {month_selector_block(selected_year, selected_month, url_for('gastos'))}
            <div class="row" style="justify-content:flex-start; margin-top:12px;">
              <a class="btn btnGreen" href="{url_for('download_template')}">Baixar template</a>
              <a class="btn" href="{url_for('lancamentos')}?Ano={selected_year}&Mes={selected_month}">Lançamentos</a>
            </div>
          </div>

          {locked_block}
          {err_block}
          {info_block}

          <div class="card">
            <h3>Extra manual</h3>
            <p class="muted">Use para empréstimos, ajustes entre vocês, ou gastos fora da fatura. Você pode repetir por N meses.</p>

            <form method="post">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">
              <input type="hidden" name="action" value="manual_extra">

              <div class="grid3">
                <div>
                  <label>Data (YYYY-MM-DD)</label>
                  <input type="text" name="Data" placeholder="opcional" />
                </div>
                <div>
                  <label>Valor</label>
                  <input type="text" name="Valor" placeholder="ex: 120,50" />
                </div>
                <div>
                  <label>Repetir (meses)</label>
                  <input type="number" name="Repetir" value="1" min="1" max="36" />
                </div>
              </div>

              <div class="grid3">
                <div>
                  <label>Pagador</label>
                  <select name="Pagador">{pagador_opts}</select>
                  <div class="muted">Se Pagador = Casa, quem pagou de fato é você (perfil).</div>
                </div>
                <div>
                  <label>Rateio</label>
                  <select name="Rateio">{rateio_opts}</select>
                  <div class="muted">Casa só aceita 60/40 ou 50/50.</div>
                </div>
                <div>
                  <label>Categoria</label>
                  <select name="Categoria">{categoria_opts}</select>
                </div>
              </div>

              <div>
                <label>Descrição</label>
                <input type="text" name="Descrição" placeholder="ex: Emprestimo para X, ajuste viagem, etc" />
              </div>

              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Salvar extra</button>
              </div>
            </form>
          </div>

          <div class="card">
            <h3>Importar template (XLSX)</h3>
            <p class="muted">Preencha a aba <b>Template</b> e faça upload aqui. O app abre um preview e você confirma a importação.</p>
            <form id="excelForm" method="post" enctype="multipart/form-data">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">
              <input type="hidden" name="action" value="excel_preview">
              <label>Arquivo</label>
              <input id="fileInput" type="file" name="file" accept=".xlsx" />
              <p class="muted">Colunas obrigatórias: {", ".join(TEMPLATE_REQUIRED_COLUMNS)} (aba Template)</p>
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
    selected_year = request.values.get("Ano") or str(now_y)
    selected_month = request.values.get("Mes") or f"{now_m:02d}"
    month_ref = month_ref_from(selected_year, selected_month)

    ensure_fixed_rows(month_ref)

    filter_profile = request.values.get("filter_profile") or "Todos"

    msg = ""
    msg_ok = True
    errors = []

    if request.method == "POST":
        action = request.form.get("action", "")
        if action == "delete_batch":
            batch_id = _normalize_str(request.form.get("batch_id"))
            ok, m = delete_batch(batch_id, profile)
            msg = m
            msg_ok = ok

    rows = fetch_imported_transactions(month_ref)
    if filter_profile in ("Lucas", "Rafa"):
        rows = [r for r in rows if r["uploaded_by"] == filter_profile]

    row_html = ""
    for r in rows[:900]:
        val = signed_value(r["tipo"], r["valor"])
        src = r["source"] or ""
        fname = _normalize_str(r["filename"])
        pag_label = _normalize_str(r["pagador_label"])
        pag_real = _normalize_str(r["pagador_real"]) or _normalize_str(r["uploaded_by"])
        dono = _normalize_str(r["dono"])
        rateio = _normalize_str(r["rateio_display"])

        row_html += f"""
          <tr>
            <td class="mono">{r['id']}</td>
            <td class="small">{src}</td>
            <td class="small">{fname}</td>
            <td>{_normalize_str(r['uploaded_by'])}</td>
            <td>{pag_label}</td>
            <td>{pag_real}</td>
            <td class="small">{_normalize_str(r['dt_text'])}</td>
            <td class="small">{_normalize_str(r['categoria'])}</td>
            <td class="small">{_normalize_str(r['descricao'])}</td>
            <td class="small">{dono}</td>
            <td class="small">{rateio}</td>
            <td class="right">{brl(val)}</td>
          </tr>
        """

    if not row_html:
        row_html = "<tr><td colspan='12' class='muted'>Sem lançamentos importados para esse mês</td></tr>"

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
        can_del_batch = (b["uploaded_by"] == profile) or (b["uploaded_by"] == "system")
        btn = ""
        if can_del_batch:
            btn = f"""
              <form method="post" style="display:inline;"
                    onsubmit="return confirm('Excluir batch {b['batch_id'][:10]}...?');">
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
          {month_top_block(month_ref, profile)}

          <div class="card">
            <h2>Lançamentos</h2>
            {month_selector_block(selected_year, selected_month, url_for('lancamentos'))}

            <form method="get" style="margin-top:10px;">
              <input type="hidden" name="Ano" value="{selected_year}">
              <input type="hidden" name="Mes" value="{selected_month}">
              <label>Filtrar por uploader</label>
              <select name="filter_profile">{filter_opts}</select>
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Aplicar</button>
                <a class="btn" href="{url_for('gastos')}?Ano={selected_year}&Mes={selected_month}">Gastos</a>
                <a class="btn btnGreen" href="{url_for('download_template')}">Baixar template</a>
              </div>
            </form>
          </div>

          {msg_block}

          <div class="card">
            <h3>Lista</h3>
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Fonte</th>
                  <th>Arquivo</th>
                  <th>Uploader</th>
                  <th>Pagador (label)</th>
                  <th>Pagador real</th>
                  <th>Data</th>
                  <th>Categoria</th>
                  <th>Descrição</th>
                  <th>Dono</th>
                  <th>Rateio</th>
                  <th class="right">Valor</th>
                </tr>
              </thead>
              <tbody>{row_html}</tbody>
            </table>
          </div>

          <div class="card">
            <h3>Batches do mês</h3>
            <p class="muted">Preview aparece aqui também. Você pode excluir batches do seu perfil e os fixos do sistema.</p>
            <table>
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Uploader</th>
                  <th>Status</th>
                  <th>Fonte</th>
                  <th>Arquivo</th>
                  <th class="right">Linhas</th>
                  <th>Batch</th>
                  <th>Ação</th>
                </tr>
              </thead>
              <tbody>{batches_html}</tbody>
            </table>
          </div>

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
    month_ref = request.args.get("month_ref") or f"{now_y}{now_m:02d}"

    ensure_fixed_rows(month_ref)

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
        cats_rows = "<tr><td colspan='4' class='muted'>Sem lançamentos de Casa para esse mês</td></tr>"

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
          {month_top_block(month_ref, profile)}

          <div class="card">
            <h2>Casa</h2>
            <form method="get">
              <label>Mês de referência</label>
              <input type="text" name="month_ref" value="{month_ref}" placeholder="YYYYMM" />
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Atualizar</button>
                <a class="btn" href="{url_for('lancamentos')}?Ano={month_ref[:4]}&Mes={month_ref[4:]}">Lançamentos</a>
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
              <tbody>{cats_rows}</tbody>
            </table>
          </div>

        </div>
      </body>
    </html>
    """
    return html


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
