from flask import Flask, redirect, url_for, session, request
import io
import sqlite3
import datetime as dt
import uuid
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
  input[type="text"], input[type="file"], select { width: 100%; padding: 10px 12px; border: 1px solid #ddd; border-radius: 12px; background: #fff; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .errorBox { border: 1px solid #f3b6b6; background: #fff3f3; padding: 12px; border-radius: 12px; }
  .okBox { border: 1px solid #bfe6c8; background: #f3fff6; padding: 12px; border-radius: 12px; }
  table { width: 100%; border-collapse: collapse; margin-top: 10px; }
  th, td { border-bottom: 1px solid #eee; padding: 10px 8px; text-align: left; font-size: 13px; vertical-align: top;}
  th { background: #fafafa; }
  .muted { color: #777; font-size: 12px; }
  .nav { display: inline-flex; gap: 8px; flex-wrap: wrap; }
  .kpi { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }
  .kpi .box { background: #fff; border: 1px solid #eee; border-radius: 16px; padding: 14px; }
  .kpi .label { font-size: 12px; color: #666; margin-bottom: 6px; }
  .kpi .value { font-size: 22px; font-weight: 800; }
  .right { text-align: right; }
  @media (max-width: 720px) { .grid2 { grid-template-columns: 1fr; } .kpi { grid-template-columns: 1fr; } }
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

def column_exists(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols

def init_db():
    conn = get_db()
    cur = conn.cursor()

    # Imports (lotes)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS imports (
      batch_id TEXT PRIMARY KEY,
      month_ref TEXT NOT NULL,
      uploaded_by TEXT NOT NULL,
      filename TEXT,
      row_count INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL,              -- 'preview' ou 'imported'
      created_at TEXT NOT NULL
    )
    """)

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
          <a class="btn" href="{url_for('imports_list')}">Importações</a>
          <a class="btn" href="{url_for('casa')}">Casa</a>
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

def read_excel_from_upload(file_storage) -> pd.DataFrame:
    raw = file_storage.read()
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

def create_preview_batch(month_ref: str, uploaded_by: str, filename: str, rows: list[dict]) -> str:
    batch_id = uuid.uuid4().hex
    now = dt.datetime.utcnow().isoformat(timespec="seconds")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
      INSERT INTO imports (batch_id, month_ref, uploaded_by, filename, row_count, status, created_at)
      VALUES (?, ?, ?, ?, ?, 'preview', ?)
    """, (batch_id, month_ref, uploaded_by, filename, len(rows), now))

    for r in rows:
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
            now
        ))

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

def signed_value(tipo: str, valor: float) -> float:
    # Saida soma, Entrada subtrai
    if tipo == "Entrada":
        return -abs(valor)
    return abs(valor)

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

    # categoria -> {total, lucas_paid, rafa_paid}
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
        "rows": rows,
        "total_casa": total_casa,
        "paid_lucas": paid_lucas,
        "paid_rafa": paid_rafa,
        "expected_lucas": expected_lucas,
        "expected_rafa": expected_rafa,
        "settlement_text": settlement_text,
        "settlement_value": settlement_value,
        "cats_sorted": cats_sorted,
    }

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
            <p>Upload gera preview automaticamente. Depois você importa e vê Casa.</p>
            <div class="row" style="justify-content:flex-start;">
              <a class="btn btnPrimary" href="{url_for('upload')}">Ir para Upload</a>
              <a class="btn" href="{url_for('imports_list')}">Ver Importações</a>
              <a class="btn" href="{url_for('casa')}">Ver Casa</a>
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

    selected_year = request.form.get("Ano") or default_year
    selected_month = request.form.get("Mes") or default_month
    month_ref = month_ref_from(selected_year, selected_month)

    errors = []
    preview_rows = []
    batch_id = ""
    imported_msg = ""
    imported_ok = False

    # Ações:
    # - POST com file -> cria preview batch
    # - POST com action=import -> finaliza import do batch_id
    action = request.form.get("action", "")

    if request.method == "POST" and action == "import":
        batch_id = _normalize_str(request.form.get("batch_id"))
        ok, msg = finalize_import(batch_id, profile)
        imported_ok = ok
        imported_msg = msg

    elif request.method == "POST":
        # Preview: precisa de file
        if not (selected_year.isdigit() and len(selected_year) == 4):
            errors.append("Ano inválido")
        if not (selected_month.isdigit() and len(selected_month) == 2 and 1 <= int(selected_month) <= 12):
            errors.append("Mês inválido")

        file = request.files.get("file")
        if not file or file.filename.strip() == "":
            errors.append("Arquivo obrigatório")

        if not errors:
            try:
                df = read_excel_from_upload(file)
                errors, preview_rows = validate_transactions(df)

                if not errors:
                    # cria preview batch e salva as linhas já no banco como preview
                    batch_id = create_preview_batch(month_ref, profile, file.filename, preview_rows)

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

              <p class="muted" style="margin-top:12px;">
                Ao escolher o arquivo, o preview abre automaticamente.
              </p>
              <p class="muted">
                Colunas obrigatórias: {", ".join(REQUIRED_COLUMNS)}
              </p>
            </form>
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

    # Mostrar imports do mês (todos), mas com delete só nos meus
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
            <td>{imp['filename'] or ''}</td>
            <td class="right">{imp['row_count']}</td>
            <td>{imp['batch_id'][:10]}...</td>
            <td>{del_btn}</td>
          </tr>
        """

    if not rows_html:
        rows_html = "<tr><td colspan='7' class='muted'>Sem importações para esse mês</td></tr>"

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
                <a class="btn" href="{url_for('upload')}">Novo Upload</a>
              </div>
            </form>

            <table>
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Perfil</th>
                  <th>Status</th>
                  <th>Arquivo</th>
                  <th class="right">Linhas</th>
                  <th>Batch</th>
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
            <p>Mostrando somente Dono Casa, com rateio 60_40 e 50_50 (somente batches importados).</p>
            <form method="get">
              <label>Mês de referência</label>
              <input type="text" name="month_ref" value="{month_ref}" placeholder="202602" />
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Atualizar</button>
                <a class="btn" href="{url_for('imports_list')}?month_ref={month_ref}">Ver Importações do mês</a>
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
            </div>

            <div style="margin-top:12px;" class="okBox">
              <b>Acerto do mês</b><br/>
              {settle_line}
            </div>
          </div>

          <div class="card">
            <h3>Casa por categoria</h3>
            <p class="muted">Aqui já aparece o total por categoria e quanto cada um pagou dentro daquela categoria.</p>
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
