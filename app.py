from flask import Flask, redirect, url_for, session, request, render_template_string
import io
import sqlite3
import datetime as dt
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
  .wrap { max-width: 1100px; margin: 0 auto; padding: 18px; }
  .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; justify-content: space-between; }
  .pill { display: inline-block; padding: 6px 12px; border-radius: 999px; background: #f2f2f2; font-size: 12px; }
  .btn { display: inline-flex; justify-content: center; align-items: center; padding: 10px 12px; border-radius: 12px;
         text-decoration: none; border: 1px solid #ddd; background: #fff; color: #111; font-weight: 700; }
  .btn:hover { border-color: #bbb; }
  .btnPrimary { background: #111; color: #fff; border-color: #111; }
  .btnPrimary:hover { opacity: .92; }
  .card { background: #fff; border: 1px solid #eee; border-radius: 16px; padding: 18px; margin-top: 14px; box-shadow: 0 8px 24px rgba(0,0,0,.05); }
  h1, h2, h3 { margin: 0 0 10px; }
  p { margin: 0 0 10px; color: #444; }
  label { font-weight: 700; display: block; margin-top: 10px; margin-bottom: 6px; }
  input[type="text"], input[type="file"], select { width: 100%; padding: 10px 12px; border: 1px solid #ddd; border-radius: 12px; background: #fff; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .errorBox { border: 1px solid #f3b6b6; background: #fff3f3; padding: 12px; border-radius: 12px; }
  .okBox { border: 1px solid #bfe6c8; background: #f3fff6; padding: 12px; border-radius: 12px; }
  table { width: 100%; border-collapse: collapse; margin-top: 10px; }
  th, td { border-bottom: 1px solid #eee; padding: 10px 8px; text-align: left; font-size: 13px; }
  th { background: #fafafa; }
  .muted { color: #777; font-size: 12px; }
  .nav { display: inline-flex; gap: 8px; flex-wrap: wrap; }
  .kpi { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }
  .kpi .box { background: #fff; border: 1px solid #eee; border-radius: 16px; padding: 14px; }
  .kpi .label { font-size: 12px; color: #666; margin-bottom: 6px; }
  .kpi .value { font-size: 22px; font-weight: 800; }
  @media (max-width: 720px) { .grid2 { grid-template-columns: 1fr; } .kpi { grid-template-columns: 1fr; } }
</style>
"""

def brl(x: float) -> str:
    if x is None:
        return "R$ 0,00"
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

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
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

def insert_transactions(month_ref: str, uploaded_by: str, rows: list[dict]):
    now = dt.datetime.utcnow().isoformat(timespec="seconds")
    conn = get_db()
    cur = conn.cursor()
    for r in rows:
        cur.execute("""
          INSERT INTO transactions
          (month_ref, uploaded_by, dt_text, estabelecimento, categoria, valor, tipo, dono, rateio, observacao, parcela, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
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

def fetch_house_transactions(month_ref: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
      SELECT * FROM transactions
      WHERE month_ref = ?
        AND dono = 'Casa'
        AND rateio IN ('60_40','50_50')
      ORDER BY id ASC
    """, (month_ref,))
    rows = cur.fetchall()
    conn.close()
    return rows

def signed_value(tipo: str, valor: float) -> float:
    # Saida soma, Entrada subtrai
    if tipo == "Entrada":
        return -abs(valor)
    return abs(valor)

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
        by_category[cat] = by_category.get(cat, 0.0) + val

        # Quem pagou de fato
        if r["uploaded_by"] == "Lucas":
            paid_lucas += val
        elif r["uploaded_by"] == "Rafa":
            paid_rafa += val

        # Quanto deveria pagar, baseado no rateio
        if r["rateio"] == "60_40":
            expected_lucas += val * LUCAS_SHARE
            expected_rafa += val * RAFA_SHARE
        elif r["rateio"] == "50_50":
            expected_lucas += val * 0.5
            expected_rafa += val * 0.5

    # Saldo positivo significa que pagou a mais do que deveria receber de volta
    lucas_diff = paid_lucas - expected_lucas
    rafa_diff = paid_rafa - expected_rafa

    settlement_text = "Sem acerto necessário"
    settlement_value = 0.0

    # Se Lucas pagou mais que o esperado, Rafa deve para Lucas
    if lucas_diff > 0.01:
        settlement_text = "Rafa deve passar para Lucas"
        settlement_value = lucas_diff
    elif rafa_diff > 0.01:
        settlement_text = "Lucas deve passar para Rafa"
        settlement_value = rafa_diff

    # Ordenar categorias por valor desc
    cats_sorted = sorted(by_category.items(), key=lambda x: x[1], reverse=True)

    return {
        "rows": rows,
        "total_casa": total_casa,
        "paid_lucas": paid_lucas,
        "paid_rafa": paid_rafa,
        "expected_lucas": expected_lucas,
        "expected_rafa": expected_rafa,
        "lucas_diff": lucas_diff,
        "rafa_diff": rafa_diff,
        "settlement_text": settlement_text,
        "settlement_value": settlement_value,
        "cats_sorted": cats_sorted,
    }

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
            <p>Use Upload para importar o mês e depois confira o dashboard Casa.</p>
            <div class="row" style="justify-content:flex-start;">
              <a class="btn btnPrimary" href="{url_for('upload')}">Ir para Upload</a>
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
    default_year = 2026
    default_month = now_m

    selected_year = request.form.get("Ano") or str(default_year)
    selected_month = request.form.get("Mes") or f"{default_month:02d}"

    errors = []
    preview_rows = []
    month_ref = f"{selected_year}{selected_month}"

    action = request.form.get("action", "")
    imported = False
    imported_count = 0

    if request.method == "POST":
        # valida ano e mes
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

                # Se clicou em Importar e não tem erro, grava
                if action == "import" and not errors:
                    insert_transactions(month_ref, profile, preview_rows)
                    imported = True
                    imported_count = len(preview_rows)
            except Exception as e:
                errors.append(str(e))

    year_options = "".join([f"<option value='{y}' {'selected' if str(y)==str(selected_year) else ''}>{y}</option>" for y in range(2024, 2031)])
    month_options = "".join([f"<option value='{m:02d}' {'selected' if f'{m:02d}'==str(selected_month) else ''}>{m:02d}</option>" for m in range(1, 13)])

    error_block = ""
    ok_block = ""

    if request.method == "POST":
        if errors:
            items = "".join([f"<li>{e}</li>" for e in errors[:60]])
            error_block = f"""
              <div class="card">
                <h3>Erros</h3>
                <div class="errorBox"><ul>{items}</ul></div>
              </div>
            """
        elif imported:
            ok_block = f"""
              <div class="card">
                <h3>Importação concluída</h3>
                <div class="okBox">
                  <p>Linhas importadas: <b>{imported_count}</b></p>
                  <p class="muted">Agora você pode abrir Casa para ver o total e o acerto.</p>
                  <div class="row" style="justify-content:flex-start; margin-top:10px;">
                    <a class="btn btnPrimary" href="{url_for('casa')}">Abrir Casa</a>
                  </div>
                </div>
              </div>
            """
        else:
            ok_block = f"""
              <div class="card">
                <h3>Validação ok</h3>
                <div class="okBox">
                  <p>Arquivo válido. Se estiver tudo certo, clique em Importar.</p>
                </div>
              </div>
            """

    table_html = ""
    if preview_rows:
        head = "".join([f"<th>{c}</th>" for c in REQUIRED_COLUMNS])
        body_rows = ""
        for r in preview_rows[:20]:
            tds = "".join([f"<td>{'' if r.get(c) is None else r.get(c)}</td>" for c in REQUIRED_COLUMNS])
            body_rows += f"<tr>{tds}</tr>"
        table_html = f"""
          <div class="card">
            <h3>Preview</h3>
            <p class="muted">Mostrando as primeiras 20 linhas.</p>
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

            <form method="post" enctype="multipart/form-data">
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
              <input type="file" name="file" accept=".xlsx,.xls" />

              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit" name="action" value="preview">Pré visualizar</button>
                <button class="btn" type="submit" name="action" value="import">Importar</button>
              </div>

              <p class="muted" style="margin-top:12px;">
                Colunas obrigatórias: {", ".join(REQUIRED_COLUMNS)}
              </p>
            </form>
          </div>

          {error_block}
          {ok_block}
          {table_html}

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
    default_year = 2026
    default_month = now_m
    month_ref = request.args.get("month_ref") or f"{default_year}{default_month:02d}"

    data = compute_casa(month_ref)

    cats_rows = ""
    for cat, val in data["cats_sorted"]:
        cats_rows += f"<tr><td>{cat}</td><td>{brl(val)}</td></tr>"

    if not cats_rows:
        cats_rows = "<tr><td colspan='2' class='muted'>Sem lançamentos de Casa para esse mês</td></tr>"

    settle_line = f"{data['settlement_text']}: {brl(data['settlement_value'])}"

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
            <p>Mostrando somente Dono Casa, com rateio 60_40 e 50_50.</p>

            <form method="get">
              <label>Mês de referência</label>
              <input type="text" name="month_ref" value="{month_ref}" placeholder="202602" />
              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Atualizar</button>
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
            <table>
              <thead><tr><th>Categoria</th><th>Total</th></tr></thead>
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
