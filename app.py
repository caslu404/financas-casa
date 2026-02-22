from flask import Flask, redirect, url_for, session, render_template_string, request
import io
import pandas as pd

app = Flask(__name__)
app.secret_key = "dev-secret-change-later"

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

ALLOWED_TIPO = {"Saida", "Entrada"}
ALLOWED_DONO = {"Casa", "Lucas", "Rafa"}
ALLOWED_RATEIO = {"60_40", "50_50", "100_individual", "100_reembolso"}


def _normalize_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()


def read_excel_from_upload(file_storage) -> pd.DataFrame:
    # Lê o arquivo inteiro em memória
    raw = file_storage.read()
    buf = io.BytesIO(raw)
    df = pd.read_excel(buf, engine="openpyxl")

    # Garante que todas as colunas existam
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas faltando: {', '.join(missing)}")

    # Reordena e garante colunas extras não atrapalham
    df = df.copy()

    # Normalizações básicas
    for col in ["Estabelecimento", "Categoria", "Tipo", "Dono", "Rateio", "Observacao", "Parcela"]:
        df[col] = df[col].apply(_normalize_str)

    # Data pode vir como datetime, string, etc
    # Mantemos como string para preview
    df["Data"] = df["Data"].apply(lambda v: "" if pd.isna(v) else str(v))

    # Valor deve ser numérico
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

    return df


def validate_transactions(df: pd.DataFrame):
    errors = []
    normalized_rows = []

    for idx, row in df.iterrows():
        # Linha do Excel para o usuário começa em 2 (1 é header)
        line_number = idx + 2

        tipo = row["Tipo"]
        dono = row["Dono"]
        rateio = row["Rateio"]
        valor = row["Valor"]

        # Valor
        if pd.isna(valor) or float(valor) <= 0:
            errors.append(f"Linha {line_number}: Valor inválido, precisa ser maior que 0")

        # Tipo
        if tipo not in ALLOWED_TIPO:
            errors.append(f"Linha {line_number}: Tipo inválido, use Saida ou Entrada")

        # Dono
        if dono not in ALLOWED_DONO:
            errors.append(f"Linha {line_number}: Dono inválido, use Casa, Lucas ou Rafa")

        # Rateio
        if rateio not in ALLOWED_RATEIO:
            errors.append(
                f"Linha {line_number}: Rateio inválido, use 60_40, 50_50, 100_individual ou 100_reembolso"
            )

        # Regras de coerência entre Dono e Rateio
        if rateio in {"60_40", "50_50"}:
            if dono != "Casa":
                errors.append(f"Linha {line_number}: Rateio {rateio} exige Dono Casa")

        if rateio == "100_reembolso":
            if dono == "Casa":
                errors.append(f"Linha {line_number}: Rateio 100_reembolso nunca pode ter Dono Casa")

        if rateio == "100_individual":
            if dono == "Casa":
                errors.append(f"Linha {line_number}: Rateio 100_individual nunca pode ter Dono Casa")

        # Se Dono é Casa, não pode usar os rateios individuais
        if dono == "Casa" and rateio in {"100_individual", "100_reembolso"}:
            errors.append(f"Linha {line_number}: Dono Casa não pode usar Rateio {rateio}")

        # Salva linha normalizada para preview
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


BASE_CSS = """
<style>
  body { font-family: Arial, sans-serif; margin: 0; background: #fafafa; }
  .topbar { background: #fff; border-bottom: 1px solid #eee; padding: 14px 18px; }
  .wrap { max-width: 980px; margin: 0 auto; padding: 18px; }
  .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; justify-content: space-between; }
  .pill { display: inline-block; padding: 6px 12px; border-radius: 999px; background: #f2f2f2; font-size: 12px; }
  .btn { display: inline-flex; justify-content: center; align-items: center; padding: 10px 12px; border-radius: 12px;
         text-decoration: none; border: 1px solid #ddd; background: #fff; color: #111; font-weight: 700; }
  .btn:hover { border-color: #bbb; }
  .btnPrimary { background: #111; color: #fff; border-color: #111; }
  .btnPrimary:hover { opacity: .92; }
  .card { background: #fff; border: 1px solid #eee; border-radius: 16px; padding: 18px; margin-top: 14px; box-shadow: 0 8px 24px rgba(0,0,0,.05); }
  h1, h2 { margin: 0 0 10px; }
  p { margin: 0 0 10px; color: #444; }
  label { font-weight: 700; display: block; margin-top: 10px; margin-bottom: 6px; }
  input[type="text"], input[type="file"] { width: 100%; padding: 10px 12px; border: 1px solid #ddd; border-radius: 12px; background: #fff; }
  .errorBox { border: 1px solid #f3b6b6; background: #fff3f3; padding: 12px; border-radius: 12px; }
  .okBox { border: 1px solid #bfe6c8; background: #f3fff6; padding: 12px; border-radius: 12px; }
  table { width: 100%; border-collapse: collapse; margin-top: 10px; }
  th, td { border-bottom: 1px solid #eee; padding: 10px 8px; text-align: left; font-size: 13px; }
  th { background: #fafafa; }
  .muted { color: #777; font-size: 12px; }
  .nav { display: inline-flex; gap: 8px; flex-wrap: wrap; }
</style>
"""


def topbar_html(profile: str):
    nav = ""
    if profile:
        nav = f"""
        <div class="nav">
          <a class="btn" href="{url_for('dashboard')}">Painel</a>
          <a class="btn" href="{url_for('upload')}">Upload</a>
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


@app.route("/")
def home():
    active_profile = session.get("profile")
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
        {topbar_html(active_profile if active_profile else "")}
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
    if profile not in ("Lucas", "Rafa"):
        return "Perfil inválido", 400
    session["profile"] = profile
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    profile = session.get("profile")
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
            <p>Próximo passo: usar o Upload para validar seu Excel e mostrar preview.</p>
            <div class="row" style="justify-content:flex-start;">
              <a class="btn btnPrimary" href="{url_for('upload')}">Ir para Upload</a>
            </div>
          </div>
        </div>
      </body>
    </html>
    """
    return html


@app.route("/upload", methods=["GET", "POST"])
def upload():
    profile = session.get("profile")
    if not profile:
        return redirect(url_for("home"))

    errors = []
    preview_rows = []
    month_ref = ""

    if request.method == "POST":
        month_ref = _normalize_str(request.form.get("MesReferencia"))
        file = request.files.get("file")

        if not month_ref:
            errors.append("Mês de referência obrigatório. Use formato 202602")
        else:
            # Validação leve do formato
            if not (month_ref.isdigit() and len(month_ref) == 6):
                errors.append("Mês de referência inválido. Use formato 202602")

        if not file or file.filename.strip() == "":
            errors.append("Arquivo obrigatório")

        if not errors:
            try:
                df = read_excel_from_upload(file)
                errors, preview_rows = validate_transactions(df)
            except Exception as e:
                errors.append(str(e))

    # Render HTML
    error_block = ""
    ok_block = ""

    if request.method == "POST":
        if errors:
            items = "".join([f"<li>{e}</li>" for e in errors[:50]])
            error_block = f"""
              <div class="card">
                <h2>Erros</h2>
                <div class="errorBox">
                  <ul>{items}</ul>
                </div>
              </div>
            """
        else:
            ok_block = f"""
              <div class="card">
                <h2>Validação ok</h2>
                <div class="okBox">
                  <p>Arquivo lido e validado.</p>
                  <p class="muted">Por enquanto o MVP só faz preview. Próximo passo é salvar no banco.</p>
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
            <h2>Preview</h2>
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
              <label>Mês de referência</label>
              <input type="text" name="MesReferencia" value="{month_ref}" placeholder="202602" />

              <label>Arquivo Excel</label>
              <input type="file" name="file" accept=".xlsx,.xls" />

              <div class="row" style="justify-content:flex-start; margin-top:12px;">
                <button class="btn btnPrimary" type="submit">Pré visualizar</button>
              </div>

              <p class="muted" style="margin-top:12px;">
                O arquivo precisa ter as colunas: {", ".join(REQUIRED_COLUMNS)}
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


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
