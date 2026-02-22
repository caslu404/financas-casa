from flask import Flask, redirect, url_for, session, render_template_string, request

app = Flask(__name__)

# Para MVP sem login/senha, a session já resolve bem.
# Troque depois por uma variável no Render se quiser mais segurança.
app.secret_key = "dev-secret-change-later"


HOME_HTML = """
<!doctype html>
<html lang="pt-br">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Finanças da Casa</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 0; background: #fafafa; }
      .wrap { max-width: 720px; margin: 0 auto; padding: 32px 20px; }
      .card { background: #fff; border: 1px solid #eee; border-radius: 16px; padding: 28px; box-shadow: 0 8px 24px rgba(0,0,0,.05); }
      h1 { margin: 0 0 8px; font-size: 26px; }
      p { margin: 0 0 18px; color: #555; }
      .btns { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 18px; }
      .btn { display: inline-flex; justify-content: center; align-items: center; padding: 14px 16px; border-radius: 14px;
             text-decoration: none; border: 1px solid #ddd; background: #fff; color: #111; font-weight: 700; }
      .btn:hover { border-color: #bbb; }
      .hint { margin-top: 16px; font-size: 12px; color: #777; }
      .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #f2f2f2; font-size: 12px; }
      @media (max-width: 520px) { .btns { grid-template-columns: 1fr; } }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <h1>Finanças da Casa</h1>
        <p>Escolha seu perfil para continuar.</p>

        {% if active_profile %}
          <div class="pill">Perfil atual: <b>{{ active_profile }}</b></div>
        {% endif %}

        <div class="btns">
          <a class="btn" href="{{ url_for('set_profile', profile='Lucas') }}">Entrar como Lucas</a>
          <a class="btn" href="{{ url_for('set_profile', profile='Rafa') }}">Entrar como Rafa</a>
        </div>

        <div class="hint">
          Dica: depois a gente coloca um botão de “Trocar perfil” no topo e separa Casa / Meu painel / Upload.
        </div>
      </div>
    </div>
  </body>
</html>
"""


DASHBOARD_HTML = """
<!doctype html>
<html lang="pt-br">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Painel - {{ profile }}</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 0; background: #fafafa; }
      .topbar { background: #fff; border-bottom: 1px solid #eee; padding: 14px 18px; }
      .wrap { max-width: 900px; margin: 0 auto; padding: 18px; }
      .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; justify-content: space-between; }
      .pill { display: inline-block; padding: 6px 12px; border-radius: 999px; background: #f2f2f2; font-size: 12px; }
      .btn { display: inline-flex; justify-content: center; align-items: center; padding: 10px 12px; border-radius: 12px;
             text-decoration: none; border: 1px solid #ddd; background: #fff; color: #111; font-weight: 700; }
      .card { background: #fff; border: 1px solid #eee; border-radius: 16px; padding: 18px; margin-top: 14px; box-shadow: 0 8px 24px rgba(0,0,0,.05); }
      h2 { margin: 0 0 10px; }
      ul { margin: 8px 0 0 18px; color: #444; }
    </style>
  </head>
  <body>
    <div class="topbar">
      <div class="wrap">
        <div class="row">
          <div>
            <b>Finanças da Casa</b>
            <span class="pill">Perfil: <b>{{ profile }}</b></span>
          </div>
          <div class="row" style="gap:8px;">
            <a class="btn" href="{{ url_for('home') }}">Trocar perfil</a>
          </div>
        </div>
      </div>
    </div>

    <div class="wrap">
      <div class="card">
        <h2>Painel do {{ profile }}</h2>
        <p>Isso aqui é um placeholder. Próximo passo: criar abas e a tela de upload.</p>
        <ul>
          <li>Casa: total por categoria e acerto do mês</li>
          <li>Meu painel: parte da casa + gastos pessoais + extras</li>
          <li>Upload: mês de referência + preview + validações</li>
          <li>Ganhos: salário e extras (2 campos)</li>
        </ul>
      </div>
    </div>
  </body>
</html>
"""


@app.route("/")
def home():
    active_profile = session.get("profile")
    return render_template_string(HOME_HTML, active_profile=active_profile)


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
    return render_template_string(DASHBOARD_HTML, profile=profile)


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
