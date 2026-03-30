from __future__ import annotations
import io
import math
import sqlite3
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "cockpit_intralogistica_v60.db"
CSV_PARAMS = BASE_DIR / "parametros_v60.csv"
LOGO_B64 = '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'

st.set_page_config(page_title="Cockpit Intralogístico", layout="wide", initial_sidebar_state="expanded")

# -----------------------
# Autenticação simples
# -----------------------
CREDENCIAIS_PATH = BASE_DIR / "usuarios_v60.csv"

if "auth_ok_v59" not in st.session_state:
    st.session_state["auth_ok_v59"] = False
if "auth_user_v59" not in st.session_state:
    st.session_state["auth_user_v59"] = ""

@st.cache_data
def carregar_credenciais_csv(caminho: str) -> dict:
    p = Path(caminho)
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    if "usuario" not in df.columns or "senha" not in df.columns:
        return {}
    return {str(r["usuario"]).strip(): str(r["senha"]) for _, r in df.iterrows()}

def carregar_credenciais() -> dict:
    # prioridade para Streamlit secrets em publicação
    try:
        if "auth" in st.secrets and "users" in st.secrets["auth"]:
            users = st.secrets["auth"]["users"]
            if isinstance(users, dict):
                return {str(k).strip(): str(v) for k, v in users.items()}
    except Exception:
        pass

    # fallback local para desenvolvimento
    return carregar_credenciais_csv(str(CREDENCIAIS_PATH))

def autenticar(usuario: str, senha: str) -> bool:
    usuarios_autorizados = carregar_credenciais()
    return usuarios_autorizados.get(str(usuario).strip()) == str(senha)

def tela_login():
    top_a, top_b, top_c = st.columns([1, 2, 1])
    with top_b:
        if LOGO_B64:
            st.image(f"data:image/png;base64,{LOGO_B64}", width=220)
        st.markdown('<div class="title-center">Cockpit Intralogística</div>', unsafe_allow_html=True)
        st.markdown('<div class="subtitle-center">Acesso restrito. Informe usuário e senha para entrar.</div>', unsafe_allow_html=True)

        with st.container(border=True):
            usuario = st.text_input("Usuário", key="login_usuario_v59b")
            senha = st.text_input("Senha", type="password", key="login_senha_v59b")
            if st.button("Entrar", type="primary", use_container_width=True, key="btn_entrar_v59b"):
                if autenticar(usuario, senha):
                    st.session_state["auth_ok_v59"] = True
                    st.session_state["auth_user_v59"] = str(usuario).strip()
                    st.success("Acesso liberado.")
                    st.rerun()
                else:
                    st.session_state["auth_ok_v59"] = False
                    st.session_state["auth_user_v59"] = ""
                    st.error("Usuário ou senha inválidos.")

        st.caption("Em publicação, use Streamlit secrets. O CSV local fica como fallback para desenvolvimento.")

def exigir_login():
    auth_ok = bool(st.session_state.get("auth_ok_v59", False))
    auth_user = str(st.session_state.get("auth_user_v59", "")).strip()
    if (not auth_ok) or (not auth_user):
        st.session_state["auth_ok_v59"] = False
        st.session_state["auth_user_v59"] = ""
        tela_login()
        st.stop()

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots_intradia (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_operacao TEXT NOT NULL,
            hora_registro TEXT NOT NULL,
            hora_bucket TEXT NOT NULL,
            setor TEXT NOT NULL,
            turno TEXT NOT NULL,
            funcionarios_presentes REAL NOT NULL,
            pedidos_feitos_hora REAL NOT NULL,
            pedidos_faltantes_hora REAL NOT NULL,
            observacao TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS layout_preferences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_layout TEXT NOT NULL,
            col_width_feitos INTEGER NOT NULL,
            col_width_faltantes INTEGER NOT NULL,
            quadro_altura INTEGER NOT NULL,
            linha_altura INTEGER NOT NULL,
            salvo_em TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS diario_bordo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_operacao TEXT NOT NULL,
            hora_registro TEXT NOT NULL,
            decisao TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

init_db()

st.markdown(
    """
    <style>
    :root {
        --br-orange: #FF782D;
        --br-dark: #4E4E4E;
    }
    div[data-testid="stButton"] > button {
        min-height: 42px;
        white-space: nowrap;
        font-weight: 600;
    }
    div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid rgba(78,78,78,0.12);
        border-radius: 14px;
        padding: 10px 12px;
    }
    .title-center {
        text-align:center;
        color: var(--br-dark);
        font-size: 3.1rem;
        font-weight: 900;
        line-height: 1.02;
        margin-top: 0.15rem;
        margin-bottom: 0.2rem;
    }
    .subtitle-center {
        text-align:center;
        color: var(--br-dark);
        opacity: 0.82;
        font-size: 1.0rem;
        margin-bottom: 0.55rem;
    }
    .pill-wrap {
        text-align:center;
        margin-bottom: 0.8rem;
    }
    .pill {
        display:inline-block;
        padding: 0.25rem 0.65rem;
        border-radius: 999px;
        font-weight: 700;
        font-size: 0.85rem;
        margin-right: 0.4rem;
    }
    .pill-orange {
        background: rgba(255,120,45,0.12);
        color: var(--br-orange);
        border: 1px solid rgba(255,120,45,0.25);
    }
    .pill-gray {
        background: rgba(78,78,78,0.08);
        color: var(--br-dark);
        border: 1px solid rgba(78,78,78,0.2);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

@st.cache_data
def carregar_csv(caminho: str) -> pd.DataFrame:
    return pd.read_csv(caminho)

def limpar_cache():
    try:
        st.cache_data.clear()
    except Exception:
        pass

def parse_hhmm(v: str) -> datetime:
    return datetime.strptime(str(v), "%H:%M")

def format_hour_bucket(hora_registro: str) -> str:
    h = parse_hhmm(hora_registro)
    return f"{h.hour:02d}:00"

def horas_entre(inicio_str: str, fim_str: str) -> float:
    ini = parse_hhmm(inicio_str)
    fim = parse_hhmm(fim_str)
    horas = (fim - ini).total_seconds() / 3600
    if horas < 0:
        horas += 24
    return horas

def horas_liquidas(inicio_str: str, fim_str: str, intervalo_min: float) -> float:
    return max(horas_entre(inicio_str, fim_str) - intervalo_min / 60, 0)

def horas_apos_horario(inicio_str: str, fim_str: str, corte_str: str) -> float:
    total = horas_entre(inicio_str, fim_str)
    ini = parse_hhmm(inicio_str)
    corte = parse_hhmm(corte_str)
    horas_ate_corte = (corte - ini).total_seconds() / 3600
    if horas_ate_corte < 0:
        horas_ate_corte += 24
    if horas_ate_corte >= total:
        return 0
    return max(total - horas_ate_corte, 0)

def horas_decorridas_no_turno(inicio_str: str, fim_str: str, intervalo_min: float, atual_str: str) -> float:
    ini = parse_hhmm(inicio_str)
    fim = parse_hhmm(fim_str)
    atual = parse_hhmm(atual_str)
    total = (fim - ini).total_seconds() / 3600
    if total < 0:
        total += 24
    decorridas = (atual - ini).total_seconds() / 3600
    if decorridas < 0:
        decorridas += 24
    decorridas = min(decorridas, total)
    liquidas = max(total - intervalo_min / 60, 0)
    if total == 0:
        return 0
    return min(liquidas * (decorridas / total), liquidas)

def turno_ativo_agora(inicio_str: str, fim_str: str, atual_str: str) -> bool:
    ini = parse_hhmm(inicio_str)
    fim = parse_hhmm(fim_str)
    atual = parse_hhmm(atual_str)
    if fim >= ini:
        return ini <= atual <= fim
    return atual >= ini or atual <= fim

def classificar_status(saldo: float, demanda: float) -> str:
    if demanda <= 0:
        return "VIÁVEL"
    perc = saldo / demanda
    if perc >= 0.10:
        return "FOLGA"
    if perc >= 0:
        return "RISCO"
    return "INVIÁVEL"

def classificar_status_intradia(saldo: float, demanda: float) -> str:
    if demanda <= 0:
        return "ATENDE"
    perc = saldo / demanda
    if perc >= 0.10:
        return "ATENDE"
    if perc >= 0:
        return "ATENÇÃO"
    return "NÃO ATENDE"

def status_com_icone(status: str) -> str:
    mapa = {
        "VIÁVEL": "🟢 VIÁVEL",
        "FOLGA": "🟢 FOLGA",
        "ATENDE": "🟢 ATENDE",
        "RISCO": "🟡 RISCO",
        "ATENÇÃO": "🟡 ATENÇÃO",
        "INVIÁVEL": "🔴 INVIÁVEL",
        "NÃO ATENDE": "🔴 NÃO ATENDE",
    }
    return mapa.get(str(status), str(status))

def pessoas_adicionais(deficit: float, produtividade: float, horas: float) -> int:
    capacidade_por_pessoa = produtividade * horas
    if deficit <= 0 or capacidade_por_pessoa <= 0:
        return 0
    return math.ceil(deficit / capacidade_por_pessoa)

def format_num(x):
    try:
        return f"{int(round(float(x), 0)):,}".replace(",", ".")
    except Exception:
        return str(x)

def salvar_snapshot(data_operacao, hora_registro, df_snapshot: pd.DataFrame):
    hora_bucket = format_hour_bucket(hora_registro)
    conn = get_conn()
    cur = conn.cursor()
    registros = []
    for _, row in df_snapshot.iterrows():
        registros.append((
            str(data_operacao),
            str(hora_registro),
            str(hora_bucket),
            str(row["setor"]),
            str(row["turno"]),
            float(row["funcionarios_presentes"]),
            float(row["pedidos_feitos_hora"]),
            float(row["pedidos_faltantes_hora"]),
            str(row.get("observacao", "")),
        ))
    cur.executemany("""
        INSERT INTO snapshots_intradia
        (data_operacao,hora_registro,hora_bucket,setor,turno,funcionarios_presentes,pedidos_feitos_hora,pedidos_faltantes_hora,observacao)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, registros)
    conn.commit()
    conn.close()

def existe_snapshot_no_bucket(data_operacao: str, hora_bucket: str, setores: list[str]) -> bool:
    if not setores:
        return False
    conn = get_conn()
    placeholders = ",".join(["?"] * len(setores))
    query = f"""
        SELECT COUNT(*) AS qtd
        FROM snapshots_intradia
        WHERE data_operacao = ?
          AND hora_bucket = ?
          AND setor IN ({placeholders})
    """
    params = [str(data_operacao), str(hora_bucket)] + list(setores)
    qtd = pd.read_sql_query(query, conn, params=params).iloc[0]["qtd"]
    conn.close()
    return int(qtd) > 0

def apagar_snapshot_no_bucket(data_operacao: str, hora_bucket: str, setores: list[str]):
    if not setores:
        return
    conn = get_conn()
    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(setores))
    query = f"""
        DELETE FROM snapshots_intradia
        WHERE data_operacao = ?
          AND hora_bucket = ?
          AND setor IN ({placeholders})
    """
    params = [str(data_operacao), str(hora_bucket)] + list(setores)
    cur.execute(query, params)
    conn.commit()
    conn.close()

def carregar_historico(data_operacao: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM snapshots_intradia WHERE data_operacao = ? ORDER BY hora_bucket, setor, turno, id",
        conn,
        params=[data_operacao],
    )
    conn.close()
    return df

def salvar_layout(nome_layout: str, col_width_feitos: int, col_width_faltantes: int, quadro_altura: int, linha_altura: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO layout_preferences (nome_layout, col_width_feitos, col_width_faltantes, quadro_altura, linha_altura, salvo_em)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        """,
        (nome_layout, int(col_width_feitos), int(col_width_faltantes), int(quadro_altura), int(linha_altura)),
    )
    conn.commit()
    conn.close()

def carregar_ultimo_layout():
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM layout_preferences ORDER BY id DESC LIMIT 1",
        conn,
    )
    conn.close()
    if df.empty:
        return None
    return df.iloc[0].to_dict()

def salvar_decisao_bordo(data_operacao: str, hora_registro: str, decisao: str):
    if not str(decisao).strip():
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO diario_bordo (data_operacao, hora_registro, decisao) VALUES (?, ?, ?)",
        (str(data_operacao), str(hora_registro), str(decisao).strip()),
    )
    conn.commit()
    conn.close()

def carregar_decisoes_bordo(data_operacao: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT hora_registro, decisao FROM diario_bordo WHERE data_operacao = ? ORDER BY hora_registro, id",
        conn,
        params=[str(data_operacao)],
    )
    conn.close()
    return df

def resumo_executivo_historico(data_operacao: str, df_params: pd.DataFrame, demanda_total_pedidos: float, demanda_total_linhas: float, df_turnos_planejados: pd.DataFrame) -> dict:
    df_hist = carregar_historico(str(data_operacao))

    total_demanda_pedidos = float(demanda_total_pedidos)
    total_demanda_linhas = float(demanda_total_linhas)
    total_pedidos_produzidos = float(df_hist["pedidos_feitos_hora"].sum()) if not df_hist.empty else 0.0
    total_linhas_produzidas = 0.0
    total_pessoas = float(df_params["funcionarios_planejados"].sum()) if "funcionarios_planejados" in df_params.columns else 0.0

    total_faltantes = float(df_hist["pedidos_faltantes_hora"].sum()) if not df_hist.empty else total_demanda_pedidos
    if total_pedidos_produzidos <= 0 and total_faltantes <= 0:
        saude = "🟡 ATENÇÃO"
    elif total_faltantes <= max(total_demanda_pedidos * 0.1, 1):
        saude = "🟢 SAUDÁVEL"
    elif total_faltantes <= max(total_demanda_pedidos * 0.35, 1):
        saude = "🟡 ATENÇÃO"
    else:
        saude = "🔴 CRÍTICA"

    return {
        "total_demanda_pedidos": total_demanda_pedidos,
        "total_demanda_linhas": total_demanda_linhas,
        "total_pedidos_produzidos": total_pedidos_produzidos,
        "total_linhas_produzidas": total_linhas_produzidas,
        "total_pessoas": total_pessoas,
        "saude": saude,
    }

def montar_grade_historica(df_hist: pd.DataFrame, setores_base: list[str], valor_col: str) -> pd.DataFrame:
    horas = [f"{h:02d}:00" for h in range(24)]
    base = pd.DataFrame({"setor": setores_base})
    for h in horas:
        base[h] = 0

    if df_hist.empty:
        return base

    agg = df_hist.groupby(["setor", "hora_bucket"], as_index=False)[valor_col].sum()
    for _, row in agg.iterrows():
        setor = row["setor"]
        hora = row["hora_bucket"]
        if setor in base["setor"].values and hora in horas:
            base.loc[base["setor"] == setor, hora] = row[valor_col]
    return base

def excel_historico(df_feitos: pd.DataFrame, df_faltantes: pd.DataFrame, resumo_grafico: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_feitos.to_excel(writer, sheet_name="Pedidos Feitos HH", index=False)
        df_faltantes.to_excel(writer, sheet_name="Pedidos Faltantes HH", index=False)
        resumo_grafico.to_excel(writer, sheet_name="Resumo Grafico", index=False)
    return output.getvalue()

def preparar_parametros(df_base, absenteismo, perda_prod_pct, perda_a_partir_hora):
    df = df_base.copy()
    for col in [
        "funcionarios_planejados","produtividade_pedidos_hora","produtividade_linhas_hora",
        "intervalo_min","participacao_turno_pedidos","participacao_turno_linhas",
        "backlog_local_pedidos","backlog_local_linhas"
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["horas_brutas_turno"] = df.apply(lambda x: horas_entre(x["inicio_turno"], x["fim_turno"]), axis=1)
    df["horas_liquidas_turno"] = df.apply(lambda x: horas_liquidas(x["inicio_turno"], x["fim_turno"], x["intervalo_min"]), axis=1)
    df["horas_apos_corte"] = df.apply(lambda x: horas_apos_horario(x["inicio_turno"], x["fim_turno"], perda_a_partir_hora), axis=1)
    df["fracao_apos_corte"] = df.apply(lambda x: 0 if x["horas_brutas_turno"] <= 0 else min(x["horas_apos_corte"] / x["horas_brutas_turno"], 1), axis=1)
    df["funcionarios_disponiveis"] = df["funcionarios_planejados"] * (1 - absenteismo)
    df["fator_perda_produtividade"] = 1 - (perda_prod_pct * df["fracao_apos_corte"])
    df["produtividade_pedidos_ajustada"] = df["produtividade_pedidos_hora"] * df["fator_perda_produtividade"]
    df["produtividade_linhas_ajustada"] = df["produtividade_linhas_hora"] * df["fator_perda_produtividade"]
    df["setor_turno"] = df["setor"] + " | " + df["turno"]
    return df

def construir_planejamento_setorial(df_params, demanda_total, unidade):
    df = df_params.copy()
    col_prod = "produtividade_pedidos_ajustada" if unidade == "Pedidos" else "produtividade_linhas_ajustada"
    col_part = "participacao_turno_pedidos" if unidade == "Pedidos" else "participacao_turno_linhas"
    col_backlog = "backlog_local_pedidos" if unidade == "Pedidos" else "backlog_local_linhas"

    df["capacidade_turno"] = df["funcionarios_disponiveis"] * df[col_prod] * df["horas_liquidas_turno"]

    res_turnos = []
    res_setores = []

    for setor, g in df.groupby("setor", sort=False):
        g = g.sort_values("inicio_turno").copy()
        backlog_setor = float(g[col_backlog].sum())
        demanda_setor = float(demanda_total + backlog_setor)
        soma_part = float(g[col_part].sum())

        restante = demanda_setor
        for _, row in g.iterrows():
            capacidade_turno = float(row["capacidade_turno"])
            participacao = float(row[col_part])
            demanda_referencia = demanda_setor * participacao
            programado_turno = min(restante, capacidade_turno)
            restante -= programado_turno
            saldo_turno = capacidade_turno - programado_turno
            ociosidade_turno = max(capacidade_turno - programado_turno, 0)
            status_turno = classificar_status(saldo_turno, programado_turno if programado_turno > 0 else capacidade_turno)

            res_turnos.append({
                "setor": setor,
                "turno": row["turno"],
                "setor_turno": row["setor_turno"],
                "inicio_turno": row["inicio_turno"],
                "fim_turno": row["fim_turno"],
                "intervalo_min": row["intervalo_min"],
                "funcionarios_planejados": row["funcionarios_planejados"],
                "funcionarios_disponiveis": row["funcionarios_disponiveis"],
                "horas_liquidas_turno": row["horas_liquidas_turno"],
                "produtividade_operacional": row[col_prod],
                "participacao_turno": participacao,
                "demanda_referencia": demanda_referencia,
                "programado_turno": programado_turno,
                "capacidade_turno": capacidade_turno,
                "saldo_turno": saldo_turno,
                "ociosidade_turno": ociosidade_turno,
                "status_turno": status_turno,
            })

        capacidade_total_setor = float(g["capacidade_turno"].sum())
        deficit_setor = max(demanda_setor - capacidade_total_setor, 0)
        saldo_setor = capacidade_total_setor - demanda_setor
        capacidade_por_pessoa_media = capacidade_total_setor / float(g["funcionarios_disponiveis"].sum()) if float(g["funcionarios_disponiveis"].sum()) > 0 else 0
        pessoas_extra_setor = math.ceil(deficit_setor / capacidade_por_pessoa_media) if deficit_setor > 0 and capacidade_por_pessoa_media > 0 else 0

        res_setores.append({
            "setor": setor,
            "demanda_setor": demanda_setor,
            "capacidade_total_setor": capacidade_total_setor,
            "saldo_setor": saldo_setor,
            "deficit_setor": deficit_setor,
            "status_setor": classificar_status(saldo_setor, demanda_setor),
            "pessoas_extra_setor": pessoas_extra_setor,
            "soma_participacao_setor": soma_part,
        })

    return pd.DataFrame(res_turnos), pd.DataFrame(res_setores)

def sugerir_remanejamento_setorial(df_turnos, df_setores):
    sobra = df_turnos[df_turnos["ociosidade_turno"] > 0].copy()
    sobra["moviveis"] = sobra.apply(
        lambda x: max(0, math.floor(x["ociosidade_turno"] / (x["produtividade_operacional"] * x["horas_liquidas_turno"])))
        if x["produtividade_operacional"] * x["horas_liquidas_turno"] > 0 else 0,
        axis=1,
    )
    sobra = sobra[sobra["moviveis"] > 0].sort_values("ociosidade_turno", ascending=False)
    deficit = df_setores[df_setores["pessoas_extra_setor"] > 0].copy().sort_values("deficit_setor", ascending=False)

    recs = []
    sobra_list = sobra.to_dict("records")
    for _, row in deficit.iterrows():
        faltam = int(row["pessoas_extra_setor"])
        for s in sobra_list:
            if faltam <= 0:
                break
            if s["moviveis"] <= 0:
                continue
            mover = min(int(s["moviveis"]), faltam)
            recs.append({"origem": s["setor_turno"], "destino": row["setor"], "pessoas_sugeridas": mover})
            s["moviveis"] -= mover
            faltam -= mover
        if faltam > 0:
            recs.append({"origem": "CONTRATAÇÃO / HORA EXTRA", "destino": row["setor"], "pessoas_sugeridas": faltam})
    return pd.DataFrame(recs)

if "params_editor_df" not in st.session_state:
    st.session_state["params_editor_df"] = carregar_csv(str(CSV_PARAMS))
if "layout_pref_v55" not in st.session_state:
    ultimo_layout = carregar_ultimo_layout()
    st.session_state["layout_pref_v55"] = ultimo_layout or {
        "col_width_feitos": 70,
        "col_width_faltantes": 70,
        "quadro_altura": 260,
        "linha_altura": 35,
    }
if "current_page_v58" not in st.session_state:
    st.session_state["current_page_v58"] = "Planejamento D-1"
if "overwrite_pending_v55" not in st.session_state:
    st.session_state["overwrite_pending_v55"] = None

exigir_login()

top_left, top_center, top_right = st.columns([0.5, 5, 1.5])
with top_left:
    st.write("")
with top_center:
    n1, n2, n3, n4 = st.columns(4)
    ativo = st.session_state["current_page_v58"]
    lab1 = ("🟠 " if ativo == "Parâmetros e Cadastros" else "⚪ ") + "Parâmetros e Cadastros"
    lab2 = ("🟠 " if ativo == "Planejamento D-1" else "⚪ ") + "Planejamento D-1"
    lab3 = ("🟠 " if ativo == "Acompanhamento Intradia" else "⚪ ") + "Acompanhamento Intradia"
    lab4 = ("🟠 " if ativo == "Histórico Hora/Hora" else "⚪ ") + "Histórico Hora/Hora"
    if n1.button(lab1, use_container_width=True):
        st.session_state["current_page_v58"] = "Parâmetros e Cadastros"
        st.rerun()
    if n2.button(lab2, use_container_width=True):
        st.session_state["current_page_v58"] = "Planejamento D-1"
        st.rerun()
    if n3.button(lab3, use_container_width=True):
        st.session_state["current_page_v58"] = "Acompanhamento Intradia"
        st.rerun()
    if n4.button(lab4, use_container_width=True):
        st.session_state["current_page_v58"] = "Histórico Hora/Hora"
        st.rerun()
with top_right:
    if LOGO_B64:
        st.image(f"data:image/png;base64,{LOGO_B64}", width=250)

st.markdown('<div class="title-center">Cockpit Intralogística', unsafe_allow_html=True)
st.markdown('<div class="subtitle-center">Planejamento e Acompanhamento de Operações', unsafe_allow_html=True)
st.markdown(
    '<div class="pill-wrap"><span class="pill pill-orange">CD SBC</span><span class="pill pill-gray">Acompanhamento Operacional</span></div>',
    unsafe_allow_html=True,
)

pagina = st.session_state["current_page_v58"]

with st.sidebar:
    st.caption(f"Usuário: {st.session_state.get('auth_user_v59', '')}")
    if st.button("Sair", use_container_width=True):
        st.session_state["auth_ok_v59"] = False
        st.session_state["auth_user_v59"] = ""
        st.rerun()
    st.header("Configuração")
    data_operacao = st.date_input("Data da operação", value=date.today())
    st.subheader("Demanda global do dia")
    demanda_total_pedidos = st.number_input("Pedidos do dia", min_value=0, value=1000, step=100)
    demanda_total_linhas = st.number_input("Linhas do dia", min_value=0, value=5000, step=100)
    st.subheader("Premissas operacionais")
    absenteismo = st.slider("Absenteísmo médio (%)", 0, 30, 0, 1) / 100
    perda_produtividade_pct = st.slider("Perda de produtividade (%)", 0, 40, 0, 1) / 100
    perda_a_partir_hora = st.text_input("Perda de produtividade a partir de qual horário? (HH:MM)", value="14:00")

df_params = preparar_parametros(st.session_state["params_editor_df"], absenteismo, perda_produtividade_pct, perda_a_partir_hora)
df_turnos_pedidos, df_setores_pedidos = construir_planejamento_setorial(df_params, demanda_total_pedidos, "Pedidos")
df_turnos_linhas, df_setores_linhas = construir_planejamento_setorial(df_params, demanda_total_linhas, "Linhas")
df_turnos = df_turnos_pedidos.copy()
df_setores = df_setores_pedidos.copy()
setores_base = df_turnos["setor"].drop_duplicates().tolist()

if pagina == "Parâmetros e Cadastros":
    st.subheader("Parâmetros e cadastros")
    editor_df = st.data_editor(st.session_state["params_editor_df"], use_container_width=True, num_rows="dynamic", key="cadastros_v56")
    c1, c2 = st.columns(2)
    if c1.button("Salvar parâmetros", type="primary", use_container_width=True):
        st.session_state["params_editor_df"] = editor_df.copy()
        editor_df.to_csv(CSV_PARAMS, index=False)
        limpar_cache()
        st.success("Parâmetros salvos.")
    if c2.button("Recalcular", use_container_width=True):
        st.session_state["params_editor_df"] = editor_df.copy()
        st.success("Parâmetros aplicados ao cálculo atual.")
    st.download_button("Baixar cadastros em CSV", data=editor_df.to_csv(index=False).encode("utf-8"), file_name="parametros_v57_editado.csv", mime="text/csv")

elif pagina == "Planejamento D-1":
    st.subheader("Planejamento D-1 por setor")
    gargalo_row = df_setores.sort_values("saldo_setor").iloc[0] if not df_setores.empty else None
    capacidade_setor_critico = float(gargalo_row["capacidade_total_setor"]) if gargalo_row is not None else 0
    saldo_setor_critico = float(gargalo_row["saldo_setor"]) if gargalo_row is not None else 0
    hc_extra = int(df_setores["pessoas_extra_setor"].sum())
    status_geral = "VIÁVEL" if (df_setores["deficit_setor"] <= 0).all() else "INVIÁVEL"
    setor_critico = str(gargalo_row["setor"]) if gargalo_row is not None else "-"
    total_dia = demanda_total_pedidos

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Pedidos totais do dia", format_num(total_dia))
    c2.metric("Capacidade do setor crítico", format_num(capacidade_setor_critico))
    c3.metric("Folga/Déficit do setor crítico", format_num(saldo_setor_critico))
    c4.metric("Status geral", status_com_icone(status_geral))
    c5.metric("Contagem de pessoal extra", hc_extra)

    if status_geral == "INVIÁVEL":
        st.error(f"Setor crítico do dia: {setor_critico}")
    else:
        st.success("O planejamento indica atendimento do cenário.")

    df_setores_view = df_setores.copy()
    if "status_setor" in df_setores_view.columns:
        df_setores_view["status_setor"] = df_setores_view["status_setor"].apply(status_com_icone)
    st.dataframe(df_setores_view, use_container_width=True)

elif pagina == "Acompanhamento Intradia":
    st.subheader("Acompanhamento intradia")
    hora_registro = st.text_input("Hora do registro (HH:MM)", value=datetime.now().strftime("%H:%M"))
    hora_bucket = format_hour_bucket(hora_registro)
    ativos = df_turnos[df_turnos.apply(lambda x: turno_ativo_agora(x["inicio_turno"], x["fim_turno"], hora_registro), axis=1)].copy()

    if ativos.empty:
        st.warning("Não há turnos em andamento no horário informado.")
    else:
        st.caption("Somente turnos em andamento aparecem nesta tela.")
        base_snapshot = ativos[["setor", "turno"]].copy()
        base_snapshot["funcionarios_presentes"] = ativos["funcionarios_planejados"]
        base_snapshot["pedidos_feitos_hora"] = 0
        base_snapshot["pedidos_faltantes_hora"] = ativos["programado_turno"]
        base_snapshot["observacao"] = ""

        df_snapshot = st.data_editor(base_snapshot, use_container_width=True, num_rows="dynamic", key="intradia_editor_v56")
        for col in ["funcionarios_presentes", "pedidos_feitos_hora", "pedidos_faltantes_hora"]:
            df_snapshot[col] = pd.to_numeric(df_snapshot[col], errors="coerce").fillna(0)

        setores_snapshot = df_snapshot["setor"].drop_duplicates().tolist()

        decisao_bordo = st.text_area("Diário de bordo / decisão tomada", value="", height=90, key="bordo_texto_v55", help="Registre a decisão tomada neste horário. Ex.: remanejar 2 pessoas do Fracionado para Consolidação.")
        cdb1, cdb2 = st.columns([1,1])
        if cdb1.button("Salvar decisão no diário de bordo", use_container_width=True):
            salvar_decisao_bordo(str(data_operacao), str(hora_registro), decisao_bordo)
            st.success("Decisão registrada no diário de bordo.")
        decisoes_hoje = carregar_decisoes_bordo(str(data_operacao))
        if not decisoes_hoje.empty:
            with st.expander("Ver diário de bordo do dia", expanded=False):
                st.dataframe(decisoes_hoje, use_container_width=True)

        if st.button("Salvar snapshot hora/hora"):
            if existe_snapshot_no_bucket(str(data_operacao), hora_bucket, setores_snapshot):
                st.session_state["overwrite_pending_v55"] = {
                    "data_operacao": str(data_operacao),
                    "hora_registro": str(hora_registro),
                    "hora_bucket": str(hora_bucket),
                    "df_snapshot": df_snapshot.to_dict("records"),
                    "setores": setores_snapshot,
                }
                st.warning(f"Já existe dado salvo para {hora_bucket} em pelo menos um desses setores. Confirme abaixo se deseja sobrescrever.")
            else:
                salvar_snapshot(data_operacao, hora_registro, df_snapshot)
                st.success(f"Snapshot salvo na coluna horária {hora_bucket}.")

        pending = st.session_state.get("overwrite_pending_v55")
        if pending is not None and pending["hora_bucket"] == hora_bucket:
            st.error(f"Confirma sobrescrever os dados já salvos em {pending['hora_bucket']} para os setores selecionados?")
            b1, b2 = st.columns(2)
            if b1.button("Sim, sobrescrever", type="primary", use_container_width=True):
                apagar_snapshot_no_bucket(pending["data_operacao"], pending["hora_bucket"], pending["setores"])
                salvar_snapshot(
                    pending["data_operacao"],
                    pending["hora_registro"],
                    pd.DataFrame(pending["df_snapshot"])
                )
                st.session_state["overwrite_pending_v55"] = None
                st.success(f"Dados sobrescritos com sucesso na coluna {hora_bucket}.")
            if b2.button("Cancelar sobrescrita", use_container_width=True):
                st.session_state["overwrite_pending_v55"] = None
                st.info("Sobrescrita cancelada.")

        df_merge = df_snapshot.merge(ativos[["setor", "turno", "horas_liquidas_turno", "inicio_turno", "fim_turno", "intervalo_min", "produtividade_operacional", "programado_turno"]], on=["setor", "turno"], how="left")
        df_merge["horas_decorridas"] = df_merge.apply(lambda x: horas_decorridas_no_turno(x["inicio_turno"], x["fim_turno"], x["intervalo_min"], hora_registro), axis=1)
        df_merge["horas_restantes"] = (df_merge["horas_liquidas_turno"] - df_merge["horas_decorridas"]).clip(lower=0)
        df_merge["capacidade_restante"] = df_merge["funcionarios_presentes"] * df_merge["produtividade_operacional"] * df_merge["horas_restantes"]
        df_merge["saldo_restante"] = df_merge["capacidade_restante"] - df_merge["pedidos_faltantes_hora"]
        df_merge["status_intradia"] = df_merge.apply(lambda x: classificar_status_intradia(x["saldo_restante"], x["pedidos_faltantes_hora"]), axis=1)
        df_merge["pessoas_adicionais_necessarias"] = df_merge.apply(lambda x: pessoas_adicionais(-x["saldo_restante"], x["produtividade_operacional"], x["horas_restantes"]), axis=1)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Hora do registro", hora_registro)
        c2.metric("Turnos em andamento", len(df_merge))
        c3.metric("Turnos em risco", int((df_merge["status_intradia"] == "NÃO ATENDE").sum()))
        c4.metric("Headcount extra agora", int(df_merge["pessoas_adicionais_necessarias"].sum()))

        df_merge_view = df_merge.copy()
        df_merge_view["status_intradia"] = df_merge_view["status_intradia"].apply(status_com_icone)
        st.dataframe(df_merge_view[["setor", "turno", "funcionarios_presentes", "pedidos_feitos_hora", "pedidos_faltantes_hora", "horas_restantes", "capacidade_restante", "saldo_restante", "status_intradia", "pessoas_adicionais_necessarias"]], use_container_width=True)

else:
    st.subheader("Histórico hora/hora")
    data_historico = st.date_input("Data para consulta do histórico", value=data_operacao, key="data_historico_v57_fix2")

    try:
        resumo_exec = resumo_executivo_historico(str(data_historico), df_params, demanda_total_pedidos, demanda_total_linhas, df_turnos_pedidos)
        rc1, rc2, rc3, rc4, rc5, rc6 = st.columns(6)
        rc1.metric("Demanda planejada pedidos", format_num(resumo_exec["total_demanda_pedidos"]))
        rc2.metric("Demanda planejada linhas", format_num(resumo_exec["total_demanda_linhas"]))
        rc3.metric("Pedidos produzidos", format_num(resumo_exec["total_pedidos_produzidos"]))
        rc4.metric("Linhas produzidas", format_num(resumo_exec["total_linhas_produzidas"]))
        rc5.metric("Pessoas na operação", format_num(resumo_exec["total_pessoas"]))
        rc6.metric("Saúde da operação", resumo_exec["saude"])
    except Exception:
        pass

    layout_atual = st.session_state["layout_pref_v55"]
    with st.expander("Ajustes de layout do histórico", expanded=False):
        l1, l2, l3, l4 = st.columns(4)
        col_width_feitos = l1.slider("Largura colunas quadro 1", 50, 140, int(layout_atual.get("col_width_feitos", 70)), 5)
        col_width_faltantes = l2.slider("Largura colunas quadro 2", 50, 140, int(layout_atual.get("col_width_faltantes", 70)), 5)
        quadro_altura = l3.slider("Altura dos quadros", 180, 600, int(layout_atual.get("quadro_altura", 260)), 10)
        linha_altura = l4.slider("Altura das linhas", 28, 60, int(layout_atual.get("linha_altura", 35)), 1)
        nome_layout = st.text_input("Nome do layout", value="Layout operacional")
        s1, s2 = st.columns(2)
        if s1.button("Aplicar layout", use_container_width=True):
            st.session_state["layout_pref_v55"] = {
                "col_width_feitos": col_width_feitos,
                "col_width_faltantes": col_width_faltantes,
                "quadro_altura": quadro_altura,
                "linha_altura": linha_altura,
            }
            st.success("Layout aplicado.")
            st.rerun()
        if s2.button("Salvar layout", use_container_width=True):
            st.session_state["layout_pref_v55"] = {
                "col_width_feitos": col_width_feitos,
                "col_width_faltantes": col_width_faltantes,
                "quadro_altura": quadro_altura,
                "linha_altura": linha_altura,
            }
            salvar_layout(nome_layout, col_width_feitos, col_width_faltantes, quadro_altura, linha_altura)
            st.success("Layout salvo.")
            st.rerun()

    df_hist = carregar_historico(str(data_historico))
    df_feitos = montar_grade_historica(df_hist, setores_base, "pedidos_feitos_hora")
    df_faltantes = montar_grade_historica(df_hist, setores_base, "pedidos_faltantes_hora")

    st.write("**Quadro 1 — Pedidos feitos por hora**")
    cfg1 = {col: st.column_config.NumberColumn(width=int(st.session_state["layout_pref_v55"]["col_width_feitos"])) for col in df_feitos.columns if col != "setor"}
    st.dataframe(df_feitos, use_container_width=True, height=int(st.session_state["layout_pref_v55"]["quadro_altura"]), column_config=cfg1)

    st.write("**Quadro 2 — Pedidos faltantes por hora**")
    cfg2 = {col: st.column_config.NumberColumn(width=int(st.session_state["layout_pref_v55"]["col_width_faltantes"])) for col in df_faltantes.columns if col != "setor"}
    st.dataframe(df_faltantes, use_container_width=True, height=int(st.session_state["layout_pref_v55"]["quadro_altura"]), column_config=cfg2)

    resumo_grafico = pd.DataFrame({"hora": [f"{h:02d}:00" for h in range(24)]})
    if df_hist.empty:
        resumo_grafico["pedidos_feitos_hora"] = 0
        resumo_grafico["pedidos_faltantes_hora"] = 0
    else:
        agg = df_hist.groupby("hora_bucket", as_index=False)[["pedidos_feitos_hora", "pedidos_faltantes_hora"]].sum()
        resumo_grafico = resumo_grafico.merge(agg, left_on="hora", right_on="hora_bucket", how="left").drop(columns=["hora_bucket"])
        resumo_grafico["pedidos_feitos_hora"] = resumo_grafico["pedidos_feitos_hora"].fillna(0)
        resumo_grafico["pedidos_faltantes_hora"] = resumo_grafico["pedidos_faltantes_hora"].fillna(0)

    excel_bytes = excel_historico(df_feitos, df_faltantes, resumo_grafico)
    st.download_button(
        "Exportar histórico em Excel",
        data=excel_bytes,
        file_name=f"historico_hora_hora_{data_historico}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.subheader("Evolução ao longo do dia")
    st.line_chart(resumo_grafico.set_index("hora")[["pedidos_feitos_hora", "pedidos_faltantes_hora"]], use_container_width=True)

    st.subheader("Diário de bordo do dia")
    decisoes_hist = carregar_decisoes_bordo(str(data_historico))
    if decisoes_hist.empty:
        st.info("Ainda não há decisões registradas para este dia.")
    else:
        st.dataframe(decisoes_hist, use_container_width=True)

    if not df_hist.empty:
        st.subheader("Base de lançamentos do dia")
        st.dataframe(df_hist, use_container_width=True)
