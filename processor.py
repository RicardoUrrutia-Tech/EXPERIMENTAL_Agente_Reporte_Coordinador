# ===============================================================
#   PROCESSOR.PY — VERSION PONDERADA 2025 (PARTE 1/2)
#   Compatible 100% con tu app actual
#   Semanal NO se modifica
#   Nuevo: resumen por supervisor con TOT SUPERVISOR (ponderado)
# ===============================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# =========================================================
# UTILIDADES DE FECHAS
# =========================================================

def to_date(x):
    """Convierte cualquier valor a fecha real (date), ignorando horas y timestamps."""
    if pd.isna(x):
        return None

    s = str(x).strip()

    # Si viene como número Excel (float)
    if isinstance(x, (int, float)) and x > 30000:
        try:
            return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
        except:
            pass

    # YYYY/MM/DD
    if "/" in s and len(s.split("/")[0]) == 4:
        try:
            return datetime.strptime(s, "%Y/%m/%d").date()
        except:
            pass

    # DD-MM-YYYY
    if "-" in s and len(s.split("-")[2]) == 4 and len(s.split("-")[0]) <= 2:
        try:
            return datetime.strptime(s, "%d-%m-%Y").date()
        except:
            pass

    # MM/DD/YYYY
    if "/" in s and len(s.split("/")[2]) == 4:
        try:
            return datetime.strptime(s, "%m/%d/%Y").date()
        except:
            pass

    try:
        return pd.to_datetime(s).date()
    except:
        return None


# =========================================================
# LIMPIEZA HEADERS
# =========================================================

def normalize_headers(df):
    df.columns = (
        df.columns.astype(str)
        .str.replace("﻿", "")
        .str.replace("\ufeff", "")
        .str.strip()
    )
    return df


# =========================================================
# FILTRO RANGO FECHAS
# =========================================================

def filtrar_rango(df, col, d_from, d_to):
    if col not in df.columns:
        return df

    df[col] = df[col].apply(to_date)
    df = df[df[col].notna()]
    df = df[(df[col] >= d_from) & (df[col] <= d_to)]
    return df


# =========================================================
# PROCESO VENTAS
# =========================================================

def process_ventas(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    df["fecha"] = df["date"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df["agente"] = df["ds_agent_email"]

    df["qt_price_local"] = (
        df["qt_price_local"]
        .astype(str)
        .str.replace(",", "")
        .str.replace("$", "")
        .str.replace(".", "")
        .str.strip()
    )

    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0).astype(int)

    df["Ventas_Totales"] = df["qt_price_local"]

    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"]
        if str(x["ds_product_name"]).lower().strip() == "van_compartida"
        else 0,
        axis=1,
    )

    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"]
        if str(x["ds_product_name"]).lower().strip() == "van_exclusive"
        else 0,
        axis=1,
    )

    return df.groupby(["agente", "fecha"], as_index=False)[
        ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]
    ].sum()


# =========================================================
# PROCESO PERFORMANCE
# =========================================================

def process_performance(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    if "Group Support Service" not in df.columns:
        return pd.DataFrame()

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = df["Fecha de Referencia"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df["agente"] = df["Assignee Email"]

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x.get("CSAT")) or not pd.isna(x.get("NPS Score"))) else 0,
        axis=1,
    )

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(
        lambda x: 1 if str(x).strip().lower() == "solved" else 0
    )

    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0).astype(int)

    conv = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]
    for c in conv:
        df[c] = pd.to_numeric(df.get(c, np.nan), errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg(
        {
            "Q_Encuestas": "sum",
            "CSAT": "mean",
            "NPS Score": "mean",
            "Firt (h)": "mean",
            "% Firt": "mean",
            "Furt (h)": "mean",
            "% Furt": "mean",
            "Q_Reopen": "sum",
            "Q_Tickets": "sum",
            "Q_Tickets_Resueltos": "sum",
        }
    )

    out = out.rename(
        columns={
            "NPS Score": "NPS",
            "Firt (h)": "FIRT",
            "% Firt": "%FIRT",
            "Furt (h)": "FURT",
            "% Furt": "%FURT",
        }
    )

    return out


# =========================================================
# PROCESO AUDITORÍAS
# =========================================================

def process_auditorias(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    df["fecha"] = df["Date Time"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df = df[df["Audited Agent"].astype(str).str.contains("@")]

    df["agente"] = df["Audited Agent"]

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg(
        {"Q_Auditorias": "sum", "Nota_Auditorias": "mean"}
    )

    if out.empty:
        return pd.DataFrame(
            columns=["agente", "fecha", "Q_Auditorias", "Nota_Auditorias"]
        )

    out["Nota_Auditorias"] = out["Nota_Auditorias"].fillna(0)

    return out
# =========================================================
# MERGE CON LISTADO DE AGENTES
# =========================================================

def merge_agentes(df, agentes_df):
    if df is None or df.empty:
        return df

    agentes_df = normalize_headers(agentes_df.copy())
    agentes_df["Email Cabify"] = agentes_df["Email Cabify"].str.lower().str.strip()

    df["agente"] = df["agente"].str.lower().str.strip()

    df = df.merge(
        agentes_df,
        left_on="agente",
        right_on="Email Cabify",
        how="left",
    )

    df = df[df["Email Cabify"].notna()]

    df = df.drop(columns=["agente"])
    return df


# =========================================================
# MATRIZ DIARIA
# =========================================================

def build_daily(df_list, agentes_df):
    merged = None

    for df in df_list:
        if df is not None and not df.empty:
            merged = df if merged is None else merged.merge(
                df, on=["agente", "fecha"], how="outer"
            )

    if merged is None or merged.empty:
        return pd.DataFrame()

    merged = merge_agentes(merged, agentes_df)
    merged = merged.sort_values(["fecha", "Email Cabify"])

    # Conteos
    q_cols = [
        "Q_Encuestas", "Q_Tickets", "Q_Tickets_Resueltos", "Q_Reopen",
        "Q_Auditorias", "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"
    ]
    for c in q_cols:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0).astype(int)

    # Promedios
    avg_cols = ["NPS", "CSAT", "FIRT", "%FIRT", "FURT", "%FURT", "Nota_Auditorias"]
    for c in avg_cols:
        if c in merged.columns:
            merged[c] = merged[c].round(4)

    # Orden final
    order = [
        "fecha",
        "Nombre", "Primer Apellido", "Segundo Apellido",
        "Email Cabify", "Tipo contrato", "Ingreso",
        "Supervisor", "Correo Supervisor",
    ] + [
        c for c in merged.columns if c not in [
            "fecha",
            "Nombre", "Primer Apellido", "Segundo Apellido",
            "Email Cabify", "Tipo contrato", "Ingreso",
            "Supervisor", "Correo Supervisor"
        ]
    ]

    return merged[order]


# =========================================================
# MATRIZ SEMANAL (NO SE MODIFICA)
# =========================================================

def build_weekly(df_daily):
    if df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()

    meses = {
        1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",
        6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",
        10:"Octubre",11:"Noviembre",12:"Diciembre"
    }

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(fecha):
        delta = (fecha - inicio_sem).days
        s = delta // 7
        ini = inicio_sem + timedelta(days=s*7)
        fin = ini + timedelta(days=6)
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)

    agg = {
        "Q_Encuestas":"sum",
        "NPS":"mean",
        "CSAT":"mean",
        "FIRT":"mean",
        "%FIRT":"mean",
        "FURT":"mean",
        "%FURT":"mean",
        "Q_Reopen":"sum",
        "Q_Tickets":"sum",
        "Q_Tickets_Resueltos":"sum",
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean",
        "Ventas_Totales":"sum",
        "Ventas_Compartidas":"sum",
        "Ventas_Exclusivas":"sum"
    }

    weekly = df.groupby(["Email Cabify","Semana"], as_index=False).agg(agg)

    for c in ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]:
        if c in weekly.columns:
            weekly[c] = weekly[c].round(2)

    agentes_cols = [
        "Email Cabify",
        "Nombre","Primer Apellido","Segundo Apellido",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]

    weekly = weekly.merge(
        df_daily[agentes_cols].drop_duplicates(),
        on="Email Cabify",
        how="left"
    )

    order = [
        "Semana","Nombre","Primer Apellido","Segundo Apellido",
        "Email Cabify","Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ] + [
        c for c in weekly.columns
        if c not in [
            "Semana","Nombre","Primer Apellido","Segundo Apellido",
            "Email Cabify","Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
        ]
    ]

    return weekly[order]


# =========================================================
# RESUMEN AGENTE
# =========================================================

def build_summary_agents(df_daily):
    if df_daily.empty:
        return pd.DataFrame()

    agg = {
        "Q_Encuestas":"sum",
        "NPS":"mean",
        "CSAT":"mean",
        "FIRT":"mean",
        "%FIRT":"mean",
        "FURT":"mean",
        "%FURT":"mean",
        "Q_Reopen":"sum",
        "Q_Tickets_Resueltos":"sum",
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean",
        "Ventas_Totales":"sum",
        "Ventas_Compartidas":"sum",
        "Ventas_Exclusivas":"sum",
    }

    resumen = df_daily.groupby("Email Cabify", as_index=False).agg(agg)

    for c in ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]:
        resumen[c] = resumen[c].round(4)

    resumen = resumen.merge(
        df_daily[
            [
                "Email Cabify","Nombre","Primer Apellido","Segundo Apellido",
                "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
            ]
        ].drop_duplicates(),
        on="Email Cabify",
        how="left",
    )

    resumen["Tipo Registro"] = ""

    order = [
        "Supervisor", "Tipo Registro",
        "Nombre","Primer Apellido","Segundo Apellido",
        "Email Cabify","Tipo contrato","Ingreso","Correo Supervisor"
    ] + [
        c for c in resumen.columns
        if c not in [
            "Supervisor","Tipo Registro",
            "Nombre","Primer Apellido","Segundo Apellido",
            "Email Cabify","Tipo contrato","Ingreso","Correo Supervisor"
        ]
    ]

    return resumen[order]


# =========================================================
# RESUMEN POR SUPERVISOR — PONDERADO
# =========================================================

def build_summary_supervisors(df_agents):

    if df_agents.empty:
        return pd.DataFrame()

    # separo conteos y promedios
    count_cols = [
        "Q_Encuestas","Q_Reopen","Q_Tickets_Resueltos",
        "Q_Auditorias","Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]

    avg_cols = ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]

    # Agrupar para conteos = suma
    grouped_sum = df_agents.groupby("Supervisor", as_index=False)[count_cols].sum()

    # Cálculo ponderado para promedios
    sup_rows = []
    for sup, df in df_agents.groupby("Supervisor"):
        row = {"Supervisor": sup, "Tipo Registro": "TOTAL SUPERVISOR"}

        for c in count_cols:
            row[c] = df[c].sum()

        # peso = Q_Tickets_Resueltos (si no, usar Q_Encuestas)
        peso = df["Q_Tickets_Resueltos"]
        peso = peso.replace(0, np.nan)

        for c in avg_cols:
            if peso.notna().sum() == 0:
                row[c] = np.nan
            else:
                row[c] = (df[c] * peso).sum() / peso.sum()

        sup_rows.append(row)

    df_super = pd.DataFrame(sup_rows)

    return df_super


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================

def procesar_reportes(df_ventas, df_perf, df_auds, agentes_df, d_from, d_to):

    ventas = process_ventas(df_ventas, d_from, d_to)
    perf = process_performance(df_perf, d_from, d_to)
    auds = process_auditorias(df_auds, d_from, d_to)

    diario = build_daily([ventas, perf, auds], agentes_df)
    semanal = build_weekly(diario)

    resumen_agentes = build_summary_agents(diario)
    resumen_super = build_summary_supervisors(resumen_agentes)

    # unir: supervisor arriba + agentes abajo
    resumen_total = pd.concat([resumen_super, resumen_agentes], ignore_index=True)

    return {
        "diario": diario,
        "semanal": semanal,
        "resumen": resumen_total,
    }



