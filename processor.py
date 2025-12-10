# ===============================================================
#   PROCESSOR.PY — VERSIÓN FINAL 2025 (Blindado + Fechas Chile)
#   Resumen unificado por Supervisor + Agentes
# ===============================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ===============================================================
# FUNCIÓN DE FECHA — VERSIÓN CORREGIDA PARA CHILE/LATAM
# ===============================================================

def to_date(x):
    """Convierte fechas DD/MM/YYYY y D/M/YYYY correctamente.
       Evita errores con meses/días ambiguos como 3/12/2025."""
    
    if pd.isna(x):
        return None

    s = str(x).strip()

    # Caso Excel serial
    if isinstance(x, (int, float)) and x > 30000:
        try:
            return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
        except:
            return None

    # FORMATO LATAM FORZADO — DD/MM/YYYY o D/M/YYYY
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except:
        pass

    # FORMATO LATAM con guión
    try:
        return datetime.strptime(s, "%d-%m-%Y").date()
    except:
        pass

    # Como último recurso, intenta interpretar con dayfirst=True
    try:
        return pd.to_datetime(s, dayfirst=True).date()
    except:
        return None


# ===============================================================
# FUNCIONES AUXILIARES
# ===============================================================

def normalize_headers(df):
    df.columns = (
        df.columns.astype(str)
        .str.replace("﻿", "")
        .str.replace("\ufeff", "")
        .str.strip()
    )
    return df


def empty_df(columns):
    """Devuelve un DF vacío pero con columnas esperadas."""
    return pd.DataFrame(columns=columns)


def filtrar_rango(df, col, d_from, d_to):
    """Filtra fechas asegurando que las columnas requeridas existan."""
    
    if col not in df.columns:
        return empty_df(df.columns)

    df[col] = df[col].apply(to_date)
    df = df[df[col].notna()]
    if df.empty:
        return empty_df(df.columns)

    df = df[(df[col] >= d_from) & (df[col] <= d_to)]
    if df.empty:
        return empty_df(df.columns)

    return df


# ===============================================================
# PROCESO VENTAS (Blindado)
# ===============================================================

def process_ventas(df, d_from, d_to):
    base_cols = ["agente","fecha","Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"]

    if df is None or df.empty:
        return empty_df(base_cols)

    df = normalize_headers(df.copy())

    if "date" not in df.columns or "ds_agent_email" not in df.columns:
        return empty_df(base_cols)

    df["fecha"] = df["date"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)
    if df.empty:
        return empty_df(base_cols)

    df["agente"] = df["ds_agent_email"].astype(str).str.lower().str.strip()

    # Limpieza de montos
    df["qt_price_local"] = (
        df["qt_price_local"].astype(str)
        .str.replace(",", "")
        .str.replace("$", "")
        .str.replace(".", "")
        .str.strip()
    )
    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x.get("ds_product_name","")).lower().strip()=="van_compartida" else 0,
        axis=1
    )
    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x.get("ds_product_name","")).lower().strip()=="van_exclusive" else 0,
        axis=1
    )

    out = df.groupby(["agente","fecha"], as_index=False)[
        ["Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"]
    ].sum()

    return out if not out.empty else empty_df(base_cols)


# ===============================================================
# PROCESO PERFORMANCE (Blindado)
# ===============================================================

def process_performance(df, d_from, d_to):
    base_cols = [
        "agente","fecha","Q_Encuestas","CSAT","NPS","FIRT","%FIRT",
        "FURT","%FURT","Q_Reopen","Q_Tickets","Q_Tickets_Resueltos"
    ]

    if df is None or df.empty:
        return empty_df(base_cols)

    df = normalize_headers(df.copy())

    if "Group Support Service" not in df.columns:
        return empty_df(base_cols)

    df = df[df["Group Support Service"] == "C_Ops Support"]
    if df.empty:
        return empty_df(base_cols)

    if "Fecha de Referencia" not in df.columns:
        return empty_df(base_cols)

    df["fecha"] = df["Fecha de Referencia"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)
    if df.empty:
        return empty_df(base_cols)

    df["agente"] = df["Assignee Email"].astype(str).str.lower().str.strip()

    # KPIs
    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x.get("CSAT")) or not pd.isna(x.get("NPS Score"))) else 0,
        axis=1
    )

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].astype(str).str.lower().str.strip().eq("solved").astype(int)
    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen",0), errors="coerce").fillna(0)

    # Convertir numéricos
    for c in ["CSAT","NPS Score","Firt (h)","% Firt","Furt (h)","% Furt"]:
        df[c] = pd.to_numeric(df.get(c, np.nan), errors="coerce")

    out = df.groupby(["agente","fecha"], as_index=False).agg({
        "Q_Encuestas":"sum",
        "CSAT":"mean",
        "NPS Score":"mean",
        "Firt (h)":"mean",
        "% Firt":"mean",
        "Furt (h)":"mean",
        "% Furt":"mean",
        "Q_Reopen":"sum",
        "Q_Tickets":"sum",
        "Q_Tickets_Resueltos":"sum"
    })

    out = out.rename(columns={
        "NPS Score":"NPS",
        "Firt (h)":"FIRT",
        "% Firt":"%FIRT",
        "Furt (h)":"FURT",
        "% Furt":"%FURT"
    })

    return out if not out.empty else empty_df(base_cols)


# ===============================================================
# PROCESO AUDITORÍAS (Blindado)
# ===============================================================

def process_auditorias(df, d_from, d_to):
    base_cols = ["agente","fecha","Q_Auditorias","Nota_Auditorias"]

    if df is None or df.empty:
        return empty_df(base_cols)

    df = normalize_headers(df.copy())

    if "Date Time" not in df.columns or "Audited Agent" not in df.columns:
        return empty_df(base_cols)

    df["fecha"] = df["Date Time"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)
    if df.empty:
        return empty_df(base_cols)

    df = df[df["Audited Agent"].astype(str).str.contains("@")]
    if df.empty:
        return empty_df(base_cols)

    df["agente"] = df["Audited Agent"].astype(str).str.lower().str.strip()
    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce").fillna(0)

    out = df.groupby(["agente","fecha"], as_index=False).agg({
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean"
    })

    return out if not out.empty else empty_df(base_cols)


# ===============================================================
# MERGE AGENTES — Blindado
# ===============================================================

def merge_agentes(df, agentes_df):
    info_cols = [
        "Email Cabify","Nombre","Primer Apellido","Segundo Apellido",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]

    if df is None or df.empty:
        return empty_df(["fecha"] + info_cols)

    agentes_df = normalize_headers(agentes_df.copy())

    for col in info_cols:
        if col not in agentes_df.columns:
            agentes_df[col] = ""

    agentes_df["Email Cabify"] = agentes_df["Email Cabify"].astype(str).str.lower().str.strip()
    df["agente"] = df["agente"].astype(str).str.lower().str.strip()

    merged = df.merge(
        agentes_df,
        left_on="agente",
        right_on="Email Cabify",
        how="left"
    )

    merged = merged.drop(columns=["agente"])
    return merged


# ===============================================================
# DIARIO (Blindado)
# ===============================================================

def build_daily(df_list, agentes_df):

    merged = None
    for df in df_list:
        if df is not None and not df.empty:
            merged = df if merged is None else merged.merge(df, on=["agente","fecha"], how="outer")

    # Si no hay nada, devolver DF diario vacío pero estructurado
    if merged is None or merged.empty:
        return empty_df([
            "fecha","Nombre","Primer Apellido","Segundo Apellido","Email Cabify",
            "Tipo contrato","Ingreso","Supervisor","Correo Supervisor",
            "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
            "Q_Auditorias","Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas",
            "NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"
        ])

    merged = merge_agentes(merged, agentes_df)

    # Columnas que deben existir sí o sí
    needed_cols = [
        "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen","Q_Auditorias",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas",
        "NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"
    ]
    for c in needed_cols:
        if c not in merged.columns:
            merged[c] = 0

    # Tipos
    ints = ["Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen","Q_Auditorias",
            "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"]
    for c in ints:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0).astype(int)

    floats = ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]
    for c in floats:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").round(2)

    merged = merged.sort_values(["fecha","Email Cabify"], na_position="first")
    return merged


# ===============================================================
# SEMANAL (Blindado)
# ===============================================================

def build_weekly(df_daily):
    if df_daily.empty:
        return empty_df(df_daily.columns)

    df = df_daily.copy()
    meses = {
        1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",
        6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",
        10:"Octubre",11:"Noviembre",12:"Diciembre"
    }

    fecha_min = df["fecha"].min()
    if pd.isna(fecha_min):
        return empty_df(df.columns)

    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(fecha):
        delta = (fecha - inicio_sem).days
        s = delta // 7
        ini = inicio_sem + timedelta(days=s*7)
        fin = ini + timedelta(days=6)

        if ini.month == fin.month:
            return f"Semana {ini.day} al {fin.day} de {meses[fin.month]}"
        else:
            return f"Semana {ini.day} de {meses[ini.month]} al {fin.day} de {meses[fin.month]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)

    agg = {
        "Q_Encuestas":"sum","NPS":"mean","CSAT":"mean","FIRT":"mean","%FIRT":"mean",
        "FURT":"mean","%FURT":"mean","Q_Reopen":"sum","Q_Tickets":"sum",
        "Q_Tickets_Resueltos":"sum","Q_Auditorias":"sum","Nota_Auditorias":"mean",
        "Ventas_Totales":"sum","Ventas_Compartidas":"sum","Ventas_Exclusivas":"sum"
    }

    try:
        weekly = df.groupby(["Email Cabify","Semana"], as_index=False).agg(agg)
    except:
        return empty_df(df.columns)

    return weekly


# ===============================================================
# RESUMEN UNIFICADO — SUPERVISORES + AGENTES (Blindado)
# ===============================================================

def build_summary(df_daily):

    needed_cols = [
        "Supervisor","Tipo Registro","Nombre","Primer Apellido","Segundo Apellido",
        "Email Cabify","Tipo contrato","Ingreso","Correo Supervisor",
        "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen","Q_Auditorias",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas",
        "NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"
    ]

    if df_daily.empty:
        return empty_df(needed_cols)

    df = df_daily.copy()

    agg_sum = {
        "Q_Encuestas":"sum","Q_Tickets":"sum","Q_Tickets_Resueltos":"sum",
        "Q_Reopen":"sum","Q_Auditorias":"sum",
        "Ventas_Totales":"sum","Ventas_Compartidas":"sum","Ventas_Exclusivas":"sum"
    }

    try:
        resumen_ag = df.groupby("Email Cabify", as_index=False).agg(agg_sum)
    except:
        return empty_df(needed_cols)

    info_cols = [
        "Email Cabify","Nombre","Primer Apellido","Segundo Apellido",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]

    resumen_ag = resumen_ag.merge(df[info_cols].drop_duplicates(),
                                  on="Email Cabify", how="left")

    # PROMEDIOS PONDERADOS
    def weighted(vals, weights):
        if weights.sum() == 0:
            return np.nan
        return (vals * weights).sum() / weights.sum()

    registros = []
    for ag in resumen_ag["Email Cabify"]:
        temp = df[df["Email Cabify"] == ag]
        registros.append({
            "Email Cabify": ag,
            "NPS": weighted(temp["NPS"], temp["Q_Encuestas"]),
            "CSAT": weighted(temp["CSAT"], temp["Q_Encuestas"]),
            "FIRT": weighted(temp["FIRT"], temp["Q_Tickets_Resueltos"]),
            "%FIRT": weighted(temp["%FIRT"], temp["Q_Tickets_Resueltos"]),
            "FURT": weighted(temp["FURT"], temp["Q_Tickets_Resueltos"]),
            "%FURT": weighted(temp["%FURT"], temp["Q_Tickets_Resueltos"]),
            "Nota_Auditorias": weighted(temp["Nota_Auditorias"], temp["Q_Auditorias"]),
        })

    resumen_ag = resumen_ag.merge(pd.DataFrame(registros), on="Email Cabify", how="left")

    # REDONDEOS
    for c in ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]:
        resumen_ag[c] = pd.to_numeric(resumen_ag[c], errors="coerce").round(2)

    # SUPERVISORES
    supervisores = resumen_ag["Supervisor"].dropna().unique().tolist()
    registros_sup = []

    def w(vals, wts):
        if wts.sum() == 0:
            return np.nan
        return (vals * wts).sum() / weights.sum()

    for sup in supervisores:
        temp = resumen_ag[resumen_ag["Supervisor"] == sup]

        registros_sup.append({
            "Supervisor": sup,
            "Tipo Registro":"TOTAL SUPERVISOR",
            "Nombre":"","Primer Apellido":"","Segundo Apellido":"",
            "Email Cabify":"","Tipo contrato":"","Ingreso":"",
            "Correo Supervisor": temp["Correo Supervisor"].iloc[0],

            "Q_Encuestas": temp["Q_Encuestas"].sum(),
            "Q_Tickets": temp["Q_Tickets"].sum(),
            "Q_Tickets_Resueltos": temp["Q_Tickets_Resueltos"].sum(),
            "Q_Reopen": temp["Q_Reopen"].sum(),
            "Q_Auditorias": temp["Q_Auditorias"].sum(),

            "Ventas_Totales": temp["Ventas_Totales"].sum(),
            "Ventas_Compartidas": temp["Ventas_Compartidas"].sum(),
            "Ventas_Exclusivas": temp["Ventas_Exclusivas"].sum(),

            "NPS": weighted(temp["NPS"], temp["Q_Encuestas"]),
            "CSAT": weighted(temp["CSAT"], temp["Q_Encuestas"]),
            "FIRT": weighted(temp["FIRT"], temp["Q_Tickets_Resueltos"]),
            "%FIRT": weighted(temp["%FIRT"], temp["Q_Tickets_Resueltos"]),
            "FURT": weighted(temp["FURT"], temp["Q_Tickets_Resueltos"]),
            "%FURT": weighted(temp["%FURT"], temp["Q_Tickets_Resueltos"]),
            "Nota_Auditorias": weighted(temp["Nota_Auditorias"], temp["Q_Auditorias"]),
        })

    df_sup = pd.DataFrame(registros_sup)
    df_agents = resumen_ag.copy()
    df_agents.insert(1, "Tipo Registro","")

    final = pd.concat([df_sup, df_agents], ignore_index=True)
    final = final[needed_cols]

    return final


# ===============================================================
# FUNCIÓN PRINCIPAL (Blindada)
# ===============================================================

def procesar_reportes(df_ventas, df_performance, df_auditorias, agentes_df, d_from, d_to):

    ventas = process_ventas(df_ventas, d_from, d_to)
    perf = process_performance(df_performance, d_from, d_to)
    auds = process_auditorias(df_auditorias, d_from, d_to)

    diario = build_daily([ventas, perf, auds], agentes_df)
    semanal = build_weekly(diario)
    resumen = build_summary(diario)

    return {
        "diario": diario,
        "semanal": semanal,
        "resumen": resumen
    }
