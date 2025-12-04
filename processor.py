# ===============================================================
#   ⬛⬛⬛   PROCESSOR.PY — VERSION FINAL INTEGRADA 2025
#   Unifica resumen por agente + supervisor en un solo reporte
# ===============================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# =========================================================
# UTILIDADES DE FECHA
# =========================================================

def to_date(x):
    if pd.isna(x):
        return None
    s = str(x).strip()

    # Excel serial
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
# NORMALIZACIÓN DE ENCABEZADOS
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
# FILTRAR RANGO
# =========================================================

def filtrar_rango(df, col, d_from, d_to):
    if col not in df.columns:
        return df

    df[col] = df[col].apply(to_date)
    df = df[df[col].notna()]
    df = df[(df[col] >= d_from) & (df[col] <= d_to)]
    return df


# =========================================================
# PROCESO DE VENTAS
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
    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x["ds_product_name"]).lower().strip() == "van_compartida" else 0,
        axis=1
    )
    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x["ds_product_name"]).lower().strip() == "van_exclusive" else 0,
        axis=1
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
    df["Q_Tickets_Resueltos"] = df["Status"].apply(lambda x: 1 if str(x).lower().strip() == "solved" else 0)
    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0)

    conv = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]
    for c in conv:
        df[c] = pd.to_numeric(df.get(c, np.nan), errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg({
        "Q_Encuestas":"sum",
        "CSAT":"mean",
        "NPS Score":"mean",
        "Firt (h)":"mean",
        "% Firt":"mean",
        "Furt (h)":"mean",
        "% Furt":"mean",
        "Q_Reopen":"sum",
        "Q_Tickets":"sum",
        "Q_Tickets_Resueltos":"sum",
    })

    return out.rename(columns={
        "NPS Score":"NPS",
        "Firt (h)":"FIRT",
        "% Firt":"%FIRT",
        "Furt (h)":"FURT",
        "% Furt":"%FURT"
    })


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

    out = df.groupby(["agente", "fecha"], as_index=False).agg({
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean"
    })

    if out.empty:
        return pd.DataFrame(columns=["agente","fecha","Q_Auditorias","Nota_Auditorias"])

    out["Nota_Auditorias"] = out["Nota_Auditorias"].fillna(0)
    return out


# =========================================================
# MERGE CON LISTADO DE AGENTES
# =========================================================

def merge_agentes(df, agentes_df):
    if df.empty:
        return df

    agentes_df = normalize_headers(agentes_df.copy())
    agentes_df["Email Cabify"] = agentes_df["Email Cabify"].str.lower().str.strip()
    df["agente"] = df["agente"].str.lower().str.strip()

    df = df.merge(agentes_df, left_on="agente", right_on="Email Cabify", how="left")
    df = df[df["Email Cabify"].notna()]
    df = df.drop(columns=["agente"])
    return df


# =========================================================
# MATRIZ DIARIA COMPLETA
# =========================================================

def build_daily(df_list, agentes_df):
    merged = None
    for df in df_list:
        if df is not None and not df.empty:
            merged = df if merged is None else merged.merge(df, on=["agente","fecha"], how="outer")

    if merged is None or merged.empty:
        return pd.DataFrame()

    merged = merge_agentes(merged, agentes_df)
    merged = merged.sort_values(["fecha","Email Cabify"])

    # KPIs enteros
    int_cols = ["Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen","Q_Auditorias",
                "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"]
    for c in int_cols:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0).astype(int)

    # Promedios
    avg_cols = ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]
    for c in avg_cols:
        if c in merged.columns:
            merged[c] = merged[c].astype(float).round(2)

    order = [
        "fecha","Nombre","Primer Apellido","Segundo Apellido","Email Cabify",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ] + [c for c in merged.columns if c not in [
        "fecha","Nombre","Primer Apellido","Segundo Apellido","Email Cabify",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]]

    return merged[order]


# =========================================================
# MATRIZ SEMANAL (label corregido)
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

    def nombre_semana(f):
        delta = (f - inicio_sem).days
        s = delta // 7
        ini = inicio_sem + timedelta(days=s*7)
        fin = ini + timedelta(days=6)

        if ini.month == fin.month:
            return f"Semana {ini.day} al {fin.day} de {meses[fin.month]}"
        else:
            return f"Semana {ini.day} de {meses[ini.month]} al {fin.day} de {meses[fin.month]}"

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

    info_cols = [
        "Email Cabify","Nombre","Primer Apellido","Segundo Apellido",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]

    weekly = weekly.merge(df[info_cols].drop_duplicates(), on="Email Cabify", how="left")

    for c in ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]:
        weekly[c] = weekly[c].round(2)

    order = [
        "Semana","Nombre","Primer Apellido","Segundo Apellido",
        "Email Cabify","Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ] + [c for c in weekly.columns if c not in [
        "Semana","Nombre","Primer Apellido","Segundo Apellido",
        "Email Cabify","Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]]

    return weekly[order]


# =========================================================
# RESUMEN UNIFICADO (AGENTES + SUPERVISOR)
# =========================================================

def build_summary(df_daily):
    if df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()

    # SUMAS
    agg_sum = {
        "Q_Encuestas":"sum",
        "Q_Tickets":"sum",
        "Q_Tickets_Resueltos":"sum",
        "Q_Reopen":"sum",
        "Q_Auditorias":"sum",
        "Ventas_Totales":"sum",
        "Ventas_Compartidas":"sum",
        "Ventas_Exclusivas":"sum",
    }

    resumen_ag = df.groupby("Email Cabify", as_index=False).agg(agg_sum)

    info_cols = [
        "Email Cabify","Nombre","Primer Apellido","Segundo Apellido",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]

    resumen_ag = resumen_ag.merge(df[info_cols].drop_duplicates(), on="Email Cabify", how="left")

    # PROMEDIOS PONDERADOS
    def weighted(values, weights):
        if weights.sum() == 0:
            return np.nan
        return (values * weights).sum() / weights.sum()

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

    dfw = pd.DataFrame(registros)
    resumen_ag = resumen_ag.merge(dfw, on="Email Cabify", how="left")

    # Redondeos
    avg_cols = ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]
    for c in avg_cols:
        resumen_ag[c] = resumen_ag[c].round(2)

    # =====================================================
    # RESUMEN POR SUPERVISOR
    # =====================================================

    supervisors = resumen_ag["Supervisor"].unique().tolist()
    registros_sup = []

    for sup in supervisors:
        temp = resumen_ag[resumen_ag["Supervisor"] == sup]

        def w(vals, wts):
            if wts.sum() == 0:
                return np.nan
            return (vals * wts).sum() / wts.sum()

        registros_sup.append({
            "Supervisor": sup,
            "Tipo Registro": "TOTAL SUPERVISOR",
            "Nombre":"", "Primer Apellido":"", "Segundo Apellido":"",
            "Email Cabify":"", "Tipo contrato":"", "Ingreso":"",
            "Correo Supervisor": temp["Correo Supervisor"].iloc[0],
            "Q_Encuestas": temp["Q_Encuestas"].sum(),
            "Q_Tickets": temp["Q_Tickets"].sum(),
            "Q_Tickets_Resueltos": temp["Q_Tickets_Resueltos"].sum(),
            "Q_Reopen": temp["Q_Reopen"].sum(),
            "Q_Auditorias": temp["Q_Auditorias"].sum(),
            "Ventas_Totales": temp["Ventas_Totales"].sum(),
            "Ventas_Compartidas": temp["Ventas_Compartidas"].sum(),
            "Ventas_Exclusivas": temp["Ventas_Exclusivas"].sum(),
            "NPS": w(temp["NPS"], temp["Q_Encuestas"]),
            "CSAT": w(temp["CSAT"], temp["Q_Encuestas"]),
            "FIRT": w(temp["FIRT"], temp["Q_Tickets_Resueltos"]),
            "%FIRT": w(temp["%FIRT"], temp["Q_Tickets_Resueltos"]),
            "FURT": w(temp["FURT"], temp["Q_Tickets_Resueltos"]),
            "%FURT": w(temp["%FURT"], temp["Q_Tickets_Resueltos"]),
            "Nota_Auditorias": w(temp["Nota_Auditorias"], temp["Q_Auditorias"]),
        })

    df_sup = pd.DataFrame(registros_sup)

    # Agentes (tipo registro vacío)
    df_agents = resumen_ag.copy()
    df_agents.insert(1, "Tipo Registro", "")

    # Unión final
    final = pd.concat([df_sup, df_agents], ignore_index=True)

    # Orden final
    order = [
        "Supervisor","Tipo Registro","Nombre","Primer Apellido","Segundo Apellido",
        "Email Cabify","Tipo contrato","Ingreso","Correo Supervisor",
        "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen","Q_Auditorias",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas",
        "NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"
    ]

    return final[order]


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================

def procesar_reportes(df_ventas, df_performance, df_auditorias, agentes_df, d_from, d_to):

    ventas = process_ventas(df_ventas, d_from, d_to)
    perf = process_performance(df_performance, d_from, d_to)
    auds = process_auditorias(df_auditorias, d_from, d_to)

    diario = build_daily([ventas, perf, auds], agentes_df)
    semanal = build_weekly(diario)
    resumen = build_summary(diario)   # <— YA INCLUYE SUPERVISORES + AGENTES

    return {
        "diario": diario,
        "semanal": semanal,
        "resumen": resumen
    }
