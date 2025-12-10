import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# =========================================================
#   FUNCIÓN DE FECHA — VERSIÓN FINAL (ROBUSTA LATAM)
# =========================================================

def to_date(x):
    """Convierte fechas DD/MM/YYYY, D/M/YYYY, DD-MM-YYYY, D-M-YYYY, y serial Excel.
       Nunca interpreta MM/DD, fuerza siempre día/mes."""
    
    if pd.isna(x):
        return None

    s = str(x).strip()

    # Excel serial
    if isinstance(x, (int, float)):
        try:
            if x > 30000:
                return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
        except:
            pass

    # Intentar formatos típicos chilenos
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass

    # Intento manual D/M/YYYY o D-M-YYYY
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 3:
            d, m, y = parts
            try:
                return datetime(int(y), int(m), int(d)).date()
            except:
                pass

    if "-" in s:
        parts = s.split("-")
        if len(parts) == 3:
            d, m, y = parts
            try:
                return datetime(int(y), int(m), int(d)).date()
            except:
                pass

    return None


# =========================================================
#   AUXILIARES
# =========================================================

def normalize_headers(df):
    df.columns = (
        df.columns.astype(str)
        .str.replace("﻿", "")
        .str.replace("\ufeff", "")
        .str.strip()
    )
    return df

def empty_df(cols):
    return pd.DataFrame(columns=cols)

def filtrar_rango(df, col, d_from, d_to):
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


# =========================================================
#   VENTAS — createdAt_local (ISO)
# =========================================================

def process_ventas(df, d_from, d_to):

    base_cols = [
        "agente","fecha",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]

    if df is None or df.empty:
        return empty_df(base_cols)

    df = normalize_headers(df.copy())

    if "createdAt_local" not in df.columns or "ds_agent_email" not in df.columns:
        return empty_df(base_cols)

    df["fecha"] = pd.to_datetime(df["createdAt_local"], errors="coerce").dt.date
    df = df[df["fecha"].notna()]
    df = df[(df["fecha"] >= d_from) & (df["fecha"] <= d_to)]
    if df.empty:
        return empty_df(base_cols)

    df["agente"] = df["ds_agent_email"].astype(str).str.lower().str.strip()

    if "qt_price_local" in df.columns:
        df["qt_price_local"] = (
            df["qt_price_local"]
            .astype(str)
            .str.replace(",", "")
            .str.replace("$", "")
            .str.replace(".", "")
            .str.strip()
        )
        df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)
    else:
        df["qt_price_local"] = 0

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = np.where(
        df["ds_product_name"].astype(str).str.lower().str.strip() == "van_compartida",
        df["qt_price_local"],
        0
    )
    df["Ventas_Exclusivas"] = np.where(
        df["ds_product_name"].astype(str).str.lower().str.strip() == "van_exclusive",
        df["qt_price_local"],
        0
    )

    out = df.groupby(["agente","fecha"], as_index=False)[
        ["Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"]
    ].sum()

    return out if not out.empty else empty_df(base_cols)


# =========================================================
#   PERFORMANCE — Fecha de Referencia (MM/DD/YYYY)
# =========================================================

def process_performance(df, d_from, d_to):

    base_cols = [
        "agente","fecha",
        "Q_Encuestas","CSAT","NPS",
        "FIRT","%FIRT","FURT","%FURT",
        "Q_Reopen","Q_Tickets","Q_Tickets_Resueltos"
    ]

    if df is None or df.empty:
        return empty_df(base_cols)

    df = normalize_headers(df.copy())

    if "Fecha de Referencia" not in df.columns or "Assignee Email" not in df.columns:
        return empty_df(base_cols)

    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce").dt.date
    df = df[df["fecha"].notna()]
    df = df[(df["fecha"] >= d_from) & (df["fecha"] <= d_to)]
    if df.empty:
        return empty_df(base_cols)

    df["agente"] = df["Assignee Email"].astype(str).str.lower().str.strip()

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x.get("CSAT")) or not pd.isna(x.get("NPS Score"))) else 0,
        axis=1
    )

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].astype(str).str.lower().str.strip().eq("solved").astype(int)

    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0)

    for c in ["CSAT","NPS Score","Firt (h)","% Firt","Furt (h)","% Furt"]:
        df[c] = pd.to_numeric(df.get(c, np.nan), errors="coerce")

    agg = df.groupby(["agente","fecha"], as_index=False).agg({
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

    agg = agg.rename(columns={
        "NPS Score":"NPS",
        "Firt (h)":"FIRT",
        "% Firt":"%FIRT",
        "Furt (h)":"FURT",
        "% Furt":"%FURT"
    })

    return agg if not agg.empty else empty_df(base_cols)

# =========================================================
#   AUDITORÍAS — Date Time (DD-MM-YYYY / DD/MM/YYYY)
# =========================================================

def process_auditorias(df, d_from, d_to):

    base_cols = ["agente","fecha","Q_Auditorias","Nota_Auditorias"]

    if df is None or df.empty:
        return empty_df(base_cols)

    df = normalize_headers(df.copy())

    if "Date Time" not in df.columns or "Audited Agent" not in df.columns:
        return empty_df(base_cols)

    df["fecha"] = df["Date Time"].apply(to_date)
    df = df[df["fecha"].notna()]
    df = df[(df["fecha"] >= d_from) & (df["fecha"] <= d_to)]
    if df.empty:
        return empty_df(base_cols)

    df = df[df["Audited Agent"].astype(str).str.contains("@")]
    if df.empty:
        return empty_df(base_cols)

    df["agente"] = df["Audited Agent"].astype(str).str.lower().str.strip()
    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df.get("Total Audit Score", 0), errors="coerce").fillna(0)

    out = df.groupby(["agente","fecha"], as_index=False).agg({
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean"
    })

    return out if not out.empty else empty_df(base_cols)



# =========================================================
#   MERGE AGENTES
# =========================================================

def merge_agentes(df, agentes_df):

    info_cols = [
        "Email Cabify","Nombre","Primer Apellido","Segundo Apellido",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]

    if df is None or df.empty:
        return empty_df(["fecha"] + info_cols)

    agentes_df = normalize_headers(agentes_df.copy())

    for c in info_cols:
        if c not in agentes_df.columns:
            agentes_df[c] = ""

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



# =========================================================
#   DIARIO
# =========================================================

def build_daily(df_list, agentes_df):

    merged = None
    for df in df_list:
        if df is not None and not df.empty:
            merged = df if merged is None else merged.merge(df, on=["agente","fecha"], how="outer")

    if merged is None or merged.empty:
        merged = empty_df(["agente","fecha"])

    merged = merge_agentes(merged, agentes_df)

    for c in [
        "Q_Encuestas","CSAT","NPS","FIRT","%FIRT","FURT","%FURT",
        "Q_Auditorias","Nota_Auditorias",
        "Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]:
        if c not in merged.columns:
            merged[c] = 0

    ints = [
        "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
        "Q_Auditorias",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]
    for c in ints:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0).astype(int)

    floats = ["CSAT","NPS","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]
    for c in floats:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").round(2)

    merged = merged.sort_values(["fecha","Email Cabify"], na_position="first")

    final_cols = [
        "fecha",
        "Nombre","Primer Apellido","Segundo Apellido","Email Cabify",
        "Supervisor","Correo Supervisor","Tipo contrato","Ingreso",
        "Q_Encuestas","CSAT","NPS","FIRT","%FIRT","FURT","%FURT",
        "Q_Auditorias","Nota_Auditorias",
        "Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]

    return merged[final_cols]



# =========================================================
#   SEMANAL
# =========================================================

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
        "Q_Encuestas":"sum","CSAT":"mean","NPS":"mean",
        "FIRT":"mean","%FIRT":"mean","FURT":"mean","%FURT":"mean",
        "Q_Auditorias":"sum","Nota_Auditorias":"mean",
        "Q_Tickets":"sum","Q_Tickets_Resueltos":"sum","Q_Reopen":"sum",
        "Ventas_Totales":"sum","Ventas_Compartidas":"sum","Ventas_Exclusivas":"sum"
    }

    weekly = df.groupby(["Semana","Email Cabify"], as_index=False).agg(agg)

    personal_cols = [
        "Email Cabify","Nombre","Primer Apellido","Segundo Apellido",
        "Supervisor","Correo Supervisor","Tipo contrato","Ingreso"
    ]
    weekly = weekly.merge(df[personal_cols].drop_duplicates(), on="Email Cabify", how="left")

    cols = [
        "Semana",
        "Nombre","Primer Apellido","Segundo Apellido","Email Cabify",
        "Supervisor","Correo Supervisor","Tipo contrato","Ingreso",
        "Q_Encuestas","CSAT","NPS","FIRT","%FIRT","FURT","%FURT",
        "Q_Auditorias","Nota_Auditorias",
        "Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]

    return weekly[cols]
# =========================================================
#   RESUMEN — Supervisor → agentes
# =========================================================

def build_summary(df_daily):

    if df_daily.empty:
        return empty_df([
            "Tipo Registro","Supervisor",
            "Nombre","Primer Apellido","Segundo Apellido","Email Cabify",
            "Correo Supervisor","Tipo contrato","Ingreso",
            "Q_Encuestas","CSAT","NPS","FIRT","%FIRT","FURT","%FURT",
            "Q_Auditorias","Nota_Auditorias",
            "Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
            "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
        ])

    df = df_daily.copy()

    agg_sum = {
        "Q_Encuestas":"sum","Q_Tickets":"sum","Q_Tickets_Resueltos":"sum",
        "Q_Reopen":"sum","Q_Auditorias":"sum",
        "Ventas_Totales":"sum","Ventas_Compartidas":"sum","Ventas_Exclusivas":"sum"
    }

    resumen_ag = df.groupby("Email Cabify", as_index=False).agg(agg_sum)

    info_cols = [
        "Email Cabify","Nombre","Primer Apellido","Segundo Apellido",
        "Tipo contrato","Ingreso","Supervisor","Correo Supervisor"
    ]

    resumen_ag = resumen_ag.merge(
        df[info_cols].drop_duplicates(), on="Email Cabify", how="left"
    )

    def w(vals, weights):
        vals = pd.to_numeric(vals, errors="coerce")
        weights = pd.to_numeric(weights, errors="coerce")
        if weights.sum() == 0:
            return np.nan
        return (vals * weights).sum() / weights.sum()

    registros = []
    for ag in resumen_ag["Email Cabify"]:
        temp = df[df["Email Cabify"] == ag]
        registros.append({
            "Email Cabify": ag,
            "NPS": w(temp["NPS"], temp["Q_Encuestas"]),
            "CSAT": w(temp["CSAT"], temp["Q_Encuestas"]),
            "FIRT": w(temp["FIRT"], temp["Q_Tickets_Resueltos"]),
            "%FIRT": w(temp["%FIRT"], temp["Q_Tickets_Resueltos"]),
            "FURT": w(temp["FURT"], temp["Q_Tickets_Resueltos"]),
            "%FURT": w(temp["%FURT"], temp["Q_Tickets_Resueltos"]),
            "Nota_Auditorias": w(temp["Nota_Auditorias"], temp["Q_Auditorias"]),
        })

    resumen_ag = resumen_ag.merge(pd.DataFrame(registros), on="Email Cabify", how="left")

    for c in ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]:
        resumen_ag[c] = pd.to_numeric(resumen_ag[c], errors="coerce").round(2)

    supervisors = resumen_ag["Supervisor"].dropna().unique().tolist()
    registros_sup = []

    for sup in supervisors:
        temp = resumen_ag[resumen_ag["Supervisor"] == sup]

        registros_sup.append({
            "Tipo Registro":"TOTAL SUPERVISOR",
            "Supervisor": sup,
            "Nombre": "",
            "Primer Apellido":"",
            "Segundo Apellido":"",
            "Email Cabify":"",
            "Correo Supervisor": temp["Correo Supervisor"].iloc[0],
            "Tipo contrato":"",
            "Ingreso":"",
            "Q_Encuestas": temp["Q_Encuestas"].sum(),
            "CSAT": w(temp["CSAT"], temp["Q_Encuestas"]),
            "NPS": w(temp["NPS"], temp["Q_Encuestas"]),
            "FIRT": w(temp["FIRT"], temp["Q_Tickets_Resueltos"]),
            "%FIRT": w(temp["%FIRT"], temp["Q_Tickets_Resueltos"]),
            "FURT": w(temp["FURT"], temp["Q_Tickets_Resueltos"]),
            "%FURT": w(temp["%FURT"], temp["Q_Tickets_Resueltos"]),
            "Q_Auditorias": temp["Q_Auditorias"].sum(),
            "Nota_Auditorias": w(temp["Nota_Auditorias"], temp["Q_Auditorias"]),
            "Q_Tickets": temp["Q_Tickets"].sum(),
            "Q_Tickets_Resueltos": temp["Q_Tickets_Resueltos"].sum(),
            "Q_Reopen": temp["Q_Reopen"].sum(),
            "Ventas_Totales": temp["Ventas_Totales"].sum(),
            "Ventas_Compartidas": temp["Ventas_Compartidas"].sum(),
            "Ventas_Exclusivas": temp["Ventas_Exclusivas"].sum(),
        })

    df_sup = pd.DataFrame(registros_sup)

    df_agents = resumen_ag.copy()
    df_agents.insert(0, "Tipo Registro", "")

    final = pd.concat([df_sup, df_agents], ignore_index=True)

    final_cols = [
        "Tipo Registro","Supervisor",
        "Nombre","Primer Apellido","Segundo Apellido","Email Cabify",
        "Correo Supervisor","Tipo contrato","Ingreso",
        "Q_Encuestas","CSAT","NPS","FIRT","%FIRT","FURT","%FURT",
        "Q_Auditorias","Nota_Auditorias",
        "Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]

    return final[final_cols]



# =========================================================
#   FUNCIÓN PRINCIPAL
# =========================================================

def procesar_reportes(df_ventas, df_perf, df_aud, agentes_df, d_from, d_to):

    ventas = process_ventas(df_ventas, d_from, d_to)
    perf   = process_performance(df_perf, d_from, d_to)
    auds   = process_auditorias(df_aud, d_from, d_to)

    diario  = build_daily([ventas, perf, auds], agentes_df)
    semanal = build_weekly(diario)
    resumen = build_summary(diario)

    return {
        "diario": diario,
        "semanal": semanal,
        "resumen": resumen
    }
