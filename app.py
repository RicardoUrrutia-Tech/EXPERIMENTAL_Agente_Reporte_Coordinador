import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_reportes

# ------------------------------------------------------------
# CONFIGURACIÓN
# ------------------------------------------------------------
st.set_page_config(page_title="Consolidador CMI Aeropuerto", layout="wide")
st.title("🟦 Consolidador CMI – Aeropuerto Cabify")

st.markdown("""
Sube los reportes correspondientes, selecciona el rango de fechas
y la app consolidará **Ventas**, **Performance** y **Auditorías**, además del
**Resumen Global por Supervisor**.

Incluye:

- 📅 Reporte Diario  
- 📆 Reporte Semanal (por agente)  
- 📊 Resumen Total del Periodo  
- ⭐ Consolidado por Supervisor  
""")

# ------------------------------------------------------------
# SUBIDA DE ARCHIVOS
# ------------------------------------------------------------
st.header("📤 Cargar Archivos")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader("Reporte de Ventas (.xlsx)", type=["xlsx"])
    performance_file = st.file_uploader("Reporte de Performance (.csv)", type=["csv"])

with col2:
    auditorias_file = st.file_uploader("Reporte Auditorías (.csv ;)", type=["csv"])
    agentes_file = st.file_uploader("Listado de Agentes (.xlsx)", type=["xlsx"])

st.divider()

# ------------------------------------------------------------
# RANGO DE FECHAS
# ------------------------------------------------------------
st.header("📅 Seleccionar Rango de Fechas")

colf1, colf2 = st.columns(2)
date_from = colf1.date_input("Desde:")
date_to = colf2.date_input("Hasta:")

if date_from > date_to:
    st.error("❌ La fecha inicial no puede ser mayor que la final.")
    st.stop()

st.divider()
# ------------------------------------------------------------
# BOTÓN DE PROCESAR
# ------------------------------------------------------------
if st.button("🔄 Procesar Reportes"):

    if not ventas_file or not performance_file or not auditorias_file or not agentes_file:
        st.error("❌ Debes cargar los 4 archivos para continuar.")
        st.stop()

    # === LEER VENTAS ===
    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error leyendo Ventas: {e}")
        st.stop()

    # === LEER PERFORMANCE ===
    try:
        df_performance = pd.read_csv(performance_file, sep=",", encoding="utf-8")
    except:
        try:
            df_performance = pd.read_csv(performance_file, sep=",", encoding="latin-1")
        except Exception as e:
            st.error(f"❌ Error leyendo Performance: {e}")
            st.stop()

    # === LEER AUDITORÍAS ===
    try:
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(
            auditorias_file, sep=";", encoding="utf-8-sig", engine="python"
        )
    except Exception as e:
        st.error(f"❌ Error leyendo Auditorías: {e}")
        st.stop()

    # === LEER AGENTES ===
    try:
        df_agentes = pd.read_excel(agentes_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error leyendo Listado de Agentes: {e}")
        st.stop()

    # =====================================================
    # PROCESAR TODO
    # =====================================================
    try:
        resultados = procesar_reportes(
            df_ventas,
            df_performance,
            df_auditorias,
            df_agentes,
            date_from,
            date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    df_diario = resultados["diario"]
    df_semanal = resultados["semanal"]
    df_resumen = resultados["resumen"]

    st.success("✔ Reportes procesados correctamente.")
    # ----------------------------------------------------
    # MOSTRAR RESULTADOS
    # ----------------------------------------------------
    st.header("📅 Reporte Diario")
    st.dataframe(df_diario, use_container_width=True)

    st.header("📆 Reporte Semanal (Agentes)")
    st.dataframe(df_semanal, use_container_width=True)

    st.header("⭐ Resumen Total del Periodo (Incluye Supervisores + Agentes)")
    st.dataframe(df_resumen, use_container_width=True)

    # ----------------------------------------------------
    # DESCARGA
    # ----------------------------------------------------
    st.header("📥 Descargar Excel Consolidado")

    def to_excel(diario, semanal, resumen):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")

        # Escribir hojas
        diario.to_excel(writer, sheet_name="Diario", index=False)
        semanal.to_excel(writer, sheet_name="Semanal", index=False)
        resumen.to_excel(writer, sheet_name="Resumen", index=False)

        # ==========================================================
        # FORMATO: Colorear filas TOTAL SUPERVISOR con #D6CCF8
        # ==========================================================
        workbook = writer.book
        ws = writer.sheets["Resumen"]

        format_super = workbook.add_format({
            "bg_color": "#D6CCF8",
            "bold": True
        })

        # Buscar filas con "TOTAL SUPERVISOR"
        for row_idx in range(1, len(resumen) + 1):
            if str(resumen.loc[row_idx - 1, "Tipo Registro"]).upper() == "TOTAL SUPERVISOR":
                ws.set_row(row_idx, cell_format=format_super)

        writer.close()
        return output.getvalue()

    excel_bytes = to_excel(df_diario, df_semanal, df_resumen)

    st.download_button(
        "⬇ Descargar Excel Consolidado",
        data=excel_bytes,
        file_name="CMI_Aeropuerto_Consolidado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Sube los archivos, selecciona rango de fechas y presiona **Procesar Reportes**.")



