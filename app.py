import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_reportes


# ---------------------------------------------------------
# CONFIGURACIÓN GENERAL
# ---------------------------------------------------------
st.set_page_config(
    page_title="CMI Aeropuerto - Reportes",
    layout="wide"
)

st.title("📊 Consolidado CMI Aeropuerto - Reportes")


# ---------------------------------------------------------
# FUNCIÓN PARA CARGAR ARCHIVOS (VERSIÓN QUE SÍ FUNCIONABA)
# ---------------------------------------------------------
def cargar_archivo(f):
    if f is None:
        return None

    try:
        nombre = f.name.lower()

        # Excel
        if nombre.endswith(".xlsx") or nombre.endswith(".xls"):
            return pd.read_excel(f)

        # CSV — intentar coma primero
        if nombre.endswith(".csv"):
            f.seek(0)
            try:
                return pd.read_csv(f, sep=",", encoding="utf-8-sig")
            except:
                pass

            # CSV — intentar punto y coma
            f.seek(0)
            try:
                return pd.read_csv(f, sep=";", encoding="utf-8-sig")
            except:
                pass

            # Fallback: autodetección
            f.seek(0)
            try:
                return pd.read_csv(f, sep=None, engine="python", encoding="utf-8-sig")
            except Exception as e:
                st.error(f"No se pudo leer el archivo CSV: {e}")
                return None

        st.error("Formato no soportado (usa CSV o XLSX).")
        return None

    except Exception as e:
        st.error(f"Error cargando archivo {f.name}: {e}")
        return None


# ---------------------------------------------------------
# FUNCIÓN PARA GENERAR EXCEL CON FORMATO (2 DECIMALES)
# ---------------------------------------------------------
def generar_excel(resultados):

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Exportar hojas
        resultados["diario"].to_excel(writer, sheet_name="Diario", index=False)
        resultados["semanal"].to_excel(writer, sheet_name="Semanal", index=False)
        resultados["resumen"].to_excel(writer, sheet_name="Resumen", index=False)

        workbook = writer.book

        fmt_decimal = workbook.add_format({"num_format": "0.00"})  # 2 decimales
        fmt_int = workbook.add_format({"num_format": "0"})         # enteros

        columnas_decimales = [
            "CSAT", "NPS", "FIRT", "%FIRT",
            "FURT", "%FURT", "Nota_Auditorias"
        ]

        for sheet_name in ["Diario", "Semanal", "Resumen"]:
            ws = writer.sheets[sheet_name]
            df = resultados[sheet_name.lower()]

            for col_idx, col_name in enumerate(df.columns):

                # Indicadores con 2 decimales
                if col_name in columnas_decimales:
                    ws.set_column(col_idx, col_idx, 12, fmt_decimal)

                # Ventas y contadores como enteros
                elif col_name.startswith("Q_") or col_name.startswith("Ventas_"):
                    ws.set_column(col_idx, col_idx, 10, fmt_int)

                # Otras columnas sin formato especial
                else:
                    ws.set_column(col_idx, col_idx, 16)

    return output.getvalue()



# ---------------------------------------------------------
# ENTRADA DE FECHAS
# ---------------------------------------------------------
st.header("📅 Seleccionar rango de fechas")

col1, col2 = st.columns(2)
with col1:
    fecha_inicio = st.date_input("Fecha inicio")
with col2:
    fecha_fin = st.date_input("Fecha término")

if fecha_inicio > fecha_fin:
    st.error("⚠️ La fecha de inicio no puede ser mayor que la de término.")
    st.stop()


# ---------------------------------------------------------
# CARGA DE ARCHIVOS
# ---------------------------------------------------------
st.header("📂 Cargar archivos")

ventas_file      = st.file_uploader("Ventas (CSV o Excel)", type=["csv", "xlsx"])
performance_file = st.file_uploader("Performance (CSV o Excel)", type=["csv", "xlsx"])
auditorias_file  = st.file_uploader("Auditorías (CSV o Excel)", type=["csv", "xlsx"])
agentes_file     = st.file_uploader("Agentes (CSV o Excel)", type=["csv", "xlsx"])

df_ventas      = cargar_archivo(ventas_file)
df_performance = cargar_archivo(performance_file)
df_auditorias  = cargar_archivo(auditorias_file)
df_agentes     = cargar_archivo(agentes_file)


# ---------------------------------------------------------
# BOTÓN PARA PROCESAR
# ---------------------------------------------------------
st.header("⚙️ Generar Reportes")

if st.button("Procesar"):

    if df_ventas is None or df_performance is None or df_auditorias is None or df_agentes is None:
        st.error("⚠️ Debes cargar todos los archivos para continuar.")
        st.stop()

    try:
        resultados = procesar_reportes(
            df_ventas,
            df_performance,
            df_auditorias,
            df_agentes,
            fecha_inicio,
            fecha_fin
        )

        st.success("✅ Reportes generados correctamente.")

        # Mostrar tablas
        st.subheader("📅 Reporte Diario")
        st.dataframe(resultados["diario"], use_container_width=True)

        st.subheader("🗓️ Reporte Semanal")
        st.dataframe(resultados["semanal"], use_container_width=True)

        st.subheader("📊 Resumen por Supervisor")
        st.dataframe(resultados["resumen"], use_container_width=True)

        # Descargar Excel
        excel_bytes = generar_excel(resultados)

        st.download_button(
            label="⬇️ Descargar Excel (3 hojas)",
            data=excel_bytes,
            file_name="CMI_Aeropuerto_Reporte.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"❌ Error al procesar: {e}")

