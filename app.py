import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_reportes

st.set_page_config(page_title="CMI Aeropuerto - Reportes", layout="wide")

# ---------------------------------------------------------
# Función para descargar Excel con 3 hojas
# ---------------------------------------------------------
def generar_excel(resultados):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        resultados["diario"].to_excel(writer, sheet_name="Diario", index=False)
        resultados["semanal"].to_excel(writer, sheet_name="Semanal", index=False)
        resultados["resumen"].to_excel(writer, sheet_name="Resumen", index=False)
    return output.getvalue()

# ---------------------------------------------------------
# Título
# ---------------------------------------------------------
st.title("📊 Consolidado CMI Aeropuerto - Reportes")

st.markdown("""
Carga los archivos de **ventas**, **performance**, **auditorías** y **agentes**, 
junto con el rango de fechas.  
El sistema generará:
- Reporte **Diario**
- Reporte **Semanal**
- Reporte **Resumen (Supervisores → Agentes)**
""")

# ---------------------------------------------------------
# Entrada de fechas
# ---------------------------------------------------------
col1, col2 = st.columns(2)
with col1:
    fecha_inicio = st.date_input("📅 Fecha inicio")
with col2:
    fecha_fin = st.date_input("📅 Fecha fin")

if fecha_inicio > fecha_fin:
    st.error("La fecha de inicio no puede ser mayor que la fecha de fin.")
    st.stop()

# ---------------------------------------------------------
# Carga de archivos
# ---------------------------------------------------------
st.header("📂 Cargar archivos")

ventas_file      = st.file_uploader("Ventas (CSV o Excel)", type=["csv", "xlsx"])
performance_file = st.file_uploader("Performance (CSV o Excel)", type=["csv", "xlsx"])
auditorias_file  = st.file_uploader("Auditorías (CSV o Excel)", type=["csv", "xlsx"])
agentes_file     = st.file_uploader("Agentes (CSV o Excel)", type=["csv", "xlsx"])

def cargar_archivo(f):
    if f is None:
        return None
    try:
        if f.name.endswith(".csv"):
            return pd.read_csv(f, encoding="utf-8-sig")
        else:
            return pd.read_excel(f)
    except Exception as e:
        st.error(f"Error cargando archivo {f.name}: {e}")
        return None

# Cargar los archivos
df_ventas      = cargar_archivo(ventas_file)
df_performance = cargar_archivo(performance_file)
df_auditorias  = cargar_archivo(auditorias_file)
df_agentes     = cargar_archivo(agentes_file)

# ---------------------------------------------------------
# Procesamiento
# ---------------------------------------------------------
st.header("⚙️ Procesar datos")

if st.button("Generar Reportes"):

    if df_ventas is None or df_performance is None or df_auditorias is None or df_agentes is None:
        st.error("⚠️ Debes cargar TODOS los archivos antes de procesar.")
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

        st.subheader("📊 Resumen (Supervisor → Agentes)")
        st.dataframe(resultados["resumen"], use_container_width=True)

        # Botón de descarga
        excel_data = generar_excel(resultados)
        st.download_button(
            label="⬇️ Descargar Excel con 3 Hojas",
            data=excel_data,
            file_name="CMI_Aeropuerto_Reporte.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")


