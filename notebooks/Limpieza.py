import os
import glob
import pandas as pd
import numpy as np

# =============================
# CONFIGURACIÓN GENERAL
# =============================

# Directorio actual (notebooks/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Rutas reales de tu estructura
DATA_PATH = os.path.abspath(os.path.join(BASE_DIR, '..', 'data'))
SEC_PATH = os.path.join(BASE_DIR,'data')          # CSVs
EDGAR_PATH = os.path.join(BASE_DIR, 'sec-edgar-filings')   # TXT de reportes
OUTPUT_TEXT = os.path.join(DATA_PATH, 'reports_text')      # Salida

os.makedirs(OUTPUT_TEXT, exist_ok=True)


# =============================
# FUNCIONES AUXILIARES
# =============================

def detectar_columna_fecha(df):
    posibles = ["date", "fecha", "timestamp", "time", "period"]
    for col in df.columns:
        if col.lower() in posibles:
            return col
    return None


def limpiar_datos(df):
    """Limpia y normaliza el DataFrame sin eliminar datos válidos."""
    # 1️⃣ Detectar si existe columna de fecha o si está en el índice
    col_fecha = detectar_columna_fecha(df)
    if col_fecha is None:
        # Si no hay columna de fecha, intentar usar el índice
        df = df.reset_index()
        if "index" in df.columns:
            df.rename(columns={"index": "fecha"}, inplace=True)
            col_fecha = "fecha"

    # 2️⃣ Convertir la columna de fecha
    if col_fecha in df.columns:
        df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")
        df = df.dropna(subset=[col_fecha])
        df = df.sort_values(col_fecha)

    # 3️⃣ Convertir solo las columnas numéricas, sin borrar el resto
    for col in df.columns:
        if col != col_fecha:
            # Si la columna tiene muchos números, la convertimos
            if df[col].astype(str).str.replace('.', '', 1).str.isnumeric().sum() > len(df) * 0.5:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4️⃣ Rellenar valores faltantes de forma segura
    df = df.ffill().bfill()

    # 5️⃣ Eliminar duplicados por fecha
    if col_fecha in df.columns:
        df = df.drop_duplicates(subset=[col_fecha])

    return df



def eliminar_outliers(df):
    num_cols = df.select_dtypes(include=[np.number]).columns
    if len(num_cols) == 0:
        print("⚠️ No hay columnas numéricas para eliminar outliers.")
        return df

    for col in num_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        filtro = (df[col] >= (Q1 - 1.5 * IQR)) & (df[col] <= (Q3 + 1.5 * IQR))
        df = df.loc[filtro]
    return df


def estandarizar_columnas(df):
    columnas_nuevas = {}
    for col in df.columns:
        nombre = col.strip().lower()
        if "open" in nombre:
            columnas_nuevas[col] = "apertura"
        elif "high" in nombre:
            columnas_nuevas[col] = "maximo"
        elif "low" in nombre:
            columnas_nuevas[col] = "minimo"
        elif "close" in nombre:
            columnas_nuevas[col] = "cierre"
        elif "adj" in nombre:
            columnas_nuevas[col] = "cierre_ajustado"
        elif "vol" in nombre:
            columnas_nuevas[col] = "volumen"
        elif any(x in nombre for x in ["date", "fecha", "time", "period"]):
            columnas_nuevas[col] = "fecha"
    return df.rename(columns=columnas_nuevas)


# =============================
# PROCESAMIENTO DE PRECIOS
# =============================
def procesar_precios():
    print("📊 Iniciando limpieza de precios históricos...\n")
    archivos = glob.glob(os.path.join(SEC_PATH, "*_prices.csv"))

    if not archivos:
        print(f"⚠️ No se encontraron archivos *_prices.csv en:\n   {SEC_PATH}\n")
        return

    for archivo in archivos:
        nombre = os.path.basename(archivo)
        df = pd.read_csv(archivo)

        df = limpiar_datos(df)
        df = eliminar_outliers(df)
        df = estandarizar_columnas(df)

        df.to_csv(archivo, index=False)
        print(f"✅ {nombre} limpiado y estandarizado correctamente.")

    dfs = [pd.read_csv(a) for a in archivos]
    df_total = pd.concat(dfs, ignore_index=True)
    df_total.to_csv(os.path.join(DATA_PATH, "precios_limpios.csv"), index=False)
    print("\n💾 Dataset unificado 'precios_limpios.csv' generado correctamente.\n")


# =============================
# LIMPIEZA DE REPORTES TXT
# =============================
def limpiar_reportes_txt():
    print("🧾 Iniciando limpieza de reportes 10-K (archivos .txt)...\n")

    archivos_txt = glob.glob(os.path.join(EDGAR_PATH, "**", "full-submission.txt"), recursive=True)

    if not archivos_txt:
        print(f"⚠️ No se encontraron archivos full-submission.txt en:\n   {EDGAR_PATH}\n")
        return

    for archivo in archivos_txt:
        try:
            with open(archivo, "r", encoding="utf-8", errors="ignore") as f:
                texto = f.read()

            texto_limpio = " ".join(texto.replace("\r", " ").replace("\n", " ").replace("\t", " ").split())

            empresa = os.path.basename(os.path.dirname(os.path.dirname(archivo)))
            reporte = os.path.basename(os.path.dirname(archivo))
            nombre_final = f"{empresa}_{reporte}_10K.txt"

            destino = os.path.join(OUTPUT_TEXT, nombre_final)
            with open(destino, "w", encoding="utf-8") as out:
                out.write(texto_limpio)

            print(f"🧩 {nombre_final} limpiado y guardado en /reports_text")

        except Exception as e:
            print(f"❌ Error procesando {archivo}: {e}")

    print("\n✅ Todos los reportes .txt fueron limpiados y almacenados correctamente.\n")


# =============================
# MAIN
# =============================
def main():
    print("🚀 Iniciando FASE 1.2 – Limpieza y Estandarización de Datos SIPAF...\n")
    procesar_precios()
    limpiar_reportes_txt()
    print("🎯 Proceso completado con éxito.")
    print("📂 Archivos listos en:")
    print(f"   → {SEC_PATH}")
    print(f"   → {OUTPUT_TEXT}\n")


if __name__ == "__main__":
    main()
