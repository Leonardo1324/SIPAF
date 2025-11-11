import os
import glob
import sqlite3
import pandas as pd
import yfinance as yf
from sec_edgar_downloader import Downloader

# ==========================================================
# 🧱 CONFIGURACIÓN INICIAL
# ==========================================================

# Crear carpeta 'data' si no existe
os.makedirs("data/sec_filings", exist_ok=True)

# Lista de tickers a descargar
TICKERS = ["AAPL", "MSFT", "GOOGL"]

# Rango de fechas
START_DATE = "2018-01-01"
END_DATE = "2025-01-01"

# ==========================================================
# 📈 DESCARGA DE PRECIOS HISTÓRICOS
# ==========================================================
def descargar_precios():
    print("📥 Descargando precios históricos...\n")
    for ticker in TICKERS:
        df = yf.download(ticker, start=START_DATE, end=END_DATE)
        df["Ticker"] = ticker
        ruta_csv = f"data/{ticker}_prices.csv"
        df.to_csv(ruta_csv)
        print(f"✅ {ticker}: datos guardados en {ruta_csv}")
    print("\n✅ Todos los precios descargados correctamente.\n")


# ==========================================================
# 📰 DESCARGA DE REPORTES 10-K DESDE SEC EDGAR
# ==========================================================
def descargar_reportes():
    print("📥 Descargando reportes 10-K desde SEC EDGAR...\n")
    dl = Downloader("data/sec_filings", "leonardo.sipaf@gmail.com")

    for ticker in TICKERS:
        try:
            # Intentar con versiones nuevas
            dl.get("10-K", ticker, num_filings_to_download=3)
        except TypeError:
            # Si falla, intentar con versiones antiguas
            dl.get("10-K", ticker)
        print(f"✅ {ticker}: reportes 10-K descargados.")

    print("\n✅ Todos los reportes descargados exitosamente.\n")


# ==========================================================
# 💾 GUARDADO EN BASE DE DATOS SQLITE
# ==========================================================
def guardar_en_sqlite():
    print("💾 Guardando datos en SQLite...\n")
    conn = sqlite3.connect("data/sipaf.db")

    for csv_file in glob.glob("data/*_prices.csv"):
        df = pd.read_csv(csv_file)
        nombre_tabla = os.path.basename(csv_file).replace("_prices.csv", "")
        df.to_sql(nombre_tabla, conn, if_exists="replace", index=False)
        print(f"✅ {nombre_tabla}: tabla creada en la base de datos.")

    conn.close()
    print("\n✅ Todos los datos guardados en 'data/sipaf.db'.\n")


# ==========================================================
# 🚀 FUNCIÓN PRINCIPAL
# ==========================================================
def main():
    print("🚀 Iniciando proceso de adquisición de datos SIPAF...\n")

    descargar_precios()
    descargar_reportes()
    guardar_en_sqlite()

    print("🎯 Proceso finalizado correctamente.")


# ==========================================================
# 🏁 EJECUCIÓN DEL MAIN
# ==========================================================
if __name__ == "__main__":
    main()
