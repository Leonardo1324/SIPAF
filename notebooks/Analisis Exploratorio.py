import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

import nltk
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('wordnet')
nltk.download('stopwords')

# ============================
# CONFIGURACIÓN DE RUTAS
# ============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.abspath(os.path.join(BASE_DIR, '..', 'data'))
PRICES_PATH = os.path.join(DATA_PATH, 'precios_limpios.csv')
REPORTS_PATH = os.path.join(DATA_PATH, 'reports_text')

# ============================
# 1️⃣ ANÁLISIS DE PRECIOS
# ============================

def analizar_precios():
    print("📊 Analizando precios...")

    df = pd.read_csv(PRICES_PATH)
    df['fecha'] = pd.to_datetime(df['fecha'])
    df = df.sort_values('fecha')

    # Cálculo de retornos y volatilidad
    df['retorno'] = df['cierre'].pct_change()
    df['volatilidad'] = df['retorno'].rolling(window=20).std()

    # Indicadores técnicos
    df['SMA_20'] = df['cierre'].rolling(window=20).mean()
    df['SMA_50'] = df['cierre'].rolling(window=50).mean()
    df['RSI'] = calcular_rsi(df['cierre'])
    df['MACD'], df['Signal'] = calcular_macd(df['cierre'])

    # Visualización
    plt.figure(figsize=(10,5))
    plt.plot(df['fecha'], df['cierre'], label='Cierre')
    plt.plot(df['fecha'], df['SMA_20'], label='SMA 20')
    plt.plot(df['fecha'], df['SMA_50'], label='SMA 50')
    plt.title('Evolución del Precio y Medias Móviles')
    plt.legend()
    plt.tight_layout()
    plt.show()

    return df

def calcular_rsi(series, window=14):
    delta = series.diff()
    ganancia = (delta.where(delta > 0, 0)).rolling(window).mean()
    perdida = (-delta.where(delta < 0, 0)).rolling(window).mean()
    rs = ganancia / perdida
    return 100 - (100 / (1 + rs))

def calcular_macd(series, short=12, long=26, signal=9):
    exp1 = series.ewm(span=short, adjust=False).mean()
    exp2 = series.ewm(span=long, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    return macd, signal_line

# ============================
# 2️⃣ ANÁLISIS NLP INICIAL
# ============================

def analizar_textos():
    print("\n🧾 Procesando reportes de texto...\n")

    textos = []
    for archivo in os.listdir(REPORTS_PATH):
        if archivo.endswith(".txt"):
            ruta = os.path.join(REPORTS_PATH, archivo)
            with open(ruta, "r", encoding="utf-8") as f:
                texto = f.read()
                textos.append({"empresa": archivo.split("_")[0], "texto": texto})

    df_textos = pd.DataFrame(textos)

    # Tokenización + Lematización + Limpieza
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()

    def limpiar_texto(texto):
        tokens = word_tokenize(texto.lower())
        tokens = [lemmatizer.lemmatize(t) for t in tokens if t.isalpha() and t not in stop_words]
        return " ".join(tokens)

    df_textos['texto_limpio'] = df_textos['texto'].apply(limpiar_texto)

    # Sentimiento con TextBlob
    df_textos['polaridad'] = df_textos['texto_limpio'].apply(lambda t: TextBlob(t).sentiment.polarity)
    df_textos['subjetividad'] = df_textos['texto_limpio'].apply(lambda t: TextBlob(t).sentiment.subjectivity)

    # TF-IDF
    vectorizer = TfidfVectorizer(max_features=100)
    tfidf_matrix = vectorizer.fit_transform(df_textos['texto_limpio'])
    tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=vectorizer.get_feature_names_out())

    # Guardar features textuales
    salida = os.path.join(DATA_PATH, "features_textuales.csv")
    df_final = pd.concat([df_textos[['empresa', 'polaridad', 'subjetividad']], tfidf_df], axis=1)
    df_final.to_csv(salida, index=False)

    print(f"✅ Features textuales generadas en:\n   {salida}")
    return df_final


# ============================
# MAIN
# ============================

def main():
    print("🚀 Iniciando FASE 1.3 – Análisis Exploratorio y NLP Inicial...\n")
    df_precios = analizar_precios()
    df_textos = analizar_textos()
    print("\n🎯 Fase completada con éxito.")
    print("📂 Archivos listos en:")
    print(f"   → {PRICES_PATH}")
    print(f"   → {REPORTS_PATH}")

if __name__ == "__main__":
    main()
