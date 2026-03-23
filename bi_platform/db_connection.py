import pandas as pd
import psycopg2
from psycopg2 import pool
import streamlit as st

# ============================================================
# Configuration de la connexion PostgreSQL
# ============================================================
# Modifiez ces paramètres selon votre environnement

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "dataflow360",
    "user":     "postgres",
    "password": "nnbbvv"
    ""
}

# Pool de connexions (1 à 5 connexions simultanées)
@st.cache_resource
def get_connection_pool():
    return psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        **DB_CONFIG
    )


def run_query(sql: str, params=None) -> pd.DataFrame:
    """
    Exécute une requête SQL et retourne un DataFrame pandas.
    Gère automatiquement les connexions via le pool.
    """
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    except Exception as e:
        st.error(f"❌ Erreur de requête SQL : {e}")
        return pd.DataFrame()
    finally:
        pool.putconn(conn)


def run_query_single(sql: str, params=None):
    """
    Exécute une requête et retourne une seule valeur scalaire.
    """
    df = run_query(sql, params)
    if df.empty:
        return 0
    return df.iloc[0, 0]


def test_connection() -> bool:
    """
    Teste la connexion à la base de données.
    Retourne True si la connexion est établie.
    """
    try:
        pool = get_connection_pool()
        conn = pool.getconn()
        pool.putconn(conn)
        return True
    except Exception as e:
        st.error(f"❌ Connexion impossible : {e}")
        return False
