import streamlit as st
from db_connection import test_connection
from views.model_prediction import show as show_model

# ============================================================
# Imports des pages
# ============================================================

from views.performance_globale      import page_performance_globale
from views.performance_competition  import page_performance_competition
from views.performance_equipe       import page_performance_equipe
from views.resultats_historiques    import page_resultats_historiques
from views.stats_derniers_matchs    import page_stats_derniers_matchs
from views.chatbot                  import page_chatbot

# ============================================================
# Configuration de la page
# ============================================================

st.set_page_config(
    page_title="FootStream",
    # page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CSS personnalisé
# ============================================================

st.markdown("""
<style>
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #0f1117;
    }
    [data-testid="stSidebar"] * {
        color: #ffffff !important;
    }

    /* Métriques KPI */
    [data-testid="stMetric"] {
        background-color: #1e2130;
        border-radius: 10px;
        padding: 15px;
        border-left: 4px solid #1f77b4;
    }
    [data-testid="stMetricLabel"] {
        font-size: 13px !important;
        color: #aaaaaa !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 26px !important;
        font-weight: bold !important;
        color: #ffffff !important;
    }

    /* Titre principal */
    h1 { color: #1f77b4; }
    h2 { color: #e0e0e0; }
    h3 { color: #cccccc; }

    /* Boutons */
    .stButton > button {
        border-radius: 6px;
        border: 1px solid #1f77b4;
        color: #1f77b4;
    }
    .stButton > button:hover {
        background-color: #1f77b4;
        color: white;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# Sidebar — Logo & Navigation
# ============================================================

with st.sidebar:
    st.markdown("## ⚽ FootStream")
    st.markdown("**Saison 2025–2026**")
    st.markdown("---")

    page = st.radio(
        "Navigation",
        options=[
            " Performance Globale",
            " Performance par Compétition",
            " Performance par Équipe",
            " Résultats Historiques",
            " Stats Derniers Matchs",
            " Chatbot BI",
            " Prédiction de Match",

        ],
        label_visibility="collapsed",
    )

    st.markdown("---")

    # # Statut de la connexion DB
    # if test_connection():
    #     st.success("✅ Base de données connectée")
    # else:
    #     st.error("❌ Base de données déconnectée")

# ============================================================
# Routing des pages
# ============================================================

if   page == " Performance Globale":
    page_performance_globale()

elif page == " Performance par Compétition":
    page_performance_competition()

elif page == " Performance par Équipe":
    page_performance_equipe()

elif page == " Résultats Historiques":
    page_resultats_historiques()

elif page == " Stats Derniers Matchs":
    page_stats_derniers_matchs()

elif page == " Chatbot BI":
    page_chatbot()

elif page == " Prédiction de Match":
    show_model()
