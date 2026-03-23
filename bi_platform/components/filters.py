import streamlit as st
from db_connection import run_query


# ============================================================
# Chargement des listes pour les filtres
# ============================================================

@st.cache_data(ttl=300)
def get_liste_mois():
    df = run_query("""
        SELECT DISTINCT d.mois, d.annee
        FROM dw.dim_date d
        INNER JOIN dw.fait_match f ON f.id_date = d.id_date
        ORDER BY d.annee, d.mois
    """)
    return df


@st.cache_data(ttl=300)
def get_liste_equipes():
    df = run_query("""
        SELECT id_equipe, nom_equipe
        FROM dw.dim_equipe
        ORDER BY nom_equipe
    """)
    return df


@st.cache_data(ttl=300)
def get_liste_competitions():
    df = run_query("""
        SELECT id_competition, nom_competition
        FROM dw.dim_competition
        ORDER BY nom_competition
    """)
    return df


# ============================================================
# Rendu du sidebar avec filtres
# ============================================================

def render_filters(show_mois=True, show_equipe=True, show_competition=True):
    """
    Affiche les filtres dans la sidebar selon les besoins de chaque page.
    Retourne un dict avec les valeurs sélectionnées.
    """

    st.sidebar.markdown("---")
    st.sidebar.header("SELECTION")

    filters = {
        "mois": None,
        "annee": None,
        "id_equipe": None,
        "nom_equipe": "Toutes",
        "id_competition": None,
        "nom_competition": "Toutes",
    }

    # ---- Filtre Mois ----
    if show_mois:
        df_mois = get_liste_mois()
        options_mois = ["Tous"] + [
            f"{int(row['mois']):02d}/{int(row['annee'])}"
            for _, row in df_mois.iterrows()
        ]
        mois_sel = st.sidebar.selectbox(" Mois", options_mois)

        if mois_sel != "Tous":
            mois_val, annee_val = mois_sel.split("/")
            filters["mois"] = int(mois_val)
            filters["annee"] = int(annee_val)

    # ---- Filtre Équipe ----
    if show_equipe:
        df_equipes = get_liste_equipes()
        noms = ["Toutes"] + df_equipes["nom_equipe"].tolist()
        equipe_sel = st.sidebar.selectbox(" Équipe", noms)

        if equipe_sel != "Toutes":
            row = df_equipes[df_equipes["nom_equipe"] == equipe_sel].iloc[0]
            filters["id_equipe"] = int(row["id_equipe"])
            filters["nom_equipe"] = equipe_sel

    # ---- Filtre Compétition ----
    if show_competition:
        df_comp = get_liste_competitions()
        noms_comp = ["Toutes"] + df_comp["nom_competition"].tolist()
        comp_sel = st.sidebar.selectbox(" Compétition", noms_comp)

        if comp_sel != "Toutes":
            row = df_comp[df_comp["nom_competition"] == comp_sel].iloc[0]
            filters["id_competition"] = int(row["id_competition"])
            filters["nom_competition"] = comp_sel

    return filters
