import streamlit as st
from db_connection import run_query
from components.kpi_cards import render_kpi_row
from components.charts import double_courbe_equipe
from components.filters import render_filters


def page_performance_equipe():

    st.header(" Performance par Équipe")
    st.divider()

    filters   = render_filters(show_mois=True, show_equipe=True, show_competition=True)
    id_equipe = filters["id_equipe"]
    nom_equipe = filters["nom_equipe"]
    id_comp   = filters["id_competition"]
    nom_comp  = filters["nom_competition"]
    mois      = filters["mois"]
    annee     = filters["annee"]

    if not id_equipe:
        st.info(" Sélectionnez une équipe pour afficher ses statistiques.")
        return

    # ---- Clauses WHERE ----
    clauses = [
        "(f.id_equipe_dom = %(id_eq)s OR f.id_equipe_ext = %(id_eq)s)"
    ]
    params = {"id_eq": id_equipe}

    if id_comp:
        clauses.append("f.id_competition = %(id_comp)s")
        params["id_comp"] = id_comp

    if mois:
        clauses.append("d.mois = %(mois)s AND d.annee = %(annee)s")
        params["mois"]  = mois
        params["annee"] = annee

    where = "WHERE " + " AND ".join(clauses)

    # ---- KPI : Total Matchs ----
    df_matchs = run_query(f"""
        SELECT COUNT(*) AS total
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
    """, params)
    total_matchs = int(df_matchs.iloc[0]["total"])

    # ---- KPI : Buts marqués ----
    df_marques = run_query(f"""
        SELECT COALESCE(SUM(
            CASE WHEN f.id_equipe_dom = %(id_eq)s THEN f.score_dom
                 WHEN f.id_equipe_ext = %(id_eq)s THEN f.score_ext
                 ELSE 0 END
        ), 0) AS buts_marques
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
    """, params)
    buts_marques = int(df_marques.iloc[0]["buts_marques"])

    # ---- KPI : Buts encaissés ----
    df_encaisses = run_query(f"""
        SELECT COALESCE(SUM(
            CASE WHEN f.id_equipe_dom = %(id_eq)s THEN f.score_ext
                 WHEN f.id_equipe_ext = %(id_eq)s THEN f.score_dom
                 ELSE 0 END
        ), 0) AS buts_encaisses
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
    """, params)
    buts_encaisses = int(df_encaisses.iloc[0]["buts_encaisses"])

    # ---- KPI : Moyenne Buts/Match ----
    moyenne = round(buts_marques / total_matchs, 2) if total_matchs > 0 else 0

    # ---- KPI : Classement de l'équipe dans la compétition ----
    position  = "—"
    nom_comp_affiche = "—"

    if id_comp:
        df_classement = run_query("""
            SELECT equipe, rang
            FROM dw.v_kpi_classement_equipes
            WHERE id_equipe = %(id_eq)s AND id_competition = %(id_comp)s
            LIMIT 1
        """, {"id_eq": id_equipe, "id_comp": id_comp})

        if not df_classement.empty and "rang" in df_classement.columns:
            position = f"#{int(df_classement.iloc[0]['rang'])}"
        nom_comp_affiche = nom_comp

    # ---- Affichage KPIs ----
    st.subheader(f" Statistiques — {nom_equipe}")

    render_kpi_row([
        {"label": "Matchs joués",       "value": total_matchs,     },
        {"label": "Buts marqués",       "value": buts_marques,     },
        {"label": "Buts encaissés",     "value": buts_encaisses,   },
        {"label": "Moyenne Buts/Match", "value": moyenne,          },
        {"label": "Compétition",        "value": nom_comp, },
    ])

    st.divider()

    # ---- Graphique double courbe ----
    st.subheader(" Évolution des buts marqués et encaissés")

    df_graph = run_query(f"""
        SELECT
            d.mois,
            d.annee,
            SUM(CASE WHEN f.id_equipe_dom = %(id_eq)s THEN f.score_dom
                     WHEN f.id_equipe_ext = %(id_eq)s THEN f.score_ext
                     ELSE 0 END) AS buts_marques,
            SUM(CASE WHEN f.id_equipe_dom = %(id_eq)s THEN f.score_ext
                     WHEN f.id_equipe_ext = %(id_eq)s THEN f.score_dom
                     ELSE 0 END) AS buts_encaisses
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
        GROUP BY d.annee, d.mois
        ORDER BY d.annee, d.mois
    """, params)

    if not df_graph.empty:
        fig = double_courbe_equipe(df_graph, nom_equipe)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ Aucune donnée pour cette sélection.")
