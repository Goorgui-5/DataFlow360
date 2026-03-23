import streamlit as st
import pandas as pd
from db_connection import run_query
from components.charts import barres_vdn_par_mois
from components.filters import render_filters


def page_stats_derniers_matchs():

    st.header(" Statistiques des Derniers Matchs")
    st.divider()

    filters   = render_filters(show_mois=False, show_equipe=True, show_competition=True)
    id_equipe = filters["id_equipe"]
    nom_equipe = filters["nom_equipe"]
    id_comp   = filters["id_competition"]
    nom_comp  = filters["nom_competition"]

    if not id_equipe:
        st.info(" Sélectionnez une équipe.")
        return

    # ---- Clauses WHERE ----
    clauses = ["(f.id_equipe_dom = %(id_eq)s OR f.id_equipe_ext = %(id_eq)s)"]
    params  = {"id_eq": id_equipe}

    if id_comp:
        clauses.append("f.id_competition = %(id_comp)s")
        params["id_comp"] = id_comp

    where = "WHERE " + " AND ".join(clauses)

    # ---- Données V/D/N par mois ----
    df_vdn = run_query(f"""
        SELECT
            d.mois,
            d.annee,
            SUM(CASE
                WHEN (f.id_equipe_dom = %(id_eq)s AND f.score_dom > f.score_ext)
                  OR (f.id_equipe_ext = %(id_eq)s AND f.score_ext > f.score_dom)
                THEN 1 ELSE 0 END) AS victoires,
            SUM(CASE
                WHEN (f.id_equipe_dom = %(id_eq)s AND f.score_dom < f.score_ext)
                  OR (f.id_equipe_ext = %(id_eq)s AND f.score_ext < f.score_dom)
                THEN 1 ELSE 0 END) AS defaites,
            SUM(CASE WHEN f.score_dom = f.score_ext THEN 1 ELSE 0 END) AS nuls
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
        GROUP BY d.annee, d.mois
        ORDER BY d.annee, d.mois
    """, params)

    # ---- Totaux globaux ----
    total_v = int(df_vdn["victoires"].sum()) if not df_vdn.empty else 0
    total_d = int(df_vdn["defaites"].sum())  if not df_vdn.empty else 0
    total_n = int(df_vdn["nuls"].sum())      if not df_vdn.empty else 0
    total_m = total_v + total_d + total_n

    # ---- KPIs ----
    st.subheader(f" Bilan global — {nom_equipe}")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(" Matchs joués", total_m)
    col2.metric(" Victoires",     total_v)
    col3.metric(" Défaites",      total_d)
    col4.metric(" Nuls",          total_n)

    st.divider()

    # ---- Graphique V/D/N par mois ----
    st.subheader(" Victoires / Défaites / Nuls par mois")

    if not df_vdn.empty:
        fig = barres_vdn_par_mois(df_vdn, nom_equipe)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(" Aucune donnée disponible.")

    st.divider()

    # ---- Comparaison : domicile vs extérieur ----
    st.subheader(" Comparaison Domicile vs Extérieur")

    df_dom_ext = run_query(f"""
        SELECT
            'Domicile' AS lieu,
            SUM(CASE WHEN f.score_dom > f.score_ext THEN 1 ELSE 0 END) AS victoires,
            SUM(CASE WHEN f.score_dom < f.score_ext THEN 1 ELSE 0 END) AS defaites,
            SUM(CASE WHEN f.score_dom = f.score_ext THEN 1 ELSE 0 END) AS nuls,
            COUNT(*) AS matchs
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        WHERE f.id_equipe_dom = %(id_eq)s
          {"AND f.id_competition = %(id_comp)s" if id_comp else ""}

        UNION ALL

        SELECT
            'Extérieur' AS lieu,
            SUM(CASE WHEN f.score_ext > f.score_dom THEN 1 ELSE 0 END) AS victoires,
            SUM(CASE WHEN f.score_ext < f.score_dom THEN 1 ELSE 0 END) AS defaites,
            SUM(CASE WHEN f.score_dom = f.score_ext THEN 1 ELSE 0 END) AS nuls,
            COUNT(*) AS matchs
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        WHERE f.id_equipe_ext = %(id_eq)s
          {"AND f.id_competition = %(id_comp)s" if id_comp else ""}
    """, params)

    if not df_dom_ext.empty:
        st.dataframe(df_dom_ext, use_container_width=True, hide_index=True)
