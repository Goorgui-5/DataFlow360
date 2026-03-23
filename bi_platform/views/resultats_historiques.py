import streamlit as st
import pandas as pd
from db_connection import run_query
from components.filters import render_filters


def page_resultats_historiques():

    st.header(" Résultats Historiques")
    st.divider()

    filters   = render_filters(show_mois=True, show_equipe=True, show_competition=True)
    id_equipe = filters["id_equipe"]
    nom_equipe = filters["nom_equipe"]
    id_comp   = filters["id_competition"]
    mois      = filters["mois"]
    annee     = filters["annee"]

    if not id_equipe:
        st.info(" Sélectionnez une équipe pour voir ses résultats.")
        return

    # ---- Clauses WHERE ----
    clauses = ["(f.id_equipe_dom = %(id_eq)s OR f.id_equipe_ext = %(id_eq)s)"]
    params  = {"id_eq": id_equipe}

    if id_comp:
        clauses.append("f.id_competition = %(id_comp)s")
        params["id_comp"] = id_comp

    if mois:
        clauses.append("d.mois = %(mois)s AND d.annee = %(annee)s")
        params["mois"]  = mois
        params["annee"] = annee

    where = "WHERE " + " AND ".join(clauses)

    # ---- Requête des matchs ----
    sql = f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY d.date_complete DESC) AS "Rencontre #",
            d.date_complete                                    AS "Date",
            c.nom_competition                                  AS "Compétition",
            e_dom.nom_equipe                                   AS "Domicile",
            f.score_dom                                        AS "Score Dom",
            f.score_ext                                        AS "Score Ext",
            e_ext.nom_equipe                                   AS "Extérieur",
            CASE
                WHEN f.id_equipe_dom = %(id_eq)s AND f.score_dom > f.score_ext THEN '🟢 Victoire'
                WHEN f.id_equipe_ext = %(id_eq)s AND f.score_ext > f.score_dom THEN '🟢 Victoire'
                WHEN f.score_dom = f.score_ext                                 THEN '🟡 Nul'
                ELSE '🔴 Défaite'
            END AS "Résultat"
        FROM dw.fait_match f
        JOIN dw.dim_date        d     ON d.id_date        = f.id_date
        JOIN dw.dim_equipe      e_dom ON e_dom.id_equipe  = f.id_equipe_dom
        JOIN dw.dim_equipe      e_ext ON e_ext.id_equipe  = f.id_equipe_ext
        JOIN dw.dim_competition c     ON c.id_competition = f.id_competition
        {where}
        ORDER BY d.date_complete DESC
    """
    df = run_query(sql, params)

    if df.empty:
        st.warning(f" Aucun match trouvé pour **{nom_equipe}**.")
        return

    # ---- Bandeau 5 derniers matchs ----
    st.subheader(f" 5 derniers matchs — {nom_equipe}")

    derniers = df.head(5)
    badges = []
    for _, row in derniers.iterrows():
        r = row["Résultat"]
        if "Victoire" in r:
            badges.append("🟢")
        elif "Défaite" in r:
            badges.append("🔴")
        else:
            badges.append("🟡")

    st.markdown("**Forme récente :** " + "  ".join(badges))

    cols = st.columns(len(derniers))
    for col, (_, row) in zip(cols, derniers.iterrows()):
        score = f"{int(row['Score Dom'])} — {int(row['Score Ext'])}"
        with col:
            st.markdown(
                f"""
                <div style="
                    border: 1px solid #ddd;
                    border-radius: 8px;
                    padding: 10px;
                    text-align: center;
                    font-size: 12px;
                ">
                    <b>{row['Domicile']}</b><br>
                    <span style="font-size:18px; font-weight:bold">{score}</span><br>
                    <b>{row['Extérieur']}</b><br>
                    <span>{row['Résultat']}</span><br>
                    <small>{str(row['Date'])[:10]}</small>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.divider()

    # ---- Tableau complet ----
    st.subheader(f" Tous les matchs — {nom_equipe}")
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(f" {len(df)} rencontres au total.")
