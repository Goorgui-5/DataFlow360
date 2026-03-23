import streamlit as st
from db_connection import run_query
from components.kpi_cards import render_kpi_row
from components.charts import courbe_buts_par_mois
from components.filters import render_filters


def page_performance_competition():

    st.header(" Performance par Compétition")
    st.divider()

    filters = render_filters(show_mois=True, show_equipe=False, show_competition=True)
    id_comp    = filters["id_competition"]
    nom_comp   = filters["nom_competition"]
    mois       = filters["mois"]
    annee      = filters["annee"]

    # ---- Clauses WHERE ----
    clauses = []
    params  = {}

    if id_comp:
        clauses.append("f.id_competition = %(id_comp)s")
        params["id_comp"] = id_comp

    if mois:
        clauses.append("d.mois = %(mois)s AND d.annee = %(annee)s")
        params["mois"]  = mois
        params["annee"] = annee

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    # ---- KPI : Total Matchs ----
    df_matchs = run_query(f"""
        SELECT COUNT(*) AS total
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
    """, params)
    total_matchs = int(df_matchs.iloc[0]["total"])

    # ---- KPI : Total Buts ----
    df_buts = run_query(f"""
        SELECT COALESCE(SUM(f.nb_buts_total), 0) AS total
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
    """, params)
    total_buts = int(df_buts.iloc[0]["total"])

    # ---- KPI : Moyenne Buts/Match ----
    moyenne = round(total_buts / total_matchs, 2) if total_matchs > 0 else 0

    # ---- KPI : Leader de la compétition ----ans le panneau de gauche 
    leader_nom    = "—"
    leader_points = 0
    if id_comp:
        df_leader = run_query("""
            SELECT
                e.nom_equipe,
                SUM(CASE
                    WHEN (f.id_equipe_dom = e.id_equipe AND f.score_dom > f.score_ext)
                      OR (f.id_equipe_ext = e.id_equipe AND f.score_ext > f.score_dom)
                    THEN 3
                    WHEN f.score_dom = f.score_ext THEN 1
                    ELSE 0 END
                ) AS points
            FROM dw.fait_match f
            JOIN dw.dim_equipe e
              ON e.id_equipe = f.id_equipe_dom OR e.id_equipe = f.id_equipe_ext
            WHERE f.id_competition = %(id_comp)s
            GROUP BY e.id_equipe, e.nom_equipe
            ORDER BY points DESC LIMIT 1
        """, {"id_comp": id_comp})
        if not df_leader.empty:
            leader_nom    = df_leader.iloc[0]["nom_equipe"] if "nom_equipe" in df_leader.columns else df_leader.iloc[0]["equipe"]
            leader_points = int(df_leader.iloc[0]["points"])

    # ---- Affichage KPIs ----
    titre_ctx = f" — {nom_comp}" if nom_comp != "Toutes" else " — Toutes compétitions"
    st.subheader(f"Indicateurs{titre_ctx}")

    render_kpi_row([
        {"label": "Total Matchs",       "value": total_matchs,  },
        {"label": "Total Buts",         "value": total_buts,    },
        {"label": "Moyenne Buts/Match", "value": moyenne,       },
        {"label": "Leader",             "value": leader_nom,
         "delta": f"{leader_points} pts" if id_comp else None,  },
    ])

    st.divider()

    # ---- Graphique : Courbe buts par mois ----
    st.subheader(" Évolution des buts")

    df_graph = run_query(f"""
        SELECT d.mois, d.annee, SUM(f.nb_buts_total) AS total_buts
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
        GROUP BY d.annee, d.mois
        ORDER BY d.annee, d.mois
    """, params)

    if not df_graph.empty:
        titre = f"Buts par mois — {nom_comp}"
        fig   = courbe_buts_par_mois(df_graph, titre)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(" Aucune donnée pour cette sélection.")
