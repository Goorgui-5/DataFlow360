import streamlit as st
from db_connection import run_query
from components.kpi_cards import render_kpi_row
from components.charts import courbe_buts_par_mois
from components.filters import render_filters


def page_performance_globale():

    st.header(" Performance Globale — Saison 2025–2026")
    st.markdown("Vue d'ensemble de toutes les compétitions et équipes.")
    st.divider()

    filters = render_filters(show_mois=True, show_equipe=False, show_competition=False)
    mois   = filters["mois"]
    annee  = filters["annee"]

    # ---- Clause WHERE commune ----
    where = ""
    params = {}
    if mois:
        where  = "WHERE d.mois = %(mois)s AND d.annee = %(annee)s"
        params = {"mois": mois, "annee": annee}

    # ---- KPI : Total Buts ----
    df_buts = run_query(f"""
        SELECT COALESCE(SUM(f.nb_buts_total), 0) AS total
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
    """, params)
    total_buts = int(df_buts.iloc[0]["total"])

    # ---- KPI : Total Matchs ----
    df_matchs = run_query(f"""
        SELECT COUNT(*) AS total
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
    """, params)
    total_matchs = int(df_matchs.iloc[0]["total"])

    # ---- KPI : Moyenne Buts/Match ----
    moyenne = round(total_buts / total_matchs, 2) if total_matchs > 0 else 0

    # ---- KPI : Leader ----
    df_leader = run_query("""
        SELECT * FROM dw.v_kpi_classement_equipes
        ORDER BY total_points DESC LIMIT 1
    """)
    leader_nom    = df_leader.iloc[0]["equipe"]       if not df_leader.empty else "—"
    leader_points = df_leader.iloc[0]["total_points"] if not df_leader.empty else 0

    # ---- Affichage KPIs ----
    render_kpi_row([
        {"label": "Total Buts",         "value": total_buts,   },
        {"label": "Total Matchs",       "value": total_matchs, },
        {"label": "Moyenne Buts/Match", "value": moyenne,      },
        {"label": "Leader",             "value": leader_nom,
         "delta": f"{leader_points} pts",                      },
    ])

    st.divider()

    # ---- Graphique : Évolution des buts par mois ----
    st.subheader(" Courbe d'évolution des buts")

    df_graph = run_query(f"""
        SELECT d.mois, d.annee, SUM(f.nb_buts_total) AS total_buts
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
        {where}
        GROUP BY d.annee, d.mois
        ORDER BY d.annee, d.mois
    """, params)

    if not df_graph.empty:
        fig = courbe_buts_par_mois(df_graph, "Évolution globale des buts par mois")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ Aucune donnée pour la période sélectionnée.")
