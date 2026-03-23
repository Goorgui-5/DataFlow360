import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


# Palette de couleurs cohérente
COULEUR_PRINCIPALE = "#1f77b4"
COULEUR_SECONDAIRE = "#ff7f0e"
COULEUR_VERTE      = "#2ca02c"
COULEUR_ROUGE      = "#d62728"

LAYOUT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Arial", size=13),
    margin=dict(l=40, r=20, t=50, b=40),
)


# ============================================================
# Courbe buts par mois (globale ou par équipe)
# ============================================================

def courbe_buts_par_mois(df: pd.DataFrame, titre: str = "Évolution des buts par mois"):
    """
    df doit contenir : mois, annee, total_buts
    """
    if df.empty:
        return _graphique_vide(titre)

    df = df.copy()
    df["periode"] = df["annee"].astype(str) + "-" + df["mois"].astype(str).str.zfill(2)
    df = df.sort_values("periode")

    fig = px.line(
        df, x="periode", y="total_buts",
        title=titre,
        labels={"periode": "Période", "total_buts": "Buts"},
        markers=True,
        color_discrete_sequence=[COULEUR_PRINCIPALE],
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=8))
    fig.update_layout(**LAYOUT_BASE)
    return fig


# ============================================================
# Double courbe : buts marqués vs encaissés (par équipe)
# ============================================================

def double_courbe_equipe(df: pd.DataFrame, nom_equipe: str):
    """
    df doit contenir : mois, annee, buts_marques, buts_encaisses
    """
    titre = f" — {nom_equipe} —"

    if df.empty:
        return _graphique_vide(titre)

    df = df.copy()
    df["periode"] = df["annee"].astype(str) + "-" + df["mois"].astype(str).str.zfill(2)
    df = df.sort_values("periode")

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["periode"], y=df["buts_marques"],
        mode="lines+markers",
        name=" Buts marqués",
        line=dict(color=COULEUR_VERTE, width=3),
        marker=dict(size=8),
    ))

    fig.add_trace(go.Scatter(
        x=df["periode"], y=df["buts_encaisses"],
        mode="lines+markers",
        name=" Buts encaissés",
        line=dict(color=COULEUR_ROUGE, width=3),
        marker=dict(size=8),
    ))

    fig.update_layout(
        title=titre,
        xaxis_title="Période",
        yaxis_title="Buts",
        legend=dict(orientation="v", yanchor="bottom", y=1.02),
        **LAYOUT_BASE,
    )
    return fig


# ============================================================
# Diagramme barres : V / D / N par mois
# ============================================================

def barres_vdn_par_mois(df: pd.DataFrame, nom_equipe: str):
    """
    df doit contenir : mois, annee, victoires, defaites, nuls
    """
    titre = f" — {nom_equipe} —"

    if df.empty:
        return _graphique_vide(titre)

    df = df.copy()
    df["periode"] = df["annee"].astype(str) + "-" + df["mois"].astype(str).str.zfill(2)
    df = df.sort_values("periode")

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df["periode"], y=df["victoires"],
        name=" Victoires",
        marker_color=COULEUR_VERTE,
    ))
    fig.add_trace(go.Bar(
        x=df["periode"], y=df["defaites"],
        name=" Défaites",
        marker_color=COULEUR_ROUGE,
    ))
    fig.add_trace(go.Bar(
        x=df["periode"], y=df["nuls"],
        name=" Nuls",
        marker_color=COULEUR_SECONDAIRE,
    ))

    fig.update_layout(
        barmode="group",
        title=titre,
        xaxis_title="Période",
        yaxis_title="Nombre de matchs",
        legend=dict(orientation="v", yanchor="bottom", y=1.02),
        **LAYOUT_BASE,
    )
    return fig


# ============================================================
# Graphique vide (fallback)
# ============================================================

def _graphique_vide(titre: str):
    fig = go.Figure()
    fig.update_layout(
        title=titre,
        annotations=[dict(
            text="Aucune donnée disponible",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="gray"),
        )],
        **LAYOUT_BASE,
    )
    return fig
