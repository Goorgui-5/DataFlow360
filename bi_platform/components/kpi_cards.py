import streamlit as st


def kpi_card(col, label: str, value, delta=None, icon: str = ""):
    """
    Affiche une carte KPI dans la colonne Streamlit fournie.
    
    Args:
        col       : colonne st (ex: col1)
        label     : titre du KPI
        value     : valeur principale
        delta     : variation ou sous-titre (optionnel)
        icon      : emoji ou icône (optionnel)
    """
    with col:
        titre = f"{icon} {label}" if icon else label
        if delta is not None:
            st.metric(label=titre, value=value, delta=str(delta))
        else:
            st.metric(label=titre, value=value)


def render_kpi_row(kpis: list):
    """
    Affiche une ligne de cartes KPI.
    
    Args:
        kpis : liste de dicts avec clés :
               label, value, delta (opt), icon (opt)
    
    Exemple:
        render_kpi_row([
            {"label": "Total Buts", "value": 342"},
            {"label": "Total Matchs", "value": 120"},
        ])
    """
    cols = st.columns(len(kpis))
    for col, kpi in zip(cols, kpis):
        kpi_card(
            col=col,
            label=kpi.get("label", ""),
            value=kpi.get("value", 0),
            delta=kpi.get("delta", None),
            icon=kpi.get("icon", ""),
        )
