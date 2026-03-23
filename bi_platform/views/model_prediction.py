import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import streamlit as st
import plotly.graph_objects as go
from db_connection import run_query

# ─── CHARGEMENT & PRÉPARATION DES DONNÉES ────────────────────────────────────

def load_match_data():
    """Charge tous les matchs terminés depuis le DW"""
    
    query = """
        SELECT 
            f.match_id,
            f.id_equipe_dom,
            f.id_equipe_ext,
            e_dom.nom_equipe AS equipe_dom,
            e_ext.nom_equipe AS equipe_ext,
            f.score_dom,
            f.score_ext,
            CASE 
                WHEN f.score_dom > f.score_ext THEN 1
                WHEN f.score_dom = f.score_ext THEN 0
                ELSE 2
            END AS resultat,
            c.nom_competition,
            d.date_complete
        FROM dw.fait_match f
        JOIN dw.dim_equipe e_dom ON f.id_equipe_dom = e_dom.id_equipe
        JOIN dw.dim_equipe e_ext ON f.id_equipe_ext = e_ext.id_equipe
        JOIN dw.dim_competition c ON f.id_competition = c.id_competition
        JOIN dw.dim_date d ON f.id_date = d.id_date
        WHERE f.score_dom IS NOT NULL AND f.score_ext IS NOT NULL
        ORDER BY d.date_complete
    """
    return run_query(query)
    

# Fonction pour calculer les statistiques d'équipe à partir des données de match
def compute_team_stats(df):
    """
    Calcule pour chaque équipe :
    - Moyenne buts marqués / encaissés
    - Points par match
    - Forme récente (5 derniers matchs)
    """
    stats = {}

    equipes = pd.concat([
        df[["id_equipe_dom", "equipe_dom"]].rename(columns={"id_equipe_dom": "id", "equipe_dom": "nom"}),
        df[["id_equipe_ext", "equipe_ext"]].rename(columns={"id_equipe_ext": "id", "equipe_ext": "nom"})
    ]).drop_duplicates()

    for _, row in equipes.iterrows():
        eid  = row["id"]
        enom = row["nom"]

        matchs_dom = df[df["id_equipe_dom"] == eid].copy()
        matchs_ext = df[df["id_equipe_ext"] == eid].copy()

        # Points
        pts_dom = matchs_dom["resultat"].map({1: 3, 0: 1, 2: 0}).sum()
        pts_ext = matchs_ext["resultat"].map({2: 3, 0: 1, 1: 0}).sum()
        total_matchs = len(matchs_dom) + len(matchs_ext)
        pts_par_match = (pts_dom + pts_ext) / total_matchs if total_matchs > 0 else 0

        # Buts marqués / encaissés
        buts_marques = matchs_dom["score_dom"].sum() + matchs_ext["score_ext"].sum()
        buts_encaisses = matchs_dom["score_ext"].sum() + matchs_ext["score_dom"].sum()
        moy_buts_marques  = buts_marques  / total_matchs if total_matchs > 0 else 0
        moy_buts_encaisses = buts_encaisses / total_matchs if total_matchs > 0 else 0

        # Forme récente (5 derniers matchs)
        matchs_dom_f = matchs_dom[["date_complete", "resultat"]].copy()
        matchs_dom_f["pts"] = matchs_dom_f["resultat"].map({1: 3, 0: 1, 2: 0})

        matchs_ext_f = matchs_ext[["date_complete", "resultat"]].copy()
        matchs_ext_f["pts"] = matchs_ext_f["resultat"].map({2: 3, 0: 1, 1: 0})

        tous_matchs = pd.concat([matchs_dom_f, matchs_ext_f]).sort_values("date_complete", ascending=False)
        forme = tous_matchs.head(5)["pts"].sum() / 15  # normalisé sur 15 pts max

        stats[eid] = {
            "nom":               enom,
            "pts_par_match":     pts_par_match,
            "moy_buts_marques":  moy_buts_marques,
            "moy_buts_encaisses": moy_buts_encaisses,
            "forme":             forme,
            "total_matchs":      total_matchs
        }

    return stats

# fonction pour construire la matrice de features à partir des stats calculées
def build_features(df, stats):
    """
    Construit la matrice de features pour chaque match :
    - Stats équipe domicile
    - Stats équipe extérieure
    - Différentiels
    """
    rows = []
    for _, match in df.iterrows():
        dom = stats.get(match["id_equipe_dom"])
        ext = stats.get(match["id_equipe_ext"])
        if not dom or not ext:
            continue

        rows.append({
            "match_id":           match["match_id"],
            "equipe_dom":         match["equipe_dom"],
            "equipe_ext":         match["equipe_ext"],
            "nom_competition":    match["nom_competition"],
            # Features domicile
            "dom_pts_par_match":     dom["pts_par_match"],
            "dom_buts_marques":      dom["moy_buts_marques"],
            "dom_buts_encaisses":    dom["moy_buts_encaisses"],
            "dom_forme":             dom["forme"],
            # Features extérieur
            "ext_pts_par_match":     ext["pts_par_match"],
            "ext_buts_marques":      ext["moy_buts_marques"],
            "ext_buts_encaisses":    ext["moy_buts_encaisses"],
            "ext_forme":             ext["forme"],
            # Différentiels
            "diff_pts":              dom["pts_par_match"]    - ext["pts_par_match"],
            "diff_buts":             dom["moy_buts_marques"] - ext["moy_buts_marques"],
            "diff_defense":          ext["moy_buts_encaisses"] - dom["moy_buts_encaisses"],
            "diff_forme":            dom["forme"] - ext["forme"],
            # Target
            "resultat":              match["resultat"]
        })

    return pd.DataFrame(rows)


# ─── ENTRAÎNEMENT DU MODÈLE ───────────────────────────────────────────────────

FEATURES = [
    "dom_pts_par_match", "dom_buts_marques", "dom_buts_encaisses", "dom_forme",
    "ext_pts_par_match", "ext_buts_marques", "ext_buts_encaisses", "ext_forme",
    "diff_pts", "diff_buts", "diff_defense", "diff_forme"
]

# Le modèle se réentraîne automatiquement toutes les heures grâce à la mise en cache de Streamlit
@st.cache_resource(ttl=3600)

# Fonction pour entraîner le modèle de régression logistique à partir des données chargées et préparées 
def train_model():
    """
    Charge les données, calcule les stats, construit les features
    et entraîne le modèle — se réentraîne automatiquement toutes les heures
    """
    df     = load_match_data()
    stats  = compute_team_stats(df)
    df_feat = build_features(df, stats)

    X = df_feat[FEATURES]
    y = df_feat["resultat"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    scaler = StandardScaler()
    X_train_sc = scaler.fit_transform(X_train)
    X_test_sc  = scaler.transform(X_test)

    model = LogisticRegression(
    max_iter=1000,
    random_state=42
)
    model.fit(X_train_sc, y_train)

    y_pred   = model.predict(X_test_sc)
    accuracy = accuracy_score(y_test, y_pred)
    report   = classification_report(y_test, y_pred,
                    target_names=["Match nul", "Victoire dom", "Victoire ext"],
                    output_dict=True)

    return model, scaler, stats, accuracy, report, len(df)

# Fonction pour prédire le résultat d'un match à partir des stats des équipes et du modèle entraîné
def predict_match(equipe_dom_id, equipe_ext_id, stats, model, scaler):
    """Prédit le résultat d'un match entre deux équipes"""
    dom = stats.get(equipe_dom_id)
    ext = stats.get(equipe_ext_id)
    if not dom or not ext:
        return None

    features = pd.DataFrame([{
        "dom_pts_par_match":     dom["pts_par_match"],
        "dom_buts_marques":      dom["moy_buts_marques"],
        "dom_buts_encaisses":    dom["moy_buts_encaisses"],
        "dom_forme":             dom["forme"],
        "ext_pts_par_match":     ext["pts_par_match"],
        "ext_buts_marques":      ext["moy_buts_marques"],
        "ext_buts_encaisses":    ext["moy_buts_encaisses"],
        "ext_forme":             ext["forme"],
        "diff_pts":              dom["pts_par_match"]    - ext["pts_par_match"],
        "diff_buts":             dom["moy_buts_marques"] - ext["moy_buts_marques"],
        "diff_defense":          ext["moy_buts_encaisses"] - dom["moy_buts_encaisses"],
        "diff_forme":            dom["forme"] - ext["forme"],
    }])

    features_sc = scaler.transform(features)
    proba       = model.predict_proba(features_sc)[0]
    prediction  = model.predict(features_sc)[0]

    # classes : 0=nul, 1=dom, 2=ext
    classes = model.classes_
    proba_dict = {c: p for c, p in zip(classes, proba)}

    return {
        "prediction":   prediction,
        "proba_nul":    proba_dict.get(0, 0),
        "proba_dom":    proba_dict.get(1, 0),
        "proba_ext":    proba_dict.get(2, 0),
        "dom_nom":      dom["nom"],
        "ext_nom":      ext["nom"],
        "dom_forme":    dom["forme"],
        "ext_forme":    ext["forme"],
        "dom_pts":      dom["pts_par_match"],
        "ext_pts":      ext["pts_par_match"],
    }


# ─── PAGE STREAMLIT ───────────────────────────────────────────────────────────

# Fonction principale de la page de prédiction de résultat de match 
# — affiche les métriques du modèle, permet de sélectionner un match et affiche la prédiction et les probabilités
def show():
    st.title(" Prédiction de Résultat de Match")
    st.markdown("*Modèle de Régression Logistique — réentraîné automatiquement à chaque nouvelles données*")

    # Entraînement
    with st.spinner("Entraînement du modèle en cours..."):
        model, scaler, stats, accuracy, report, nb_matchs = train_model()

    # ── Métriques du modèle ───────────────────────────────────────
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(" Précision du modèle", f"{accuracy:.1%}")
    with col2:
        st.metric(" Matchs d'entraînement", f"{int(nb_matchs * 0.8):,}")
    with col3:
        st.metric(" Matchs de test", f"{int(nb_matchs * 0.2):,}")

    st.markdown("---")

    # ── Sélection des équipes ─────────────────────────────────────
    st.subheader(" Sélectionner le match à prédire")

    equipes_list = sorted([(v["nom"], k) for k, v in stats.items()])
    equipes_noms = [e[0] for e in equipes_list]
    equipes_ids  = {e[0]: e[1] for e in equipes_list}

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("Équipe Domicile")
        dom_nom = st.selectbox("", equipes_noms, key="dom", index=equipes_noms.index("Arsenal FC") if "Arsenal FC" in equipes_noms else 0)
    with col2:
        st.markdown("Équipe Extérieure")
        ext_list = [e for e in equipes_noms if e != dom_nom]
        ext_nom  = st.selectbox("", ext_list, key="ext", index=ext_list.index("Manchester City FC") if "Manchester City FC" in ext_list else 0)

    if st.button(" Prédire le résultat", type="primary", use_container_width=True):

        dom_id = equipes_ids[dom_nom]
        ext_id = equipes_ids[ext_nom]
        result = predict_match(dom_id, ext_id, stats, model, scaler)

        if result:
            st.markdown("---")
            st.subheader(f" {result['dom_nom']}  vs  {result['ext_nom']}")

            # Résultat principal
            labels = {1: f" Victoire {result['dom_nom']}", 0: "🤝 Match Nul", 2: f" Victoire {result['ext_nom']}"}
            couleurs = {1: "#28a745", 0: "#ffc107", 2: "#dc3545"}
            pred = result["prediction"]

            st.markdown(
                f"""
                <div style='background-color:{couleurs[pred]}22; border-left: 6px solid {couleurs[pred]};
                padding: 20px; border-radius: 8px; text-align:center; margin: 20px 0'>
                    <h2 style='color:{couleurs[pred]}; margin:0'>
                        {labels[pred]}
                    </h2>
                    <p style='color:#666; margin:4px 0'>Résultat le plus probable selon le modèle</p>
                </div>
                """,
                unsafe_allow_html=True
            )

            # Jauge des probabilités
            st.markdown("#### Probabilités détaillées")
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=[f" {result['dom_nom']}", "🤝 Match Nul", f" {result['ext_nom']}"],
                y=[result["proba_dom"]*100, result["proba_nul"]*100, result["proba_ext"]*100],
                marker_color=["#28a745", "#ffc107", "#dc3545"],
                text=[f"{result['proba_dom']:.1%}", f"{result['proba_nul']:.1%}", f"{result['proba_ext']:.1%}"],
                textposition="outside",
                textfont=dict(size=16, color="black")
            ))
            fig.update_layout(
                yaxis_title="Probabilité (%)",
                yaxis=dict(range=[0, 110]),
                plot_bgcolor="white",
                paper_bgcolor="white",
                height=350,
                showlegend=False,
                margin=dict(t=20, b=40)
            )
            st.plotly_chart(fig, use_container_width=True)

            # Comparaison des stats
            st.markdown("#### Comparaison des équipes")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**{result['dom_nom']}**")
                st.progress(result["dom_forme"], text=f"Forme : {result['dom_forme']:.0%}")
                st.progress(min(result["dom_pts"] / 3, 1.0), text=f"Pts/match : {result['dom_pts']:.2f}")
            with col2:
                st.markdown(f"**{result['ext_nom']}**")
                st.progress(result["ext_forme"], text=f"Forme : {result['ext_forme']:.0%}")
                st.progress(min(result["ext_pts"] / 3, 1.0), text=f"Pts/match : {result['ext_pts']:.2f}")

    # ── Performances du modèle ────────────────────────────────────
    st.markdown("---")
    with st.expander(" Performances détaillées du modèle"):
        st.markdown(f"**Précision globale : {accuracy:.1%}**")
        st.markdown("*Rapport de classification :*")
        df_report = pd.DataFrame(report).T
        df_report = df_report.drop(["accuracy", "macro avg", "weighted avg"], errors="ignore")
        df_report = df_report.round(2)
        st.dataframe(df_report, use_container_width=True)

        st.info("""
        💡 **Comment interpréter ?**
        - **Précision** : % de fois où le modèle a raison
        - **Recall** : capacité à détecter chaque type de résultat
        - Le modèle se réentraîne automatiquement à chaque nouveaux matchs du weekend
        """)

    with st.expander(" Comment fonctionne ce modèle ?"):
        st.markdown("""
        **Algorithme :** Régression Logistique Multinomiale

        **Features utilisées (12 variables) :**
        - Pts/match domicile et extérieur
        - Moyenne buts marqués/encaissés
        - Indice de forme (5 derniers matchs)
        - Différentiels entre les deux équipes

        **Entraînement :**
        - 80% des matchs pour entraîner
        - 20% pour tester la précision

        **Réentraînement automatique :**
        - Le modèle recharge les données toutes les heures
        - Chaque nouveau match du weekend améliore les prédictions

        *⚠️ Ce modèle est basé sur les statistiques — il ne tient pas compte des blessures, suspensions ou autres facteurs externes.*
        """)
