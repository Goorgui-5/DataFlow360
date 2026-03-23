import re
import pandas as pd
from db_connection import run_query

# ============================================================
# Moteur Chatbot BI — Logique SQL Pure (100% Français)
# ============================================================
# Le chatbot comprend les questions sur les données du DW
# et répond uniquement sur la base des données disponibles.
# ============================================================


# --- Réponses hors-sujet ---
HORS_SUJET = (
    " Je suis un assistant BI spécialisé sur les données football de cette plateforme. "
    "Je ne peux répondre qu'aux questions sur les matchs, équipes, compétitions et statistiques disponibles."
)

INCOMPRIS = (
    "🤔 Je n'ai pas compris votre question. Essayez par exemple :\n"
    "- *Combien de buts a marqué [équipe] ?*\n"
    "- *Quel est le meilleur buteur ?*\n"
    "- *Classement de la Premier League ?*\n"
    "- *Derniers matchs de [équipe] ?*\n"
    "- *Combien de matchs en [compétition] ?*"
)


# ============================================================
# Détection d'entités (équipe / compétition) dans la question
# ============================================================

def _detecter_equipe(question: str) -> dict | None:
    """Cherche un nom d'équipe dans la question."""
    df = run_query("SELECT id_equipe, nom_equipe FROM dw.dim_equipe")
    q = question.lower()
    for _, row in df.iterrows():
        if row["nom_equipe"].lower() in q:
            return {"id": int(row["id_equipe"]), "nom": row["nom_equipe"]}
    return None


def _detecter_competition(question: str) -> dict | None:
    """Cherche un nom de compétition dans la question."""
    df = run_query("SELECT id_competition, nom_competition FROM dw.dim_competition")
    q = question.lower()
    for _, row in df.iterrows():
        if row["nom_competition"].lower() in q:
            return {"id": int(row["id_competition"]), "nom": row["nom_competition"]}
    return None


def _detecter_mois(question: str) -> int | None:
    """Extrait un numéro de mois de la question (ex: 'en août' → 8)."""
    mois_map = {
        "janvier": 1, "février": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
        "jan": 1, "fev": 2, "fév": 2, "avr": 4,
        "juil": 7, "sep": 9, "oct": 10, "nov": 11, "dec": 12, "déc": 12,
    }
    q = question.lower()
    for nom, num in mois_map.items():
        if nom in q:
            return num
    # Cherche un chiffre de mois direct (ex: "mois 8")
    match = re.search(r"\bmois\s+(\d{1,2})\b", q)
    if match:
        return int(match.group(1))
    return None


# ============================================================
# Handlers — chaque fonction gère un type de question
# ============================================================

def _handler_buts_equipe(equipe: dict, mois: int | None) -> tuple:
    """Buts marqués par une équipe."""
    sql = """
        SELECT
            SUM(f.score_dom) FILTER (WHERE f.id_equipe_dom = %(id)s) AS buts_domicile,
            SUM(f.score_ext) FILTER (WHERE f.id_equipe_ext = %(id)s) AS buts_exterieur,
            SUM(
                CASE WHEN f.id_equipe_dom = %(id)s THEN f.score_dom
                     WHEN f.id_equipe_ext = %(id)s THEN f.score_ext
                     ELSE 0 END
            ) AS total_buts_marques
        FROM dw.fait_match f
        JOIN dw.dim_date d ON d.id_date = f.id_date
    """
    params = {"id": equipe["id"]}

    if mois:
        sql += " WHERE d.mois = %(mois)s"
        params["mois"] = mois

    df = run_query(sql, params)
    if df.empty or df.iloc[0]["total_buts_marques"] is None:
        return f"Aucune donnée trouvée pour **{equipe['nom']}**.", None

    total = int(df.iloc[0]["total_buts_marques"] or 0)
    dom   = int(df.iloc[0]["buts_domicile"] or 0)
    ext   = int(df.iloc[0]["buts_exterieur"] or 0)

    mois_txt = f" en mois {mois}" if mois else ""
    reponse = (
        f" **{equipe['nom']}** a marqué **{total} buts**{mois_txt}.\n"
        f"- Domicile : {dom} buts\n"
        f"- Extérieur : {ext} buts"
    )
    return reponse, None


def _handler_classement(competition: dict | None) -> tuple:
    """Classement général ou d'une compétition."""
    if competition:
        sql = """
            SELECT
                e.nom_equipe AS "Équipe",
                SUM(CASE
                    WHEN (f.id_equipe_dom = e.id_equipe AND f.score_dom > f.score_ext)
                      OR (f.id_equipe_ext = e.id_equipe AND f.score_ext > f.score_dom)
                    THEN 3
                    WHEN f.score_dom = f.score_ext THEN 1
                    ELSE 0 END
                ) AS "Points",
                COUNT(*) AS "Matchs joués"
            FROM dw.fait_match f
            JOIN dw.dim_equipe e
              ON e.id_equipe = f.id_equipe_dom OR e.id_equipe = f.id_equipe_ext
            WHERE f.id_competition = %(id_comp)s
            GROUP BY e.id_equipe, e.nom_equipe
            ORDER BY "Points" DESC
            LIMIT 15
        """
        df = run_query(sql, {"id_comp": competition["id"]})
        reponse = f" Classement de la **{competition['nom']}** :"
    else:
        df = run_query("""
            SELECT equipe AS "Équipe", matchs_joues AS "Matchs", victoires AS "V", nuls AS "N", defaites AS "D", total_points AS "Points" FROM dw.v_kpi_classement_equipes
            ORDER BY total_points DESC
            LIMIT 15
        """)
        reponse = " Classement général (toutes compétitions) :"

    if df.empty:
        return "Aucune donnée de classement disponible.", None
    return reponse, df


def _handler_derniers_matchs(equipe: dict) -> tuple:
    """5 derniers matchs d'une équipe."""
    sql = """
        SELECT
            d.date_complete AS "Date",
            c.nom_competition AS "Compétition",
            e_dom.nom_equipe AS "Domicile",
            e_ext.nom_equipe AS "Extérieur",
            f.score_dom AS "Score Dom",
            f.score_ext AS "Score Ext",
            CASE
                WHEN f.id_equipe_dom = %(id)s AND f.score_dom > f.score_ext THEN '🟢 Victoire'
                WHEN f.id_equipe_ext = %(id)s AND f.score_ext > f.score_dom THEN '🟢 Victoire'
                WHEN f.score_dom = f.score_ext                               THEN '🟡 Nul'
                ELSE '🔴 Défaite'
            END AS "Résultat"
        FROM dw.fait_match f
        JOIN dw.dim_date        d     ON d.id_date         = f.id_date
        JOIN dw.dim_equipe      e_dom ON e_dom.id_equipe   = f.id_equipe_dom
        JOIN dw.dim_equipe      e_ext ON e_ext.id_equipe   = f.id_equipe_ext
        JOIN dw.dim_competition c     ON c.id_competition  = f.id_competition
        WHERE f.id_equipe_dom = %(id)s OR f.id_equipe_ext = %(id)s
        ORDER BY d.date_complete DESC
        LIMIT 5
    """
    df = run_query(sql, {"id": equipe["id"]})
    if df.empty:
        return f"Aucun match trouvé pour **{equipe['nom']}**.", None
    reponse = f" 5 derniers matchs de **{equipe['nom']}** :"
    return reponse, df


def _handler_stats_competition(competition: dict) -> tuple:
    """Stats globales d'une compétition."""
    sql = """
        SELECT
            COUNT(*)                      AS total_matchs,
            SUM(f.nb_buts_total)          AS total_buts,
            ROUND(AVG(f.nb_buts_total), 2) AS moyenne_buts
        FROM dw.fait_match f
        WHERE f.id_competition = %(id)s
    """
    df = run_query(sql, {"id": competition["id"]})
    if df.empty:
        return f"Aucune donnée pour **{competition['nom']}**.", None

    row = df.iloc[0]
    reponse = (
        f" **{competition['nom']}** :\n"
        f"- Matchs joués : **{int(row['total_matchs'])}**\n"
        f"- Total buts : **{int(row['total_buts'] or 0)}**\n"
        f"- Moyenne buts/match : **{row['moyenne_buts'] or 0}**"
    )
    return reponse, None


def _handler_meilleure_attaque() -> tuple:
    """Équipe avec le plus de buts marqués."""
    df = run_query("""
        SELECT equipe AS "Équipe", total_buts_marques AS "Buts marqués"
        FROM dw.v_kpi_meilleure_attaque
        ORDER BY total_buts_marques DESC
        LIMIT 10
    """)
    if df.empty:
        return "Données indisponibles.", None
    return " Meilleures attaques :", df


def _handler_meilleure_defense() -> tuple:
    """Équipe avec le moins de buts encaissés."""
    df = run_query("""
        SELECT equipe AS "Équipe", total_buts_encaisses AS "Buts encaissés"
        FROM dw.v_kpi_meilleure_defense
        ORDER BY total_buts_encaisses ASC
        LIMIT 10
    """)
    if df.empty:
        return "Données indisponibles.", None
    return " Meilleures défenses :", df


def _handler_matchs_nuls(equipe: dict | None) -> tuple:
    """Nombre de matchs nuls."""
    if equipe:
        sql = """
            SELECT COUNT(*) AS matchs_nuls
            FROM dw.fait_match f
            WHERE (f.id_equipe_dom = %(id)s OR f.id_equipe_ext = %(id)s)
              AND f.score_dom = f.score_ext
        """
        df = run_query(sql, {"id": equipe["id"]})
        n = int(df.iloc[0]["matchs_nuls"])
        return f" **{equipe['nom']}** a joué **{n} matchs nuls**.", None
    else:
        df = run_query("""
            SELECT COUNT(*) AS matchs_nuls FROM dw.fait_match WHERE score_dom = score_ext
        """)
        n = int(df.iloc[0]["matchs_nuls"])
        return f" Total de **{n} matchs nuls** dans la base de données.", None


def _handler_total_matchs(competition: dict | None, mois: int | None) -> tuple:
    """Nombre total de matchs."""
    sql = "SELECT COUNT(*) AS total FROM dw.fait_match f"
    clauses, params = [], {}

    if competition:
        clauses.append("f.id_competition = %(id_comp)s")
        params["id_comp"] = competition["id"]
    if mois:
        sql += " JOIN dw.dim_date d ON d.id_date = f.id_date"
        clauses.append("d.mois = %(mois)s")
        params["mois"] = mois

    if clauses:
        sql += " WHERE " + " AND ".join(clauses)

    df = run_query(sql, params)
    n = int(df.iloc[0]["total"])
    ctx = f" en **{competition['nom']}**" if competition else ""
    ctx += f" (mois {mois})" if mois else ""
    return f" Total de **{n} matchs**{ctx} dans la base.", None


# ============================================================
# Routeur principal
# ============================================================

MOTS_BUTS        = ["but", "buts", "marqué", "marque", "scorer", "score", "inscrit", "goal"]
MOTS_CLASSEMENT  = ["classement", "classe", "classé", "rang", "position", "points", "leader", "tête"]
MOTS_DERNIERS    = ["dernier", "derniers", "récent", "récents", "historique", "résultat", "résultats"]
MOTS_ATTAQUE     = ["attaque", "meilleure attaque", "plus de buts marqués", "top buteur", "offensif"]
MOTS_DEFENSE     = ["défense", "défensive", "moins de buts encaissés", "meilleure défense"]
MOTS_NUL         = ["nul", "nuls", "match nul", "égalité"]
MOTS_MATCHS      = ["match", "matchs", "rencontre", "rencontres", "joué", "joués", "disputé"]
MOTS_COMPETITION = ["compétition", "competition", "ligue", "championnat", "copa", "coupe"]


def _contient(question: str, mots: list) -> bool:
    q = question.lower()
    return any(m in q for m in mots)


def chatbot_response(question: str, memory: dict) -> tuple:
    """
    Point d'entrée principal du chatbot.
    
    Retourne : (reponse_texte, dataframe_ou_None)
    """
    q = question.strip()

    if not q:
        return INCOMPRIS, None

    equipe     = _detecter_equipe(q)
    competition = _detecter_competition(q)
    mois       = _detecter_mois(q)

    # --- Vérification hors-sujet basique ---
    mots_football = (
        MOTS_BUTS + MOTS_CLASSEMENT + MOTS_DERNIERS + MOTS_ATTAQUE +
        MOTS_DEFENSE + MOTS_NUL + MOTS_MATCHS + MOTS_COMPETITION +
        ["équipe", "equipe", "football", "foot", "victoire", "défaite",
         "résultat", "saison", "statistique", "stat", "performance"]
    )
    if not equipe and not competition and not _contient(q, mots_football):
        return HORS_SUJET, None

    # --- Routing ---

    # Buts d'une équipe
    if equipe and _contient(q, MOTS_BUTS):
        return _handler_buts_equipe(equipe, mois)

    # Derniers matchs d'une équipe
    if equipe and _contient(q, MOTS_DERNIERS):
        return _handler_derniers_matchs(equipe)

    # Matchs nuls
    if _contient(q, MOTS_NUL):
        return _handler_matchs_nuls(equipe)

    # Classement
    if _contient(q, MOTS_CLASSEMENT):
        return _handler_classement(competition)

    # Meilleure attaque
    if _contient(q, MOTS_ATTAQUE):
        return _handler_meilleure_attaque()

    # Meilleure défense
    if _contient(q, MOTS_DEFENSE):
        return _handler_meilleure_defense()

    # Stats d'une compétition
    if competition and _contient(q, MOTS_COMPETITION + MOTS_MATCHS):
        return _handler_stats_competition(competition)

    # Total matchs
    if _contient(q, MOTS_MATCHS):
        return _handler_total_matchs(competition, mois)

    # Si équipe détectée mais intention inconnue
    if equipe:
        return _handler_derniers_matchs(equipe)

    return INCOMPRIS, None
