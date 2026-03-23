import streamlit as st
from chatbot_engine import chatbot_response


# Exemples de questions pour guider l'utilisateur
EXEMPLES = [
    "Combien de buts a marqué Fluminense FC ?",
    "Classement de la Premier League ?",
    "Derniers matchs de SSC Napoli",
    "Meilleure attaque de la saison",
    "Meilleure défense",
    "Combien de matchs nuls en Ligue 1 ?",
    "Stats de la Bundesliga",
    "Combien de matchs au total ?",
]


def page_chatbot():

    st.header(" Assistant BI Football")
    st.markdown(
        "Posez vos questions en **français** sur les données football. "
        "Je réponds uniquement sur la base des données disponibles dans le data warehouse."
    )
    st.divider()

    # ---- Initialisation de l'historique ----
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    if "memory" not in st.session_state:
        st.session_state.memory = {}

    # ---- Exemples cliquables ----
    with st.expander(" Exemples de questions", expanded=False):
        cols = st.columns(2)
        for i, exemple in enumerate(EXEMPLES):
            with cols[i % 2]:
                if st.button(exemple, key=f"ex_{i}", use_container_width=True):
                    st.session_state["question_auto"] = exemple

    # ---- Champ de saisie ----
    question_auto = st.session_state.pop("question_auto", "")
    question = st.chat_input("Posez votre question ici...")

    # Priorité : exemple cliqué ou question saisie
    q = question_auto or question

    # ---- Traitement de la question ----
    if q:
        st.session_state.chat_history.append({"role": "user", "content": q})

        with st.spinner("🔍 Recherche en cours..."):
            reponse, df = chatbot_response(q, st.session_state.memory)

        st.session_state.chat_history.append({
            "role": "assistant",
            "content": reponse,
            "dataframe": df,
        })

    # ---- Affichage de l'historique ----
    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.markdown(msg["content"])
        else:
            with st.chat_message("assistant"):
                st.markdown(msg["content"])
                if msg.get("dataframe") is not None and not msg["dataframe"].empty:
                    st.dataframe(msg["dataframe"], use_container_width=True, hide_index=True)

    # ---- Bouton effacer l'historique ----
    if st.session_state.chat_history:
        st.divider()
        if st.button("🗑️ Effacer la conversation", use_container_width=False):
            st.session_state.chat_history = []
            st.rerun()
