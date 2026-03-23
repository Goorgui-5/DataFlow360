# FootStream

> Plateforme de Business Intelligence dédiée à l'analyse de la performance sportive — pipeline de données complet, Data Warehouse analytique, dashboard BI interactif, chatbot en langage naturel et monitoring en temps réel.

---

## Description

FootStream est une architecture data moderne construite de bout en bout autour des données de matchs de football. Elle couvre l'intégralité du cycle de la donnée :

- **Ingestion** : collecte automatisée depuis l'API [football-data.org](https://www.football-data.org/) via Apache Kafka
- **Stockage** : persistance dans PostgreSQL (couche OLTP)
- **Data Warehouse** : modélisation en étoile dans le schéma `dw` (PostgreSQL)
- **Analyse** : 11 vues SQL analytiques (KPIs)
- **Restitution** : plateforme BI Streamlit + Plotly (6 pages navigables)
- **Chatbot** : assistant analytique en langage naturel (français)
- **Orchestration** : pipeline automatisé via Apache Airflow (DAG horaire)
- **Monitoring** : dashboard Grafana avec 10 panneaux et 5 checks qualité

---

## Architecture

```
API football-data.org
        │
        ▼
streaming_api_producer.py  ──►  Kafka (topic: live_matches)
                                        │
                                        ▼
                            kafka_to_postgres.py
                                        │
                                        ▼
                            PostgreSQL — schéma public
                            (matchs_en_direct, matchs_historique)
                                        │
                                        ▼
                                  load_dw.py
                                        │
                                        ▼
                            PostgreSQL — schéma dw
                            ┌───────────────────────────┐
                            │  dw.fait_match (2 703...) │
                            │  dw.dim_equipe (258)      │
                            │  dw.dim_competition (13)  │
                            │  dw.dim_date (177...)     │
                            └───────────────────────────┘
                                        │
                          ┌─────────────┴─────────────┐
                          ▼                           ▼
                  Streamlit + Plotly            Grafana
                  (Plateforme BI)            (Monitoring)
```

---

## Stack technologique

┌────────────────────────────────────────────────────────┐
| Technologie        | Rôle                              |
|--------------------|-----------------------------------|
| PostgreSQL         | Stockage OLTP + Data Warehouse    |
| Apache Kafka       | Streaming / ingestion temps réel  |
| Apache Airflow     | Orchestration du pipeline         |
| Streamlit          | Plateforme BI interactive         |
| Plotly             | Visualisations graphiques         |
| Grafana            | Monitoring & qualité des données  |
| Docker Compose     | Infrastructure Kafka + Zookeeper  |
| Python             | Développement ETL + BI + Chatbot  |
| GitHub             | Versioning du code source         |
└────────────────────────────────────────────────────────┘

---

## Structure du projet

```
DATAFLOW360/
├── airflow/
│   └── dags/
│       └── dag_dataflow360.py          # DAG Airflow
├── bi_platform/
│   ├── app.py                         # Point d'entrée Streamlit
│   ├── chatbot_engine.py              # Moteur chatbot NLP
│   ├── db_connection.py               # Connexion PostgreSQL
│   ├── requirements.txt
│   ├── components/
│   │   ├── charts.py
│   │   ├── filters.py
│   │   └── kpi_cards.py
│   └── views/
│       ├── chatbot.py
│       ├── performance_competition.py
│       ├── performance_equipe.py
│       ├── performance_globale.py
│       ├── resultats_historiques.py
│       └── stats_derniers_matchs.py
├── generator/streaming_foot/
│   ├── streaming_api_producer.py      # Producteur Kafka
│   ├── kafka_to_postgres.py           # Consommateur Kafka
│   └── load_dw.py                     # Chargement Data Warehouse
├── EnvDF/                             # Environnement virtuel Python
├── .env                               # Variables d'environnement (non versionné)
├── .gitignore
├── docker-compose.yml                 # Kafka + Zookeeper
└── README.md
```

---

## Installation & Lancement

### Prérequis

- Python
- PostgreSQL 
- Docker & Docker Compose
- Git

### 1. Cloner le projet

```bash
git clone https://github.com/Goorgui/DATAFLOW360.git
cd footstream
```

### 2. Créer l'environnement virtuel

```bash
python3 -m venv EnvDF
source EnvDF/bin/activate
pip install -r requirements.txt
```

### 3. Configurer les variables d'environnement

Créer un fichier `.env` à la racine :

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=footstream
DB_USER=postgres
DB_PASSWORD=votre_mot_de_passe
API_TOKEN=votre_token_football_data
```

### 4. Lancer Kafka avec Docker

```bash
docker-compose up -d
```

### 5. Créer la base de données

```bash
psql -U postgres -c "CREATE DATABASE footstream;"
psql -U postgres -d footstream -f scripts/init_db.sql
```

### 6. Lancer le pipeline

```bash
# Producteur Kafka
python3 generator/streaming_foot/streaming_api_producer.py

# Consommateur Kafka (dans un autre terminal)
python3 generator/streaming_foot/kafka_to_postgres.py

# Chargement Data Warehouse
python3 generator/streaming_foot/load_dw.py
```

### 7. Lancer la plateforme BI

```bash
cd bi_platform
streamlit run app.py
```

Accès : [http://localhost:8501](http://localhost:8501)

### 8. Lancer Airflow

```bash
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
airflow standalone
```

Accès : [http://localhost:8090](http://localhost:8090) — admin / admin123

### 9. Lancer Grafana

```bash
sudo systemctl start grafana-server
```

Accès : [http://localhost:3000](http://localhost:3000)

---

## Données couvertes

- **13 compétitions** : Premier League, Ligue 1, Bundesliga, Serie A, Primera Division, Eredivisie, Primeira Liga, Championship, Copa Libertadores, Campeonato Brasileiro, UEFA Champions League, Ligue 1 Sénégal, Ligue 2 Sénégal
- **258 équipes**
- **2 703 matchs** (saison 2025–2026)

---

## Auteur

**Serigne Babacar KANE**
Formation Développement Data — Orange Digital Center / Sonatel Académie
Dakar, Sénégal — 2025/2026

---

## Licence

Projet académique — Orange Digital Center / Sonatel Académie