-- Schéma PostgreSQL pour les données de saisons de football pour barcelone

-- Création de la table saisons
CREATE TABLE IF NOT EXISTS saisons (
    id SERIAL PRIMARY KEY,
    saison VARCHAR(20),
    equipe VARCHAR(100),
    pays VARCHAR(50),
    competition VARCHAR(100),
    classement VARCHAR(50),
    matchs_joues SMALLINT,
    victoires SMALLINT,
    nuls SMALLINT,
    defaites SMALLINT,
    buts_pour SMALLINT,
    buts_contre SMALLINT,
    difference_buts SMALLINT,
    points SMALLINT,
    presence_moyenne BIGINT,
    meilleur_buteur VARCHAR(150),
    gardien_de_but VARCHAR(150),
    remarques TEXT
);

