# 🧊 Lab — Analyse des Offres d’Emploi LinkedIn avec Snowflake

---

## 🎯 Objectif

Ce projet a pour objectif d’analyser les offres d’emploi issues de LinkedIn en utilisant Snowflake pour le traitement des données et Streamlit pour la visualisation.

L’objectif est de transformer des données brutes en indicateurs exploitables afin de comprendre les tendances du marché de l’emploi.

---

## 🏗️ Architecture des données

Le pipeline suit une architecture **Medallion** :

* **BRONZE** : ingestion des données brutes
* **SILVER** : nettoyage et transformation
* **GOLD** : données prêtes pour l’analyse

---

# 🟫 1) BRONZE — Ingestion des données

## 🔹 Description

Cette étape consiste à charger les données brutes depuis un bucket S3 dans Snowflake sans transformation.

Nous utilisons :

* un stage externe
* des formats CSV et JSON
* la commande COPY INTO

---

## 🔹 Création du stage et formats

```sql
CREATE OR REPLACE STAGE linkedin_stage
URL = 's3://snowflake-lab-bucket/';

CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE FILE FORMAT json_format
TYPE = JSON;
```

👉 Le stage permet de connecter Snowflake au bucket S3 contenant les données.

---

## 🔹 Chargement des données

```sql
COPY INTO BRONZE.JOB_POSTINGS
FROM @linkedin_stage/job_postings.csv
FILE_FORMAT = csv_format;
```

👉 COPY INTO permet de charger les données sans transformation.

---

# ⚪ 2) SILVER — Nettoyage des données

## 🔹 Description

Cette étape permet de nettoyer et transformer les données afin de les rendre exploitables.

---

## 🔹 Nettoyage des entreprises

```sql
CREATE OR REPLACE TABLE SILVER.COMPANIES_CLEAN AS
SELECT
    TRIM(TO_VARCHAR(f.value:"company_id")) AS company_id,
    TRIM(f.value:"name") AS company_name,
    f.value:"company_size"::NUMBER AS company_size,

    CASE
        WHEN f.value:"company_size"::NUMBER BETWEEN 0 AND 1 THEN 'Startup'
        WHEN f.value:"company_size"::NUMBER = 2 THEN 'Small'
        WHEN f.value:"company_size"::NUMBER = 3 THEN 'Medium'
        WHEN f.value:"company_size"::NUMBER >= 4 THEN 'Large'
        ELSE 'Unknown'
    END AS company_size_label

FROM BRONZE.COMPANIES,
LATERAL FLATTEN(input => data) f;
```

👉 Les données JSON sont transformées en colonnes exploitables.

---

## 🔹 Nettoyage des salaires

```sql
TRY_TO_NUMBER(REGEXP_REPLACE(max_salary, '[^0-9]', ''))
```

👉 On supprime les caractères non numériques puis on convertit en nombre.

---

# 🟨 3) GOLD — Analyse métier

## 🔹 Top 10 des postes par industrie

```sql
CREATE OR REPLACE TABLE GOLD.TOP_JOBS_BY_INDUSTRY AS
SELECT *
FROM (
    SELECT
        industry_name,
        title,
        COUNT(*) AS total_jobs,
        ROW_NUMBER() OVER (
            PARTITION BY industry_name
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM ...
)
WHERE rn <= 10;
```

👉 ROW_NUMBER permet de récupérer les 10 premiers résultats par industrie.

---

## 🔹 Répartition des entreprises

```sql
CREATE OR REPLACE TABLE GOLD.COMPANY_SIZE AS
SELECT
    company_size_label,
    COUNT(*) AS total_offers
FROM SILVER.COMPANIES_CLEAN
GROUP BY company_size_label;
```

---

# 📊 4) Visualisation avec Streamlit

## 🔹 Description

Nous avons développé un tableau de bord interactif permettant :

* de filtrer par industrie
* de visualiser les salaires
* d’explorer les tendances

---

## 🔹 Exemple de code

```python
session = get_active_session()
data = session.sql("SELECT * FROM GOLD.TOP_JOBS_BY_INDUSTRY").to_pandas()
st.bar_chart(data)
```

---

# 📸 Résultats

### Dashboard

![Dashboard](images/dashboard.png)

---

# ❗ Problèmes rencontrés

* Données manquantes dans company_size
* Salaires stockés en texte
* Jointure incorrecte

---

# ✅ Solutions apportées

* Utilisation de TRY_TO_NUMBER
* Nettoyage avec REGEXP
* Jointure corrigée avec table enrichie

---

# 👥 Répartition des tâches

* Jihane : pipeline Snowflake
* Partenaire : dashboard Streamlit

---

# 🚀 Conclusion

Ce projet met en œuvre un pipeline de données complet permettant de transformer des données brutes en analyses exploitables via un tableau de bord interactif.
