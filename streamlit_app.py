import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# =========================
# CONFIG
# =========================
st.set_page_config(
    page_title="LinkedIn Analyse des Emplois",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================
# COLORS (PRO PALETTE)
# =========================
COLORS = {
    "primary": "#0A66C2",
    "secondary": "#10B981",
    "accent": "#F59E0B",
    "neutral": "#94A3B8"
}

# =========================
# STYLE
# =========================
st.markdown("""
<style>
    .main-title {
        font-size: 40px;
        font-weight: 700;
        color: #0A66C2;
        text-align: center;
        margin-bottom: 10px;
    }

    .sub-title {
        font-size: 18px;
        text-align: center;
        color: #666;
        margin-bottom: 30px;
    }

    .logo-center {
        display: flex;
        justify-content: center;
        margin-top: 10px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# =========================
# HEADER (CENTRÉ)
# =========================
st.markdown(
    """
    <div class="logo-center">
        <img src="https://upload.wikimedia.org/wikipedia/commons/c/ca/LinkedIn_logo_initials.png" width="90">
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown('<div class="main-title">🧊 Tableau de bord - Analyse des emplois LinkedIn</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">Explorer les tendances du marché de l’emploi : postes, salaires, industries et entreprises</div>', unsafe_allow_html=True)

# =========================
# SESSION
# =========================
session = get_active_session()

# =========================
# DATA
# =========================
jobs = session.sql("SELECT * FROM GOLD.TOP_JOBS_BY_INDUSTRY").to_pandas()
salary = session.sql("SELECT * FROM GOLD.TOP_SALARIES_BY_INDUSTRY").to_pandas()
industry_dist = session.sql("SELECT * FROM GOLD.INDUSTRY_DISTRIBUTION").to_pandas()
company_size = session.sql("SELECT * FROM GOLD.COMPANY_SIZE").to_pandas()
work_type = session.sql("SELECT * FROM GOLD.WORK_TYPE").to_pandas()

# =========================
# CLEAN DATA
# =========================
industry_dist = industry_dist[
    ~industry_dist["INDUSTRY_NAME"].str.contains("NOT CLASSIFIED|OTHER", case=False, na=False)
]

jobs = jobs[
    ~jobs["INDUSTRY_NAME"].str.contains("NOT CLASSIFIED|OTHER", case=False, na=False)
]

# =========================
# CHECK
# =========================
if jobs.empty or salary.empty:
    st.error("❌ Aucune donnée disponible. Vérifiez le pipeline Snowflake.")
    st.stop()

# =========================
# KPI
# =========================
st.markdown("## 📊 Indicateurs globaux")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Industries", jobs["INDUSTRY_NAME"].nunique())
col2.metric("Titres de postes", jobs["TITLE"].nunique())
col3.metric("Données salariales", len(salary))
col4.metric("Nombre total d’offres", len(jobs))

st.divider()

# =========================================================
# JOB ANALYSIS
# =========================================================
st.markdown("## 📌 Analyse du marché de l’emploi")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🎯 Sélectionner une industrie (postes)")

    industry_job = st.selectbox(
        "Choisir une industrie",
        sorted(jobs["INDUSTRY_NAME"].unique())
    )

    filtered_jobs = jobs[jobs["INDUSTRY_NAME"] == industry_job]
    top_jobs = filtered_jobs.sort_values("TOTAL_JOBS", ascending=False).head(10)

    chart_jobs = alt.Chart(top_jobs).mark_bar().encode(
        x=alt.X("TITLE:N", sort="-y", axis=alt.Axis(labelAngle=-40)),
        y=alt.Y("TOTAL_JOBS:Q"),
        color=alt.value(COLORS["primary"]),
        tooltip=["TITLE", "TOTAL_JOBS"]
    )

    st.altair_chart(chart_jobs, use_container_width=True)

with col2:
    st.markdown("### 💰 Sélectionner une industrie (salaires)")

    industry_salary = st.selectbox(
        "Choisir une industrie",
        sorted(salary["INDUSTRY_NAME"].unique()),
        key="salary"
    )

    filtered_salary = salary[salary["INDUSTRY_NAME"] == industry_salary]
    top_salary = filtered_salary.sort_values("MAX_SALARY_YEARLY", ascending=False).head(10)

    chart_salary = alt.Chart(top_salary).mark_bar().encode(
        x=alt.X("TITLE:N", sort="-y", axis=alt.Axis(labelAngle=-40)),
        y=alt.Y("MAX_SALARY_YEARLY:Q"),
        color=alt.value(COLORS["secondary"]),
        tooltip=["TITLE", "MAX_SALARY_YEARLY"]
    )

    st.altair_chart(chart_salary, use_container_width=True)

st.divider()

# =========================================================
# MARKET OVERVIEW
# =========================================================
st.markdown("## 🌍 Vue d’ensemble du marché")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🏭 Offres par industrie")

    chart_industry = alt.Chart(industry_dist).mark_bar().encode(
        x=alt.X("INDUSTRY_NAME:N", sort="-y"),
        y=alt.Y("TOTAL_JOBS:Q"),
        color=alt.Color("INDUSTRY_NAME:N", legend=None, scale=alt.Scale(scheme="blues"))
    )

    st.altair_chart(chart_industry, use_container_width=True)

with col2:
    st.markdown("### 💼 Répartition des types de contrat")

    pie = alt.Chart(work_type).mark_arc().encode(
        theta="TOTAL_JOBS:Q",
        color=alt.Color("FORMATTED_WORK_TYPE:N", scale=alt.Scale(scheme="set2"))
    )

    st.altair_chart(pie, use_container_width=True)

st.divider()

# =========================================================
# COMPANY SIZE
# =========================================================
st.markdown("## 🏢 Répartition des offres d’emploi par taille d’entreprise")

company_size = company_size.sort_values("TOTAL_OFFERTS", ascending=True)

chart_company = alt.Chart(company_size).mark_bar().encode(
    x=alt.X("TOTAL_OFFERTS:Q"),
    y=alt.Y("COMPANY_SIZE_LABEL:N", sort=None),
    color=alt.Color("COMPANY_SIZE_LABEL:N", legend=None, scale=alt.Scale(scheme="tealblues"))
)

st.altair_chart(chart_company, use_container_width=True)

st.divider()

# =========================================================
# INSIGHT
# =========================================================
top_industry = jobs.groupby("INDUSTRY_NAME")["TOTAL_JOBS"].sum().idxmax()

st.success(f"""
Industrie la plus active : {top_industry}  
Cette industrie présente le plus grand nombre d’offres d’emploi.
""")