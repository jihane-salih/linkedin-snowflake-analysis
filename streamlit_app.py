import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# =========================
# CONFIG
# =========================
st.set_page_config(
    page_title="Analyse des emplois LinkedIn",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================
# STYLE (ADAPTATIF)
# =========================
st.markdown("""
<style>

.main-title {
    font-size: 42px;
    font-weight: 800;
    text-align: center;
    color: var(--text-color);
    margin-bottom: 5px;
}

.sub-title {
    text-align: center;
    color: var(--text-color);
    opacity: 0.7;
    font-size: 16px;
    margin-bottom: 20px;
}

.kpi-card {
    background: var(--secondary-background-color);
    padding: 20px;
    border-radius: 16px;
    text-align: center;
    box-shadow: 0 6px 15px rgba(0,0,0,0.1);
    border: 1px solid rgba(0,0,0,0.05);
}

.kpi-value {
    font-size: 28px;
    font-weight: 700;
    color: #0A66C2;
}

.kpi-label {
    color: var(--text-color);
    opacity: 0.7;
    font-size: 13px;
}

.section-title {
    font-size: 20px;
    font-weight: 700;
    color: var(--text-color);
    margin-top: 25px;
    margin-bottom: 10px;
}

</style>
""", unsafe_allow_html=True)

# =========================
# HEADER
# =========================
st.markdown("""
<div style="text-align:center; margin-top:10px;">
    <img src="https://upload.wikimedia.org/wikipedia/commons/c/ca/LinkedIn_logo_initials.png" width="55">
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-title">Analyse du marché de l’emploi LinkedIn</div>
<div class="sub-title">Analyse des tendances : postes, salaires et industries</div>
""", unsafe_allow_html=True)

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
# CLEAN
# =========================
jobs = jobs[~jobs["INDUSTRY_NAME"].str.contains("NOT CLASSIFIED|OTHER", na=False)]
industry_dist = industry_dist[~industry_dist["INDUSTRY_NAME"].str.contains("NOT CLASSIFIED|OTHER", na=False)]

# =========================
# KPI
# =========================
st.markdown("## 📊 Vue globale")

c1, c2, c3, c4 = st.columns(4)

kpis = [
    ("Industries", jobs["INDUSTRY_NAME"].nunique()),
    ("Postes", jobs["TITLE"].nunique()),
    ("Données salariales", len(salary)),
    ("Total des offres", len(jobs))
]

for col, (label, value) in zip([c1, c2, c3, c4], kpis):
    col.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)

# =========================
# ESPACE
# =========================
st.markdown("<div style='margin-top:50px;'></div>", unsafe_allow_html=True)

# =========================
# ANALYSE
# =========================
st.markdown("## 📌 Analyse détaillée du marché")

col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section-title">🎯 Top 10 des titres de postes les plus publiés par industrie</div>', unsafe_allow_html=True)

    industry_job = st.selectbox(
        "Choisir une industrie",
        sorted(jobs["INDUSTRY_NAME"].unique())
    )

    filtered_jobs = jobs[jobs["INDUSTRY_NAME"] == industry_job]
    top_jobs = filtered_jobs.sort_values("TOTAL_JOBS", ascending=False).head(10)

    chart_jobs = alt.Chart(top_jobs).mark_bar(color="#0A66C2").encode(
        x="TOTAL_JOBS:Q",
        y=alt.Y("TITLE:N", sort="-x"),
        tooltip=["TITLE", "TOTAL_JOBS"]
    )

    st.altair_chart(chart_jobs, use_container_width=True)

with col2:
    st.markdown('<div class="section-title">💰 Top 10 des postes les mieux rémunérés par industrie</div>', unsafe_allow_html=True)

    industry_salary = st.selectbox(
        "Choisir une industrie",
        sorted(salary["INDUSTRY_NAME"].unique()),
        key="salary"
    )

    filtered_salary = salary[salary["INDUSTRY_NAME"] == industry_salary]
    top_salary = filtered_salary.sort_values("MAX_SALARY_YEARLY").tail(10)

    chart_salary = alt.Chart(top_salary).mark_bar(color="#1D9BF0").encode(
        x="MAX_SALARY_YEARLY:Q",
        y=alt.Y("TITLE:N", sort="-x"),
        tooltip=["TITLE", "MAX_SALARY_YEARLY"]
    )

    st.altair_chart(chart_salary, use_container_width=True)

# =========================
# OVERVIEW
# =========================
st.markdown("## 🌍 Vue d’ensemble du marché")

c1, c2 = st.columns(2)

with c1:
    st.markdown('<div class="section-title">Répartition des offres d’emploi par secteur d’activité</div>', unsafe_allow_html=True)

    chart_industry = alt.Chart(industry_dist).mark_bar(color="#60A5FA").encode(
        x="TOTAL_JOBS:Q",
        y=alt.Y("INDUSTRY_NAME:N", sort="-x")
    )

    st.altair_chart(chart_industry, use_container_width=True)

with c2:
    st.markdown('<div class="section-title">💼 Répartition des offres d’emploi par type d’emploi</div>', unsafe_allow_html=True)

    pie = alt.Chart(work_type).mark_arc().encode(
        theta="TOTAL_JOBS:Q",
        color=alt.Color("FORMATTED_WORK_TYPE:N", scale=alt.Scale(scheme="blues")),
        tooltip=["FORMATTED_WORK_TYPE", "TOTAL_JOBS"]
    )

    st.altair_chart(pie, use_container_width=True)

# =========================
# COMPANY SIZE
# =========================
st.markdown("## 🏢 Répartition des offres par taille d’entreprise")

company_size = company_size.sort_values("TOTAL_OFFERTS")

chart_company = alt.Chart(company_size).mark_bar(color="#1D9BF0").encode(
    x="TOTAL_OFFERTS:Q",
    y="COMPANY_SIZE_LABEL:N"
)

st.altair_chart(chart_company, use_container_width=True)

# =========================
# FOOTER
# =========================
st.markdown("---")

st.markdown("""
<div style="text-align:center; opacity:0.6;">
    <img src="https://upload.wikimedia.org/wikipedia/commons/c/ca/LinkedIn_logo_initials.png" width="22">
</div>
""", unsafe_allow_html=True)