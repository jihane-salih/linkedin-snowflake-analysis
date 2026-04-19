# 🧊 LinkedIn Job Market Analysis (Snowflake + Streamlit)

---

## 🎯 Project Objective

This project aims to analyze LinkedIn job postings using a complete data pipeline built with **Snowflake** and visualized with **Streamlit**.

The goal is to transform raw job data into meaningful insights to understand:
- Job market trends
- Salary distribution
- Industry demand
- Company size distribution

---

## 🏗️ Data Architecture

The project follows a **Medallion Architecture**:

- 🟫 **Bronze Layer**: Raw data ingestion from S3 (CSV & JSON)
- ⚪ **Silver Layer**: Data cleaning and transformation
- 🟡 **Gold Layer**: Business-ready analytical tables

![Architecture](images/architecture_linkedin.png)

---

## 📊 Key Analyses

This project includes several analytical views:

### 📌 Job Demand Analysis
Top job titles per industry to identify market demand.

### 💰 Salary Analysis
Highest-paying roles across different industries.

### 🌍 Industry Distribution
Which industries publish the most job offers.

### 💼 Work Type Analysis
Distribution of job types (full-time, part-time, etc.).

### 🏢 Company Size Analysis
Breakdown of job offers by company size.

---

## 🛠️ Tech Stack

- Snowflake ❄️ (Data Warehouse)
- SQL (Data Modeling)
- Python 🐍
- Streamlit 📊 (Dashboard)
- Altair (Data Visualization)
- Git & GitHub

---

## 🚀 How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run Streamlit app
streamlit run streamlit_app.py