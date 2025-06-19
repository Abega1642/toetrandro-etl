# 🌤️ Toetrandro-etl Ø Travel Recommendation Based on Climate

## 🧭 Project Overview

**Toetrandro-etl** is a complete ETL and data analytics solution designed to
collect, process, model, and visualize weather data to answer a real-world
question:

> 🗺️ *When is the best time to visit a city based on weather conditions?*__

The project is built using:

- **Python**, **Pandas**, and **Jupyter Notebooks** for data processing and
  analysis
- **Apache Airflow** for automation and orchestration
- **OpenWeather API** and external sources for weather data
- **Interactive dashboard** for business intelligence and insights

---

## 🎯 Project Goals

- 📦 Automate data extraction, transformation, and loading (ETL) using Apache 
  Airflow
- 🌍 Combine historical and real-time weather data
- 🧼 Clean and model data to extract meaningful indicators
- 📊 Build an interactive dashboard to recommend the best travel periods based 
  on weather

---

## 🌐 Use Case: Climate & Tourism

### ❓ Problem Statement

> Can we recommend the best times to visit a city based on weather criteria?

### 📈 Example Metrics

- ✅ Ideal temperature range (e.g., between 22°C and 28°C)
- 🌧️ Lowest precipitation and wind speed
- 📅 Monthly weather scores for each city

---

## ⚙️ Technical Stack

| Layer         | Tools/Tech Used                   |
| ------------- |-----------------------------------|
| Automation    | Apache Airflow                    |
| Data Handling | Python, Pandas                    |
| Data Sources  | OpenWeather API, CSV/Kaggle       |
| Visualization | Jupyter Notebooks, Metabase       |
| Orchestration | DAGs with task-based architecture |

---

## 🛠️ Features

- 📡 **Daily automated extraction** of real-time weather data
- 📂 **Historical dataset integration** from various sources (CSV, APIs, etc.)
- 🔄 **ETL Pipeline** structured into clear Airflow tasks: `extract`, `clean`, 
  `merge`, `save`
- 🧽 **Data Cleaning & Normalization** for consistency across sources
- 🌟 **Star or Snowflake Schema Modeling** for reporting-friendly structure
- 📊 **Interactive Dashboard** with dynamic filters (city, month, metric…)

---

## 📁 Repository Structure

```
toetrandro-etl/
├── airflow/
│   ├── dags/                    # Airflow DAGs
│   └── config/                  # Airflow variables, settings
├── data/
│   ├── raw/                     # Raw extracted data
│   └── processed/               # Cleaned, transformed data
├── notebooks/                  # Jupyter Notebooks for analysis & viz
├── src/
│   ├── api/                     # OpenWeather API client
│   ├── processing/             # Data transformation scripts
│   └── utils/                  # Logging, helpers
├── tests/                      # Unit tests
├── requirements.txt            # Dependencies
└── README.md
```

---

## 🔁 Pipeline Logic (Airflow DAG)

1. **Extract Task** – Calls OpenWeather API and fetches raw weather data
2. **Clean Task** – Cleans and standardizes the data
3. **Merge Task** – Combines real-time and historical weather data
4. **Save Task** – Saves processed data for dashboard use

---

## 📊 Dashboard Overview

- 🌍 Weather indicators by **city** and **month**
- 📅 Monthly **climate scores**
- 🔄 Dynamic filters (city, time, metric)
- 📈 Comparison charts to evaluate best travel windows

---

## 🧪 Testing & Reliability

- Unit tests for each component (`api`, `processing`, etc.)
- Logs and retry logic integrated into Airflow tasks
- Secure handling of API keys via `.env` and Airflow Connections

---

## 🚀 Getting Started

1. Clone the repository\
   `git clone https://github.com/Abega1642/toetrandro-etl.git`

2. Install dependencies\
   `pip install -r requirements.txt`

3. Set environment variables\
   Create a `.env` file with your OpenWeather API key.

4. Initialize Airflow
   ```bash
   airflow db init
   airflow users create --username admin ...
   ```

5. Launch Airflow\
   
   ```bash
     airflow api-server & airflow scheduler & airflow dag-processor
   ```

---

## 📌 Future Improvements

- Add support for more cities and weather APIs
- Improve dashboard interactivity with maps and heatmaps
- Automate deployment with Docker & CI/CD

---

## 👥 Authors

- Abegà Razafindratelo

---

## 📄 License

This project is licensed under the MIT License.
