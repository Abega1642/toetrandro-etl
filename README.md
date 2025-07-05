# 🌤️ Toetrandro-etl — Travel Recommendation Based on Climate

## 🧭 Project Overview

**Toetrandro-etl** is a full-stack ETL and analytics pipeline that collects, processes, and visualizes weather data to answer a real-world question:

> 🗺️ *When is the best time to visit a city based on weather conditions?*

This project combines automation, data modeling, and interactive dashboards to deliver actionable travel recommendations.

---

## 🎯 Project Goals

- 📦 Automate daily ETL workflows using Apache Airflow
- 🌍 Integrate real-time and historical weather data
- 🧼 Clean and model data for climate-based travel scoring
- 📊 Visualize insights through an interactive dashboard

---

## 🌐 Use Case: Climate & Tourism

### ❓ Problem Statement

> Can we recommend the best times to visit a city based on weather comfort?

### 📈 Key Metrics

- ✅ Ideal temperature range (e.g., 22°C–28°C)
- 🌧️ Low precipitation and wind speed
- 📅 Monthly comfort scores and ideal day counts

---

## ⚙️ Technical Stack

| Layer         | Tools/Tech Used                   |
|---------------|-----------------------------------|
| Automation    | Apache Airflow                    |
| Data Handling | Python, Pandas, GeoCoder          |
| Data Sources  | OpenWeather API, CSV/OpenMeteo    |
| Visualization | Jupyter Notebooks, Metabase       |
| Orchestration | DAGs with task-based architecture |

---

## 🛠️ Features

- 📡 **Daily automated extraction** of weather data
- 📂 **Historical dataset integration** (CSV, APIs)
- 🔄 **ETL Pipeline** with modular Airflow tasks: `extract`, `transform`, `merge`, `migrate`
- 🧽 **Data Cleaning & Normalization** for schema consistency
- 🌟 **Star Schema Modeling** for analytics-ready structure
- 📊 **Interactive Dashboard** with filters by city, month, and metric

---

## 📁 Repository Structure

```
toetrandro-etl/
├── workflows/
│   ├── dags/                   # Airflow DAGs
│   ├── scripts/                # Task logic
│   └── config/                 # Airflow variables/settings
├── data/
│   ├── raw/                    # Raw extracted data
│   ├── merged/                 # Final merged dataset
│   └── processed/              # Cleaned, transformed data
├── notebooks/                  # Jupyter Notebooks for EDA & modeling
├── src/
│   ├── api/                    # OpenWeather API client
│   ├── core/                   # ETL logic
│   └── utils/                  # Logging, helpers
├── tests/                      # Unit tests
├── requirements.txt            # Dependencies
└── README.md
```

---

## 🔁 Pipeline Logic (Airflow DAG)

1. **establish_city_config** – Defines cities and config
2. **extract_weather_data** – Pulls real-time weather from OpenWeather API
3. **transform_enriched_data** – Cleans and enriches the dataset
4. **merge_processed_files** – Combines historical and real-time data
5. **migrate_data_to_postgres** – Loads data into a star schema in PostgreSQL

---

## 📊 Dashboard Overview

The dashboard provides a rich, visual summary of climate comfort across cities and seasons.

Navigate through the global insights :

![Global_Toetrandro Dashboard](doc/dashboard/global.png)


Or navigate through the city that interests you :

![Local_Toetrandro_Dashboard](doc/dashboard/local.png)

### Key Insights Displayed:

- 🏆 **City with Highest Annual Comfort Score**: Mahajanga
- 📅 **Total Ideal Days Recorded**: 688
- 🌟 **Best Cities to Visit**: Ranked by average comfort score
- 📊 **Top 3 Cities with Most Ideal Days**: Toliara, Mahajanga, Paris
- 📆 **Best Months to Travel**: June, July, and September
- ❄️ **Seasonal Comfort by City**: Compare comfort scores across seasons

> This dashboard helps travelers identify the most comfortable times and places to visit, based on real weather data.

---

## 🧪 Testing & Reliability

- ✅ Unit tests for all ETL components
- 🔁 Retry logic and logging in Airflow tasks
- 🔐 Secure API key handling via `.env` and Airflow Connections

---

## 🚀 Getting Started

1. **Clone the repository**  
   ```bash
   git clone https://github.com/Abega1642/toetrandro-etl.git
   ```

2. **Install dependencies**  
   ```bash
   pip install -r requirements.txt
   ```

3. **Set environment variables**  
   Create a `.env` file with your OpenWeather API key.

4. **Initialize Airflow**  
   ```bash
   airflow db init
   airflow users create --username admin ...
   ```

5. **Launch Airflow**  
   ```bash
   airflow scheduler &
   airflow webserver &
   ```

---

## 📌 Future Improvements

- 🌍 Add more cities and weather APIs
- 🗺️ Enhance dashboard with maps and geospatial filters
- 🐳 Dockerize the pipeline for easier deployment
- 🔁 Add CI/CD for automated testing and deployment

---

## 👥 Author

- **Abegà Razafindratelo**

---

## 📄 License

This project is licensed under the MIT License.
```
