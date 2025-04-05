

# 🚀 Scala Data Pipeline – Open Data Project

> **Scala Template** is a modular data engineering application built with Scala and Spark.  
> It demonstrates a complete pipeline for reading, processing, and exporting open data.

---

## 📌 Description

This application simulates a real-world data engineering pipeline based on open data related to salaries in France from **2015 to 2025**.  
The project includes three main components:
- **Reader**: Loads data from various sources (CSV, Parquet, Hive)
- **Processor**: Transforms and aggregates the data (reports, statistics)
- **Writer**: Outputs the results in the configured format

> 🔧 The app is entirely configurable via `application.properties`

---

## 🗂️ Project Structure

```
scala_template/
├── src/
│   └── main/
│       ├── scala/fr/mosef/scala/template/
│       │   ├── reader/
│       │   ├── processor/
│       │   ├── writer/
│       │   ├── job/
│       │   ├── utils/
│       │   └── Main.scala
│       └── resources/
│           ├── application.properties
│           ├── salaires_france_2015_2025.csv
│           └── salaires_api.csv
├── output/
│   └── (exported CSV reports)
├── .github/
├── pom.xml
└── README.md
```

---

## ⚙️ Features

### ✅ Reader Module
- Reads CSV (configurable separator, with/without headers)
- Reads Hive tables
- Reads Parquet files
- Supports schema inference or manual schema definition

### 🔄 Processor Module
Generates 5 reports:
1. Average salary by gender
2. Average salary by age group and gender
3. Top 10 highest-paying regions
4. Salary gap by socio-professional category
5. Gender pay gap evolution over time

### 📤 Writer Module
- Outputs in CSV, Parquet or Hive
- Output format & path controlled via `application.properties`

### 🌐 API Fetcher (Bonus)
- Simulates API data ingestion (downloads or copies file into resources folder)
- Prepares pipeline input with up-to-date data

---

## 🛠️ Technologies

- **Scala 2.13**
- **Apache Spark**
- **Maven**
- **GitHub Actions** (CI/CD)
- Optional: Shell script / API mock

---

## 🔧 Configuration (application.properties)

```properties
input.path=src/main/resources/salaires_api.csv
input.format=csv
separator=;
hasHeader=true

output.format=csv
output.path=output/
output.separator=,
```

---

## 🧪 Run the Application

### 1. Build the JAR
```bash
mvn clean package
```

### 2. Run the full pipeline
```bash
spark-submit --class fr.mosef.scala.template.Main target/scala_template-1.5-jar-with-dependencies.jar
```

### 3. (Optional) Run the API fetcher
```bash
sbt "runMain fr.mosef.scala.template.utils.ApiFetcher"
```



## 🚀 Deployment

This project is configured to:
- **Build automatically via GitHub Actions**
- **Publish a Maven artifact**
- Package as a runnable JAR with all dependencies



## 📂 Output Example

Once executed, the pipeline generates several CSV reports in the `output/` folder:
- `ecart_salarial_par_csp.csv`
- `evolution_ecart_salarial_temps.csv`
- etc.



## 📚 Educational Purpose

> This project is intended for **training and academic demonstration**, showcasing:
- Modular architecture in Scala
- Spark transformations
- Integration of real-world Open Data
- Deployment with CI/CD (GitHub Actions + Maven)


## 👨‍💻 Author

**Marvin KADA_SEDODE**  
**Samanta LAMOUR**  
**Bethuel ASSE**  
**Dimitri GUIFT**