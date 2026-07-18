# End-to-End ETL Pipeline: E-Commerce Orders

An end-to-end ETL (Extract, Transform, Load) pipeline for e-commerce order data, built with multiple execution paths — Dagster orchestration, a standalone Python orchestrator script, a Jupyter notebook, and a Spyder-run script — with CLI notifications on completion.

## Table of Contents

1. [Overview](#1-overview)
2. [Tech Stack](#2-tech-stack)
3. [Project Structure](#3-project-structure)
4. [Data Flow](#4-data-flow)
5. [Pipeline Stages](#5-pipeline-stages)
6. [Execution Methods](#6-execution-methods)
7. [Getting Started](#7-getting-started)
8. [Monitoring & Notifications](#8-monitoring--notifications)
9. [Future Improvements](#9-future-improvements)
10. [Screenshots](#10-screenshots)

## 1. Overview

This project demonstrates a complete ETL pipeline for e-commerce order data. Raw order data is extracted, cleaned and transformed into analysis-ready datasets, and loaded into a destination for downstream reporting. The pipeline can be run and monitored in several ways: through Dagster's asset-based orchestration UI, a standalone orchestrator script, a notebook environment, or directly in Spyder — with a CLI notification summary on completion.

## 2. Tech Stack

- **Orchestration:** Dagster
- **Scripting / Orchestrator:** Python (`orchestrator.py`)
- **Development / Prototyping:** Jupyter Notebook, Spyder
- **Language:** Python

## 3. Project Structure

```
End-to-End-ETL-Pipeline_E-Commerce-Orders/
├── data/
├── screenshot/
│   ├── dagster_assets_list.png
│   ├── dagster_asset_lineage.png
│   ├── dagster_run_success.png
│   ├── dagster_runs_list.png
│   ├── dagster_code_location.png
│   ├── orchestrator_full_run.png
│   ├── orchestrator_full_terminal.png
│   ├── notebook_pipeline_start.png
│   ├── cli_notification_summary.png
│   └── spyder_full_log.png
├── src/
├── README.md
├── dagster_project
├── docs
├── notebooks
└── tests
```

## 4. Data Flow

Raw e-commerce order data → Extraction → Transformation/Cleaning → Load → Reporting-ready output. Each stage is represented as a Dagster asset, with dependencies visible in the global asset lineage graph.

## 5. Pipeline Stages

- **Extract:** Pull raw e-commerce order data from source.
- **Transform:** Clean, validate, and reshape the data.
- **Load:** Write processed data to the destination store.
- **Notify:** Emit a CLI summary once the run completes.

## 6. Execution Methods

This pipeline can be run through any of the following:

- **Dagster UI:** Materialize assets and monitor runs interactively.
- **`orchestrator.py`:** Run the full pipeline end-to-end from the command line.
- **Notebook:** Step through each stage interactively for development/debugging.
- **Spyder (`etl_pipeline_spyder.py`):** Run and inspect the pipeline in the Spyder IDE.

## 7. Getting Started

1. Clone the repository
   ```bash
   git clone https://github.com/<your-username>/End-to-End-ETL-Pipeline_E-Commerce-Orders.git
   cd End-to-End-ETL-Pipeline_E-Commerce-Orders
   ```
2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```
3. Launch the Dagster UI
   ```bash
   dagster dev
   ```
   Then navigate to `http://localhost:3000` to view assets and trigger runs.

   Or run the orchestrator directly:
   ```bash
   python pipeline/orchestrator.py
   ```

## 8. Monitoring & Notifications

Pipeline runs can be monitored through the Dagster UI (assets, lineage, and run history), or via the CLI, which prints a stage-by-stage log and a final notification summary once the pipeline completes.

## 9. Future Improvements

- Add automated data quality checks
- Integrate with a cloud data warehouse (e.g., BigQuery, Snowflake)
- Schedule recurring pipeline runs
- Add email/Slack notifications alongside CLI output

## 10. Screenshots

### 1. Assets Grouped by Layer (Dagster UI)
![Dagster assets list](screenshot/dagster_assets_list.png)

### 2. Global Asset Lineage Graph (Dagster UI)
![Dagster global asset lineage](screenshot/dagster_asset_lineage.png)

### 3. Successful Run Detail (Dagster UI)
![Dagster run success detail](screenshot/dagster_run_success.png)

### 4. Runs List (Dagster UI)
![Dagster runs list](screenshot/dagster_runs_list.png)

### 5. Code Location Overview (Dagster UI)
![Dagster code location overview](screenshot/dagster_code_location.png)

### 6. Full Pipeline Log (`orchestrator.py`)
![Orchestrator full run output](screenshot/orchestrator_full_run.png)

### 7. Pipeline Start / Stage-by-Stage Log (Notebook)
![Notebook pipeline start log](screenshot/notebook_pipeline_start.png)

### 8. Final Notification Summary (CLI)
![CLI notification summary](screenshot/cli_notification_summary.png)

### 9. Spyder Run (`etl_pipeline_spyder.py`)
![Spyder full log](screenshot/spyder_full_log.png)

### 10. Complete Pipeline Execution Log
![Complete pipeline execution log](screenshot/orchestrator_full_terminal.png)

## Author

**Alifia Chika Intan (Nevie)**
[LinkedIn](https://linkedin.com/in/alifia-chika-intan-880b94202)
