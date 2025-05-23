# World Happiness Prediction 

## Overview

This project implements a complete process to predict happiness scores across countries using World Happiness Report data from 2015 to 2019. The solution integrates Exploratory Data Analysis (EDA), Extract-Transform-Load (ETL) processes, machine learning model training, real-time data streaming with Kafka, and database storage with PostgreSQL. The trained CatBoost regression model predicts `happiness_score` based on engineered features, with predictions stored for further analysis. This README provides instructions to set up, run, and utilize the project.

## Prerequisites

- **Docker** and **Docker Compose**: For containerized services (Kafka, ZooKeeper, PostgreSQL).
- **Python 3.12.10**: For running the Python scripts and notebooks.
- **Poetry**: For managing Python dependencies (install via `curl -sSL https://install.python-poetry.org | python3 -`).
- **Kafka**: Local instance managed by Docker Compose.
- **PostgreSQL**: Local instance managed by Docker Compose.
- **Git**: To clone the repository.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/ntlg72/world-happiness-prediction.git
   cd world-happiness-prediction
   ```

2. **Install Poetry Dependencies**:
   - Initialize Poetry if not already set up:
     ```bash
     poetry init  # Follow prompts or skip with defaults
     ```
   - Install project dependencies using Poetry:
     ```bash
     poetry install
     ```
   - This command creates a virtual environment and installs dependencies listed in `pyproject.toml` (e.g., `pandas`, `kafka-python`, `scikit-learn`, `catboost`, `sqlalchemy`, `joblib`, `rapidfuzz`, `pycountry`, `pycountry-convert`).

3. **Prepare Environment Variables**:
   - Create a `.env` file in the project root based on the provided template:
     ```
     DB_DATABASE=your_database_name
     DB_USERNAME=your_username
     DB_PASSWORD=your_password
     DB_PORT=5434
     ```
   - Update values with your PostgreSQL credentials.



## Usage

### 1. Start Docker Services
Launch the ZooKeeper, Kafka, and PostgreSQL containers:
```bash
docker-compose up -d
```
- This starts:
  - ZooKeeper on port 2181.
  - Kafka on port 9092 with the `happiness_data` topic.
  - PostgreSQL on the port specified in `.env` (default 5432).

### 2. Activate the Poetry Environment
Activate the Poetry-managed virtual environment:
```bash
poetry shell
```
Alternatively, run commands directly with Poetry:
```bash
poetry run jupyter notebook
```

### 3. Run the EDA and Model Training
Execute the Jupyter notebooks to process data and train the model:
- Open Jupyter Notebook:
  ```bash
  poetry run jupyter notebook
  ```
- Run `1.0-nlg-EDA.ipynb` to perform EDA, clean data, and save engineered datasets (`happiness_data_primary.csv`, `happiness_data_alternative.csv`) in `data/interim/`.
- Run `2.0-nlg-model_training.ipynb` to train the CatBoost model and save it as `models/catboost_model.pkl`. The notebook evaluates multiple models and selects the best performer (CatBoost with R² ~0.863).

### 4. Run the Producer
Stream transformed data to Kafka:
```bash
poetry run python kafka/producer.py
```
- This script loads the CSV files, applies feature engineering, and sends data to the `happiness_data` topic with a 0.5-second delay. Logs are output to the console.

### 5. Run the Consumer
Process Kafka messages and store predictions:
```bash
poetry run python kafka/consumer.py
```
- This script consumes data from the `happiness_data` topic, uses the trained CatBoost model to predict `happiness_score`, and upserts results into the PostgreSQL `predicted_data` table. Logs track the process.

### 6. Verify Results
- Connect to PostgreSQL (e.g., using `psql` or a GUI like pgAdmin) with the credentials from `.env`:
  ```bash
  docker exec it kafka_ws3 bash
  psql -h localhost -U ${DB_USERNAME} -d ${DB_DATABASE}
  ```
- Query the `predicted_data` table to view predictions:
  ```sql
  SELECT * FROM predicted_data;
  ```

### 7. Stop Docker Services
Shut down the containers when done:
```bash
docker-compose down
```

## Project Structure

```
world-happiness-prediction/
├── data/
│   ├── external/         # Raw CSV files (2015-2019)
│   └── interim/          # Processed datasets (e.g., happiness_data_primary.csv)
├── models/               # Trained CatBoost model (catboost_model.pkl)
├── kafka/                # Kafka-related scripts
│   ├── consumer.py       # Kafka consumer script
│   └── producer.py       # Kafka producer script
├── db_connection/        # Database connection modules (params.py, client.py)
├── 1.0-nlg-EDA.ipynb     # EDA and feature engineering notebook
├── 2.0-nlg-model_training.ipynb  # Model training notebook
├── docker-compose.yml    # Docker Compose configuration
├── .env                  # Environment variables
├── pyproject.toml        # Poetry configuration and dependencies
├── poetry.lock           # Locked dependency versions
├── README.md             # This file
└── init-db.sql           # PostgreSQL initialization script
```

## Customization

- **Adjust Kafka Topics**: Modify `KAFKA_CREATE_TOPICS` in `docker-compose.yml` to create additional topics if needed.
- **Scale Services**: Increase `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` or add more Kafka brokers for high availability.
- **Model Tuning**: Edit hyperparameters in `2.0-nlg-model_training.ipynb` (e.g., CatBoost `iterations`, `learning_rate`) to improve performance.
- **Database Schema**: Update `init-db.sql` or the `create_table_query` in `consumer.py` to modify the `predicted_data` table structure.

## Troubleshooting

- **Kafka Connection Issues**: Ensure `localhost:9092` is accessible and ZooKeeper is running.
- **PostgreSQL Errors**: Verify `.env` credentials and port mapping.
- **Missing Data Files**: Place CSV files in `data/external/` or update file paths in `producer.py`.
- **Poetry Issues**: Ensure `poetry install` completes without errors; check `pyproject.toml` for correct dependency versions.
- **Logs**: Check console output or Docker logs (`docker-compose logs`) for errors.

