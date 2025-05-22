import os
import sys
from pathlib import Path
import pandas as pd
import json
from kafka import KafkaConsumer
import joblib
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

import logging
from datetime import datetime, timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_connection.params import Params
from db_connection.client import DatabaseClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Params and DatabaseClient
params = Params()
db_client = DatabaseClient(params)

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / 'models' / 'catboost_model.pkl'

# Load CatBoost model
try:
    model = joblib.load(MODEL_PATH)
    logger.info("CatBoost model loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load CatBoost model: {e}")
    db_client.close()
    exit(1)

# Create predicted_data table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS predicted_data (
    id SERIAL PRIMARY KEY,
    health_x_economy FLOAT,
    freedom FLOAT,
    family FLOAT,
    health FLOAT,
    economy_t-1_x_health_t-1 FLOAT,
    family_generosity_ratio FLOAT,
    continent VARCHAR(50),
    happiness_score FLOAT,
    trust FLOAT,
    economy_health_ratio FLOAT,
    health_x_country_economy_mean FLOAT,
    economy FLOAT,
    family_t-1_x_freedom_t-1 FLOAT,
    country_economy_mean FLOAT,
    predicted_happiness_score FLOAT,
    timestamp TIMESTAMP UNIQUE
);
"""
try:
    with db_client.engine.connect() as conn:
        conn.execute(text(create_table_query))
        conn.commit()
    logger.info("Table 'predicted_data' created or already exists.")
except SQLAlchemyError as e:
    logger.error(f"Failed to create table: {e}")
    db_client.close()
    exit(1)

# Initialize Kafka Consumer
try:
    consumer = KafkaConsumer(
        'happiness_data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='happiness_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info("Kafka Consumer initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Consumer: {e}")
    db_client.close()
    exit(1)

# Feature columns for prediction
feature_columns = [
    'health_x_economy', 'freedom', 'family', 'health', 'economy_t-1_x_health_t-1',
    'family_generosity_ratio', 'trust', 'economy_health_ratio',
    'health_x_country_economy_mean', 'economy', 'family_t-1_x_freedom_t-1',
    'country_economy_mean'
]

expected_continents = [
    'continent_Asia', 'continent_Europe', 'continent_North America',
    'continent_Oceania', 'continent_South America', 'continent_Africa',
    'continent_Unknown'
]

final_feature_order = feature_columns + expected_continents

# Rename incorrectly named keys if needed
rename_keys = {
    'economy_t1_x_health_t1': 'economy_t-1_x_health_t-1',
    'family_t1_x_freedom_t1': 'family_t-1_x_freedom_t-1'
}

# Process messages
for message in consumer:
    try:
        data = message.value
        logger.info(f"Received message: {data}")

        # Normalize key names
        for old_key, new_key in rename_keys.items():
            if old_key in data:
                data[new_key] = data.pop(old_key)

        # Prepare data for prediction
        row_df = pd.DataFrame([data])

        # Generate dummies for continent
        continent_dummies = pd.get_dummies(row_df['continent'], prefix='continent', drop_first=False)
        row_df = pd.concat([row_df, continent_dummies], axis=1)

        for col in expected_continents:
            if col not in row_df.columns:
                row_df[col] = 0

        X = row_df[final_feature_order]
        predicted_score = model.predict(X)[0]

        data['predicted_happiness_score'] = float(predicted_score)
        data['timestamp'] = datetime.now(timezone.utc)

        insert_query = """
        INSERT INTO predicted_data (
            health_x_economy, freedom, family, health, economy_t-1_x_health_t-1,
            family_generosity_ratio, continent, happiness_score, trust,
            economy_health_ratio, health_x_country_economy_mean, economy,
            family_t-1_x_freedom_t-1, country_economy_mean, predicted_happiness_score,
            timestamp
        ) VALUES (
            :health_x_economy, :freedom, :family, :health, :economy_t-1_x_health_t-1,
            :family_generosity_ratio, :continent, :happiness_score, :trust,
            :economy_health_ratio, :health_x_country_economy_mean, :economy,
            :family_t-1_x_freedom_t-1, :country_economy_mean, :predicted_happiness_score,
            :timestamp
        )
        ON CONFLICT (timestamp) DO UPDATE SET
            health_x_economy = EXCLUDED.health_x_economy,
            freedom = EXCLUDED.freedom,
            family = EXCLUDED.family,
            health = EXCLUDED.health,
            economy_t-1_x_health_t-1 = EXCLUDED.economy_t-1_x_health_t-1,
            family_generosity_ratio = EXCLUDED.family_generosity_ratio,
            continent = EXCLUDED.continent,
            happiness_score = EXCLUDED.happiness_score,
            trust = EXCLUDED.trust,
            economy_health_ratio = EXCLUDED.economy_health_ratio,
            health_x_country_economy_mean = EXCLUDED.health_x_country_economy_mean,
            economy = EXCLUDED.economy,
            family_t-1_x_freedom_t-1 = EXCLUDED.family_t-1_x_freedom_t-1,
            country_economy_mean = EXCLUDED.country_economy_mean,
            predicted_happiness_score = EXCLUDED.predicted_happiness_score
        """

        with db_client.engine.connect() as conn:
            conn.execute(text(insert_query), data)
            conn.commit()

        logger.info(f"Upserted prediction into database: predicted_happiness_score={predicted_score}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        continue

# Clean up
db_client.close()
consumer.close()
logger.info("Kafka Consumer and Database connection closed.")
