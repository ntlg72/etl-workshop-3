import os
import sys
from pathlib import Path
import pandas as pd
import json
from kafka import KafkaConsumer
from catboost import CatBoostRegressor
import joblib
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath('../'))
from db_connection.params import Params
from db_connection.client import DatabaseClient


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# Initialize Params and DatabaseClient
params = Params()
db_client = DatabaseClient(params)

# Create predicted_data table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS predicted_data (
    id SERIAL PRIMARY KEY,
    health_x_economy FLOAT,
    freedom FLOAT,
    family FLOAT,
    health FLOAT,
    economy_t1_x_health_t1 FLOAT,
    family_generosity_ratio FLOAT,
    continent VARCHAR(50),
    happiness_score FLOAT,
    trust FLOAT,
    economy_health_ratio FLOAT,
    health_x_country_economy_mean FLOAT,
    economy FLOAT,
    family_t1_x_freedom_t1 FLOAT,
    country_economy_mean FLOAT,
    predicted_happiness_score FLOAT,
    timestamp TIMESTAMP
);
"""
try:
    db_client.engine.execute(create_table_query)
    logger.info("Table 'predicted_data' created or already exists.")
except SQLAlchemyError as e:
    logger.error(f"Failed to create table: {e}")
    db_client.close()
    exit(1)

# Load CatBoost model
try:
    model = joblib.load('../models/catboost_model.pkl')
    logger.info("CatBoost model loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load CatBoost model: {e}")
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

# Process messages
for message in consumer:
    try:
        data = message.value
        logger.info(f"Received message: {data}")

        # Prepare data for prediction
        row_df = pd.DataFrame([data])
        
        # Encode continent
        continent_dummies = pd.get_dummies(row_df['continent'], prefix='continent', drop_first=True)
        row_df = pd.concat([row_df, continent_dummies], axis=1)
        
        # Ensure all expected dummy columns are present
        expected_continents = ['continent_Asia', 'continent_Europe', 'continent_North America', 
                             'continent_South America', 'continent_Oceania', 'continent_Africa']
        for col in expected_continents:
            if col not in row_df.columns:
                row_df[col] = 0
        
        # Select features for prediction
        X = row_df[feature_columns + expected_continents]
        
        # Predict happiness_score
        predicted_score = model.predict(X)[0]
        
        # Prepare data for database
        data['predicted_happiness_score'] = predicted_score
        data['timestamp'] = datetime.utcnow().isoformat()
        
        # Insert into database
        insert_query = """
        INSERT INTO predicted_data (
            health_x_economy, freedom, family, health, economy_t1_x_health_t1,
            family_generosity_ratio, continent, happiness_score, trust,
            economy_health_ratio, health_x_country_economy_mean, economy,
            family_t1_x_freedom_t1, country_economy_mean, predicted_happiness_score,
            timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        values = (
            data.get('health_x_economy'), data.get('freedom'), data.get('family'),
            data.get('health'), data.get('economy_t-1_x_health_t-1'),
            data.get('family_generosity_ratio'), data.get('continent'),
            data.get('happiness_score'), data.get('trust'),
            data.get('economy_health_ratio'), data.get('health_x_country_economy_mean'),
            data.get('economy'), data.get('family_t-1_x_freedom_t-1'),
            data.get('country_economy_mean'), data.get('predicted_happiness_score'),
            data.get('timestamp')
        )
        
        db_client.engine.execute(insert_query, values)
        logger.info(f"Inserted prediction into database: predicted_happiness_score={predicted_score}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        continue

# Clean up
db_client.close()
consumer.close()
logger.info("Kafka Consumer and Database connection closed.")