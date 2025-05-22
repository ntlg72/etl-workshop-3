import pandas as pd
import numpy as np
import json
from kafka import KafkaProducer
import logging
import re
from rapidfuzz import process
import pycountry
from pycountry_convert import country_name_to_country_alpha2, country_alpha2_to_continent_code

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka Producer initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

# Define file paths for datasets
data_files = [
    "../data/external/2015.csv",
    "../data/external/2016.csv",
    "../data/external/2017.csv",
    "../data/external/2018.csv",
    "../data/external/2019.csv"
]

# Load datasets
dfs = {}
for file in data_files:
    try:
        year = file.split("/")[-1][:4]
        df = pd.read_csv(file)
        dfs[year] = df
        logger.info(f"Loaded dataset for year {year}")
    except Exception as e:
        logger.error(f"Failed to load dataset {file}: {e}")
        continue

# Column renaming for standardization
column_renames = {
    "2015": {
        "Country": "Country",
        "Region": "Region",
        "Happiness Rank": "Happiness Rank",
        "Happiness Score": "Happiness Score",
        "Economy (GDP per Capita)": "Economy",
        "Family": "Family",
        "Health (Life Expectancy)": "Health",
        "Freedom": "Freedom",
        "Trust (Government Corruption)": "Trust",
        "Generosity": "Generosity"
    },
    "2016": {
        "Country": "Country",
        "Region": "Region",
        "Happiness Rank": "Happiness Rank",
        "Happiness Score": "Happiness Score",
        "Economy (GDP per Capita)": "Economy",
        "Family": "Family",
        "Health (Life Expectancy)": "Health",
        "Freedom": "Freedom",
        "Trust (Government Corruption)": "Trust",
        "Generosity": "Generosity"
    },
    "2017": {
        "Country": "Country",
        "Happiness.Rank": "Happiness Rank",
        "Happiness.Score": "Happiness Score",
        "Economy..GDP.per.Capita.": "Economy",
        "Family": "Family",
        "Health..Life.Expectancy.": "Health",
        "Freedom": "Freedom",
        "Trust..Government.Corruption.": "Trust",
        "Generosity": "Generosity"
    },
    "2018": {
        "Overall rank": "Happiness Rank",
        "Country or region": "Country",
        "Score": "Happiness Score",
        "GDP per capita": "Economy",
        "Social support": "Family",
        "Healthy life expectancy": "Health",
        "Freedom to make life choices": "Freedom",
        "Generosity": "Generosity",
        "Perceptions of corruption": "Trust"
    },
    "2019": {
        "Overall rank": "Happiness Rank",
        "Country or region": "Country",
        "Score": "Happiness Score",
        "GDP per capita": "Economy",
        "Social support": "Family",
        "Healthy life expectancy": "Health",
        "Freedom to make life choices": "Freedom",
        "Generosity": "Generosity",
        "Perceptions of corruption": "Trust"
    }
}

# Apply column renaming
for year, df in dfs.items():
    if year in column_renames:
        dfs[year] = df.rename(columns=column_renames[year])

# Convert column names to snake_case
def to_snake_case(column_name):
    column_name = column_name.lower()
    column_name = re.sub(r'\s+', '_', column_name)
    column_name = re.sub(r'[^\w\s]', '', column_name)
    return column_name

for year, df in dfs.items():
    df.columns = [to_snake_case(col) for col in df.columns]

# Identify common columns
common_columns = set(dfs['2015'].columns)
for year, df in dfs.items():
    common_columns &= set(df.columns)
common_columns = list(common_columns)

# Filter to common columns
for year, df in dfs.items():
    dfs[year] = df[common_columns]

# Standardize country names
def standardize_country(country_name):
    if pd.isna(country_name) or not isinstance(country_name, str):
        return "Unknown"
    country_name = country_name.strip()
    country_list = [country.name for country in pycountry.countries]
    match_result = process.extractOne(country_name, country_list)
    if match_result:
        best_match, score = match_result[:2]
        return best_match if score > 80 else "Unknown"
    return "Unknown"

# Add year and standardize countries
for year, df in dfs.items():
    df['year'] = int(year)
    df['country'] = df['country'].apply(standardize_country)

# Combine DataFrames
combined_df = pd.concat([dfs[year] for year in dfs], ignore_index=True)

# Handle missing values
combined_df['trust'] = combined_df['trust'].fillna(combined_df['trust'].median())

# Derive continent
def get_continent(country_name):
    try:
        alpha2 = country_name_to_country_alpha2(country_name)
        continent_code = country_alpha2_to_continent_code(alpha2)
        continent_map = {
            'AF': 'Africa', 'AS': 'Asia', 'EU': 'Europe', 'NA': 'North America',
            'SA': 'South America', 'OC': 'Oceania', 'AN': 'Antarctica'
        }
        return continent_map.get(continent_code, 'Unknown')
    except:
        return 'Unknown'

combined_df['continent'] = combined_df['country'].apply(get_continent)

# Feature engineering
combined_df['health_x_economy'] = combined_df['health'] * combined_df['economy']
combined_df['economy_t-1'] = combined_df.groupby('country')['economy'].shift(1)
combined_df['health_t-1'] = combined_df.groupby('country')['health'].shift(1)
combined_df['economy_t-1_x_health_t-1'] = combined_df['economy_t-1'] * combined_df['health_t-1']
combined_df['family_generosity_ratio'] = combined_df['family'] / (combined_df['generosity'] + 1e-6)
combined_df['economy_health_ratio'] = combined_df['economy'] / (combined_df['health'] + 1e-6)
combined_df['country_economy_mean'] = combined_df.groupby('country')['economy'].transform('mean')
combined_df['health_x_country_economy_mean'] = combined_df['health'] * combined_df['country_economy_mean']
combined_df['family_t-1'] = combined_df.groupby('country')['family'].shift(1)
combined_df['freedom_t-1'] = combined_df.groupby('country')['freedom'].shift(1)
combined_df['family_t-1_x_freedom_t-1'] = combined_df['family_t-1'] * combined_df['freedom_t-1']

# Drop rows with missing temporal features
df_final = combined_df.dropna()

# Define alternative columns
alt_columns = [
    'health_x_economy', 'freedom', 'family', 'health', 'economy_t-1_x_health_t-1',
    'family_generosity_ratio', 'continent', 'happiness_score', 'trust',
    'economy_health_ratio', 'health_x_country_economy_mean', 'economy',
    'family_t-1_x_freedom_t-1', 'country_economy_mean'
]

# Ensure all columns exist
missing_cols = [col for col in alt_columns if col not in df_final.columns]
if missing_cols:
    logger.error(f"Missing columns: {missing_cols}")
    exit(1)

# Send each row to Kafka
for _, row in df_final[alt_columns].iterrows():
    try:
        row_dict = row.to_dict()
        # Handle NaN values
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
        producer.send('happiness_data', value=row_dict)
        logger.info(f"Sent row to Kafka topic 'happiness_data': {row_dict}")
    except Exception as e:
        logger.error(f"Failed to send row to Kafka: {e}")
        continue

# Flush producer and close
producer.flush()
producer.close()
logger.info("Kafka Producer closed.")