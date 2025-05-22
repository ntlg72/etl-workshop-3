-- init-db.sql
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