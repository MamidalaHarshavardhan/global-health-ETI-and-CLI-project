# Global Healthcare Data ETL & Analysis CLI 🌍💉

## Overview
Command-line application that extracts healthcare data from public APIs, cleans and transforms it, loads into MySQL database, and provides analytical queries.

## Project Structure
```
healthcare_etl_cli/
├── screenshots/              # Demo screenshots
├── sql/
│   └── create_tables.sql     # Database schema
├── api_client.py            # API interaction
├── config.ini               # Configuration
├── data_transformer.py      # Data cleaning & transformation
├── main.py                  # Main CLI application
├── mysql_handler.py         # Database operations
├── requirements.txt         # Dependencies
├── vaccination_data.py      # Vaccination specific operations
└── vaccinations.csv         # Sample data
```

## Installation

1. **Install Dependencies**
```bash
pip install -r requirements.txt
```

2. **Setup MySQL Database**
```sql
CREATE DATABASE healthcare_db;
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON healthcare_db.* TO 'etl_user'@'localhost';
```

3. **Configure Database**
Edit `config.ini`:
```ini
[database]
host = localhost
user = root
password = your_password
database = healthcare

[api]
base_url = https://disease.sh/v3/covid-19/
```

## Usage

### Fetch Data
```bash
python main.py fetch_data <country> <start_date> <end_date>
```
Example:
```bash
python main.py fetch_data India 2023-01-01 2023-01-31
```

### Query Data
```bash
# Total cases for a country
python main.py query_data total_cases India

# Daily trends
python main.py query_data daily_trends USA new_cases

# Top countries by metric
python main.py query_data top_n_countries_by_metric 5 total_cases
```

### Database Management
```bash
# List tables
python main.py list_tables

# Drop tables (caution!)
python main.py drop_tables
```

## Data Source
Uses **Disease.sh API** for COVID-19 data:
- Historical cases and deaths by country
- Vaccination data
- Global statistics

## Database Schema

**daily_cases table:**
- id, report_date, country_name
- total_cases, new_cases, total_deaths, new_deaths
- etl_timestamp

**vaccination_data table:**
- id, report_date, country_name
- total_vaccinations, people_vaccinated, people_fully_vaccinated
- etl_timestamp

## Dependencies
```
requests==2.31.0
mysql-connector-python==8.2.0
pandas==2.1.3
click==8.1.7
tabulate==0.9.0
```

## Key Features
- ✅ API data extraction with error handling
- ✅ Data cleaning and transformation
- ✅ MySQL batch loading with incremental updates
- ✅ CLI interface with multiple query types
- ✅ Comprehensive logging
- ✅ Duplicate prevention

## Sample Output
```bash
$ python main.py fetch_data India 2023-01-01 2023-01-31
INFO - Fetching data for India from 2023-01-01 to 2023-01-31
INFO - Successfully fetched 31 records from API
INFO - Loaded 31 new records into daily_cases table

$ python main.py query_data total_cases India
Total COVID-19 Cases in India: 44,997,856
```
python main.py fetch_data --country "India"
<img width="975" height="284" alt="image" src="https://github.com/user-attachments/assets/391dc4b9-9add-442b-abb5-e70a1c579d86" />

python main.py fetch_data --country "India" --start_date 2023-01-01 --end_date 2023-01-31
<img width="975" height="257" alt="image" src="https://github.com/user-attachments/assets/85e27870-ffa4-4623-834e-99aff1ebacb3" />


