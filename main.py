import argparse
import configparser
import logging
from vaccination_data import VaccinationCSVLoader
from api_client import COVIDAPIService
from data_transformer import HealthDataProcessor
from mysql_handler import MySQLManager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def run_cli():
    config = read_config()

    db = MySQLManager(
        config['mysql']['host'],
        config['mysql']['user'],
        config['mysql']['password'],
        config['mysql']['database']
    )
    api = COVIDAPIService(config['api']['base_url'], config['api'].get('api_key'))
    transformer = HealthDataProcessor()

    parser = argparse.ArgumentParser(
        description="Healthcare ETL CLI",
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='command')
    # fetch_data
    fetch_parser = subparsers.add_parser('fetch_data', help="Fetch data and load into DB")
    fetch_parser.add_argument('--country', required=True, help="Country name (e.g., India)")
    fetch_parser.add_argument('--start_date', required=False, help="Start date (YYYY-MM-DD)")
    fetch_parser.add_argument('--end_date', required=False, help="End date (YYYY-MM-DD)")
    fetch_parser.add_argument('--date', required=False, help="Specific date (YYYY-MM-DD)")
    # query_data
    query_parser = subparsers.add_parser('query_data', help="Query data from database")
    query_parser.add_argument('column', help='Metric name or query type (e.g., total_cases or top_n_countries_by_metric)')
    query_parser.add_argument('country', help='Country name or number for top_n query')
    query_parser.add_argument('extra_column', nargs='?', default=None, help='Optional: trend column or metric')
    # load_csv
    csv_parser = subparsers.add_parser('load_csv', help="Load vaccination data from CSV")
    csv_parser.add_argument('file_path', help="Path to CSV file (e.g., data/vaccination_data.csv)")
    # utility
    subparsers.add_parser('list_tables', help="List all tables")
    subparsers.add_parser('drop_tables', help="Drop daily_cases table")

    args = parser.parse_args()

    try:
        if args.command is None:
            parser.print_help()
            return
        if args.command == 'fetch_data':
            params = {'country': args.country}
            if args.date:
                params['date'] = args.date
            print(f"\nFetching data for {args.country} from {args.start_date or 'beginning'} to {args.end_date or 'latest'}...")
            raw_data = api.get_data(query_params=params)
            if not raw_data:
                logging.warning("No data returned from API.")
                return
            print("Cleaned and transformed data.")
            cleaned = transformer.transform_records(raw_data, from_date=args.start_date, to_date=args.end_date)
            if cleaned.empty:
                logging.warning("No cleaned records to insert.")
                return
            db.initialize_schema()
            db.insert_records('daily_cases', cleaned.to_dict(orient="records"))
            print(f"Loaded {len(cleaned)} records into 'daily_cases' table.\n")
        elif args.command == 'load_csv':
            loader = VaccinationCSVLoader(db)
            loader.load_csv(args.file_path)
        elif args.command == 'query_data':
            if args.column == 'top_n_countries_by_metric':
                try:
                    top_n = int(args.country)
                    metric = args.extra_column
                    if not metric:
                        print("[ERROR] Metric column name required.")
                        return
                    tables = ['vaccination_data', 'daily_cases']
                    rows, used_table = None, None
                    for table in tables:
                        try:
                            sql = f"""
                                SELECT country_name, MAX({metric}) AS total
                                FROM {table}
                                GROUP BY country_name
                                ORDER BY total DESC
                                LIMIT %s
                            """
                            rows = db.fetch_results(sql, (top_n,))
                            if rows:
                                used_table = table
                                break
                        except Exception:
                            continue
                    if not rows:
                        print("[ERROR] Metric not found in both tables or no data available.")
                    else:
                        print(f"\nTop {top_n} countries by '{metric}' from table '{used_table}':")
                        print(f"Rank | Country         | {metric.replace('_', ' ').title()}")
                        print("-------------------------------------------")
                        for idx, (country, total) in enumerate(rows, 1):
                            print(f"{idx:<4} | {country:<15} | {total:,}")
                        print()
                except Exception as e:
                    print(f"[ERROR] Could not execute top_n query: {e}")
            elif args.column == 'daily_trends' and args.extra_column:
                sql = f"""
                    SELECT report_date, {args.extra_column}
                    FROM daily_cases
                    WHERE country_name = %s
                    ORDER BY report_date ASC
                """
                rows = db.fetch_results(sql, (args.country,))
                if not rows:
                    print("No data found.")
                else:
                    print(f"Date       | {args.extra_column}")
                    print("-" * 30)
                    for date, value in rows:
                        print(f"{date} | {value}")
            else:
                sql = f"""
                    SELECT MAX({args.column})
                    FROM daily_cases
                    WHERE country_name = %s
                """
                result = db.fetch_scalar(sql, (args.country,))
                if result:
                    print(f"Max {args.column} in {args.country}: {result}")
                else:
                    print("No result found.")
        elif args.command == 'list_tables':
            db.initialize_schema()
            db.show_tables()
        elif args.command == 'drop_tables':
            db.drop_schema()
            print("Tables 'daily_cases' and 'vaccination_data' dropped successfully.")
    finally:
        db.disconnect()
if __name__ == "__main__":
    run_cli()
