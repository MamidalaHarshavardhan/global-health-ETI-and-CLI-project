import mysql.connector
import logging

class MySQLManager:
    def __init__(self, db_host, db_user, db_password, db_name):
        try:
            self.connection = mysql.connector.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_name
            )
            self.cursor = self.connection.cursor()
            logging.info(" Connected to MySQL.")
        except mysql.connector.Error as e:
            logging.error(f"[ERROR] MySQL connection failed: {e}")
            self.connection = None
            self.cursor = None

    def initialize_schema(self):
        if not self.connection:
            logging.error("No DB connection.")
            return
        try:
            cursor = self.connection.cursor()
            with open("sql/create_tables.sql", "r") as file:
                script = file.read()
            for stmt in script.split(";"):
                if stmt.strip():
                    cursor.execute(stmt)
            self.connection.commit()
            cursor.close()
            logging.info(" Schema initialized.")
        except Exception as e:
            logging.error(f"[ERROR] Schema initialization failed: {e}")

    def insert_records(self, table, records):
        if not self.connection or not records:
            logging.info("No data available for insertion.")
            return

        cursor = self.connection.cursor()

        check_query = f"SELECT report_date, country_name FROM {table} WHERE report_date = %s AND country_name = %s"
        insert_query = f"INSERT INTO {table} ({', '.join(records[0].keys())}) VALUES ({', '.join(['%s'] * len(records[0]))})"

        to_insert = []
        for entry in records:
            entry = entry.copy()  # avoid mutating original
            if 'date' in entry:
                entry['report_date'] = entry.pop('date')
            if 'country' in entry:
                entry['country_name'] = entry.pop('country')

            cursor.execute(check_query, (entry['report_date'], entry['country_name']))
            if not cursor.fetchone():
                to_insert.append(tuple(entry.values()))

        if to_insert:
            try:
                cursor.executemany(insert_query, to_insert)
                self.connection.commit()
                logging.info(f"{len(to_insert)} new rows inserted into {table}.")
            except mysql.connector.Error as e:
                logging.error(f"[ERROR] Insertion failed: {e}")
                self.connection.rollback()
        else:
            logging.info("No new unique rows to insert.")

        cursor.close()

    def execute_query(self, sql, args=None):
        if not self.connection:
            logging.error("No DB connection.")
            return []
        cursor = self.connection.cursor()
        cursor.execute(sql, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def fetch_scalar(self, query, args=None):
        if not self.connection:
            return None
        cursor = self.connection.cursor(buffered=True)
        try:
            cursor.execute(query, args)
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            cursor.close()

    def fetch_results(self, query, args=()):
        if not self.connection:
            return []
        cursor = self.connection.cursor()
        cursor.execute(query, args)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def show_tables(self):
        if not self.connection:
            logging.error("No DB connection.")
            return
        cursor = self.connection.cursor()
        cursor.execute("SHOW TABLES")
        for (table_name,) in cursor.fetchall():
            print(f"  - {table_name}")
        cursor.close()

    def drop_schema(self):
        if not self.connection:
            return
        cursor = self.connection.cursor()

        tables_to_drop = ["daily_cases", "vaccination_data"]
        for table in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                self.connection.commit()
                logging.info(f"Dropped table '{table}'.")
            except Exception as e:
                logging.error(f"Error dropping table '{table}': {e}")

        cursor.close()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logging.info(" Disconnected from MySQL.")
