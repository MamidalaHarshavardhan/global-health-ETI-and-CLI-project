import pandas as pd

class HealthDataProcessor:
    def transform_records(self, input_data, from_date=None, to_date=None):
        if not input_data or 'country' not in input_data[0] or 'cases' not in input_data[0]:
            raise ValueError("Unexpected API data format")

        country_name = input_data[0]['country']
        case_stats = input_data[0]['cases']
        processed_data = []

        for date_key, stats in case_stats.items():
            processed_data.append({
                'country': country_name,
                'date': date_key,
                'total_cases': stats.get('total', 0),
                'new_cases': stats.get('new', 0)
            })

        df = pd.DataFrame(processed_data)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['total_cases'] = pd.to_numeric(df['total_cases'], errors='coerce')
        df['new_cases'] = pd.to_numeric(df['new_cases'], errors='coerce')
        df.drop_duplicates(subset=['country', 'date'], inplace=True)

        df.rename(columns={'country': 'country_name', 'date': 'report_date'}, inplace=True)

        if from_date:
            df = df[df['report_date'] >= pd.to_datetime(from_date)]
        if to_date:
            df = df[df['report_date'] <= pd.to_datetime(to_date)]

        return df
