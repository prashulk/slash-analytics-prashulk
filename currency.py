import requests
import csv
from datetime import datetime, timedelta
import os

def generate_date_range(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)

seeds_folder = 'seeds'
csv_file_path = os.path.join(seeds_folder, 'currency_conv.csv')

with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['conversion_dt', 'source_currency', 'destination_currency', 'exchange_rate'])

    for current_date in generate_date_range(datetime(2023, 1, 1), datetime(2024, 12, 31)):
        date_str = current_date.strftime('%Y-%m-%d')

        url = f"https://api.fxratesapi.com/historical?date={date_str}&base=USD"

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            if 'rates' in data:
                for currency, rate in data['rates'].items():
                    writer.writerow([date_str, 'USD', currency, rate])
                print(f"Data for {date_str} added.")
            else:
                print(f"No data for {date_str}")
        else:
            print(f"Error for {date_str}: {response.status_code}")

print(f"Data saved to {csv_file_path}")
