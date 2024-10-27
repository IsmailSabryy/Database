import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

output_csv_file_path = 'csv/codeforces_profiles_summary.csv'
MAX_WORKERS = 1
MAX_RETRIES = 5

def fetch_user_info(username):
    """Fetch user information from the Codeforces API."""
    try:
        response = requests.get(f'https://codeforces.com/api/user.info?handles={username}', timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'OK':
                return data['result'][0]
            print(f"API error for {username}: {data['comment']}")
        else:
            print(f"Error fetching data for {username}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request error for {username}: {e}")
    return None

def fetch_profile_page(username):
    """Fetch the profile page HTML."""
    try:
        response = requests.get(f'https://codeforces.com/profile/{username}', timeout=15)
        if response.status_code == 200:
            return response.text
        print(f"Error fetching profile page for {username}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request error for {username}: {e}")
    return None

def scrape_profile(username):
    """Scrape the profile data and store it in the CSV."""
    retries = 0
    while retries < MAX_RETRIES:
        print(f"Scraping data for {username}, attempt {retries + 1}")
        user_data = fetch_user_info(username)
        page_source = fetch_profile_page(username)

        if user_data and page_source:
            soup = BeautifulSoup(page_source, 'html.parser')
            activity_frame = soup.find('div', class_='_UserActivityFrame_frame')
            max_problems_solved = 0
            max_days_in_row = 0

            if activity_frame:
                counters = activity_frame.find_all('div', class_='_UserActivityFrame_counter')
                for counter in counters:
                    value = counter.find('div', class_='_UserActivityFrame_counterValue').get_text(strip=True)
                    if "problems" in value:
                        max_problems_solved = max(max_problems_solved, int(value.split()[0]))
                    elif "days" in value:
                        max_days_in_row = max(max_days_in_row, int(value.split()[0]))

                start_time_seconds = user_data.get('registrationTimeSeconds', 0)
                registration_time = datetime.fromtimestamp(start_time_seconds)
                total_days_registered = (datetime.now() - registration_time).days

                years = total_days_registered // 365
                remaining_days = total_days_registered % 365
                months = remaining_days // 30
                days = remaining_days % 30

                registration_duration = years + (months / 12) + (days / 365)
                registration_duration = round(registration_duration) if years >= 1 else round(registration_duration, 2)


            record = {
                'username': username,
                'city': user_data.get('city', '').strip(),
                'country': user_data.get('country', '').strip(),
                'organization': user_data.get('organization', '').strip(),
                'contribution': user_data.get('contribution', 0),
                'friends_count': user_data.get('friendOfCount', 0),
                'registration_duration': registration_duration,
                'max_problems_solved': max_problems_solved,
                'max_days_in_row': max_days_in_row
            }

            with open(output_csv_file_path, 'a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=record.keys())
                writer.writerow(record)
            print(f"Successfully stored data for {username}: {record}")
            return

        retries += 1
        print(f"Retrying {username}... ({retries}/{MAX_RETRIES})")
        time.sleep(random.uniform(1, 3))  # Backoff before retry

    print(f"Failed to scrape data for {username} after {MAX_RETRIES} attempts.")

def main():
    usernames = []
    processed_count = 0
    start_index = 0

    with open('csv/unique_users.csv', mode='r') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            if i < start_index:
                continue
            if i >= limit + start_index:
                break
            usernames.append(row[0])

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_username = {executor.submit(scrape_profile, username): username for username in usernames}

        for future in as_completed(future_to_username):
            username = future_to_username[future]
            try:
                future.result()
                processed_count += 1
                print(f"Processed {processed_count} users.")
            except Exception as exc:
                print(f"Exception occurred for {username}: {exc}")
            finally:
                time.sleep(random.uniform(0.1, 0.2))  # Throttle requests slightly

if __name__ == '__main__':
    main()
