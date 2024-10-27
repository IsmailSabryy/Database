import csv
import requests
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random


output_csv_file_path = 'csv/codeforces_profiles_summary.csv'
MAX_WORKERS = 1


def start_driver():
    options = uc.ChromeOptions()
    options.headless = False
    return uc.Chrome(options=options)

# Initialize the driver
driver = start_driver()

def fetch_user_info(username):
    try:
        response = requests.get(f'https://codeforces.com/api/user.info?handles={username}', timeout=10)
        if response.status_code == 403:
            print("403 Forbidden detected. Restarting driver and sleeping for 2 minutes...")
            handle_403()
            return None
        elif response.status_code == 200:
            data = response.json()
            if data['status'] == 'OK':
                return data['result'][0]
            else:
                print(f"API error for {username}: {data['comment']}")
        else:
            print(f"Error fetching data from API for {username}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request error for {username}: {e}")
    return None

def handle_403():
    global driver
    driver.quit()  # Close the driver
    time.sleep(140)  # Sleep for 2 minutes
    driver = start_driver()  # Restart the driver

def scrape_profile(username):
    global driver
    while True:  # Keep trying until successful
        try:
            driver.set_page_load_timeout(15)
            driver.get(f'https://codeforces.com/profile/{username}')
            if "403 Forbidden" in driver.page_source:
                print(f"403 Forbidden detected on profile page for {username}. Restarting...")
                handle_403()
                continue  # Try again after handling 403

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            activity_frame = soup.find('div', class_='_UserActivityFrame_frame')
            max_problems_solved = 0
            max_days_in_row = 0

            if activity_frame:
                problem_counters = activity_frame.find_all('div', class_='_UserActivityFrame_counter')

                for counter in problem_counters:
                    value = counter.find('div', class_='_UserActivityFrame_counterValue').get_text(strip=True)

                    if "problems" in value:
                        current_value = int(value.split()[0])
                        max_problems_solved = max(max_problems_solved, current_value)
                    elif "days" in value:
                        current_value = int(value.split()[0])
                        max_days_in_row = max(max_days_in_row, current_value)
            user_data = fetch_user_info(username)
            if user_data:
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
                    'username': username.strip(),
                    'city': user_data.get('city', '').strip(),
                    'country': user_data.get('country', '').strip(),
                    'organization': user_data.get('organization', '').strip(),
                    'contribution': user_data.get('contribution', 0),
                    'friends_count': user_data.get('friendOfCount', 0),
                    'registration_duration': registration_duration,
                    'max_problems_solved': max_problems_solved,
                    'max_days_in_row': max_days_in_row
                }

                with open(output_csv_file_path, 'a', newline='', encoding='utf-8') as output_file:
                    writer = csv.DictWriter(output_file, fieldnames=record.keys())
                    output_file.seek(0, 2)  # Move to the end of the file
                    writer.writerow(record)
                print(f"Stored data for {username}: {record}")
                break  # Exit the while loop on success
            else:
                print(f"No user data found for {username}. Skipping.")
                break  # Exit the loop on failure

        except Exception as e:
            print(f"Error loading profile for {username}: {e}")
            driver.quit()
            driver = start_driver()

def main():
    usernames=[]
    proccessed_count=0
    limit = 300000
    start_index =0

    with open('csv/unique_users.csv', mode='r') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            if i < start_index:  # Skip rows until reaching the start index
                continue
            if i >= limit + start_index:  # Stop reading once the limit is reached
                break
            usernames.append(row[0])
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_username = {executor.submit(scrape_profile, username): username for username in usernames}

        for future in as_completed(future_to_username):
            username = future_to_username[future]
            try:
                future.result()
                proccessed_count += 1
                if proccessed_count % 50==0 :
                    driver.quit()
                    driver = start_driver()
            except Exception as exc:
                print(f"Exception occurred for {username}: {exc}")
            finally:
                time.sleep(random.uniform(0.1, 0.2))

    driver.quit()

if __name__ == '__main__':
    main()
