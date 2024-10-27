import time
import random
import csv
import threading
import undetected_chromedriver as uc
from bs4 import BeautifulSoup

output_csv_file_path = 'csv/codeforcesprofiles_scraped.csv'
MAX_WORKERS = 1  # Maximum number of threads
RETRIES = 5  # Maximum number of retry attempts


def start_driver():
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--disable-gpu")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    return uc.Chrome(options=options)


driver = start_driver()  # Initialize the global driver


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
                continue

            max_problems_solved = 0
            max_days_in_row = 0

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            activity_frame = soup.find('div', class_='_UserActivityFrame_frame')

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

                record = {
                    'username': username.strip(),
                    'max_problems_solved': max_problems_solved,
                    'max_days_in_row': max_days_in_row
                }

                with open(output_csv_file_path, 'a', newline='', encoding='utf-8') as output_file:
                    writer = csv.DictWriter(output_file, fieldnames=record.keys())
                    output_file.seek(0, 2)  # Move to the end of the file
                    writer.writerow(record)
                print(f"Stored data for {username}: {record}")
            else:
                print(f"Failed to load activity frame for {username}. Page source:")
                print(page_source)  # Print the page source for debugging

            break  # Exit the loop if scraping is successful

        except Exception as e:
            print(f"Error for {username}: {e}")
            handle_403()  # Handle any other errors similarly


def thread_worker(username):
    for attempt in range(RETRIES):
        try:
            scrape_profile(username)
            break  # Exit loop if scraping is successful
        except Exception as e:
            print(f"Error for {username} on attempt {attempt + 1}: {e}")
            time.sleep(random.uniform(0.1, 0.5))  # Sleep for a short random duration between attempts


def main():
    usernames = []

    with open('csv/unique_users.csv', mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            usernames.append(row[0])

    threads = []
    for username in usernames:
        while len(threads) >= MAX_WORKERS:
            for t in threads:
                if not t.is_alive():
                    threads.remove(t)

        thread = threading.Thread(target=thread_worker, args=(username,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()  # Wait for all threads to complete

    driver.quit()  # Clean up the driver at the end


if __name__ == '__main__':
    main()
