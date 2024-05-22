from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import concurrent.futures

def extract_json_urls(driver_path, webpage_url):
    """
    Extracts JSON file URLs from a webpage using PySpark.

    Args:
    - driver_path (str): Path to the Chrome driver executable.
    - webpage_url (str): The URL of the webpage to scrape.

    Returns:
    - list: A list of extracted JSON file URLs.
    """
    spark = SparkSession.builder \
        .appName("Extract JSON URLs") \
        .getOrCreate()

    # Function to extract URLs from a webpage
    def extract_urls(url):
        driver = webdriver.Chrome(driver_path)
        driver.get(url)
        time.sleep(10)  # Wait for the page to load
        urls = [elem.get_attribute("href") for elem in driver.find_elements(By.XPATH, "//a[contains(@href, '.json')]")]
        driver.quit()
        return urls

    # Extract JSON URLs concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(extract_urls, webpage_url): webpage_url}

        # Process completed tasks
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                json_urls = future.result()
            except Exception as exc:
                print(f"Error occurred for {url}: {exc}")

    spark.stop()
    return json_urls

def save_urls_to_file(urls, filename="file_urls.txt"):
    """
    Saves a list of URLs to a file.

    Args:
    - urls (list): The list of URLs to save.
    - filename (str): The name of the file to save URLs. Default is "file_urls.txt".
    """
    with open(filename, "w") as f:
        for url in urls:
            f.write(f"{url}\n")

# Example usage
if _name_ == "_main_":
    # Specify the path to the Chrome driver executable
    driver_path = "/path/to/chromedriver"
    # Specify the URL of the webpage to scrape
    webpage_url = "https://transparency-in-coverage.uhc.com/"

    # Extract JSON URLs using PySpark
    json_urls = extract_json_urls(driver_path, webpage_url)

    # Save extracted URLs to a file
    save_urls_to_file(json_urls)