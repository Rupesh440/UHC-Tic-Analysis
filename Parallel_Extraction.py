import concurrent.futures
import time
import requests
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

class URLExtractor:
    def __init__(self, url, headless=True, max_urls=1000):
        self.url = url
        self.headless = headless
        self.max_urls = max_urls

    def setup_driver(self):
        options = Options()
        if self.headless:
            options.add_argument('--headless')  # Run headless Chrome
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        return driver

    def extract_urls(self):
        driver = self.setup_driver()
        driver.get(self.url)
        time.sleep(10)  # Wait for the page to load

        # Extract file URLs
        file_urls = []
        elements = driver.find_elements(By.XPATH, "//a[contains(@href, '.json')]")
        for elem in elements:
            file_urls.append(elem.get_attribute("href"))
            if len(file_urls) >= self.max_urls:
                break

        driver.quit()
        return file_urls

    def parallel_extract(self, n_workers=4):
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = [executor.submit(self.extract_urls) for _ in range(n_workers)]
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.extend(future.result())
                if len(results) >= self.max_urls:
                    break
        return results[:self.max_urls]

class FileDownloader:
    def __init__(self, urls, dest_folder, max_workers=4):
        self.urls = urls
        self.dest_folder = dest_folder
        self.max_workers = max_workers

        if not os.path.exists(self.dest_folder):
            os.makedirs(self.dest_folder)
            print(f"Created directory: {self.dest_folder}")
        else:
            print(f"Directory already exists: {self.dest_folder}")

    def download_file(self, url):
        local_filename = url.split('/')[-1].split('?')[0]
        local_path = os.path.join(self.dest_folder, local_filename)
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return f"Successfully downloaded {url}"
        except Exception as e:
            return f"Failed to download {url}. Error: {str(e)}"

    def parallel_download(self):
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.download_file, url): url for url in self.urls[:1000]}
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                result = future.result()
                results.append(result)
                print(f"{i + 1}/{min(len(self.urls), 1000)}: {result}")
        return results



if __name__ == "__main__":
    extractor = URLExtractor("https://transparency-in-coverage.uhc.com/")
    file_urls = extractor.parallel_extract(n_workers=4)
    # Directory where files will be saved
    destination_folder = 'downloaded_files'

    # Create a FileDownloader instance
    downloader = FileDownloader(file_urls, destination_folder, max_workers=4)

    # Download files in parallel
    downloader.parallel_download()

    # Save URLs to a file
    with open("file_urls.txt", "w") as f:
        for url in file_urls:
            f.write(f"{url}\n")
#The time took to extract 1000 files without using parallelization is arount 10-15 minutes but with paralell
# processing it came down to 2 minutes and can make it more less as well with increase of workers.