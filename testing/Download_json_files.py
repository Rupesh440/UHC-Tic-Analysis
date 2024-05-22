import requests
import os
import concurrent.futures
from Parallel_Extraction import file_urls
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
    # Load URLs from file
    with open("/TiC_Analysis/file_urls.txt", "r") as f:
        file_urls = [line.strip() for line in f.readlines()]

    # Directory where files will be saved
    destination_folder = 'downloaded_files'

    # Create a FileDownloader instance
    downloader = FileDownloader(file_urls, destination_folder, max_workers=2)

    # Download files in parallel
    downloader.parallel_download()
