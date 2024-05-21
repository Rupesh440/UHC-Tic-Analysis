import requests
import os
from Extract_file_urls import file_urls

# Function to download a file
def download_file(url, dest_folder):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    
    local_filename = url.split('/')[-1].split('?')[0]
    #print(local_filename)
    local_path = os.path.join(dest_folder, local_filename)
    #print(local_path)
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return f"Successfully downloaded {url}"
    except Exception as e:
        return f"Failed to download {url}. Error: {str(e)}"

# Load URLs from file
with open("file_urls.txt", "r") as f:
    file_urls = [line.strip() for line in f.readlines()]

# Directory where files will be saved
destination_folder = 'downloaded_files'

# Download files sequentially
for i, url in enumerate(file_urls[:1000]):
    result = download_file(url, destination_folder)
    print(f"{i+1}/{len(file_urls[:1000])}: {result}")
