from selenium import webdriver
from selenium.webdriver.common.by import By
import time

# Set up Selenium WebDriver
driver = webdriver.Chrome()

# Navigate to the Transparency in Coverage page
driver.get("https://transparency-in-coverage.uhc.com/")
time.sleep(10)  # Wait for the page to load

# Extract file URLs
file_urls = []
elements = driver.find_elements(By.XPATH, "//a[contains(@href, '.json')]")
for elem in elements:
    file_urls.append(elem.get_attribute("href"))
    if len(file_urls) >= 1000:
        break

driver.quit()

# Save URLs to a file
with open("file_urls.txt", "w") as f:
    for url in file_urls:
        f.write(f"{url}\n")

print(len(file_urls))