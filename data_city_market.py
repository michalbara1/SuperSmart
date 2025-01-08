from generic import WebsiteDownloader
from selenium.webdriver.common.by import By
import time
import os

class DataCityMarket(WebsiteDownloader):
    def get_website_url(self):
        return "https://citymarketgivatayim.binaprojects.com/Main.aspx"

    def download_files(self, driver, download_directory):
        self.clear_products_file()
        table = driver.find_element(By.ID, "myTable")
        rows = table.find_elements(By.TAG_NAME, "tr")
        existing_products = self.load_existing_products()

        for row in rows[1:]:  # Skip header row
            try:
                product_name = row.find_element(By.XPATH, ".//td[1]").text
                if product_name not in existing_products:
                    print(f"Processing product: {product_name}")

                    button = row.find_element(By.XPATH, ".//button[contains(text(), 'להורדה')]")
                    button.click()
                    time.sleep(5)

                    if self.wait_for_download(download_directory):
                        files = [f for f in os.listdir(download_directory)
                                 if f.endswith(('.gz', '.zip'))]

                        for file in files:
                            file_path = os.path.join(download_directory, file)
                            print(f"Processing downloaded file: {file}")

                            if self.process_downloaded_file(file_path, download_directory):
                                self.update_existing_products(product_name)
                                print(f"Successfully processed {product_name}")
                            else:
                                print(f"Failed to process {product_name}")

            except Exception as e:
                print(f"Error processing row: {str(e)}")

    def clear_products_file(self):
        with open("products.txt", "w", encoding='utf-8') as f:
            f.write("")

    def load_existing_products(self):
        try:
            with open("products.txt", "r", encoding='utf-8') as f:
                return f.read().splitlines()
        except FileNotFoundError:
            return []

    def update_existing_products(self, product_name):
        with open("products.txt", "a", encoding='utf-8') as f:
            f.write(f"{product_name}\n")

if __name__ == "__main__":
    site = DataCityMarket("city_market")
    site.run()