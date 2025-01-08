from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from generic import WebsiteDownloader
import time
import os


class RamiLevi(WebsiteDownloader):
    def __init__(self):
        super().__init__("rami_levi")

    def get_website_url(self):
        return "https://url.publishedprices.co.il/login"

    def download_files(self, driver, download_directory):
        self.clear_products_file()

        # התחברות
        login = driver.find_element(By.NAME, "username")
        login.send_keys("RamiLevi", Keys.ENTER)
        time.sleep(10)

        # איתור והורדת קבצים
        table = driver.find_element(By.ID, "fileList")
        rows = table.find_elements(By.TAG_NAME, "tr")
        existing_products = self.load_existing_products()

        for i, row in enumerate(rows):
            try:
                product_name = f"product_{i}"  # התאם לפי המבנה האמיתי של הטבלה

                if product_name not in existing_products:
                    print(f"Processing product: {product_name}")

                    download_button = row.find_element(By.CLASS_NAME, "f")
                    download_button.click()
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
    downloader = RamiLevi()
    downloader.run()