import datetime

from bson import ObjectId
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from generic import WebsiteDownloader
import time
import os
from pymongo import MongoClient


class RamiLevi(WebsiteDownloader):
    def __init__(self):
        super().__init__("rami_levi")
        # Connect to MongoDB
        self.client = MongoClient('mongodb://server:123123123@10.10.248.141:21771/SuperSmart_db', serverSelectionTimeoutMS=5000)
        self.db = self.client['SuperSmart_db']  # Replace with your DB name
        self.items_collection = self.db['items']  # Your items collection
        self.store_id = ObjectId("65a4e1e1e1e1e1e1e1e1e1e2")  # Store ID for Rami Levi

    def transform_data(self, json_data):
        """Transform Rami Levi JSON data into standardized format"""
        try:
            items = json_data.get("Root", {}).get("Items", {})
            if items.get("@Count") == "0" or not items.get("Item"):
                print("No items found in the price file")
                return None

            products = []
            item_list = items.get("Item", [])
            if not isinstance(item_list, list):
                item_list = [item_list]

            for product in item_list:
                barcode = product.get("ItemCode")
                if not barcode:
                    print(f"Skipping product without barcode: {product.get('ItemName', 'Unknown')}")
                    continue

                # Check if item exists in MongoDB
                existing_item = self.items_collection.find_one({"barcode": barcode})
                if not existing_item:
                    print(f"Product with barcode {barcode} not found in MongoDB - skipping")
                    continue

                # Create price entry
                price_entry = {
                    "date": datetime.datetime.utcnow(),
                    "price": float(product.get("ItemPrice", 0))
                }

                # Create store price entry
                store_price_entry = {
                    "storeId": self.store_id,
                    "prices": [price_entry]
                }

                # Create the product with data from both sources
                transformed_product = {
                    "name": product.get("ItemName"),
                    "barcode": barcode,
                    "category": existing_item.get("category", "Unknown"),
                    "image": existing_item.get("image", ""),
                    "code": existing_item.get("code", ""),
                    "storePrices": [store_price_entry]
                }
                products.append(transformed_product)

            return products

        except Exception as e:
            print(f"Error transforming data: {str(e)}")
            return None

    def save_to_mongodb(self, json_data):
        """Update existing items with store-specific price information"""
        if not json_data:
            print("No data to save")
            return

        transformed_data = self.transform_data(json_data)
        if not transformed_data:
            print("No valid data to save")
            return

        updated_count = 0
        current_date = datetime.datetime.utcnow().date()

        for product in transformed_data:
            barcode = product.get("barcode")
            if not barcode:
                print("Skipping product with no barcode")
                continue

            try:
                # Check if the product exists in MongoDB
                existing_product = self.items_collection.find_one({"barcode": barcode})
                if not existing_product:
                    print(f"Product {barcode} not found in MongoDB - skipping")
                    continue

                # Get the new store price
                new_store_price = product.get("storePrices", [])[0] if product.get("storePrices") else None
                if not new_store_price:
                    print(f"No price information for product {barcode}")
                    continue

                # Extract the date part from the new price entry
                new_price_date = new_store_price["prices"][0]["date"].date()
                new_store_id = new_store_price["storeId"]

                # Check if a price entry for the same date and storeId already exists
                existing_prices = existing_product.get("storePrices", [])
                price_exists = any(
                    price_entry["storeId"] == new_store_id and
                    any(price["date"].date() == new_price_date for price in price_entry.get("prices", []))
                    for price_entry in existing_prices
                )

                if price_exists:
                    print(
                        f"Price for product {barcode} on {new_price_date} for storeId {new_store_id} already exists - skipping")
                    continue

                # Add the new store price to the existing product
                result = self.items_collection.update_one(
                    {"barcode": barcode},
                    {"$addToSet": {"storePrices": new_store_price}}
                )

                if result.modified_count > 0:
                    updated_count += 1
                    print(f"Updated product with barcode: {barcode} with new price for storeId {new_store_id}")
                else:
                    print(f"No changes for product: {barcode} (price may already exist)")

            except Exception as e:
                print(f"Error updating product {barcode}: {str(e)}")

        print(f"Successfully updated {updated_count} items with new prices")
    def get_website_url(self):
        return "https://url.publishedprices.co.il/login"

    def download_files(self, driver, download_directory):
        self.clear_products_file()
        driver.maximize_window()
        time.sleep(3)

        # Login
        login = driver.find_element(By.NAME, "username")
        login.send_keys("RamiLevi", Keys.ENTER)
        time.sleep(10)

        # Click on the refresh button
        try:
            refresh_button = driver.find_element(By.XPATH, '//*[@id="refresh-btn"]/span')
            refresh_button.click()
            print("Clicked on the refresh button.")
            time.sleep(5)
        except Exception as e:
            print(f"Error clicking on the refresh button: {e}")

        # Find and download files
        table = driver.find_element(By.ID, "fileList")
        rows = table.find_elements(By.TAG_NAME, "tr")
        existing_products = self.load_existing_products()

        for i, row in enumerate(rows):
            try:
                product_name = f"product_{i}"

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