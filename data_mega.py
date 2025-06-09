from bson import ObjectId

from generic import WebsiteDownloader
from selenium.webdriver.common.by import By
import time
import os
import datetime
from pymongo import MongoClient


class Mega(WebsiteDownloader):
    def __init__(self):
        super().__init__("mega")
        # Connect to MongoDB
        self.client = MongoClient('mongodb://server:123123123@10.10.248.141:21771/SuperSmart_db', serverSelectionTimeoutMS=5000)
        self.db = self.client['SuperSmart_db']  # Using the schema's database name
        self.items_collection = self.db['items']  # Items collection
        self.store_id =  ObjectId("65a4e1e1e1e1e1e1e1e1e1e3")

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
        return "https://prices.mega.co.il/"

    # Keep the rest of the methods from the original Mega class
    def download_files(self, driver, download_directory):

        driver.maximize_window()
        time.sleep(3)
        while True:
            try:
                table = driver.find_element(By.CLASS_NAME, "filesDiv")
                rows = table.find_elements(By.CLASS_NAME, "fileDiv")

                for index, row in enumerate(rows):
                    try:
                        download_button = row.find_element(By.CLASS_NAME, "downloadBtn")
                        download_button.click()
                        time.sleep(7)

                        if self.wait_for_download(download_directory):
                            downloaded_files = [f for f in os.listdir(download_directory)
                                                if f.endswith(('.gz', '.zip'))]
                            for file_name in downloaded_files:
                                time.sleep(7)
                                self.process_downloaded_file(
                                    os.path.join(download_directory, file_name),
                                    download_directory
                                )

                    except Exception as e:
                        print(f"Error in row {index + 1}: {e}")

                next_button = driver.find_element(
                    By.XPATH,'/html/body/div[1]/div[2]/button[3]'
                )
                next_action = next_button.get_attribute("data-action")

                if next_action == "next" and next_button.is_enabled():
                    print("Moving to the next page...")
                    next_button.click()
                    time.sleep(7)
                else:
                    print("No more pages. Finished.")
                    break

            except Exception as e:
                print(f"Error while processing: {e}")
                break


if __name__ == "__main__":
    site = Mega()
    site.run()