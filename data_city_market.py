from generic import WebsiteDownloader
from selenium.webdriver.common.by import By
import time
import os
import datetime
from pymongo import MongoClient


class DataCityMarket(WebsiteDownloader):
    def __init__(self):
        super().__init__("city_market")
        # Connect to MongoDB
        self.client = MongoClient('mongodb://server:123123123@10.10.248.141:21771/SuperSmart_db', serverSelectionTimeoutMS=5000)
        self.db = self.client['SuperSmart_db']
        self.items_collection = self.db['items']
        self.store_id = "CityMarket"

    def transform_data(self, json_data):
        """Transform CityMarket JSON data into standardized format"""
        try:
            products = []

            # Handle the promotions structure in City Market data
            if "Root" in json_data and "Promotions" in json_data["Root"]:
                promotions = json_data["Root"]["Promotions"].get("Promotion", [])
                if not isinstance(promotions, list):
                    promotions = [promotions]

                print(f"Found {len(promotions)} promotions")

                for promotion in promotions:
                    if "PromotionItems" not in promotion:
                        continue

                    items = promotion["PromotionItems"].get("Item", [])
                    if not isinstance(items, list):
                        items = [items]

                    promotion_price = float(promotion.get("DiscountedPrice", 0))
                    promotion_description = promotion.get("PromotionDescription", "")

                    for item in items:
                        barcode = str(item.get("ItemCode", "")).strip()
                        if not barcode:
                            continue

                        print(f"Processing promotion item with barcode: {barcode}")

                        # Check if item exists in MongoDB
                        existing_item = self.items_collection.find_one({"barcode": barcode})
                        if not existing_item:
                            print(f"Product with barcode {barcode} not found in MongoDB - skipping")
                            continue

                        # Create price entry
                        price_entry = {
                            "date": datetime.datetime.utcnow(),
                            "price": promotion_price,
                            "promotionDescription": promotion_description
                        }

                        # Create store price entry
                        store_price_entry = {
                            "storeId": self.store_id,
                            "prices": [price_entry]
                        }

                        # Create the product with data from both sources
                        transformed_product = {
                            "name": promotion_description,
                            "barcode": barcode,
                            "category": existing_item.get("category", "Unknown"),
                            "image": existing_item.get("image", ""),  # Use the same image from MongoDB
                            "code": existing_item.get("code", ""),
                            "storePrices": [store_price_entry]
                        }
                        products.append(transformed_product)

            # Handle the standard Items structure (if exists)
            if "Root" in json_data and "Items" in json_data["Root"]:
                items = json_data["Root"]["Items"].get("Item", [])
                if items:
                    # Convert to list if it's a single item
                    if not isinstance(items, list):
                        items = [items]

                    print(f"Found {len(items)} regular items")

                    for item in items:
                        barcode = str(item.get("ItemCode", "")).strip()
                        if not barcode:
                            continue

                        # Check if item exists in MongoDB
                        existing_item = self.items_collection.find_one({"barcode": barcode})
                        if not existing_item:
                            print(f"Product with barcode {barcode} not found in MongoDB - skipping")
                            continue

                        # Get price information
                        price = float(item.get("ItemPrice", 0))

                        # Create price entry
                        price_entry = {
                            "date": datetime.datetime.utcnow(),
                            "price": price
                        }

                        # Create store price entry
                        store_price_entry = {
                            "storeId": self.store_id,
                            "prices": [price_entry]
                        }

                        # Create the product with data from both sources
                        transformed_product = {
                            "name": item.get("ItemName", existing_item.get("name", "")),
                            "barcode": barcode,
                            "category": existing_item.get("category", "Unknown"),
                            "image": existing_item.get("image", ""),  # Use the same image from MongoDB
                            "storePrices": [store_price_entry]
                        }
                        products.append(transformed_product)

            # Process hezi-hinam format if present
            if "Results" in json_data:
                if "Category" in json_data["Results"] and "Items" in json_data["Results"]["Category"]:
                    items = json_data["Results"]["Category"]["Items"]
                    if not isinstance(items, list):
                        items = [items]

                    print(f"Found {len(items)} items in Hezi-Hinam format")

                    for item in items:
                        barcode = str(item.get("BarKod", "")).strip()
                        if not barcode:
                            continue

                        # Check if item exists in MongoDB
                        existing_item = self.items_collection.find_one({"barcode": barcode})
                        if not existing_item:
                            print(f"Product with barcode {barcode} not found in MongoDB - skipping")
                            continue

                        # Get price information
                        price = float(item.get("Price_Regular", 0))

                        # Create price entry
                        price_entry = {
                            "date": datetime.datetime.utcnow(),
                            "price": price
                        }

                        # Create store price entry
                        store_price_entry = {
                            "storeId": self.store_id,
                            "prices": [price_entry]
                        }

                        # Create the product with data from both sources
                        transformed_product = {
                            "name": item.get("Name", existing_item.get("name", "")),
                            "barcode": barcode,
                            "category": existing_item.get("category", "Unknown"),
                            "image": existing_item.get("image", ""),  # Use the same image from MongoDB
                            "storePrices": [store_price_entry]
                        }
                        products.append(transformed_product)

            print(f"Total products transformed: {len(products)}")
            return products

        except Exception as e:
            print(f"Error transforming data: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def save_to_mongodb(self, json_data):
        """Update existing items with City Market price information"""
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
                # Check if original product exists in MongoDB
                existing_product = self.items_collection.find_one({"barcode": barcode})
                if not existing_product:
                    print(f"Product {barcode} not found in MongoDB - skipping")
                    continue

                # Get store prices from the transformed product
                new_store_price = product.get("storePrices", [])[0] if product.get("storePrices") else None

                if not new_store_price:
                    print(f"No price information for product {barcode}")
                    continue

                # Update the existing document by adding the new store price
                result = self.items_collection.update_one(
                    {"barcode": barcode},
                    {"$addToSet": {"storePrices": new_store_price}}
                )

                if result.modified_count > 0:
                    updated_count += 1
                    print(f"Updated product with barcode: {barcode} with City Market price")
                else:
                    print(f"No changes for product: {barcode} (price may already exist)")

            except Exception as e:
                print(f"Error updating product {barcode}: {str(e)}")

        print(f"Successfully updated {updated_count} items with City Market prices")

    def get_website_url(self):
        return "https://citymarketgivatayim.binaprojects.com/Main.aspx"

    # Keep the existing methods
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
    site = DataCityMarket()
    site.run()