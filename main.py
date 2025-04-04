import os
import json
import datetime
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from urllib.parse import quote_plus
from bson import ObjectId

DEFAULT_STORE_ID = ObjectId("65a4e1e1e1e1e1e1e1e1e1e1")


def transform_product(product):
    """Extract and validate product data for new schema"""
    barcode = str(product.get("BarKod", "")).strip()
    if not barcode:
        return None  # Skip products without barcodes

    store_price_entry = {
        "storeId": DEFAULT_STORE_ID,
        "prices": [{
            "date": datetime.datetime.utcnow(),
            "price": product.get("Price_Regular")
        }]
    }

    return {
        "name": product.get("Name"),
        "category": product.get("CategoryName"),
        "barcode": barcode,
        "image": product.get("Img"),
        "storePrices": [store_price_entry]
    }


def main():
    username = "yuval056"
    password = "yuval963852"
    cluster_url = "cluster0.ww37i.mongodb.net"
    db_name = "supersmart"
    collection_name = "items"

    # Escape credentials
    escaped_username = quote_plus(username)
    escaped_password = quote_plus(password)
    # mongo_uri = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster_url}/{db_name}?retryWrites=true&w=majority"
    mongo_uri = "mongodb://localhost:27017/supersmarDB"
    data_dir = r'C:\Users\yuval\OneDrive\שולחן העבודה\year c\SuperSmart\hezi-hinam'

    try:
        print("Connecting to MongoDB...")
        client = MongoClient(mongo_uri)
        client.server_info()
        print("✓ Connected successfully")

        db = client[db_name]
        collection = db[collection_name]

        print("Creating indexes...")
        collection.create_index("barcode", unique=True, background=True)

        processed_files = 0
        valid_items = 0
        skipped_items = 0

        print(f"\nProcessing files from: {data_dir}")
        for filename in os.listdir(data_dir):
            if not filename.endswith('.json'):
                continue

            file_path = os.path.join(data_dir, filename)
            print(f"\n• Processing {filename}...")

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                    if not data.get("IsOK") or not data.get("Results"):
                        print(f"⚠ Invalid structure - skipping")
                        continue

                    bulk_ops = []
                    file_items_count = 0

                    # Try processing via SubCategories path first
                    if data["Results"].get("SubCategories"):
                        print(f"  Structure: Using SubCategories format")
                        for subcategory in data["Results"].get("SubCategories", []):
                            for product in subcategory.get("Items", []):
                                transformed = transform_product(product)
                                if not transformed:
                                    skipped_items += 1
                                    continue

                                bulk_ops.append(
                                    UpdateOne(
                                        {"barcode": transformed["barcode"]},
                                        {"$set": transformed},
                                        upsert=True
                                    )
                                )
                                valid_items += 1
                                file_items_count += 1

                    # Try processing direct Items array if no SubCategories or no items found
                    if file_items_count == 0 and data["Results"].get("Items"):
                        print(f"  Structure: Using direct Items array")
                        for product in data["Results"].get("Items", []):
                            transformed = transform_product(product)
                            if not transformed:
                                skipped_items += 1
                                continue

                            bulk_ops.append(
                                UpdateOne(
                                    {"barcode": transformed["barcode"]},
                                    {"$set": transformed},
                                    upsert=True
                                )
                            )
                            valid_items += 1
                            file_items_count += 1

                    # Try Category-based structure (most common in your logs)
                    if file_items_count == 0 and data["Results"].get("Category"):
                        print(f"  Structure: Using Category format")
                        category_data = data["Results"].get("Category", {})

                        # Get products from Category.Items if it exists
                        for product in category_data.get("Items", []):
                            transformed = transform_product(product)
                            if not transformed:
                                skipped_items += 1
                                continue

                            bulk_ops.append(
                                UpdateOne(
                                    {"barcode": transformed["barcode"]},
                                    {"$set": transformed},
                                    upsert=True
                                )
                            )
                            valid_items += 1
                            file_items_count += 1

                        # Try processing Products array if present
                        for product in category_data.get("Products", []):
                            transformed = transform_product(product)
                            if not transformed:
                                skipped_items += 1
                                continue

                            bulk_ops.append(
                                UpdateOne(
                                    {"barcode": transformed["barcode"]},
                                    {"$set": transformed},
                                    upsert=True
                                )
                            )
                            valid_items += 1
                            file_items_count += 1

                        # Check for SubCategory (singular) structure
                        if file_items_count == 0 and category_data.get("SubCategory"):
                            print(f"  Structure: Using Category.SubCategory format")
                            subcategories = category_data.get("SubCategory")

                            # Make sure it's a list (sometimes it's a single object)
                            if not isinstance(subcategories, list):
                                subcategories = [subcategories]

                            for subcat in subcategories:
                                # Process Items in each SubCategory
                                for product in subcat.get("Items", []):
                                    transformed = transform_product(product)
                                    if not transformed:
                                        skipped_items += 1
                                        continue

                                    bulk_ops.append(
                                        UpdateOne(
                                            {"barcode": transformed["barcode"]},
                                            {"$set": transformed},
                                            upsert=True
                                        )
                                    )
                                    valid_items += 1
                                    file_items_count += 1

                                # Also check Products in each SubCategory
                                for product in subcat.get("Products", []):
                                    transformed = transform_product(product)
                                    if not transformed:
                                        skipped_items += 1
                                        continue

                                    bulk_ops.append(
                                        UpdateOne(
                                            {"barcode": transformed["barcode"]},
                                            {"$set": transformed},
                                            upsert=True
                                        )
                                    )
                                    valid_items += 1
                                    file_items_count += 1

                    # Handle potential other structures
                    if file_items_count == 0:
                        print(f"  ⚠ No processable items found in the file")
                        print(f"  Structure: {list(data['Results'].keys())}")

                        # Dump a sample of the structure for debugging
                        print(f"  Sample structure:")
                        if "Category" in data["Results"]:
                            cat_keys = list(data["Results"]["Category"].keys())
                            print(f"    Category keys: {cat_keys}")

                            # Check if there's a SubCategory with nested SubCategory
                            if "SubCategory" in cat_keys:
                                subcats = data["Results"]["Category"]["SubCategory"]
                                if not isinstance(subcats, list):
                                    subcats = [subcats]

                                for i, sc in enumerate(subcats):
                                    print(f"    SubCategory {i + 1} keys: {list(sc.keys())}")

                                    # Try to find items in a more deeply nested structure
                                    if "SubCategory" in sc:
                                        nested_subcats = sc["SubCategory"]
                                        if not isinstance(nested_subcats, list):
                                            nested_subcats = [nested_subcats]

                                        print(f"    Found nested SubCategories, processing...")
                                        for nested_sc in nested_subcats:
                                            for product in nested_sc.get("Items", []):
                                                transformed = transform_product(product)
                                                if transformed:
                                                    bulk_ops.append(
                                                        UpdateOne(
                                                            {"barcode": transformed["barcode"]},
                                                            {"$set": transformed},
                                                            upsert=True
                                                        )
                                                    )
                                                    valid_items += 1
                                                    file_items_count += 1

                    if bulk_ops:
                        try:
                            result = collection.bulk_write(bulk_ops, ordered=False)
                            print(f"  ✓ Inserted/Updated: {result.upserted_count + result.modified_count}")
                        except BulkWriteError as bwe:
                            print(f"  ⚠ Bulk write errors (continuing): {str(bwe.details)}")

                    processed_files += 1

            except json.JSONDecodeError as e:
                print(f"⚠ Invalid JSON: {str(e)}")
            except Exception as e:
                print(f"⚠ Error processing file: {str(e)}")

        print("\n" + "=" * 50)
        print("IMPORT COMPLETE")
        print(f"• Processed files: {processed_files}")
        print(f"• Valid products processed: {valid_items}")
        print(f"• Products skipped (no barcode): {skipped_items}")
        print(f"• Total products in collection: {collection.count_documents({})}")

    except Exception as e:
        print(f"⛔ Fatal error: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()
            print("\nDatabase connection closed")


if __name__ == "__main__":
    main()