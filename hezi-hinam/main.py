import os
import json
import datetime
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from urllib.parse import quote_plus


def transform_product(product):
    """Extract and validate product data"""
    # Ensure barcode exists and is valid
    barcode = str(product.get("BarKod", "")).strip()
    if not barcode:
        return None  # Skip products without barcodes

    return {
        "product_id": product.get("Id"),
        "barcode": barcode,
        "name": product.get("Name"),
        "category": product.get("CategoryName"),
        "brand": product.get("ManufacturerName"),
        "price": product.get("Price_Regular"),
        "unit_price": product.get("PricePerUnit"),
        "image_url": product.get("Img"),
        "in_stock": product.get("IsInStock"),
        "last_updated": datetime.datetime.utcnow()
    }


def get_mongo_client(uri):
    """Create MongoDB client with proper timeout settings"""
    return MongoClient(
        uri,
        serverSelectionTimeoutMS=10000,
        socketTimeoutMS=30000,
        connectTimeoutMS=10000,
        retryWrites=True,
        w="majority"
    )


def main():
    # Configuration
    username = "yuval056"
    password = "yuval963852"
    cluster_url = "cluster0.ww37i.mongodb.net"
    db_name = "supersmart"
    collection_name = "items"

    # Escape credentials
    escaped_username = quote_plus(username)
    escaped_password = quote_plus(password)
    mongo_uri = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster_url}/{db_name}?retryWrites=true&w=majority"

    # Directory containing JSON files
    data_dir = r'C:\Users\yuval\OneDrive\שולחן העבודה\year c\pythonProject\hezi-hinam' # change it accordign to your path

    try:
        # Establish connection
        print("Connecting to MongoDB...")
        client = get_mongo_client(mongo_uri)
        client.server_info()  # Test connection
        print("✓ Connected successfully")

        db = client[db_name]
        collection = db[collection_name]

        # Clean existing data if needed (optional)
        # collection.delete_many({})

        # Create non-unique index first
        print("Creating indexes...")
        collection.create_index("barcode", background=True)
        collection.create_index([("category", 1), ("subcategory", 1)], background=True)

        # Process files
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

                    # Prepare bulk operations
                    bulk_ops = []

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

                    # Execute bulk write
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

        # After all data loaded, convert to unique index
        print("\nConverting to unique index...")
        try:
            collection.drop_index("barcode_1")  # Drop existing non-unique
            collection.create_index("barcode", unique=True, background=True)
            print("✓ Unique index created successfully")
        except Exception as e:
            print(f"⚠ Could not create unique index: {str(e)}")
            print("  Some products may have duplicate/null barcodes")

        # Final report
        print("\n" + "=" * 50)
        print("IMPORT COMPLETE")
        print(f"• Processed files: {processed_files}")
        print(f"• Valid products processed: {valid_items}")
        print(f"• Products skipped (no barcode): {skipped_items}")
        print(f"• Total products in collection: {collection.count_documents({})}")

        # Check for potential duplicates
        duplicates = collection.aggregate([
            {"$group": {
                "_id": "$barcode",
                "count": {"$sum": 1},
                "docs": {"$push": "$_id"}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ])

        dup_count = 0
        for dup in duplicates:
            dup_count += 1
            if dup_count == 1:
                print("\n⚠ Duplicate barcodes found:")
            print(f"- Barcode {dup['_id']} appears {dup['count']} times")

        if dup_count == 0:
            print("✓ No duplicate barcodes found")
        print("=" * 50)



    except Exception as e:
        print(f"⛔ Fatal error: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()
            print("\nDatabase connection closed")



if __name__ == "__main__":
    main()