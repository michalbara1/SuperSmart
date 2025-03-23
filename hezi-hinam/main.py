import os
import json
from pymongo import MongoClient

def main():
    # Connect to MongoDB
    mongo_uri = 'mongodb+srv://yuval056:yuval963852@cluster0.ww37i.mongodb.net/'
    mongo_client = MongoClient(mongo_uri)

    # Reference the correct database and collection
    db = mongo_client['supersmart']  # Database name
    collection = db['database']['items']  # Correct collection reference

    # Check if the connection works
    try:
        print("Connected to MongoDB.")
        print("Existing collections:", db.list_collection_names())  # List collections
    except Exception as e:
        print(f"Connection error: {e}")
        return

    # Directory containing JSON files
    directory = os.path.abspath('C:\\Users\\yuval\\OneDrive\\שולחן העבודה\\year c\\pythonProject\\hezi-hinam')

    # Validate the directory
    if not os.path.exists(directory) or not os.path.isdir(directory):
        print(f"Error: The path '{directory}' is not a valid directory.")
        return

    # Check initial document count
    print(f"Documents in 'items' before insert: {collection.count_documents({})}")

    # Iterate through all JSON files in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)

            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)  # Load JSON data
                    print(f"Processing {filename}: {json.dumps(data, indent=4)[:500]}...")  # Show first 500 chars

                    # Insert into MongoDB
                    collection.insert_one(data)
                    print(f"Saved {filename} to MongoDB")

            except json.JSONDecodeError as json_error:
                print(f"JSON error in {filename}: {json_error}")
            except Exception as e:
                print(f"Failed to insert {filename}: {e}")

    # Check final document count
    print(f"Documents in 'items' after insert: {collection.count_documents({})}")

if __name__ == "__main__":
    main()