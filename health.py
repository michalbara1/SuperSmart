import re
import csv
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple, Any
from pymongo import MongoClient

def extract_brands(name: str) -> list:
    brands = {'תנובה', 'טרה', 'הרדוף', 'יטבתה', 'מהדרין', 'שטראוס',
              'נסטלה', 'פלדמן', 'גד', 'עלית', 'פריגת', 'צוריאל', 'דנונה'}
    return [brand for brand in brands if brand in name]

def extract_key_attributes(name: str) -> dict:
    attributes = {}
    fat_match = re.search(r'(\d+%)', name)
    if fat_match:
        attributes['fat_percent'] = fat_match.group(1)
    product_types = {'חלב', 'יוגורט', 'גבינה', 'שמנת', 'גלידה', 'משקה'}
    for p_type in product_types:
        if p_type in name:
            attributes['type'] = p_type
            break
    return attributes

def similarity_score(name1: str, name2: str) -> float:
    return SequenceMatcher(None, name1, name2).ratio()

def try_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def deep_clean(name: str) -> str:
    if not name:
        return ""
    name = name.lower()
    name = re.sub(r'[^\w\s%]', ' ', name)
    stop_words = {'עם', 'ללא', 'בטעם', 'של', 'את', 'ה', 'קרטון', 'טרי'}
    brands = {'תנובה', 'טרה', 'הרדוף', 'יטבתה', 'מהדרין', 'שטראוס', 'נסטלה'}
    words = [word for word in name.split()
             if word not in stop_words and word not in brands]
    return ' '.join(sorted(words))

def find_best_match(hezi_item: dict, csv_items: list) -> Optional[Tuple[dict, float]]:
    hezi_name = deep_clean(hezi_item.get('name', ''))
    hezi_attrs = extract_key_attributes(hezi_item.get('name', ''))
    hezi_numeric = set(re.findall(r'\d+%', hezi_item.get('name', '')))

    best_match, best_score = None, 0

    for csv_item in csv_items:
        csv_name = deep_clean(csv_item.get('shmmitzrach', ''))
        score = similarity_score(hezi_name, csv_name)

        csv_attrs = extract_key_attributes(csv_item.get('shmmitzrach', ''))
        csv_numeric = set(re.findall(r'\d+%', csv_item.get('shmmitzrach', '')))

        if hezi_attrs.get('type') and csv_attrs.get('type') == hezi_attrs.get('type'):
            score *= 1.3

        if hezi_numeric & csv_numeric:
            score *= 1.5

        if score > best_score:
            best_match = csv_item
            best_score = score

    return (best_match, best_score) if best_score > 0.65 else None

def update_products_with_fuzzy_match(csv_path, mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    with open(csv_path, encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        nutrition_items = list(reader)

    hezi_items = list(collection.find({}))
    matched_count = 0

    for hezi_item in hezi_items:
        best_match = find_best_match(hezi_item, nutrition_items)
        if best_match:
            matched_item, score = best_match
            nutrition_data = {
                'nutrition': {
                    'protein': try_float(matched_item.get('protein')),
                    'fat': try_float(matched_item.get('total_fat')),
                    'carbs': try_float(matched_item.get('carbohydrates')),
                    'calories': try_float(matched_item.get('food_energy')),
                    'sodium': try_float(matched_item.get('sodium')),
                    'calcium': try_float(matched_item.get('calcium')),
                    'vitamin_c': try_float(matched_item.get('vitamin_c')),
                    'cholesterol': try_float(matched_item.get('cholesterol')),
                    'match_score': score,
                    'source': 'moh_mitzrachim.csv',
                    'matched_name': matched_item.get('shmmitzrach')
                }
            }
            collection.update_one({'_id': hezi_item['_id']}, {'$set': nutrition_data})
            print(f"✔ נמצא התאמה: '{hezi_item.get('name')}' ← '{matched_item.get('shmmitzrach')}' (ציון: {score:.2f})")
            matched_count += 1
        else:
            print(f"✘ לא נמצאה התאמה עבור: {hezi_item.get('name')}")

    print(f"\n✅ בסך הכול עודכנו {matched_count} מוצרים עם ערכים תזונתיים")

if __name__ == "__main__":
    CSV_PATH = r"C:\Users\yuval\OneDrive\שולחן העבודה\year c\SuperSmart\moh_mitzrachim.csv"
    MONGO_URI = "mongodb://localhost:27017"
    DB_NAME = "supersmart"
    COLLECTION_NAME = "items"

    update_products_with_fuzzy_match(CSV_PATH, MONGO_URI, DB_NAME, COLLECTION_NAME)