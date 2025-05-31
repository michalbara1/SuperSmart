import time
import os
import gzip
import zipfile
import shutil
import json
import xmltodict
import datetime
from abc import ABC, abstractmethod

from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By



class WebsiteDownloader(ABC):
    def __init__(self, site_name, mongo_uri="mongodb://server:123123123@10.10.248.141:21771/SuperSmart_db",
                 db_name="SuperSmart_db"):
        self.site_name = site_name
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db["items"]

        # Ensure 'items' collection exists by inserting a sample document
        if self.collection.count_documents({}) == 0:
            self.collection.insert_one({"initialized": True})

    def download_all_data(self):
        return list(self.collection.find({}))

    def convert_xml_to_json(self, xml_path):
        """Convert XML to JSON format and return as a dictionary"""
        try:
            with open(xml_path, 'r', encoding='utf-8') as xml_file:
                xml_content = xml_file.read()

            data_dict = xmltodict.parse(xml_content)
            return data_dict  # Return JSON as a dictionary
        except Exception as e:
            print(f"Error converting XML to JSON: {str(e)}")
            return None

    def save_to_mongodb(self, json_data, store_name):
        """Save JSON data to MongoDB if it doesn't already exist for the same day and store"""
        if json_data:
            try:
                # Get the current date
                current_date = datetime.datetime.now().strftime('%Y-%m-%d')

                # Check if an item with the same Barcode, store, and date already exists
                existing_item = self.collection.find_one({
                    "barcode": json_data.get("barcode"),  # Replace with the unique identifier key
                    "storeId": store_name,
                    "date": current_date
                })

                if existing_item:
                    print(f"Item already exists for today in store '{store_name}'. Skipping save.")
                else:
                    # Add the current date and store to the JSON data
                    json_data["date"] = current_date
                    json_data["storeId"] = store_name
                    self.collection.insert_one(json_data)
                    print("Data successfully saved to MongoDB.")
            except Exception as e:
                print(f"Error saving to MongoDB: {str(e)}")
    @abstractmethod
    def get_website_url(self) -> str:
        """Return the website URL to download from"""
        pass

    @abstractmethod
    def download_files(self, driver, download_directory):
        """Implement the site-specific download logic"""
        pass

    def create_download_directory(self):
        """Sets the download directory path within the project folder"""
        project_directory = os.path.dirname(os.path.abspath(__file__))
        download_directory = os.path.join(project_directory, f"downloads_{self.site_name}")

        if not os.path.exists(download_directory):
            os.makedirs(download_directory)

        return os.path.abspath(download_directory)

    def setup_chrome_options(self, download_directory):
        """Sets Chrome options for downloads"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option("detach", True)

        prefs = {
            "download.default_directory": download_directory,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1
        }
        chrome_options.add_experimental_option("prefs", prefs)
        return chrome_options

    def verify_file_complete(self, file_path, timeout=60):
        """Verify that file is completely downloaded and not changing size"""
        if not os.path.exists(file_path):
            return False

        previous_size = -1
        stable_count = 0
        start_time = time.time()

        while time.time() - start_time < timeout:
            current_size = os.path.getsize(file_path)
            if current_size == previous_size:
                stable_count += 1
                if stable_count >= 3:  # File size unchanged for 3 consecutive checks
                    return True
            else:
                stable_count = 0
                previous_size = current_size
            time.sleep(1)

        return False

    def convert_xml_to_json(self, xml_path, json_path):
        """Convert XML file to JSON format"""
        try:
            # Wait for the file to be fully accessible
            time.sleep(1)

            with open(xml_path, 'r', encoding='utf-8') as xml_file:
                xml_content = xml_file.read()

            # Convert XML to dict
            data_dict = xmltodict.parse(xml_content)

            # Convert dict to JSON with proper formatting
            with open(json_path, 'w', encoding='utf-8') as json_file:
                json.dump(data_dict, json_file, ensure_ascii=False, indent=2)

            return True
        except Exception as e:
            print(f"Error converting XML to JSON: {str(e)}")
            return False

    def process_downloaded_file(self, file_path, download_directory):
        """Process a single downloaded file"""
        try:
            # Verify the file is completely downloaded
            if not self.verify_file_complete(file_path):
                print(f"File {file_path} appears to be incomplete")
                return False

            # Add extra wait time after verification
            time.sleep(3)

            temp_dir = os.path.join(download_directory, 'temp_extract')
            os.makedirs(temp_dir, exist_ok=True)

            if self.extract_compressed_file(file_path, temp_dir):
                # Wait after extraction
                time.sleep(1)

                # Convert each file to JSON and save to MongoDB
                for filename in os.listdir(temp_dir):
                    if not filename.endswith('.json'):  # Skip if already JSON
                        extracted_file_path = os.path.join(temp_dir, filename)

                        # Verify extracted file is complete
                        if not self.verify_file_complete(extracted_file_path):
                            print(f"Extracted file {filename} appears to be incomplete")
                            continue

                        json_path = os.path.join(
                            download_directory,
                            os.path.splitext(filename)[0] + '.json'
                        )
                        if self.convert_xml_to_json(extracted_file_path, json_path):
                            print(f"Successfully converted {filename} to JSON")

                            # Load JSON data and save to MongoDB
                            with open(json_path, 'r', encoding='utf-8') as json_file:
                                json_data = json.load(json_file)
                                self.save_to_mongodb(json_data)
                        else:
                            print(f"Failed to convert {filename} to JSON")

                # Clean up
                shutil.rmtree(temp_dir)
                os.remove(file_path)
                return True

            return False

        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            return False

    def extract_compressed_file(self, file_path, extract_dir):
        """Extract either .gz or .zip file"""
        try:
            # Wait before attempting to read the file
            time.sleep(2)

            with open(file_path, 'rb') as f:
                header = f.read(2)

            if header.startswith(b'PK'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                    time.sleep(1)
                    return True

            elif header.startswith(b'\x1f\x8b'):
                output_path = os.path.join(extract_dir, os.path.basename(file_path)[:-3])
                with gzip.open(file_path, 'rb') as f_in:
                    with open(output_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                time.sleep(2)
                return True

            return False

        except Exception as e:
            print(f"Error extracting file {file_path}: {str(e)}")
            return False

    def wait_for_download(self, download_directory, timeout=60):
        """Wait for the download to complete"""
        seconds = 0
        while seconds < timeout:
            files = os.listdir(download_directory)
            completed = [f for f in files if f.endswith(('.gz', '.zip'))]
            in_progress = [f for f in files if f.endswith('.crdownload') or f.endswith('.tmp')]

            if completed and not in_progress:
                # Add extra wait time after seeing completed files
                time.sleep(1)
                return True

            time.sleep(1)
            seconds += 1

        return False

    def run(self):
        """Main execution method"""
        download_directory = self.create_download_directory()
        chrome_options = self.setup_chrome_options(download_directory)

        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(self.get_website_url())
            time.sleep(5)

            self.download_files(driver, download_directory)

        except Exception as e:
            print(f"Error in download process: {str(e)}")
        finally:
            if 'driver' in locals():
                driver.quit()