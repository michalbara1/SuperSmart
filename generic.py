import time
import os
import gzip
import zipfile
import shutil
from datetime import datetime
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.common.by import By


class WebsiteDownloader(ABC):
    def __init__(self, site_name):
        self.site_name = site_name

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

    def process_downloaded_file(self, file_path, download_directory):
        """Process a single downloaded file"""
        try:
            temp_dir = os.path.join(download_directory, 'temp_extract')
            os.makedirs(temp_dir, exist_ok=True)

            if self.extract_compressed_file(file_path, temp_dir):
                self.rename_to_xml(temp_dir)

                for filename in os.listdir(temp_dir):
                    if filename.endswith('.xml'):
                        source_path = os.path.join(temp_dir, filename)
                        target_path = os.path.join(download_directory, filename)
                        shutil.move(source_path, target_path)

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
            with open(file_path, 'rb') as f:
                header = f.read(4)

            if header.startswith(b'PK'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                    return True

            elif header.startswith(b'\x1f\x8b'):
                output_path = os.path.join(extract_dir, os.path.basename(file_path)[:-3])
                with gzip.open(file_path, 'rb') as f_in:
                    with open(output_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                return True

            return False

        except Exception as e:
            print(f"Error extracting file {file_path}: {str(e)}")
            return False

    def rename_to_xml(self, directory):
        """Rename extracted files to .xml"""
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path) and not filename.endswith('.xml'):
                new_path = os.path.splitext(file_path)[0] + '.xml'
                os.rename(file_path, new_path)

    def wait_for_download(self, download_directory, timeout=30):
        """Wait for the download to complete"""
        seconds = 0
        while seconds < timeout:
            files = os.listdir(download_directory)
            completed = [f for f in files if f.endswith(('.gz', '.zip'))]
            in_progress = [f for f in files if f.endswith('.crdownload') or f.endswith('.tmp')]

            if completed and not in_progress:
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