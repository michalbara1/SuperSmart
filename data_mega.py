from generic import WebsiteDownloader
from selenium.webdriver.common.by import By
import time
import os


class Mega(WebsiteDownloader):
    def get_website_url(self):
        return "https://prices.mega.co.il/"

    def download_files(self, driver, download_directory):
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
    site = Mega("mega")
    site.run()