import azure.functions as func
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from io import StringIO
from azure.storage.blob import BlobServiceClient
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="dualUpload")
def dualUpload(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # -------------------------------
    # CONFIGURATION
    # -------------------------------
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    laptop_base_url = "https://webscraper.io/test-sites/e-commerce/static/computers/laptops?page={}"
    art_api_url = "https://api.artic.edu/api/v1/artworks"
    BLOB_CONNECTION_STRING = os.getenv("BlobConnectionString")
    CONTAINER_NAME = "mycontainer"

    # -------------------------------
    # STEP 1: SCRAPE LAPTOP DATA
    # -------------------------------
    laptop_data = []

    for page in range(1, 3):  # Scrape first 2 pages
        url = laptop_base_url.format(page)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            products = soup.select('div.thumbnail')
            if not products:
                logging.warning(f"No products found on page {page}")
                continue
            for product in products:
                title = product.select_one('a.title').text.strip()
                price = product.select_one('h4.price').text.strip()
                description = product.select_one('p.description').text.strip()
                laptop_data.append({
                    'Title': title,
                    'Price': price,
                    'Description': description
                })
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error on page {page}: {e}")

    laptop_df = pd.DataFrame(laptop_data)
    logging.info(f"Scraped {len(laptop_df)} laptop products")

    # -------------------------------
    # STEP 2: FETCH ARTWORK DATA
    # -------------------------------
    try:
        params = {"page": 1, "limit": 20}
        response = requests.get(art_api_url, params=params)
        response.raise_for_status()
        art_data = response.json()["data"]

        artwork_df = pd.DataFrame([
            {
                "id": art["id"],
                "title": art["title"],
                "artist": art["artist_display"],
                "date": art["date_display"],
                "medium": art["medium_display"],
                "image_url": art["image_id"] and f"https://www.artic.edu/iiif/2/{art['image_id']}/full/843,/0/default.jpg" or None
            }
            for art in art_data
        ])
        logging.info(f"Fetched {len(artwork_df)} artworks")
    except Exception as e:
        logging.error(f"Failed to fetch artwork data: {e}")
        artwork_df = pd.DataFrame()

    # -------------------------------
    # STEP 3: UPLOAD TO AZURE BLOB
    # -------------------------------
    try:
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        try:
            container_client.create_container()
            logging.info(f"Created container: {CONTAINER_NAME}")
        except Exception:
            logging.info(f"Container '{CONTAINER_NAME}' already exists.")

        # Upload laptops.csv
        try:
            laptop_csv = StringIO()
            laptop_df.to_csv(laptop_csv, index=False, encoding='utf-8-sig')
            container_client.upload_blob(name="laptops.csv", data=laptop_csv.getvalue(), overwrite=True)
            logging.info("Laptop data uploaded to Azure Blob: laptops.csv")
        except Exception as e:
            logging.error(f"Failed to upload laptops.csv: {e}")

        # Upload artworks.csv
        try:
            artwork_csv = StringIO()
            artwork_df.to_csv(artwork_csv, index=False, encoding='utf-8-sig')
            container_client.upload_blob(name="artworks.csv", data=artwork_csv.getvalue(), overwrite=True)
            logging.info("Artwork data uploaded to Azure Blob: artworks.csv")
        except Exception as e:
            logging.error(f"Failed to upload artworks.csv: {e}")

    except Exception as e:
        logging.error(f"Blob service error: {e}")
        return func.HttpResponse("Blob upload failed.", status_code=500)

    return func.HttpResponse("This HTTP triggered function executed successfully.")