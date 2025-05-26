# import json
# import logging
# import os
# import time
# import base64
# from datetime import datetime

# import pdf2image
# import requests
# from atproto import Client
# from bs4 import BeautifulSoup
# from requests_oauthlib import OAuth1

# # Global hashtags - Change in 2 places
# GLOBAL_HASHTAGS = "#f1 #formula1 #fia #SpanishGP"

# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
# )


# class TwitterAPI:
#     def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
#         """
#         Initialize the TwitterAPI with authentication credentials.

#         Args:
#             consumer_key (str): Your API/Consumer Key
#             consumer_secret (str): Your API/Consumer Secret
#             access_token (str): Your Access Token
#             access_token_secret (str): Your Access Token Secret
#         """
#         self.consumer_key = consumer_key
#         self.consumer_secret = consumer_secret
#         self.access_token = access_token
#         self.access_token_secret = access_token_secret
#         self.auth = OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)
#         self.base_url = "https://api.twitter.com"

#     def upload_image_chunked(self, image_path, media_category="tweet_image"):
#         """
#         Upload an image using the chunked media upload endpoint.

#         Args:
#             image_path (str): Path to the image file
#             media_category (str): Media category (default: tweet_image)

#         Returns:
#             str: Media ID if successful, None otherwise
#         """
#         # Check if file exists
#         if not os.path.exists(image_path):
#             logging.error(f"Error: File {image_path} does not exist.")
#             return None

#         # Get file size
#         file_size = os.path.getsize(image_path)

#         # Step 1: INIT - Initialize the upload
#         init_url = f"{self.base_url}/1.1/media/upload.json"
#         init_params = {
#             "command": "INIT",
#             "total_bytes": file_size,
#             "media_type": self._get_media_type(image_path),
#             "media_category": media_category
#         }

#         init_response = requests.post(init_url, auth=self.auth, data=init_params)

#         if init_response.status_code != 200:
#             logging.error(f"Error initializing upload: {init_response.text}")
#             return None

#         media_id = init_response.json()["media_id_string"]

#         # Step 2: APPEND - Upload the file in chunks
#         chunk_size = 4 * 1024 * 1024  # 4MB chunks
#         segment_index = 0

#         with open(image_path, "rb") as image_file:
#             while True:
#                 chunk = image_file.read(chunk_size)
#                 if not chunk:
#                     break

#                 append_url = f"{self.base_url}/1.1/media/upload.json"
#                 append_params = {
#                     "command": "APPEND",
#                     "media_id": media_id,
#                     "segment_index": segment_index
#                 }

#                 files = {"media": chunk}
#                 append_response = requests.post(append_url, auth=self.auth, data=append_params, files=files)

#                 if append_response.status_code != 204:
#                     logging.error(f"Error appending chunk {segment_index}: {append_response.text}")
#                     return None

#                 segment_index += 1
#                 logging.info(f"Uploaded chunk {segment_index} of {image_path} to Twitter")

#         # Step 3: FINALIZE - Complete the upload
#         finalize_url = f"{self.base_url}/1.1/media/upload.json"
#         finalize_params = {
#             "command": "FINALIZE",
#             "media_id": media_id
#         }

#         finalize_response = requests.post(finalize_url, auth=self.auth, data=finalize_params)

#         if finalize_response.status_code != 200:
#             logging.error(f"Error finalizing upload: {finalize_response.text}")
#             return None

#         finalize_data = finalize_response.json()

#         # Check if processing is needed
#         if "processing_info" in finalize_data:
#             media_id = self._wait_for_processing(media_id, finalize_data["processing_info"])

#         return media_id

#     def _wait_for_processing(self, media_id, processing_info):
#         """
#         Wait for media processing to complete.

#         Args:
#             media_id (str): Media ID
#             processing_info (dict): Processing info from FINALIZE response

#         Returns:
#             str: Media ID if successful, None otherwise
#         """
#         state = processing_info.get("state")

#         while state == "pending" or state == "in_progress":
#             check_after_secs = processing_info.get("check_after_secs", 1)
#             logging.info(f"Twitter media processing in progress. Waiting {check_after_secs} seconds...")
#             time.sleep(check_after_secs)

#             # Check status
#             status_url = f"{self.base_url}/1.1/media/upload.json"
#             status_params = {
#                 "command": "STATUS",
#                 "media_id": media_id
#             }

#             status_response = requests.get(status_url, auth=self.auth, params=status_params)

#             if status_response.status_code != 200:
#                 logging.error(f"Error checking media status: {status_response.text}")
#                 return None

#             processing_info = status_response.json().get("processing_info", {})
#             state = processing_info.get("state")

#             if state == "failed":
#                 logging.error(f"Twitter media processing failed: {processing_info.get('error')}")
#                 return None

#         return media_id

#     def post_tweet_with_media(self, text, media_ids):
#         """
#         Post a tweet with media.

#         Args:
#             text (str): Tweet text
#             media_ids (list): List of media IDs to attach

#         Returns:
#             dict: Tweet data if successful, None otherwise
#         """
#         tweet_url = f"{self.base_url}/2/tweets"

#         # Ensure media_ids is a list
#         if not isinstance(media_ids, list):
#             media_ids = [media_ids]

#         payload = {
#             "text": text,
#             "media": {"media_ids": media_ids}
#         }

#         headers = {
#             "Content-Type": "application/json"
#         }

#         response = requests.post(
#             tweet_url,
#             auth=self.auth,
#             headers=headers,
#             json=payload
#         )

#         if response.status_code != 201:
#             logging.error(f"Error posting tweet: {response.text}")
#             return None

#         return response.json()

#     def _get_media_type(self, file_path):
#         """
#         Get the media type based on file extension.

#         Args:
#             file_path (str): Path to the media file

#         Returns:
#             str: Media type
#         """
#         extension = os.path.splitext(file_path)[1].lower()

#         media_types = {
#             ".jpg": "image/jpeg",
#             ".jpeg": "image/jpeg",
#             ".png": "image/png",
#             ".gif": "image/gif",
#             ".mp4": "video/mp4"
#         }

#         return media_types.get(extension, "application/octet-stream")


# class FIADocumentHandler:
#     def __init__(self):
#         self.base_url = "https://www.fia.com/documents/championships/fia-formula-one-world-championship-14/season/season-2025-2071"
#         self.download_dir = "downloads"
#         self.processed_docs = self._load_processed_docs()
#         self.bluesky_client = Client()
#         self.twitter_client = None

#     def _load_processed_docs(self):
#         try:
#             with open("processed_docs.json", "r") as f:
#                 urls = json.load(f)
#                 normalized_urls = [
#                     url.strip().lower().replace("\\", "/") for url in urls
#                 ]
#                 return {
#                     "urls": normalized_urls,
#                     "filenames": {
#                         os.path.basename(url).lower() for url in normalized_urls
#                     },
#                 }
#         except (FileNotFoundError, json.JSONDecodeError):
#             if os.path.exists("processed_docs.json"):
#                 os.rename(
#                     "processed_docs.json", f"processed_docs.json.bak.{int(time.time())}"
#                 )
#             return {"urls": [], "filenames": set()}

#     def _save_processed_docs(self):
#         with open("processed_docs.json", "w") as f:
#             json.dump(self.processed_docs["urls"], f)

#     def authenticate_bluesky(self, username, password, max_retries=3, timeout=30):
#         for attempt in range(max_retries):
#             try:
#                 self.bluesky_client = Client()
#                 self.bluesky_client.login(username, password)
#                 logging.info("Successfully authenticated with Bluesky")
#                 return
#             except Exception as e:
#                 if attempt == max_retries - 1:
#                     raise
#                 logging.warning(
#                     f"Authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
#                 )
#                 time.sleep(2**attempt)

#     def authenticate_twitter(self, consumer_key, consumer_secret, access_token, access_token_secret):
#         try:
#             self.twitter_client = TwitterAPI(
#                 consumer_key, consumer_secret, access_token, access_token_secret
#             )
#             logging.info("Successfully initialized Twitter API client")
#         except Exception as e:
#             logging.error(f"Failed to initialize Twitter API client: {str(e)}")
#             raise

#     def _make_filename_readable(self, filename):
#         # Remove file extension
#         filename = os.path.splitext(filename)[0]
#         # Replace underscores and hyphens with spaces
#         filename = filename.replace("_", " ").replace("-", " ")
#         # Capitalize words
#         filename = " ".join(word.capitalize() for word in filename.split())
#         return filename

#     def fetch_documents(self):
#         logging.info(f"Fetching documents from {self.base_url}")
#         headers = {
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
#         }
#         response = requests.get(self.base_url, headers=headers)
#         soup = BeautifulSoup(response.content, "html.parser")
#         documents = []
#         document_info = {}

#         # Process document rows which contain title and published date
#         doc_rows = soup.find_all("li", class_="document-row")
#         for row in doc_rows:
#             link_element = row.find("a", href=lambda x: x and x.endswith(".pdf"))
#             if not link_element:
#                 continue

#             href = link_element.get("href", "")
#             doc_url = f"https://www.fia.com{href}" if href.startswith("/") else href
#             normalized_url = doc_url.strip().lower().replace("\\", "/")
#             filename = os.path.basename(normalized_url).lower()

#             # Extract title from the document
#             title_div = row.find("div", class_="title")
#             title = title_div.text.strip() if title_div else ""

#             # Extract published date
#             published_element = row.find("div", class_="published")
#             published_date = ""
#             if published_element:
#                 date_span = published_element.find("span", class_="date-display-single")
#                 if date_span:
#                     published_date = date_span.text.strip()

#             # Store document metadata
#             document_info[doc_url] = {"title": title, "published": published_date}

#             if (
#                 normalized_url
#                 not in [url.lower() for url in self.processed_docs["urls"]]
#                 and filename not in self.processed_docs["filenames"]
#                 and doc_url not in documents
#             ):
#                 documents.append(doc_url)
#                 self.processed_docs["filenames"].add(filename)

#         return documents, document_info

#     def download_and_convert_pdf(self, url):
#         response = requests.get(url, allow_redirects=True)
#         pdf_path = os.path.join(self.download_dir, os.path.basename(url))
#         os.makedirs(self.download_dir, exist_ok=True)
#         with open(pdf_path, "wb") as f:
#             f.write(response.content)
#         images = pdf2image.convert_from_path(pdf_path)
#         image_paths = []
#         for i, image in enumerate(images):
#             image_path = os.path.join(self.download_dir, f"page_{i}.jpg")
#             image.save(image_path, "JPEG")
#             image_paths.append(image_path)
#         os.remove(pdf_path)
#         return image_paths

#     def _extract_timestamp_from_doc(self, doc_url):
#         filename = os.path.basename(doc_url)
#         try:
#             date_parts = [part for part in filename.split(".") if len(part) in [2, 4]]
#             if len(date_parts) >= 3:
#                 day, month, year = date_parts[-3:]
#                 if len(year) == 2:
#                     year = f"20{year}"
#                 return datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y")
#         except:
#             pass
#         return datetime.now()

#     def _parse_document_info(self, doc_url, doc_info=None):
#         if doc_info and doc_url in doc_info and doc_info[doc_url]["title"]:
#             info = doc_info[doc_url]
#             title = info["title"]
#             published_date = info["published"]

#             if not published_date.endswith("CET"):
#                 published_date = f"{published_date} CET"
#             return title, published_date

#         # Fallback to making filename human readable
#         filename = os.path.basename(doc_url)
#         readable_title = self._make_filename_readable(filename)
#         doc_date = self._extract_timestamp_from_doc(doc_url)
#         formatted_date = doc_date.strftime("%d.%m.%y %H:%M CET")
#         return readable_title, formatted_date

#     def _get_current_gp_hashtag(self):
#         f1_calendar = {
#             "2024-02-29": "#BahrainGP",
#             "2024-03-07": "#SpanishGP",
#             "2024-03-21": "#AustralianGP",
#             "2024-04-04": "#JapaneseGP",
#             "2024-04-18": "#ChineseGP",
#             "2024-05-02": "#SpanishGP",
#             "2024-05-16": "#EmiliaRomagnaGP",
#             "2024-05-23": "#SpanishGP",
#             "2024-06-06": "#CanadianGP",
#             "2024-06-20": "#SpanishGP",
#             "2024-07-04": "#AustrianGP",
#             "2024-07-18": "#BritishGP",
#             "2024-08-01": "#HungarianGP",
#             "2024-08-29": "#BelgianGP",
#             "2024-09-05": "#DutchGP",
#             "2024-09-19": "#ItalianGP",
#             "2024-09-26": "#AzerbaijanGP",
#             "2024-10-17": "#USGP",
#             "2024-10-24": "#MexicoGP",
#             "2024-11-07": "#BrazilGP",
#             "2024-11-21": "#LasVegasGP",
#             "2024-11-28": "#AbuDhabiGP",
#         }
#         current_date = datetime.now()
#         future_races = {
#             k: v
#             for k, v in f1_calendar.items()
#             if datetime.strptime(k, "%Y-%m-%d") >= current_date
#         }
#         if not future_races:
#             return ""
#         next_race_date = min(future_races.keys())
#         return future_races[next_race_date]

#     def post_to_bluesky(self, image_paths, doc_url, doc_info=None):
#         doc_title, pub_date = self._parse_document_info(doc_url, doc_info)
#         gp_hashtag = self._get_current_gp_hashtag()

#         max_title_length = 200
#         if len(doc_title) > max_title_length:
#             doc_title = doc_title[: max_title_length - 3] + "..."

#         all_hashtags = f"{GLOBAL_HASHTAGS}"
#         formatted_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

#         if len(formatted_text) + len(doc_url) + 1 <= 300:
#             formatted_text += f"\n\n{doc_url}"

#         encoded_doc_url = requests.utils.quote(doc_url, safe=":/?=")
#         facets = []

#         # Add URL facet
#         url_start = formatted_text.find(doc_url)
#         if url_start != -1:
#             byte_start = len(formatted_text[:url_start].encode("utf-8"))
#             byte_end = len(formatted_text[: url_start + len(doc_url)].encode("utf-8"))
#             facets.append(
#                 {
#                     "index": {"byteStart": byte_start, "byteEnd": byte_end},
#                     "features": [
#                         {
#                             "$type": "app.bsky.richtext.facet#link",
#                             "uri": encoded_doc_url,
#                         }
#                     ],
#                 }
#             )

#         # Make all hashtags clickable
#         all_tags = ["f1", "formula1", "fia", "SpanishGP"]
#         for tag in all_tags:
#             tag_with_hash = f"#{tag}"
#             tag_pos = formatted_text.find(tag_with_hash)
#             if tag_pos != -1:
#                 byte_start = len(formatted_text[:tag_pos].encode("utf-8"))
#                 byte_end = len(
#                     formatted_text[: tag_pos + len(tag_with_hash)].encode("utf-8")
#                 )
#                 facets.append(
#                     {
#                         "index": {"byteStart": byte_start, "byteEnd": byte_end},
#                         "features": [
#                             {"$type": "app.bsky.richtext.facet#tag", "tag": tag}
#                         ],
#                     }
#                 )

#         root_post = None
#         parent_post = None

#         for i in range(0, len(image_paths), 4):
#             chunk = image_paths[i : i + 4]
#             images = {"$type": "app.bsky.embed.images", "images": []}

#             for img_path in chunk:
#                 with open(img_path, "rb") as f:
#                     image_data = f.read()
#                 response = self.bluesky_client.upload_blob(image_data)
#                 images["images"].append({"image": response.blob, "alt": doc_title})

#             if parent_post:
#                 reply = {
#                     "root": {"uri": root_post["uri"], "cid": root_post["cid"]},
#                     "parent": {"uri": parent_post["uri"], "cid": parent_post["cid"]},
#                 }
#                 post_result = self.bluesky_client.post(
#                     text=f"Continued... ({i//4 + 1}/{(len(image_paths) + 3)//4})",
#                     embed=images,
#                     reply_to=reply,
#                 )
#                 parent_post = {"uri": post_result.uri, "cid": post_result.cid}
#             else:
#                 post_result = self.bluesky_client.post(
#                     text=formatted_text, facets=facets, embed=images
#                 )
#                 root_post = {"uri": post_result.uri, "cid": post_result.cid}
#                 parent_post = root_post

#         logging.info(f"Successfully posted to Bluesky: {doc_title}")

#     def post_to_twitter(self, image_paths, doc_url, doc_info=None):
#         if not self.twitter_client:
#             logging.warning("Twitter client not initialized. Skipping Twitter post.")
#             return

#         doc_title, pub_date = self._parse_document_info(doc_url, doc_info)
#         gp_hashtag = self._get_current_gp_hashtag()

#         # Twitter has a 280 character limit
#         max_title_length = 150
#         if len(doc_title) > max_title_length:
#             doc_title = doc_title[: max_title_length - 3] + "..."

#         all_hashtags = f"{GLOBAL_HASHTAGS}"
#         formatted_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

#         # Add URL if there's room
#         if len(formatted_text) + len(doc_url) + 1 <= 280:
#             formatted_text += f"\n\n{doc_url}"

#         # Twitter allows up to 4 images per tweet
#         tweet_id = None
#         for i in range(0, len(image_paths), 4):
#             chunk = image_paths[i : i + 4]
#             media_ids = []

#             # Upload each image in the chunk
#             for img_path in chunk:
#                 media_id = self.twitter_client.upload_image_chunked(img_path)
#                 if media_id:
#                     media_ids.append(media_id)

#             # If we have media IDs, post the tweet
#             if media_ids:
#                 if tweet_id is None:
#                     # First tweet with the full text
#                     tweet_text = formatted_text
#                 else:
#                     # Follow-up tweets as replies to the first one
#                     tweet_text = f"Continued... ({i//4 + 1}/{(len(image_paths) + 3)//4})"

#                 tweet_result = self.twitter_client.post_tweet_with_media(tweet_text, media_ids)
#                 if tweet_result:
#                     if tweet_id is None:
#                         tweet_id = tweet_result.get("data", {}).get("id")
#                     logging.info(f"Successfully posted to Twitter: {tweet_text[:30]}...")
#                 else:
#                     logging.error("Failed to post to Twitter")

#         if tweet_id:
#             logging.info(f"Successfully posted document to Twitter: {doc_title}")
#         else:
#             logging.warning(f"No tweets were posted for document: {doc_title}")


# def main():
#     logging.info("Starting FIA Document Handler")
#     handler = FIADocumentHandler()
#     try:
#         # Authenticate with Bluesky
#         handler.authenticate_bluesky(
#             os.environ["BLUESKY_USERNAME"],
#             os.environ["BLUESKY_PASSWORD"],
#             max_retries=3,
#             timeout=30,
#         )

#         # Authenticate with Twitter/X
#         handler.authenticate_twitter(
#             os.environ.get("TWITTER_API_KEY", ""),
#             os.environ.get("TWITTER_API_SECRET", ""),
#             os.environ.get("TWITTER_ACCESS_TOKEN", ""),
#             os.environ.get("TWITTER_ACCESS_TOKEN_SECRET", ""),
#         )

#         documents, document_info = handler.fetch_documents()
#         unique_documents = list(dict.fromkeys(documents))
#         logging.info(f"Found {len(unique_documents)} new documents to process")

#         for doc_url in unique_documents:
#             try:
#                 normalized_url = doc_url.strip().lower().replace("\\", "/")
#                 if normalized_url in [
#                     url.lower() for url in handler.processed_docs["urls"]
#                 ]:
#                     logging.info(f"Skipping already processed document: {doc_url}")
#                     continue

#                 image_paths = handler.download_and_convert_pdf(doc_url)

#                 # Post to Bluesky
#                 handler.post_to_bluesky(image_paths, doc_url, document_info)

#                 # Post to Twitter/X
#                 handler.post_to_twitter(image_paths, doc_url, document_info)

#                 handler.processed_docs["urls"].append(doc_url)
#                 handler._save_processed_docs()

#                 for img_path in image_paths:
#                     if os.path.exists(img_path):
#                         os.remove(img_path)

#             except Exception as e:
#                 logging.error(f"Error processing document {doc_url}: {str(e)}")
#                 continue

#     except Exception as e:
#         logging.error(f"Fatal error: {str(e)}")
#         raise

#     logging.info("FIA Document Handler completed successfully")


# if __name__ == "__main__":
#     main()

