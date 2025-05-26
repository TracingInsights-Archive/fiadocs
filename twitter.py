import base64
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
from datetime import datetime

import pdf2image
import requests
from atproto import Client
from bs4 import BeautifulSoup

# Global hashtags - Change in 2 places
GLOBAL_HASHTAGS = "#f1 #formula1 #fia #SpanishGP"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TwitterAPIClient:
    def __init__(self, bearer_token):
        self.bearer_token = bearer_token
        self.base_url = "https://api.twitter.com"

    def upload_media(self, image_data, media_type="image/jpeg"):
        """Upload media to Twitter using v2 API"""
        try:
            url = f"{self.base_url}/2/media/upload"

            files = {"media": ("image.jpg", image_data, media_type)}

            headers = {"Authorization": f"Bearer {self.bearer_token}"}

            response = requests.post(url, files=files, headers=headers)

            if response.status_code == 200:
                return response.json()["media_id_string"]
            else:
                logging.error(
                    f"Media upload failed: {response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logging.error(f"Error uploading media to Twitter: {str(e)}")
            return None

    def post_tweet(self, text, media_ids=None, reply_to_tweet_id=None):
        """Post a tweet using v2 API"""
        try:
            url = f"{self.base_url}/2/tweets"

            tweet_data = {"text": text}

            if media_ids:
                tweet_data["media"] = {"media_ids": media_ids}

            if reply_to_tweet_id:
                tweet_data["reply"] = {"in_reply_to_tweet_id": reply_to_tweet_id}

            headers = {
                "Authorization": f"Bearer {self.bearer_token}",
                "Content-Type": "application/json",
            }

            response = requests.post(url, json=tweet_data, headers=headers)

            if response.status_code == 201:
                return response.json()["data"]
            else:
                logging.error(
                    f"Tweet post failed: {response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logging.error(f"Error posting tweet: {str(e)}")
            return None


class FIADocumentHandler:
    def __init__(self):
        self.base_url = "https://www.fia.com/documents/championships/fia-formula-one-world-championship-14/season/season-2025-2071"
        self.download_dir = "downloads"
        self.processed_docs = self._load_processed_docs()
        self.bluesky_client = Client()
        self.twitter_client = None

    def _load_processed_docs(self):
        try:
            with open("processed_docs.json", "r") as f:
                urls = json.load(f)
                normalized_urls = [
                    url.strip().lower().replace("\\", "/") for url in urls
                ]
                return {
                    "urls": normalized_urls,
                    "filenames": {
                        os.path.basename(url).lower() for url in normalized_urls
                    },
                }
        except (FileNotFoundError, json.JSONDecodeError):
            if os.path.exists("processed_docs.json"):
                os.rename(
                    "processed_docs.json", f"processed_docs.json.bak.{int(time.time())}"
                )
            return {"urls": [], "filenames": set()}

    def _save_processed_docs(self):
        with open("processed_docs.json", "w") as f:
            json.dump(self.processed_docs["urls"], f)

    def authenticate_bluesky(self, username, password, max_retries=3, timeout=30):
        for attempt in range(max_retries):
            try:
                self.bluesky_client = Client()
                self.bluesky_client.login(username, password)
                logging.info("Successfully authenticated with Bluesky")
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logging.warning(
                    f"Authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)

    def authenticate_twitter(self, bearer_token):
        """Initialize Twitter API client with Bearer Token"""
        try:
            self.twitter_client = TwitterAPIClient(bearer_token)
            logging.info("Successfully initialized Twitter API client")
        except Exception as e:
            logging.error(f"Failed to initialize Twitter client: {str(e)}")
            self.twitter_client = None

    def _make_filename_readable(self, filename):
        # Remove file extension
        filename = os.path.splitext(filename)[0]
        # Replace underscores and hyphens with spaces
        filename = filename.replace("_", " ").replace("-", " ")
        # Capitalize words
        filename = " ".join(word.capitalize() for word in filename.split())
        return filename

    def fetch_documents(self):
        logging.info(f"Fetching documents from {self.base_url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(self.base_url, headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")
        documents = []
        document_info = {}

        # Process document rows which contain title and published date
        doc_rows = soup.find_all("li", class_="document-row")
        for row in doc_rows:
            link_element = row.find("a", href=lambda x: x and x.endswith(".pdf"))
            if not link_element:
                continue

            href = link_element.get("href", "")
            doc_url = f"https://www.fia.com{href}" if href.startswith("/") else href
            normalized_url = doc_url.strip().lower().replace("\\", "/")
            filename = os.path.basename(normalized_url).lower()

            # Extract title from the document
            title_div = row.find("div", class_="title")
            title = title_div.text.strip() if title_div else ""

            # Extract published date
            published_element = row.find("div", class_="published")
            published_date = ""
            if published_element:
                date_span = published_element.find("span", class_="date-display-single")
                if date_span:
                    published_date = date_span.text.strip()

            # Store document metadata
            document_info[doc_url] = {"title": title, "published": published_date}

            if (
                normalized_url
                not in [url.lower() for url in self.processed_docs["urls"]]
                and filename not in self.processed_docs["filenames"]
                and doc_url not in documents
            ):
                documents.append(doc_url)
                self.processed_docs["filenames"].add(filename)

        return documents, document_info

    def download_and_convert_pdf(self, url):
        response = requests.get(url, allow_redirects=True)
        pdf_path = os.path.join(self.download_dir, os.path.basename(url))
        os.makedirs(self.download_dir, exist_ok=True)
        with open(pdf_path, "wb") as f:
            f.write(response.content)
        images = pdf2image.convert_from_path(pdf_path)
        image_paths = []
        for i, image in enumerate(images):
            image_path = os.path.join(self.download_dir, f"page_{i}.jpg")
            image.save(image_path, "JPEG")
            image_paths.append(image_path)
        os.remove(pdf_path)
        return image_paths

    def _extract_timestamp_from_doc(self, doc_url):
        filename = os.path.basename(doc_url)
        try:
            date_parts = [part for part in filename.split(".") if len(part) in [2, 4]]
            if len(date_parts) >= 3:
                day, month, year = date_parts[-3:]
                if len(year) == 2:
                    year = f"20{year}"
                return datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y")
        except:
            pass
        return datetime.now()

    def _parse_document_info(self, doc_url, doc_info=None):
        if doc_info and doc_url in doc_info and doc_info[doc_url]["title"]:
            info = doc_info[doc_url]
            title = info["title"]
            published_date = info["published"]

            if not published_date.endswith("CET"):
                published_date = f"{published_date} CET"
            return title, published_date

        # Fallback to making filename human readable
        filename = os.path.basename(doc_url)
        readable_title = self._make_filename_readable(filename)
        doc_date = self._extract_timestamp_from_doc(doc_url)
        formatted_date = doc_date.strftime("%d.%m.%y %H:%M CET")
        return readable_title, formatted_date

    def _get_current_gp_hashtag(self):
        f1_calendar = {
            "2024-02-29": "#BahrainGP",
            "2024-03-07": "#SpanishGP",
            "2024-03-21": "#AustralianGP",
            "2024-04-04": "#JapaneseGP",
            "2024-04-18": "#ChineseGP",
            "2024-05-02": "#SpanishGP",
            "2024-05-16": "#EmiliaRomagnaGP",
            "2024-05-23": "#SpanishGP",
            "2024-06-06": "#CanadianGP",
            "2024-06-20": "#SpanishGP",
            "2024-07-04": "#AustrianGP",
            "2024-07-18": "#BritishGP",
            "2024-08-01": "#HungarianGP",
            "2024-08-29": "#BelgianGP",
            "2024-09-05": "#DutchGP",
            "2024-09-19": "#ItalianGP",
            "2024-09-26": "#AzerbaijanGP",
            "2024-10-17": "#USGP",
            "2024-10-24": "#MexicoGP",
            "2024-11-07": "#BrazilGP",
            "2024-11-21": "#LasVegasGP",
            "2024-11-28": "#AbuDhabiGP",
        }
        current_date = datetime.now()
        future_races = {
            k: v
            for k, v in f1_calendar.items()
            if datetime.strptime(k, "%Y-%m-%d") >= current_date
        }
        if not future_races:
            return ""
        next_race_date = min(future_races.keys())
        return future_races[next_race_date]

    def post_to_bluesky(self, image_paths, doc_url, doc_info=None):
        doc_title, pub_date = self._parse_document_info(doc_url, doc_info)
        gp_hashtag = self._get_current_gp_hashtag()

        max_title_length = 200
        if len(doc_title) > max_title_length:
            doc_title = doc_title[: max_title_length - 3] + "..."

        all_hashtags = f"{GLOBAL_HASHTAGS}"
        formatted_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

        if len(formatted_text) + len(doc_url) + 1 <= 300:
            formatted_text += f"\n\n{doc_url}"

        encoded_doc_url = requests.utils.quote(doc_url, safe=":/?=")
        facets = []

        # Add URL facet
        url_start = formatted_text.find(doc_url)
        if url_start != -1:
            byte_start = len(formatted_text[:url_start].encode("utf-8"))
            byte_end = len(formatted_text[: url_start + len(doc_url)].encode("utf-8"))
            facets.append(
                {
                    "index": {"byteStart": byte_start, "byteEnd": byte_end},
                    "features": [
                        {
                            "$type": "app.bsky.richtext.facet#link",
                            "uri": encoded_doc_url,
                        }
                    ],
                }
            )

        # Make all hashtags clickable
        all_tags = ["f1", "formula1", "fia", "SpanishGP"]
        for tag in all_tags:
            tag_with_hash = f"#{tag}"
            tag_pos = formatted_text.find(tag_with_hash)
            if tag_pos != -1:
                byte_start = len(formatted_text[:tag_pos].encode("utf-8"))
                byte_end = len(
                    formatted_text[: tag_pos + len(tag_with_hash)].encode("utf-8")
                )
                facets.append(
                    {
                        "index": {"byteStart": byte_start, "byteEnd": byte_end},
                        "features": [
                            {"$type": "app.bsky.richtext.facet#tag", "tag": tag}
                        ],
                    }
                )

        root_post = None
        parent_post = None

        for i in range(0, len(image_paths), 4):
            chunk = image_paths[i : i + 4]
            images = {"$type": "app.bsky.embed.images", "images": []}

            for img_path in chunk:
                with open(img_path, "rb") as f:
                    image_data = f.read()
                response = self.bluesky_client.upload_blob(image_data)
                images["images"].append({"image": response.blob, "alt": doc_title})

            if parent_post:
                reply = {
                    "root": {"uri": root_post["uri"], "cid": root_post["cid"]},
                    "parent": {"uri": parent_post["uri"], "cid": parent_post["cid"]},
                }
                post_result = self.bluesky_client.post(
                    text=f"Continued... ({i//4 + 1}/{(len(image_paths) + 3)//4})",
                    embed=images,
                    reply_to=reply,
                )
                parent_post = {"uri": post_result.uri, "cid": post_result.cid}
            else:
                post_result = self.bluesky_client.post(
                    text=formatted_text, facets=facets, embed=images
                )
                root_post = {"uri": post_result.uri, "cid": post_result.cid}
                parent_post = root_post

    def post_to_twitter(self, image_paths, doc_url, doc_info=None):
        """Post document to Twitter as a thread"""
        if not self.twitter_client:
            logging.warning("Twitter client not initialized, skipping Twitter post")
            return

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)
            gp_hashtag = self._get_current_gp_hashtag()

            # Twitter has 280 character limit, so we need to be more concise
            max_title_length = 120
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"

            # Create initial tweet text (more concise for Twitter)
            formatted_text = f"{doc_title}\nPublished: {pub_date}\n\n{all_hashtags}"

            # Add URL if it fits within character limit
            if len(formatted_text) + len(doc_url) + 2 <= 280:
                formatted_text += f"\n\n{doc_url}"

            root_tweet = None
            parent_tweet = None

            # Process images in chunks of 4 (Twitter's limit)
            for i in range(0, len(image_paths), 4):
                chunk = image_paths[i : i + 4]
                media_ids = []

                # Upload images for this chunk
                for img_path in chunk:
                    with open(img_path, "rb") as f:
                        image_data = f.read()

                    media_id = self.twitter_client.upload_media(image_data)
                    if media_id:
                        media_ids.append(media_id)
                    else:
                        logging.warning(f"Failed to upload image {img_path} to Twitter")

                if not media_ids:
                    logging.warning(f"No media uploaded for chunk {i//4 + 1}")
                    continue

                # Post tweet
                if parent_tweet:
                    # This is a continuation tweet
                    tweet_text = (
                        f"Continued... ({i//4 + 1}/{(len(image_paths) + 3)//4})"
                    )
                    tweet_result = self.twitter_client.post_tweet(
                        text=tweet_text,
                        media_ids=media_ids,
                        reply_to_tweet_id=parent_tweet["id"],
                    )
                else:
                    # This is the first tweet
                    tweet_result = self.twitter_client.post_tweet(
                        text=formatted_text, media_ids=media_ids
                    )
                    root_tweet = tweet_result

                if tweet_result:
                    parent_tweet = tweet_result
                    logging.info(
                        f"Posted Twitter thread part {i//4 + 1}/{(len(image_paths) + 3)//4}"
                    )
                else:
                    logging.error(f"Failed to post Twitter thread part {i//4 + 1}")
                    break

                # Add delay between tweets to avoid rate limiting
                time.sleep(2)

            if root_tweet:
                logging.info(
                    f"Successfully posted Twitter thread for document: {doc_title}"
                )
            else:
                logging.error("Failed to post Twitter thread")

        except Exception as e:
            logging.error(f"Error posting to Twitter: {str(e)}")

    def post_to_both_platforms(self, image_paths, doc_url, doc_info=None):
        """Post to both Bluesky and Twitter"""
        try:
            # Post to Bluesky (existing functionality)
            logging.info("Posting to Bluesky...")
            self.post_to_bluesky(image_paths, doc_url, doc_info)
            logging.info("Successfully posted to Bluesky")

            # Add small delay between platforms
            time.sleep(3)

            # Post to Twitter (new functionality)
            logging.info("Posting to Twitter...")
            self.post_to_twitter(image_paths, doc_url, doc_info)
            logging.info("Successfully posted to Twitter")

        except Exception as e:
            logging.error(f"Error posting to platforms: {str(e)}")
            raise


def main():
    logging.info("Starting FIA Document Handler")
    handler = FIADocumentHandler()

    try:
        # Authenticate with Bluesky
        handler.authenticate_bluesky(
            os.environ["BLUESKY_USERNAME"],
            # os.environ["BLUESKY_USERNAME"],
            os.environ["BLUESKY_PASSWORD"],
            max_retries=3,
            timeout=30,
        )

        # Authenticate with Twitter using Bearer Token
        twitter_bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")

        if twitter_bearer_token:
            handler.authenticate_twitter(twitter_bearer_token)
            logging.info("Twitter authentication successful")
        else:
            logging.warning(
                "Twitter Bearer Token not provided, will only post to Bluesky"
            )

        # Fetch and process documents
        documents, document_info = handler.fetch_documents()
        unique_documents = list(dict.fromkeys(documents))
        logging.info(f"Found {len(unique_documents)} new documents to process")

        for doc_url in unique_documents:
            try:
                normalized_url = doc_url.strip().lower().replace("\\", "/")
                if normalized_url in [
                    url.lower() for url in handler.processed_docs["urls"]
                ]:
                    logging.info(f"Skipping already processed document: {doc_url}")
                    continue

                # Download and convert PDF to images
                image_paths = handler.download_and_convert_pdf(doc_url)

                # Post to both platforms
                handler.post_to_both_platforms(image_paths, doc_url, document_info)

                # Mark as processed
                handler.processed_docs["urls"].append(doc_url)
                handler._save_processed_docs()

                # Clean up image files
                for img_path in image_paths:
                    if os.path.exists(img_path):
                        os.remove(img_path)

                # Add delay between documents to avoid rate limiting
                time.sleep(5)

            except Exception as e:
                logging.error(f"Error processing document {doc_url}: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

    logging.info("FIA Document Handler completed successfully")


if __name__ == "__main__":
    main()
