import json
import logging
import os
import time
from datetime import datetime

import pdf2image
import requests
from atproto import Client
from bs4 import BeautifulSoup
from twitter import TwitterAPI

# Global hashtags - Change in 2 places
GLOBAL_HASHTAGS = "#f1 #formula1 #fia #SpanishGP"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class FIADocumentHandler:
    def __init__(self):
        self.base_url = "https://www.fia.com/documents/championships/fia-formula-one-world-championship-14/season/season-2025-2071"
        self.download_dir = "downloads"
        self.processed_docs = self._load_processed_docs()
        self.bluesky_client = Client()
        self.twitter_client = None
        self.bluesky_authenticated = False
        self.twitter_authenticated = False

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
        try:
            for attempt in range(max_retries):
                try:
                    self.bluesky_client = Client()
                    self.bluesky_client.login(username, password)
                    logging.info("Successfully authenticated with Bluesky")
                    self.bluesky_authenticated = True
                    return
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logging.warning(
                        f"Bluesky authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                    )
                    time.sleep(2**attempt)
        except Exception as e:
            logging.error(f"Failed to authenticate with Bluesky: {str(e)}")
            self.bluesky_authenticated = False

    def authenticate_twitter(self, consumer_key, consumer_secret, access_token, access_token_secret):
        try:
            self.twitter_client = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)
            logging.info("Successfully authenticated with Twitter")
            self.twitter_authenticated = True
        except Exception as e:
            logging.error(f"Failed to authenticate with Twitter: {str(e)}")
            self.twitter_authenticated = False

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
        if not self.bluesky_authenticated:
            logging.warning("Bluesky not authenticated, skipping Bluesky post")
            return False

        try:
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

            logging.info("Successfully posted to Bluesky")
            return True

        except Exception as e:
            logging.error(f"Error posting to Bluesky: {str(e)}")
            return False

    def post_to_twitter(self, image_paths, doc_url, doc_info=None):
        if not self.twitter_authenticated:
            logging.warning("Twitter not authenticated, skipping Twitter post")
            return False

        try:
            logging.info(f"Starting Twitter post for {len(image_paths)} images")
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)

            max_title_length = 150
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"
            formatted_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

            if len(formatted_text) + len(doc_url) + 1 <= 270:
                formatted_text += f"\n\n{doc_url}"

            success_count = 0

            # Process images in chunks of 4 (Twitter's limit)
            for i in range(0, len(image_paths), 4):
                chunk = image_paths[i : i + 4]
                chunk_media_ids = []

                logging.info(f"Processing chunk {i//4 + 1} with {len(chunk)} images")

                for img_path in chunk:
                    logging.info(f"Uploading image: {img_path}")
                    media_id = self.twitter_client.upload_image_chunked(img_path)
                    if media_id:
                        chunk_media_ids.append(media_id)
                        logging.info(f"Successfully uploaded image, media_id: {media_id}")
                    else:
                        logging.error(f"Failed to upload image {img_path} to Twitter")

                if chunk_media_ids:
                    if i == 0:
                        tweet_text = formatted_text
                    else:
                        tweet_text = f"Continued... ({i//4 + 1}/{(len(image_paths) + 3)//4})\n\n{all_hashtags}"

                    logging.info(f"Posting tweet with {len(chunk_media_ids)} media items")
                    tweet_result = self.twitter_client.post_tweet_with_media(tweet_text, chunk_media_ids)
                    if tweet_result:
                        logging.info(f"Successfully posted Twitter thread part {i//4 + 1}")
                        success_count += 1
                    else:
                        logging.error(f"Failed to post Twitter thread part {i//4 + 1}")
                else:
                    logging.error(f"No media uploaded for Twitter chunk {i//4 + 1}")

            if success_count > 0:
                logging.info(f"Successfully posted {success_count} Twitter posts")
                return True
            else:
                logging.error("Failed to post any Twitter content")
                return False

        except Exception as e:
            logging.error(f"Error posting to Twitter: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return False

    def post_to_platforms(self, image_paths, doc_url, doc_info=None):
        """Post to all available platforms independently"""
        bluesky_success = False
        twitter_success = False

        # Post to Bluesky
        if self.bluesky_authenticated:
            bluesky_success = self.post_to_bluesky(image_paths, doc_url, doc_info)

        # Post to Twitter
        if self.twitter_authenticated:
            twitter_success = self.post_to_twitter(image_paths, doc_url, doc_info)

        # Log results
        platforms_posted = []
        if bluesky_success:
            platforms_posted.append("Bluesky")
        if twitter_success:
            platforms_posted.append("Twitter")

        if platforms_posted:
            logging.info(f"Successfully posted to: {', '.join(platforms_posted)}")
        else:
            logging.warning("Failed to post to any platform")

        return bluesky_success or twitter_success


def main():
    logging.info("Starting FIA Document Handler")
    handler = FIADocumentHandler()

    # Authenticate with Bluesky
    try:
        bluesky_username = os.environ.get("BLUESKY_USERNAME")
        bluesky_password = os.environ.get("BLUESKY_USERNAME")
        # bluesky_password = os.environ.get("BLUESKY_PASSWORD")
        if bluesky_username and bluesky_password:
            handler.authenticate_bluesky(
                bluesky_username,
                bluesky_password,
                max_retries=3,
                timeout=30,
            )
        else:
            logging.warning("Bluesky credentials not found in environment variables")
    except Exception as e:
        logging.error(f"Bluesky authentication failed: {str(e)}")

    # Authenticate with Twitter
    try:
        twitter_consumer_key = os.environ.get("TWITTER_API_KEY")
        twitter_consumer_secret = os.environ.get("TWITTER_API_SECRET")
        twitter_access_token = os.environ.get("TWITTER_ACCESS_TOKEN")
        twitter_access_token_secret = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")

        if all([twitter_consumer_key, twitter_consumer_secret, twitter_access_token, twitter_access_token_secret]):
            handler.authenticate_twitter(
                twitter_consumer_key,
                twitter_consumer_secret,
                twitter_access_token,
                twitter_access_token_secret
            )
        else:
            logging.warning("Twitter credentials not found in environment variables")
    except Exception as e:
        logging.error(f"Twitter authentication failed: {str(e)}")

    # Check if at least one platform is authenticated
    if not handler.bluesky_authenticated and not handler.twitter_authenticated:
        logging.error("No platforms authenticated. Exiting.")
        return

    try:
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

                image_paths = handler.download_and_convert_pdf(doc_url)

                # Post to all available platforms
                success = handler.post_to_platforms(image_paths, doc_url, document_info)

                # Only mark as processed if at least one platform succeeded
                if success:
                    handler.processed_docs["urls"].append(doc_url)
                    handler._save_processed_docs()
                    logging.info(f"Document {doc_url} processed and saved to processed_docs.json")
                else:
                    logging.error(f"Failed to post document {doc_url} to any platform")

                # Clean up image files
                for img_path in image_paths:
                    if os.path.exists(img_path):
                        os.remove(img_path)

            except Exception as e:
                logging.error(f"Error processing document {doc_url}: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

    logging.info("FIA Document Handler completed successfully")


if __name__ == "__main__":
    main()


