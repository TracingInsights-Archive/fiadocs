import json
import logging
import os
import time
from datetime import datetime

import pdf2image
import requests
from atproto import Client
from bs4 import BeautifulSoup
from mastodon import Mastodon

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
        self.mastodon_client = None
        self.threads_app_id = None
        self.threads_app_secret = None
        self.threads_access_token = None
        self.threads_user_id = None
        self.bluesky_authenticated = False
        self.mastodon_authenticated = False
        self.threads_authenticated = False

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
                self.bluesky_authenticated = True
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Bluesky after {max_retries} attempts: {str(e)}"
                    )
                    self.bluesky_authenticated = False
                    return False
                logging.warning(
                    f"Bluesky authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

    def authenticate_mastodon(self, access_token, max_retries=3):
        for attempt in range(max_retries):
            try:
                # Create Mastodon client
                self.mastodon_client = Mastodon(
                    access_token=access_token, api_base_url="https://mastodon.social"
                )

                # Test authentication by getting account info
                account = self.mastodon_client.me()
                logging.info(
                    f"Successfully authenticated with Mastodon as @{account['username']}"
                )
                self.mastodon_authenticated = True
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Mastodon after {max_retries} attempts: {str(e)}"
                    )
                    self.mastodon_authenticated = False
                    return False
                logging.warning(
                    f"Mastodon authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

    def authenticate_threads(self, app_id, app_secret, access_token, max_retries=3):
        for attempt in range(max_retries):
            try:
                self.threads_app_id = app_id
                self.threads_app_secret = app_secret
                self.threads_access_token = access_token

                # Get user ID from access token
                url = f"https://graph.threads.net/v1.0/me?access_token={access_token}"
                response = requests.get(url)
                response.raise_for_status()

                user_data = response.json()
                self.threads_user_id = user_data.get("id")

                if not self.threads_user_id:
                    raise Exception("Could not retrieve user ID from Threads API")

                logging.info(
                    f"Successfully authenticated with Threads (User ID: {self.threads_user_id})"
                )
                self.threads_authenticated = True
                return True

            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Threads after {max_retries} attempts: {str(e)}"
                    )
                    self.threads_authenticated = False
                    return False
                logging.warning(
                    f"Threads authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

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
            logging.warning("Skipping Bluesky post - not authenticated")
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
                byte_end = len(
                    formatted_text[: url_start + len(doc_url)].encode("utf-8")
                )
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
                        "parent": {
                            "uri": parent_post["uri"],
                            "cid": parent_post["cid"],
                        },
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
            logging.error(f"Failed to post to Bluesky: {str(e)}")
            return False

    def post_to_mastodon(self, image_paths, doc_url, doc_info=None):
        if not self.mastodon_authenticated:
            logging.warning("Skipping Mastodon post - not authenticated")
            return False

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)
            gp_hashtag = self._get_current_gp_hashtag()

            max_title_length = 200
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"
            formatted_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

            # Mastodon has a 500 character limit
            if len(formatted_text) + len(doc_url) + 1 <= 480:
                formatted_text += f"\n\n{doc_url}"

            # Upload images to Mastodon (max 4 per post)
            media_ids = []
            for i, img_path in enumerate(image_paths[:4]):  # Mastodon limit is 4 images
                try:
                    with open(img_path, "rb") as f:
                        # Explicitly specify MIME type for JPEG images
                        media = self.mastodon_client.media_post(
                            f, mime_type="image/jpeg", description=doc_title
                        )
                        media_ids.append(media["id"])
                except Exception as e:
                    logging.warning(
                        f"Failed to upload image {img_path} to Mastodon: {str(e)}"
                    )
                    continue

            # Post the main toot
            if media_ids:
                status = self.mastodon_client.status_post(
                    status=formatted_text, media_ids=media_ids
                )
                logging.info("Successfully posted to Mastodon")

                # If there are more than 4 images, post them in reply threads
                if len(image_paths) > 4:
                    parent_status = status
                    for i in range(4, len(image_paths), 4):
                        chunk = image_paths[i : i + 4]
                        chunk_media_ids = []

                        for img_path in chunk:
                            try:
                                with open(img_path, "rb") as f:
                                    # Explicitly specify MIME type for JPEG images
                                    media = self.mastodon_client.media_post(
                                        f, mime_type="image/jpeg", description=doc_title
                                    )
                                    chunk_media_ids.append(media["id"])
                            except Exception as e:
                                logging.warning(
                                    f"Failed to upload image {img_path} to Mastodon: {str(e)}"
                                )
                                continue

                        if chunk_media_ids:
                            reply_text = (
                                f"Continued... ({i//4 + 1}/{(len(image_paths) + 3)//4})"
                            )
                            parent_status = self.mastodon_client.status_post(
                                status=reply_text,
                                media_ids=chunk_media_ids,
                                in_reply_to_id=parent_status["id"],
                            )

                return True
            else:
                # Post without media if all uploads failed
                self.mastodon_client.status_post(status=formatted_text)
                logging.info("Successfully posted to Mastodon (text only)")
                return True

        except Exception as e:
            logging.error(f"Failed to post to Mastodon: {str(e)}")
            return False

    def post_to_threads(self, image_paths, doc_url, doc_info=None):
        if not self.threads_authenticated:
            logging.warning("Skipping Threads post - not authenticated")
            return False

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)
            gp_hashtag = self._get_current_gp_hashtag()

            max_title_length = 200
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"
            formatted_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

            # Threads has a 500 character limit
            if len(formatted_text) + len(doc_url) + 1 <= 480:
                formatted_text += f"\n\n{doc_url}"

            # Upload images to Threads and create posts
            root_post_id = None
            parent_post_id = None

            for i in range(
                0, len(image_paths), 10
            ):  # Threads allows up to 10 images per post
                chunk = image_paths[i : i + 10]
                media_ids = []

                # Upload images for this chunk
                for img_path in chunk:
                    try:
                        # Step 1: Upload image to Threads
                        with open(img_path, "rb") as f:
                            files = {"image": f}
                            data = {
                                "access_token": self.threads_access_token,
                                "image_url": "",  # We're uploading directly
                            }

                            upload_url = f"https://graph.threads.net/v1.0/{self.threads_user_id}/media"
                            upload_response = requests.post(
                                upload_url,
                                files=files,
                                data={
                                    "access_token": self.threads_access_token,
                                    "media_type": "IMAGE",
                                },
                            )

                            if upload_response.status_code == 200:
                                media_data = upload_response.json()
                                media_ids.append(media_data.get("id"))
                            else:
                                logging.warning(
                                    f"Failed to upload image {img_path} to Threads: {upload_response.text}"
                                )
                                continue

                    except Exception as e:
                        logging.warning(
                            f"Failed to upload image {img_path} to Threads: {str(e)}"
                        )
                        continue

                # Create post with uploaded images
                if media_ids:
                    try:
                        post_data = {
                            "access_token": self.threads_access_token,
                            "media_type": "CAROUSEL" if len(media_ids) > 1 else "IMAGE",
                        }

                        if len(media_ids) == 1:
                            post_data["image_url"] = media_ids[0]
                        else:
                            post_data["children"] = media_ids

                        # Add text for the first post or continuation text for subsequent posts
                        if parent_post_id:
                            post_data["text"] = (
                                f"Continued... ({i//10 + 1}/{(len(image_paths) + 9)//10})"
                            )
                            post_data["reply_to_id"] = parent_post_id
                        else:
                            post_data["text"] = formatted_text

                        # Create media container
                        create_url = f"https://graph.threads.net/v1.0/{self.threads_user_id}/media"
                        create_response = requests.post(create_url, data=post_data)

                        if create_response.status_code == 200:
                            container_data = create_response.json()
                            container_id = container_data.get("id")

                            # Publish the post
                            publish_data = {
                                "access_token": self.threads_access_token,
                                "creation_id": container_id,
                            }

                            publish_url = f"https://graph.threads.net/v1.0/{self.threads_user_id}/media_publish"
                            publish_response = requests.post(
                                publish_url, data=publish_data
                            )

                            if publish_response.status_code == 200:
                                publish_data = publish_response.json()
                                post_id = publish_data.get("id")

                                if not root_post_id:
                                    root_post_id = post_id
                                parent_post_id = post_id

                                logging.info(
                                    f"Successfully posted chunk {i//10 + 1} to Threads"
                                )
                            else:
                                logging.error(
                                    f"Failed to publish Threads post: {publish_response.text}"
                                )
                                return False
                        else:
                            logging.error(
                                f"Failed to create Threads media container: {create_response.text}"
                            )
                            return False

                    except Exception as e:
                        logging.error(
                            f"Failed to create Threads post for chunk {i//10 + 1}: {str(e)}"
                        )
                        return False

            logging.info("Successfully posted to Threads")
            return True

        except Exception as e:
            logging.error(f"Failed to post to Threads: {str(e)}")
            return False

    def post_to_social_media(self, image_paths, doc_url, doc_info=None):
        """Post to all available social media platforms"""
        results = {}

        # Post to Bluesky
        if self.bluesky_authenticated:
            try:
                results["bluesky"] = self.post_to_bluesky(
                    image_paths, doc_url, doc_info
                )
            except Exception as e:
                logging.error(f"Unexpected error posting to Bluesky: {str(e)}")
                results["bluesky"] = False
        else:
            results["bluesky"] = False

        # Post to Mastodon
        if self.mastodon_authenticated:
            try:
                results["mastodon"] = self.post_to_mastodon(
                    image_paths, doc_url, doc_info
                )
            except Exception as e:
                logging.error(f"Unexpected error posting to Mastodon: {str(e)}")
                results["mastodon"] = False
        else:
            results["mastodon"] = False

        # Post to Threads
        if self.threads_authenticated:
            try:
                results["threads"] = self.post_to_threads(
                    image_paths, doc_url, doc_info
                )
            except Exception as e:
                logging.error(f"Unexpected error posting to Threads: {str(e)}")
                results["threads"] = False
        else:
            results["threads"] = False

        # Log results
        successful_platforms = [
            platform for platform, success in results.items() if success
        ]
        failed_platforms = [
            platform for platform, success in results.items() if not success
        ]

        if successful_platforms:
            logging.info(f"Successfully posted to: {', '.join(successful_platforms)}")
        if failed_platforms:
            logging.warning(f"Failed to post to: {', '.join(failed_platforms)}")

        return results


def main():
    logging.info("Starting FIA Document Handler")
    handler = FIADocumentHandler()

    # Track authentication results
    auth_results = {}

    # Authenticate with Bluesky
    try:
        bluesky_username = os.environ.get("BLUESKY_USERNAME")
        bluesky_password = os.environ.get("BLUESKY_USERNAME")
        # bluesky_password = os.environ.get("BLUESKY_PASSWORD")

        if bluesky_username and bluesky_password:
            auth_results["bluesky"] = handler.authenticate_bluesky(
                bluesky_username,
                bluesky_password,
                max_retries=3,
                timeout=30,
            )
        else:
            logging.warning("Bluesky credentials not found in environment variables")
            auth_results["bluesky"] = False
    except Exception as e:
        logging.error(f"Unexpected error during Bluesky authentication: {str(e)}")
        auth_results["bluesky"] = False

    # Authenticate with Mastodon
    try:
        mastodon_access_token = os.environ.get("BLUESKY_USERNAME")
        # mastodon_access_token = os.environ.get("MASTODON_ACCESS_TOKEN")

        if mastodon_access_token:
            auth_results["mastodon"] = handler.authenticate_mastodon(
                mastodon_access_token, max_retries=3
            )
        else:
            logging.warning("Mastodon access token not found in environment variables")
            auth_results["mastodon"] = False
    except Exception as e:
        logging.error(f"Unexpected error during Mastodon authentication: {str(e)}")
        auth_results["mastodon"] = False

    # Authenticate with Threads
    try:
        threads_app_id = os.environ.get("THREADS_APP_ID")
        threads_app_secret = os.environ.get("THREADS_APP_SECRET")
        threads_access_token = os.environ.get("THREADS_ACCESS_TOKEN")

        if threads_app_id and threads_app_secret and threads_access_token:
            auth_results["threads"] = handler.authenticate_threads(
                threads_app_id, threads_app_secret, threads_access_token, max_retries=3
            )
        else:
            logging.warning("Threads credentials not found in environment variables")
            auth_results["threads"] = False
    except Exception as e:
        logging.error(f"Unexpected error during Threads authentication: {str(e)}")
        auth_results["threads"] = False

    # Check if at least one platform is authenticated
    if not any(auth_results.values()):
        logging.error("Failed to authenticate with any social media platform. Exiting.")
        return

    successful_auths = [
        platform for platform, success in auth_results.items() if success
    ]
    logging.info(f"Successfully authenticated with: {', '.join(successful_auths)}")

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

                logging.info(f"Processing document: {doc_url}")
                image_paths = handler.download_and_convert_pdf(doc_url)

                # Post to all available social media platforms
                post_results = handler.post_to_social_media(
                    image_paths, doc_url, document_info
                )

                # Only mark as processed if at least one platform succeeded
                if any(post_results.values()):
                    handler.processed_docs["urls"].append(doc_url)
                    handler._save_processed_docs()
                    logging.info(f"Document {doc_url} marked as processed")
                else:
                    logging.warning(
                        f"Failed to post document {doc_url} to any platform - not marking as processed"
                    )

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
