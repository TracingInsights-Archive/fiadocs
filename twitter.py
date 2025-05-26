import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
import urllib.parse
from datetime import datetime

import pdf2image
import requests
from atproto import Client
from bs4 import BeautifulSoup
from requests_oauthlib import OAuth1Session, OAuth2Session

# Global hashtags - Change in 2 places
GLOBAL_HASHTAGS = "#f1 #formula1 #fia #SpanishGP"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TwitterAPIClient:
    def __init__(self, client_id, client_secret=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.oauth1_session = None
        self.base_url = "https://api.x.com"
        self.media_endpoint = "https://api.x.com/2/media/upload"
        self.media_endpoint_v1 = "https://upload.twitter.com/1.1/media/upload.json"
        self.tweets_endpoint = "https://api.x.com/2/tweets"

    def authenticate_oauth1(self, access_token, access_token_secret):
        """Authenticate using OAuth 1.0a for media uploads"""
        try:
            self.oauth1_session = OAuth1Session(
                client_key=self.client_id,
                client_secret=self.client_secret,
                resource_owner_key=access_token,
                resource_owner_secret=access_token_secret,
            )
            logging.info(
                "Successfully authenticated with Twitter API v1.1 using OAuth 1.0a"
            )
            return True
        except Exception as e:
            logging.error(f"Error authenticating with Twitter OAuth 1.0a: {str(e)}")
            return False

    def authenticate_oauth2(self, redirect_uri="https://www.example.com"):
        """Authenticate using OAuth 2.0 flow"""
        try:
            # Set the scopes
            scopes = [
                "media.write",
                "users.read",
                "tweet.read",
                "tweet.write",
                "offline.access",
            ]

            # Create a code verifier
            code_verifier = base64.urlsafe_b64encode(os.urandom(30)).decode("utf-8")
            code_verifier = re.sub("[^a-zA-Z0-9]+", "", code_verifier)

            # Create a code challenge
            code_challenge = hashlib.sha256(code_verifier.encode("utf-8")).digest()
            code_challenge = base64.urlsafe_b64encode(code_challenge).decode("utf-8")
            code_challenge = code_challenge.replace("=", "")

            # Start OAuth 2.0 session
            oauth = OAuth2Session(
                self.client_id, redirect_uri=redirect_uri, scope=scopes
            )

            # Create authorization URL
            auth_url = "https://x.com/i/oauth2/authorize"
            authorization_url, state = oauth.authorization_url(
                auth_url, code_challenge=code_challenge, code_challenge_method="S256"
            )

            logging.info(f"Visit this URL to authorize: {authorization_url}")

            # For automated systems, you'll need to handle this differently
            # This is just for demonstration
            authorization_response = input("Paste the full callback URL: ")

            # Fetch access token
            token_url = "https://api.x.com/2/oauth2/token"

            # Use basic auth if client_secret is provided (confidential client)
            auth = None
            if self.client_secret:
                from requests.auth import HTTPBasicAuth

                auth = HTTPBasicAuth(self.client_id, self.client_secret)

            token = oauth.fetch_token(
                token_url=token_url,
                authorization_response=authorization_response,
                auth=auth,
                client_id=self.client_id,
                include_client_id=True,
                code_verifier=code_verifier,
            )

            self.access_token = token["access_token"]
            logging.info("Successfully authenticated with Twitter API v2")
            return True

        except Exception as e:
            logging.error(f"Error authenticating with Twitter: {str(e)}")
            return False

    def set_access_token(self, access_token):
        """Set access token directly if you have it stored"""
        self.access_token = access_token

    def get_headers(self, content_type="application/json"):
        """Get headers for API requests"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": content_type,
            "User-Agent": "FIADocumentBot/2.0",
        }

    def upload_media_image(self, image_data):
        """Upload image media using v1.1 API with OAuth 1.0a"""
        try:
            if not self.oauth1_session:
                logging.error("OAuth 1.0a session not initialized for media upload")
                return None

            # For images, we can upload directly using v1.1 endpoint
            files = {"media": ("image.jpg", image_data, "image/jpeg")}
            data = {"media_category": "tweet_image"}

            response = self.oauth1_session.post(
                self.media_endpoint_v1, files=files, data=data
            )

            if response.status_code == 200 or response.status_code == 201:
                result = response.json()
                return result.get("media_id_string")
            else:
                logging.error(
                    f"Image upload failed: {response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logging.error(f"Error uploading image to Twitter: {str(e)}")
            return None

    def upload_media_chunked(
        self, media_data, media_type="image/jpeg", media_category="tweet_image"
    ):
        """Upload media using chunked upload with OAuth 1.0a for v1.1 API"""
        try:
            if not self.oauth1_session:
                logging.error(
                    "OAuth 1.0a session not initialized for chunked media upload"
                )
                return None

            # Step 1: Initialize upload
            total_bytes = len(media_data)

            init_data = {
                "command": "INIT",
                "media_type": media_type,
                "total_bytes": total_bytes,
                "media_category": media_category,
            }

            init_response = self.oauth1_session.post(
                self.media_endpoint_v1, data=init_data
            )

            if init_response.status_code not in [200, 201]:
                logging.error(
                    f"Media init failed: {init_response.status_code} - {init_response.text}"
                )
                return None

            media_id = init_response.json()["media_id_string"]
            logging.info(f"Media upload initialized with ID: {media_id}")

            # Step 2: Upload chunks
            segment_id = 0
            bytes_sent = 0
            chunk_size = 4 * 1024 * 1024  # 4MB chunks

            while bytes_sent < total_bytes:
                chunk_end = min(bytes_sent + chunk_size, total_bytes)
                chunk = media_data[bytes_sent:chunk_end]

                files = {"media": ("chunk", chunk, "application/octet-stream")}
                data = {
                    "command": "APPEND",
                    "media_id": media_id,
                    "segment_index": segment_id,
                }

                append_response = self.oauth1_session.post(
                    self.media_endpoint_v1, data=data, files=files
                )

                if append_response.status_code not in [200, 201, 204]:
                    logging.error(
                        f"Chunk upload failed: {append_response.status_code} - {append_response.text}"
                    )
                    return None

                segment_id += 1
                bytes_sent = chunk_end
                logging.info(f"Uploaded {bytes_sent} of {total_bytes} bytes")

            # Step 3: Finalize upload
            finalize_data = {"command": "FINALIZE", "media_id": media_id}

            finalize_response = self.oauth1_session.post(
                self.media_endpoint_v1, data=finalize_data
            )

            if finalize_response.status_code not in [200, 201]:
                logging.error(
                    f"Media finalize failed: {finalize_response.status_code} - {finalize_response.text}"
                )
                return None

            # Check if processing is needed
            result = finalize_response.json()
            processing_info = result.get("processing_info")

            if processing_info:
                self._check_processing_status_oauth1(media_id, processing_info)

            return media_id

        except Exception as e:
            logging.error(f"Error in chunked media upload: {str(e)}")
            return None

    def _check_processing_status_oauth1(self, media_id, processing_info):
        """Check media processing status using OAuth 1.0a"""
        state = processing_info.get("state")
        logging.info(f"Media processing status: {state}")

        if state == "succeeded":
            return True
        elif state == "failed":
            logging.error("Media processing failed")
            return False

        # Wait and check again
        check_after_secs = processing_info.get("check_after_secs", 5)
        logging.info(f"Checking status again after {check_after_secs} seconds")
        time.sleep(check_after_secs)

        status_params = {"command": "STATUS", "media_id": media_id}

        status_response = self.oauth1_session.get(
            self.media_endpoint_v1, params=status_params
        )

        if status_response.status_code == 200:
            result = status_response.json()
            new_processing_info = result.get("processing_info")
            if new_processing_info:
                return self._check_processing_status_oauth1(
                    media_id, new_processing_info
                )

        return True

    def _check_processing_status(self, media_id, processing_info):
        """Check media processing status using OAuth 2.0"""
        state = processing_info.get("state")
        logging.info(f"Media processing status: {state}")

        if state == "succeeded":
            return True
        elif state == "failed":
            logging.error("Media processing failed")
            return False

        # Wait and check again
        check_after_secs = processing_info.get("check_after_secs", 5)
        logging.info(f"Checking status again after {check_after_secs} seconds")
        time.sleep(check_after_secs)

        status_params = {"command": "STATUS", "media_id": media_id}

        headers = self.get_headers()
        status_response = requests.get(
            self.media_endpoint, params=status_params, headers=headers
        )

        if status_response.status_code == 200:
            result = status_response.json()
            new_processing_info = result.get("data", {}).get("processing_info")
            if new_processing_info:
                return self._check_processing_status(media_id, new_processing_info)

        return True

    def upload_media(self, image_data, media_type="image/jpeg"):
        """Upload media - uses OAuth 1.0a for media uploads"""
        # Use OAuth 1.0a for all media uploads
        if self.oauth1_session:
            # For images under 5MB, try simple upload first
            if len(image_data) < 5 * 1024 * 1024 and media_type.startswith("image/"):
                media_id = self.upload_media_image(image_data)
                if media_id:
                    return media_id

            # Fall back to chunked upload
            media_category = (
                "tweet_image" if media_type.startswith("image/") else "tweet_video"
            )
            return self.upload_media_chunked(image_data, media_type, media_category)
        else:
            logging.error("OAuth 1.0a session not available for media upload")
            return None

    def post_tweet(self, text, media_ids=None, reply_to_tweet_id=None):
        """Post a tweet using v2 API with OAuth 2.0"""
        try:
            tweet_data = {"text": text}

            if media_ids:
                tweet_data["media"] = {"media_ids": media_ids}

            if reply_to_tweet_id:
                tweet_data["reply"] = {"in_reply_to_tweet_id": reply_to_tweet_id}

            headers = self.get_headers()

            response = requests.post(
                self.tweets_endpoint, json=tweet_data, headers=headers
            )

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
        self.bluesky_client = None
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
        """Authenticate with Bluesky - independent of Twitter authentication"""
        try:
            for attempt in range(max_retries):
                try:
                    self.bluesky_client = Client()
                    self.bluesky_client.login(username, password)
                    self.bluesky_authenticated = True
                    logging.info("Successfully authenticated with Bluesky")
                    return True
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
            self.bluesky_client = None
            return False

    def authenticate_twitter_oauth2(
        self, client_id, client_secret=None, access_token=None
    ):
        """Authenticate with Twitter using OAuth 2.0"""
        try:
            self.twitter_client = TwitterAPIClient(client_id, client_secret)

            if access_token:
                # Use existing access token
                self.twitter_client.set_access_token(access_token)
                self.twitter_authenticated = True
                logging.info("Successfully set Twitter access token")
                return True
            else:
                # Perform OAuth flow
                if self.twitter_client.authenticate_oauth2():
                    self.twitter_authenticated = True
                    return True
                else:
                    self.twitter_authenticated = False
                    self.twitter_client = None
                    return False

        except Exception as e:
            logging.error(f"Failed to authenticate with Twitter: {str(e)}")
            self.twitter_authenticated = False
            self.twitter_client = None
            return False

    def authenticate_twitter_oauth1(
        self, consumer_key, consumer_secret, access_token, access_token_secret
    ):
        """Authenticate with Twitter using OAuth 1.0a for media uploads"""
        try:
            self.twitter_client = TwitterAPIClient(consumer_key, consumer_secret)

            if self.twitter_client.authenticate_oauth1(
                access_token, access_token_secret
            ):
                self.twitter_authenticated = True
                logging.info("Successfully authenticated with Twitter using OAuth 1.0a")
                return True
            else:
                self.twitter_authenticated = False
                self.twitter_client = None
                return False

        except Exception as e:
            logging.error(f"Failed to authenticate with Twitter OAuth 1.0a: {str(e)}")
            self.twitter_authenticated = False
            self.twitter_client = None
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
        """Post to Bluesky - independent operation"""
        if not self.bluesky_authenticated or not self.bluesky_client:
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

                if parent_post is None:
                    # First post in thread
                    post = self.bluesky_client.send_post(
                        text=formatted_text, embed=images, facets=facets
                    )
                    root_post = post
                    parent_post = post
                else:
                    # Reply to previous post
                    reply_ref = {
                        "root": {"uri": root_post.uri, "cid": root_post.cid},
                        "parent": {"uri": parent_post.uri, "cid": parent_post.cid},
                    }
                    post = self.bluesky_client.send_post(
                        text=f"Page {i//4 + 1} continued...",
                        embed=images,
                        reply_to=reply_ref,
                    )
                    parent_post = post

            logging.info(f"Successfully posted to Bluesky: {doc_title}")
            return True

        except Exception as e:
            logging.error(f"Error posting to Bluesky: {str(e)}")
            return False

    def post_to_twitter(self, image_paths, doc_url, doc_info=None):
        """Post to Twitter - independent operation"""
        if not self.twitter_authenticated or not self.twitter_client:
            logging.warning("Twitter not authenticated, skipping Twitter post")
            return False

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)
            gp_hashtag = self._get_current_gp_hashtag()

            max_title_length = 180
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"
            formatted_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

            if len(formatted_text) + len(doc_url) + 1 <= 280:
                formatted_text += f"\n\n{doc_url}"

            root_tweet = None
            parent_tweet = None

            for i in range(0, len(image_paths), 4):
                chunk = image_paths[i : i + 4]
                media_ids = []

                # Upload images for this chunk
                for img_path in chunk:
                    with open(img_path, "rb") as f:
                        image_data = f.read()

                    media_id = self.twitter_client.upload_media(
                        image_data, "image/jpeg"
                    )
                    if media_id:
                        media_ids.append(media_id)
                    else:
                        logging.error(f"Failed to upload image: {img_path}")

                if not media_ids:
                    logging.error("No media uploaded successfully for this chunk")
                    continue

                if parent_tweet is None:
                    # First tweet in thread
                    tweet = self.twitter_client.post_tweet(
                        text=formatted_text, media_ids=media_ids
                    )
                    if tweet:
                        root_tweet = tweet
                        parent_tweet = tweet
                        logging.info(f"Posted root tweet with ID: {tweet['id']}")
                    else:
                        logging.error("Failed to post root tweet")
                        return False
                else:
                    # Reply to previous tweet
                    reply_text = f"Page {i//4 + 1} continued..."
                    tweet = self.twitter_client.post_tweet(
                        text=reply_text,
                        media_ids=media_ids,
                        reply_to_tweet_id=parent_tweet["id"],
                    )
                    if tweet:
                        parent_tweet = tweet
                        logging.info(f"Posted reply tweet with ID: {tweet['id']}")
                    else:
                        logging.error("Failed to post reply tweet")

            logging.info(f"Successfully posted to Twitter: {doc_title}")
            return True

        except Exception as e:
            logging.error(f"Error posting to Twitter: {str(e)}")
            return False

    def process_new_documents(self):
        """Main method to process new documents"""
        try:
            documents, doc_info = self.fetch_documents()

            if not documents:
                logging.info("No new documents found")
                return

            logging.info(f"Found {len(documents)} new documents")

            for doc_url in documents:
                try:
                    logging.info(f"Processing document: {doc_url}")

                    # Download and convert PDF to images
                    image_paths = self.download_and_convert_pdf(doc_url)

                    if not image_paths:
                        logging.error(f"Failed to convert PDF to images: {doc_url}")
                        continue

                    # Post to both platforms independently
                    bluesky_success = False
                    twitter_success = False

                    if self.bluesky_authenticated:
                        bluesky_success = self.post_to_bluesky(
                            image_paths, doc_url, doc_info
                        )

                    if self.twitter_authenticated:
                        twitter_success = self.post_to_twitter(
                            image_paths, doc_url, doc_info
                        )

                    # Clean up image files
                    for img_path in image_paths:
                        try:
                            os.remove(img_path)
                        except Exception as e:
                            logging.warning(
                                f"Failed to remove image file {img_path}: {str(e)}"
                            )

                    # Mark as processed if at least one platform succeeded
                    if bluesky_success or twitter_success:
                        normalized_url = doc_url.strip().lower().replace("\\", "/")
                        self.processed_docs["urls"].append(normalized_url)
                        self._save_processed_docs()

                        platforms = []
                        if bluesky_success:
                            platforms.append("Bluesky")
                        if twitter_success:
                            platforms.append("Twitter")

                        logging.info(
                            f"Successfully processed and posted to {', '.join(platforms)}: {doc_url}"
                        )
                    else:
                        logging.error(f"Failed to post to any platform: {doc_url}")

                    # Add delay between documents to avoid rate limiting
                    time.sleep(2)

                except Exception as e:
                    logging.error(f"Error processing document {doc_url}: {str(e)}")
                    continue

        except Exception as e:
            logging.error(f"Error in process_new_documents: {str(e)}")

    def run_continuous(self, check_interval=300):
        """Run the document processor continuously"""
        logging.info(
            f"Starting continuous monitoring (checking every {check_interval} seconds)"
        )

        while True:
            try:
                self.process_new_documents()
                logging.info(f"Sleeping for {check_interval} seconds...")
                time.sleep(check_interval)
            except KeyboardInterrupt:
                logging.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logging.error(f"Error in continuous run: {str(e)}")
                logging.info("Continuing after error...")
                time.sleep(60)  # Wait a minute before retrying


def main():
    """Main function to run the FIA document handler"""
    handler = FIADocumentHandler()

    # Authentication credentials from environment variables
    # Bluesky credentials
    bluesky_username = os.getenv("BLUESKY_USERNAME")
    bluesky_password = os.getenv("BLUESKY_USERNAME")
    # bluesky_password = os.getenv("BLUESKY_PASSWORD")

    # Twitter OAuth 2.0 credentials
    twitter_client_id = os.getenv("TWITTER_CLIENT_ID")
    twitter_client_secret = os.getenv("TWITTER_CLIENT_SECRET")
    twitter_access_token = os.getenv("TWITTER_ACCESS_TOKEN")

    # Twitter OAuth 1.0a credentials (for media upload)
    twitter_consumer_key = os.getenv("TWITTER_API_KEY")
    twitter_consumer_secret = os.getenv("TWITTER_API_SECRET")
    twitter_access_token_1 = os.getenv("TWITTER_ACCESS_TOKEN")
    twitter_access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

    # Authenticate with available platforms
    platforms_authenticated = []

    # Authenticate Bluesky
    if bluesky_username and bluesky_password:
        if handler.authenticate_bluesky(bluesky_username, bluesky_password):
            platforms_authenticated.append("Bluesky")
        else:
            logging.warning("Bluesky authentication failed")
    else:
        logging.warning("Bluesky credentials not provided")

    # Authenticate Twitter
    if (
        twitter_consumer_key
        and twitter_consumer_secret
        and twitter_access_token_1
        and twitter_access_token_secret
    ):
        # Use OAuth 1.0a for full functionality (including media upload)
        if handler.authenticate_twitter_oauth1(
            twitter_consumer_key,
            twitter_consumer_secret,
            twitter_access_token_1,
            twitter_access_token_secret,
        ):
            platforms_authenticated.append("Twitter")
        else:
            logging.warning("Twitter OAuth 1.0a authentication failed")
    elif twitter_client_id and twitter_access_token:
        # Use OAuth 2.0 with existing token
        if handler.authenticate_twitter_oauth2(
            twitter_client_id, twitter_client_secret, twitter_access_token
        ):
            platforms_authenticated.append("Twitter")
        else:
            logging.warning("Twitter OAuth 2.0 authentication failed")
    else:
        logging.warning("Twitter credentials not provided")

    if not platforms_authenticated:
        logging.error("No platforms authenticated successfully. Exiting.")
        return

    logging.info(
        f"Successfully authenticated with: {', '.join(platforms_authenticated)}"
    )

    # Check for command line arguments
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        # Run once and exit
        logging.info("Running once and exiting...")
        handler.process_new_documents()
    else:
        # Run continuously
        handler.run_continuous()


if __name__ == "__main__":
    main()
