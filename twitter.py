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
from requests_oauthlib import OAuth2Session

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
        self.refresh_token = None
        self.base_url = "https://api.x.com"
        self.media_endpoint = "https://upload.twitter.com/1.1/media/upload.json"
        self.tweets_endpoint = "https://api.x.com/2/tweets"

    def authenticate_oauth2(self, redirect_uri="https://www.example.com"):
        """Authenticate using OAuth 2.0 flow with User Context"""
        try:
            # Set the scopes for user context
            scopes = ["tweet.read", "tweet.write", "users.read", "offline.access"]

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
            auth_url = "https://twitter.com/i/oauth2/authorize"
            authorization_url, state = oauth.authorization_url(
                auth_url, code_challenge=code_challenge, code_challenge_method="S256"
            )

            logging.info(f"Visit this URL to authorize: {authorization_url}")

            # For automated systems, you'll need to handle this differently
            # This is just for demonstration
            authorization_response = input("Paste the full callback URL: ")

            # Fetch access token
            token_url = "https://api.twitter.com/2/oauth2/token"

            # Prepare token request
            token_data = {
                "grant_type": "authorization_code",
                "client_id": self.client_id,
                "code_verifier": code_verifier,
                "redirect_uri": redirect_uri,
            }

            # Extract authorization code from callback URL
            parsed_url = urllib.parse.urlparse(authorization_response)
            auth_code = urllib.parse.parse_qs(parsed_url.query).get("code", [None])[0]

            if not auth_code:
                raise Exception("No authorization code found in callback URL")

            token_data["code"] = auth_code

            # Use basic auth if client_secret is provided (confidential client)
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            auth = None
            if self.client_secret:
                from requests.auth import HTTPBasicAuth

                auth = HTTPBasicAuth(self.client_id, self.client_secret)
            else:
                # For public clients, include client_id in the body
                token_data["client_id"] = self.client_id

            response = requests.post(
                token_url,
                data=urllib.parse.urlencode(token_data),
                headers=headers,
                auth=auth,
            )

            if response.status_code != 200:
                logging.error(
                    f"Token request failed: {response.status_code} - {response.text}"
                )
                return False

            token = response.json()
            self.access_token = token["access_token"]
            self.refresh_token = token.get("refresh_token")

            logging.info("Successfully authenticated with Twitter API v2")
            return True

        except Exception as e:
            logging.error(f"Error authenticating with Twitter: {str(e)}")
            return False

    def set_access_token(self, access_token, refresh_token=None):
        """Set access token directly if you have it stored"""
        self.access_token = access_token
        self.refresh_token = refresh_token

    def refresh_access_token(self):
        """Refresh the access token using refresh token"""
        if not self.refresh_token:
            logging.error("No refresh token available")
            return False

        try:
            token_url = "https://api.twitter.com/2/oauth2/token"

            token_data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
            }

            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            auth = None
            if self.client_secret:
                from requests.auth import HTTPBasicAuth

                auth = HTTPBasicAuth(self.client_id, self.client_secret)

            response = requests.post(
                token_url,
                data=urllib.parse.urlencode(token_data),
                headers=headers,
                auth=auth,
            )

            if response.status_code == 200:
                token = response.json()
                self.access_token = token["access_token"]
                if "refresh_token" in token:
                    self.refresh_token = token["refresh_token"]
                logging.info("Successfully refreshed Twitter access token")
                return True
            else:
                logging.error(
                    f"Token refresh failed: {response.status_code} - {response.text}"
                )
                return False

        except Exception as e:
            logging.error(f"Error refreshing token: {str(e)}")
            return False

    def get_headers(self, content_type="application/json"):
        """Get headers for API requests"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": content_type,
            "User-Agent": "FIADocumentBot/2.0",
        }

    def upload_media_v1(self, image_data):
        """Upload media using Twitter API v1.1 (required for media uploads)"""
        try:
            # Use v1.1 endpoint for media upload
            files = {"media": ("image.jpg", image_data, "image/jpeg")}

            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "FIADocumentBot/2.0",
            }

            response = requests.post(self.media_endpoint, files=files, headers=headers)

            if response.status_code in [200, 201]:
                result = response.json()
                return result.get("media_id_string")
            else:
                logging.error(
                    f"Media upload failed: {response.status_code} - {response.text}"
                )
                # Try to refresh token if we get 401
                if response.status_code == 401 and self.refresh_token:
                    logging.info("Attempting to refresh access token...")
                    if self.refresh_access_token():
                        # Retry upload with new token
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        retry_response = requests.post(
                            self.media_endpoint, files=files, headers=headers
                        )
                        if retry_response.status_code in [200, 201]:
                            result = retry_response.json()
                            return result.get("media_id_string")
                return None

        except Exception as e:
            logging.error(f"Error uploading media to Twitter: {str(e)}")
            return None

    def upload_media_chunked_v1(self, media_data, media_type="image/jpeg"):
        """Upload media using chunked upload for larger files (v1.1 API)"""
        try:
            total_bytes = len(media_data)

            # Step 1: Initialize upload
            init_data = {
                "command": "INIT",
                "media_type": media_type,
                "total_bytes": total_bytes,
            }

            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "FIADocumentBot/2.0",
            }

            init_response = requests.post(
                self.media_endpoint, data=init_data, headers=headers
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

                append_response = requests.post(
                    self.media_endpoint, data=data, files=files, headers=headers
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

            finalize_response = requests.post(
                self.media_endpoint, data=finalize_data, headers=headers
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
                if not self._check_processing_status_v1(media_id, processing_info):
                    return None

            return media_id

        except Exception as e:
            logging.error(f"Error in chunked media upload: {str(e)}")
            return None

    def _check_processing_status_v1(self, media_id, processing_info):
        """Check media processing status for v1.1 API"""
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

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "FIADocumentBot/2.0",
        }

        status_response = requests.get(
            self.media_endpoint, params=status_params, headers=headers
        )

        if status_response.status_code == 200:
            result = status_response.json()
            new_processing_info = result.get("processing_info")
            if new_processing_info:
                return self._check_processing_status_v1(media_id, new_processing_info)

        return True

    def upload_media(self, image_data, media_type="image/jpeg"):
        """Upload media - tries simple upload first, falls back to chunked"""
        # For images under 5MB, try simple upload first
        if len(image_data) < 5 * 1024 * 1024 and media_type.startswith("image/"):
            media_id = self.upload_media_v1(image_data)
            if media_id:
                return media_id

        # Fall back to chunked upload
        return self.upload_media_chunked_v1(image_data, media_type)

    def post_tweet(self, text, media_ids=None, reply_to_tweet_id=None):
        """Post a tweet using v2 API"""
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
            elif response.status_code == 401 and self.refresh_token:
                # Try to refresh token and retry
                logging.info("Attempting to refresh access token for tweet posting...")
                if self.refresh_access_token():
                    headers = self.get_headers()
                    retry_response = requests.post(
                        self.tweets_endpoint, json=tweet_data, headers=headers
                    )
                    if retry_response.status_code == 201:
                        return retry_response.json()["data"]
                    else:
                        logging.error(
                            f"Tweet post retry failed: {retry_response.status_code} - {retry_response.text}"
                        )
                        return None
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
        self, client_id, client_secret=None, access_token=None, refresh_token=None
    ):
        """Authenticate with Twitter using OAuth 2.0"""
        try:
            self.twitter_client = TwitterAPIClient(client_id, client_secret)

            if access_token:
                # Use existing access token
                self.twitter_client.set_access_token(access_token, refresh_token)
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

            logging.info(f"Successfully posted to Bluesky: {doc_title}")
            return True

        except Exception as e:
            logging.error(f"Error posting to Bluesky: {str(e)}")
            return False

    def post_to_twitter(self, image_paths, doc_url, doc_info=None):
        """Post document to Twitter as a thread using v2 API"""
        if not self.twitter_authenticated or not self.twitter_client:
            logging.warning("Twitter not authenticated, skipping Twitter post")
            return False

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
                return True
            else:
                logging.error("Failed to post Twitter thread")
                return False

        except Exception as e:
            logging.error(f"Error posting to Twitter: {str(e)}")
            return False

    def post_to_both_platforms(self, image_paths, doc_url, doc_info=None):
        """Post to both platforms independently - one failure won't affect the other"""
        bluesky_success = False
        twitter_success = False

        # Post to Bluesky (independent operation)
        if self.bluesky_authenticated:
            try:
                logging.info("Posting to Bluesky...")
                bluesky_success = self.post_to_bluesky(image_paths, doc_url, doc_info)
                if bluesky_success:
                    logging.info("Successfully posted to Bluesky")
                else:
                    logging.error("Failed to post to Bluesky")
            except Exception as e:
                logging.error(f"Error posting to Bluesky: {str(e)}")
        else:
            logging.info("Bluesky not authenticated, skipping Bluesky post")

        # Add small delay between platforms
        time.sleep(3)

        # Post to Twitter (independent operation)
        if self.twitter_authenticated:
            try:
                logging.info("Posting to Twitter...")
                twitter_success = self.post_to_twitter(image_paths, doc_url, doc_info)
                if twitter_success:
                    logging.info("Successfully posted to Twitter")
                else:
                    logging.error("Failed to post to Twitter")
            except Exception as e:
                logging.error(f"Error posting to Twitter: {str(e)}")
        else:
            logging.info("Twitter not authenticated, skipping Twitter post")

        # Log overall results
        if bluesky_success and twitter_success:
            logging.info("Successfully posted to both platforms")
        elif bluesky_success:
            logging.info("Posted to Bluesky only (Twitter failed or not configured)")
        elif twitter_success:
            logging.info("Posted to Twitter only (Bluesky failed or not configured)")
        else:
            logging.warning("Failed to post to any platform")

        # Return True if at least one platform succeeded
        return bluesky_success or twitter_success


def main():
    logging.info("Starting FIA Document Handler")
    handler = FIADocumentHandler()

    # Track which platforms are available
    platforms_available = []

    # Try to authenticate with Bluesky (independent)
    bluesky_username = os.environ.get("BLUESKY_USERNAME")
    bluesky_password = os.environ.get("BLUESKY_USERNAME")
    # bluesky_password = os.environ.get("BLUESKY_PASSWORD")

    if bluesky_username and bluesky_password:
        try:
            if handler.authenticate_bluesky(
                bluesky_username, bluesky_password, max_retries=3, timeout=30
            ):
                platforms_available.append("Bluesky")
            else:
                logging.warning(
                    "Bluesky authentication failed, will skip Bluesky posts"
                )
        except Exception as e:
            logging.error(f"Bluesky authentication error: {str(e)}")
    else:
        logging.warning("Bluesky credentials not provided, will skip Bluesky posts")

    # Try to authenticate with Twitter using OAuth 2.0 (independent)
    twitter_client_id = os.environ.get("TWITTER_CLIENT_ID")
    twitter_client_secret = os.environ.get(
        "TWITTER_CLIENT_SECRET"
    )  # Optional for public clients
    twitter_access_token = os.environ.get(
        "TWITTER_ACCESS_TOKEN"
    )  # If you have a stored token
    twitter_refresh_token = os.environ.get(
        "TWITTER_REFRESH_TOKEN"
    )  # If you have a refresh token

    if twitter_client_id:
        try:
            if handler.authenticate_twitter_oauth2(
                twitter_client_id,
                twitter_client_secret,
                twitter_access_token,
                twitter_refresh_token,
            ):
                platforms_available.append("Twitter")
            else:
                logging.warning(
                    "Twitter authentication failed, will skip Twitter posts"
                )
        except Exception as e:
            logging.error(f"Twitter authentication error: {str(e)}")
    else:
        logging.warning("Twitter Client ID not provided, will skip Twitter posts")

    # Check if at least one platform is available
    if not platforms_available:
        logging.error("No platforms available for posting. Exiting.")
        return

    logging.info(f"Available platforms: {', '.join(platforms_available)}")

    try:
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
                logging.info(f"Processing document: {doc_url}")
                image_paths = handler.download_and_convert_pdf(doc_url)

                # Post to available platforms (independent operations)
                success = handler.post_to_both_platforms(
                    image_paths, doc_url, document_info
                )

                if success:
                    # Mark as processed only if at least one platform succeeded
                    handler.processed_docs["urls"].append(doc_url)
                    handler._save_processed_docs()
                    logging.info(
                        f"Document processed and marked as complete: {doc_url}"
                    )
                else:
                    logging.error(
                        f"Failed to post to any platform for document: {doc_url}"
                    )

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
        logging.error(f"Fatal error in main process: {str(e)}")
        raise

    logging.info("FIA Document Handler completed")


if __name__ == "__main__":
    main()
