import base64
import json
import logging
import os
import time
from datetime import datetime

import pdf2image
import requests
import requests_oauthlib
from atproto import Client
from bs4 import BeautifulSoup
from mastodon import Mastodon
from PIL import Image
from requests_oauthlib import OAuth1Session

# Global hashtags - Change in 2 places
GLOBAL_HASHTAGS = "#f1 #formula1 #fia #CanadianGP"

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
        self.instagram_app_id = None
        self.instagram_app_secret = None
        self.instagram_access_token = None
        self.instagram_business_account_id = None
        self.facebook_page_id = None
        self.facebook_page_access_token = None
        self.pixelfed_client = None
        self.telegram_bot_token = None
        self.telegram_channel_id = None
        self.linkedin_access_token = None
        self.linkedin_organization_id = None
        self.tumblr_consumer_key = None
        self.tumblr_consumer_secret = None
        self.tumblr_access_token = None
        self.tumblr_access_token_secret = None
        self.tumblr_blog_name = None
        self.tumblr_session = None
        self.tumblr_authenticated = False
        self.linkedin_authenticated = False
        self.telegram_authenticated = False
        self.pixelfed_authenticated = False
        self.bluesky_authenticated = False
        self.mastodon_authenticated = False
        self.threads_authenticated = False
        self.instagram_authenticated = False
        self.facebook_authenticated = False

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

    def authenticate_tumblr(
        self,
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret,
        blog_name,
        max_retries=3,
    ):
        """
        Authenticate with Tumblr API using OAuth 1.0a
        """
        for attempt in range(max_retries):
            try:
                self.tumblr_consumer_key = consumer_key
                self.tumblr_consumer_secret = consumer_secret
                self.tumblr_access_token = access_token
                self.tumblr_access_token_secret = access_token_secret
                self.tumblr_blog_name = blog_name

                # Create OAuth1 session
                auth = OAuth1Session(
                    consumer_key,
                    client_secret=consumer_secret,
                    resource_owner_key=access_token,
                    resource_owner_secret=access_token_secret,
                )

                self.tumblr_session = auth

                # Test authentication by getting blog info
                response = self.tumblr_session.get(
                    f"https://api.tumblr.com/v2/blog/{blog_name}/info"
                )
                response.raise_for_status()

                blog_data = response.json()

                if blog_data.get("meta", {}).get("status") == 200:
                    blog_info = blog_data.get("response", {}).get("blog", {})
                    blog_title = blog_info.get("title", "Unknown")
                    logging.info(
                        f"Successfully authenticated with Tumblr blog: {blog_title} ({blog_name})"
                    )
                    self.tumblr_authenticated = True
                    return True
                else:
                    raise Exception(f"Failed to get blog info: {blog_data}")

            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Tumblr after {max_retries} attempts: {str(e)}"
                    )
                    self.tumblr_authenticated = False
                    return False
                logging.warning(
                    f"Tumblr authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

    def post_to_tumblr(self, image_paths, doc_url, doc_info=None):
        """
        Post to Tumblr using the Neue Post Format (NPF) API
        Tumblr supports up to 10 images per post and rich content blocks
        """
        if not self.tumblr_authenticated:
            logging.warning("Skipping Tumblr post - not authenticated")
            return False

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)

            # Process images in chunks of 10 (Tumblr's limit per post)
            for i in range(0, len(image_paths), 10):
                chunk = image_paths[i : i + 10]

                try:
                    # Add delay between posts to respect rate limits
                    if i > 0:
                        time.sleep(5)

                    # Determine content for this chunk
                    if i == 0:
                        title = doc_title
                        tags = ["f1", "formula1", "fia", "CanadianGP"]
                    else:
                        title = f"{doc_title} (Part {i//10 + 1})"
                        tags = ["f1", "formula1", "fia"]

                    # Create NPF post
                    success = self._create_tumblr_npf_post(
                        chunk, title, pub_date, doc_url, tags, i == 0
                    )

                    if success:
                        logging.info(f"Successfully posted Tumblr chunk {i//10 + 1}")
                    else:
                        logging.error(f"Failed to post Tumblr chunk {i//10 + 1}")
                        return False

                except Exception as e:
                    logging.error(
                        f"Failed to process Tumblr chunk {i//10 + 1}: {str(e)}"
                    )
                    return False

            logging.info("Successfully posted to Tumblr")
            return True

        except Exception as e:
            logging.error(f"Failed to post to Tumblr: {str(e)}")
            return False

    def _create_tumblr_npf_post(
        self, image_paths, title, pub_date, doc_url, tags, include_url=True
    ):
        """Create a Tumblr post using the Neue Post Format (NPF) API"""
        try:
            # Create NPF content blocks
            content_blocks = []

            # Add title block with bold formatting
            content_blocks.append(
                {
                    "type": "text",
                    "text": title,
                    "formatting": [{"start": 0, "end": len(title), "type": "bold"}],
                }
            )

            # Add publication date block
            pub_text = f"\nPublished: {pub_date}"
            content_blocks.append({"type": "text", "text": pub_text})

            # Upload images and create media blocks
            uploaded_media = []
            for i, img_path in enumerate(image_paths):
                try:
                    media_object = self._upload_tumblr_media(img_path)
                    if media_object:
                        uploaded_media.append(media_object)

                        # Add image block to content
                        content_blocks.append(
                            {
                                "type": "image",
                                "media": [media_object],
                                "alt_text": f"FIA Document - {title} (Page {i+1})",
                            }
                        )
                    else:
                        logging.warning(f"Failed to upload image {img_path} to Tumblr")

                except Exception as e:
                    logging.warning(
                        f"Failed to process image {img_path} for Tumblr: {str(e)}"
                    )
                    continue

            # Add source link block (only for first chunk)
            if include_url:
                link_text = f"\nSource: {doc_url}"
                content_blocks.append(
                    {
                        "type": "text",
                        "text": link_text,
                        "formatting": [
                            {
                                "start": 9,  # After "Source: "
                                "end": len(link_text),
                                "type": "link",
                                "url": doc_url,
                            }
                        ],
                    }
                )

            # Add hashtags block
            hashtag_text = f"\n\n{GLOBAL_HASHTAGS}"
            content_blocks.append({"type": "text", "text": hashtag_text})

            # Create the NPF post
            post_data = {"content": content_blocks, "tags": tags, "state": "published"}

            # Post to Tumblr using NPF API
            response = self.tumblr_session.post(
                f"https://api.tumblr.com/v2/blog/{self.tumblr_blog_name}/posts",
                json=post_data,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "FIADocsBot/1.0",
                },
            )
            response.raise_for_status()

            result = response.json()

            if result.get("meta", {}).get("status") == 201:
                post_id = result.get("response", {}).get("id")
                logging.info(f"Created Tumblr NPF post: {post_id}")
                return True
            else:
                logging.error(f"Tumblr NPF post creation failed: {result}")
                return False

        except Exception as e:
            logging.error(f"Failed to create Tumblr NPF post: {str(e)}")
            # Fallback to legacy photo post
            return self._create_tumblr_legacy_post(
                image_paths, title, pub_date, doc_url, tags, include_url
            )

    def _upload_tumblr_media(self, image_path):
        """Upload media to Tumblr and return media object"""
        try:
            # Read and encode image
            with open(image_path, "rb") as f:
                image_data = f.read()

            # Encode image as base64
            image_b64 = base64.b64encode(image_data).decode("utf-8")

            # Determine MIME type
            if image_path.lower().endswith(".png"):
                mime_type = "image/png"
            elif image_path.lower().endswith(".gif"):
                mime_type = "image/gif"
            else:
                mime_type = "image/jpeg"

            # Create media object for NPF
            media_object = {
                "type": mime_type,
                "identifier": f"media_{int(time.time())}_{os.path.basename(image_path)}",
                "data": image_b64,
            }

            return media_object

        except Exception as e:
            logging.error(f"Failed to upload media to Tumblr: {str(e)}")
            return None

    def _create_tumblr_legacy_post(
        self, image_paths, title, pub_date, doc_url, tags, include_url=True
    ):
        """Fallback to legacy photo post format"""
        try:
            logging.info("Using Tumblr legacy photo post format")

            # Create caption
            caption_parts = [f"<strong>{title}</strong>"]
            caption_parts.append(f"Published: {pub_date}")

            if include_url:
                caption_parts.append(f'<a href="{doc_url}">Source</a>')

            caption_parts.append(GLOBAL_HASHTAGS)
            caption = "<br><br>".join(caption_parts)

            # Prepare form data for multipart upload
            files = {}
            data = {
                "type": "photo",
                "state": "published",
                "tags": ",".join(tags),
                "caption": caption,
            }

            # Add images as form data
            for i, img_path in enumerate(image_paths):
                try:
                    files[f"data[{i}]"] = (
                        os.path.basename(img_path),
                        open(img_path, "rb"),
                        "image/jpeg",
                    )
                except Exception as e:
                    logging.warning(f"Failed to prepare image {img_path}: {str(e)}")
                    continue

            if not files:
                logging.error("No images to upload for Tumblr legacy post")
                return False

            try:
                # Post using legacy API
                response = self.tumblr_session.post(
                    f"https://api.tumblr.com/v2/blog/{self.tumblr_blog_name}/post",
                    data=data,
                    files=files,
                    headers={"User-Agent": "FIADocsBot/1.0"},
                )
                response.raise_for_status()

                result = response.json()

                if result.get("meta", {}).get("status") == 201:
                    post_id = result.get("response", {}).get("id")
                    logging.info(f"Created Tumblr legacy post: {post_id}")
                    return True
                else:
                    logging.error(f"Tumblr legacy post creation failed: {result}")
                    return False

            finally:
                # Close all file handles
                for file_handle in files.values():
                    if hasattr(file_handle, "close"):
                        file_handle[1].close()

        except Exception as e:
            logging.error(f"Failed to create Tumblr legacy post: {str(e)}")
            return False

    def authenticate_linkedin(self, access_token, organization_id, max_retries=3):
        """
        Authenticate with LinkedIn Pages API
        """
        for attempt in range(max_retries):
            try:
                self.linkedin_access_token = access_token
                self.linkedin_organization_id = organization_id

                # Test authentication by getting organization info
                url = f"https://api.linkedin.com/v2/organizations/{organization_id}"
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "X-Restli-Protocol-Version": "2.0.0",
                }

                response = requests.get(url, headers=headers)
                response.raise_for_status()

                org_data = response.json()
                org_name = org_data.get("localizedName", "Unknown")

                logging.info(
                    f"Successfully authenticated with LinkedIn organization: {org_name}"
                )
                self.linkedin_authenticated = True
                return True

            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with LinkedIn after {max_retries} attempts: {str(e)}"
                    )
                    self.linkedin_authenticated = False
                    return False
                logging.warning(
                    f"LinkedIn authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

    def post_to_linkedin(self, image_paths, doc_url, doc_info=None):
        """
        Post to LinkedIn Page
        """
        if not self.linkedin_authenticated:
            logging.warning("Skipping LinkedIn post - not authenticated")
            return False

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)

            max_title_length = 1000
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"

            # LinkedIn post text
            post_text = f"{doc_title}\n\nPublished: {pub_date}\n\n{all_hashtags}\n\nSource: {doc_url}"

            # LinkedIn has a 3000 character limit
            if len(post_text) > 3000:
                post_text = post_text[:2997] + "..."

            # Process images in chunks (LinkedIn allows up to 20 images per post)
            for i in range(0, len(image_paths), 20):
                chunk = image_paths[i : i + 20]

                try:
                    # Add delay between posts to respect rate limits
                    if i > 0:
                        time.sleep(5)

                    # Determine message for this chunk
                    chunk_text = (
                        post_text
                        if i == 0
                        else f"{doc_title} (Part {i//20 + 1})\n\n{all_hashtags}"
                    )

                    if len(chunk) == 1:
                        # Single image post
                        success = self._create_linkedin_single_post(
                            chunk[0], chunk_text
                        )
                    else:
                        # Multiple images post
                        success = self._create_linkedin_multi_post(chunk, chunk_text)

                    if success:
                        logging.info(f"Successfully posted LinkedIn chunk {i//20 + 1}")
                    else:
                        logging.error(f"Failed to post LinkedIn chunk {i//20 + 1}")
                        return False

                except Exception as e:
                    logging.error(
                        f"Failed to process LinkedIn chunk {i//20 + 1}: {str(e)}"
                    )
                    return False

            logging.info("Successfully posted to LinkedIn")
            return True

        except Exception as e:
            logging.error(f"Failed to post to LinkedIn: {str(e)}")
            return False

    def _create_linkedin_single_post(self, image_path, text):
        """Create a single image post on LinkedIn"""
        try:
            # Step 1: Register upload
            upload_url = self._register_linkedin_upload(image_path)
            if not upload_url:
                return False

            # Step 2: Upload image
            image_urn = self._upload_to_linkedin(image_path, upload_url)
            if not image_urn:
                return False

            # Step 3: Create post
            return self._create_linkedin_post(text, [image_urn])

        except Exception as e:
            logging.error(f"Failed to create LinkedIn single post: {str(e)}")
            return False

    def _create_linkedin_multi_post(self, image_paths, text):
        """Create a multi-image post on LinkedIn"""
        try:
            image_urns = []

            # Upload all images
            for image_path in image_paths:
                upload_url = self._register_linkedin_upload(image_path)
                if upload_url:
                    image_urn = self._upload_to_linkedin(image_path, upload_url)
                    if image_urn:
                        image_urns.append(image_urn)

            if not image_urns:
                logging.error("No images were successfully uploaded to LinkedIn")
                return False

            # Create post with all images
            return self._create_linkedin_post(text, image_urns)

        except Exception as e:
            logging.error(f"Failed to create LinkedIn multi-post: {str(e)}")
            return False

    def _register_linkedin_upload(self, image_path):
        """Register upload with LinkedIn and get upload URL"""
        try:
            url = "https://api.linkedin.com/v2/assets?action=registerUpload"

            headers = {
                "Authorization": f"Bearer {self.linkedin_access_token}",
                "Content-Type": "application/json",
                "X-Restli-Protocol-Version": "2.0.0",
            }

            # Get file size
            file_size = os.path.getsize(image_path)

            data = {
                "registerUploadRequest": {
                    "recipes": ["urn:li:digitalmediaRecipe:feedshare-image"],
                    "owner": f"urn:li:organization:{self.linkedin_organization_id}",
                    "serviceRelationships": [
                        {
                            "relationshipType": "OWNER",
                            "identifier": "urn:li:userGeneratedContent",
                        }
                    ],
                    "supportedUploadMechanism": ["SYNCHRONOUS_UPLOAD"],
                    "fileSize": file_size,
                }
            }

            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()

            result = response.json()
            upload_mechanism = result["value"]["uploadMechanism"][
                "com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"
            ]
            upload_url = upload_mechanism["uploadUrl"]
            asset_urn = result["value"]["asset"]

            return {"upload_url": upload_url, "asset_urn": asset_urn}

        except Exception as e:
            logging.error(f"Failed to register LinkedIn upload: {str(e)}")
            return None

    def _upload_to_linkedin(self, image_path, upload_info):
        """Upload image to LinkedIn"""
        try:
            with open(image_path, "rb") as f:
                response = requests.put(upload_info["upload_url"], data=f)
                response.raise_for_status()

            return upload_info["asset_urn"]

        except Exception as e:
            logging.error(f"Failed to upload to LinkedIn: {str(e)}")
            return None

    def _create_linkedin_post(self, text, image_urns):
        """Create LinkedIn post with images"""
        try:
            url = "https://api.linkedin.com/v2/ugcPosts"

            headers = {
                "Authorization": f"Bearer {self.linkedin_access_token}",
                "Content-Type": "application/json",
                "X-Restli-Protocol-Version": "2.0.0",
            }

            # Prepare media
            media = []
            for image_urn in image_urns:
                media.append(
                    {
                        "status": "READY",
                        "description": {"text": "FIA Document"},
                        "media": image_urn,
                        "title": {"text": "FIA Document"},
                    }
                )

            data = {
                "author": f"urn:li:organization:{self.linkedin_organization_id}",
                "lifecycleState": "PUBLISHED",
                "specificContent": {
                    "com.linkedin.ugc.ShareContent": {
                        "shareCommentary": {"text": text},
                        "shareMediaCategory": "IMAGE",
                        "media": media,
                    }
                },
                "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
            }

            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()

            result = response.json()
            logging.info(f"Created LinkedIn post: {result.get('id', 'Unknown ID')}")
            return True

        except Exception as e:
            logging.error(f"Failed to create LinkedIn post: {str(e)}")
            return False

    def post_to_telegram(self, image_paths, doc_url, doc_info=None):
        """
        Post to Telegram channel
        """
        if not self.telegram_authenticated:
            logging.warning("Skipping Telegram post - not authenticated")
            return False

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)

            max_title_length = 3000  # Telegram has 4096 character limit
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"

            # Create message with proper Telegram formatting for channels
            message = (
                f"🏎️ <b>{doc_title}</b>\n\n📅 Published: {pub_date}\n\n{all_hashtags}"
            )

            # Add URL if it fits
            if len(message) + len(doc_url) + 30 <= 4000:  # Leave buffer for formatting
                message += f'\n\n📄 <a href="{doc_url}">View Document</a>'

            # Process images in chunks (Telegram allows up to 10 images per media group)
            for i in range(0, len(image_paths), 10):
                chunk = image_paths[i : i + 10]

                try:
                    # Add delay between posts to respect rate limits
                    if i > 0:
                        time.sleep(3)  # Longer delay for channels

                    # Determine message for this chunk
                    if i == 0:
                        chunk_message = message
                    else:
                        chunk_message = (
                            f"🏎️ <b>{doc_title}</b> (Part {i//10 + 1})\n\n{all_hashtags}"
                        )

                    if len(chunk) == 1:
                        # Single image post
                        success = self._send_telegram_photo(chunk[0], chunk_message)
                    else:
                        # Media group post
                        success = self._send_telegram_media_group(chunk, chunk_message)

                    if success:
                        logging.info(
                            f"Successfully posted Telegram channel chunk {i//10 + 1}"
                        )
                    else:
                        logging.error(
                            f"Failed to post Telegram channel chunk {i//10 + 1}"
                        )
                        return False

                except Exception as e:
                    logging.error(
                        f"Failed to process Telegram channel chunk {i//10 + 1}: {str(e)}"
                    )
                    return False

            logging.info("Successfully posted to Telegram channel")
            return True

        except Exception as e:
            logging.error(f"Failed to post to Telegram channel: {str(e)}")
            return False

    def authenticate_telegram(self, bot_token, channel_id, max_retries=3):
        """
        Authenticate with Telegram Bot API for channel posting
        """
        for attempt in range(max_retries):
            try:
                self.telegram_bot_token = bot_token
                self.telegram_channel_id = channel_id

                # Test authentication by getting bot info
                url = f"https://api.telegram.org/bot{bot_token}/getMe"
                response = requests.get(url)
                response.raise_for_status()

                result = response.json()
                if result.get("ok"):
                    bot_info = result.get("result", {})
                    bot_username = bot_info.get("username", "Unknown")

                    # Test channel access by getting chat info
                    chat_url = f"https://api.telegram.org/bot{bot_token}/getChat"
                    chat_response = requests.get(
                        chat_url, params={"chat_id": channel_id}
                    )

                    if chat_response.status_code == 200:
                        chat_result = chat_response.json()
                        if chat_result.get("ok"):
                            chat_info = chat_result.get("result", {})
                            channel_title = chat_info.get("title", "Unknown Channel")
                            logging.info(
                                f"Successfully authenticated Telegram bot @{bot_username} for channel: {channel_title}"
                            )
                            self.telegram_authenticated = True
                            return True
                        else:
                            raise Exception(f"Cannot access channel: {chat_result}")
                    else:
                        logging.warning(
                            f"Cannot verify channel access, but bot authentication successful for @{bot_username}"
                        )
                        self.telegram_authenticated = True
                        return True
                else:
                    raise Exception(f"Telegram API error: {result}")

            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Telegram after {max_retries} attempts: {str(e)}"
                    )
                    self.telegram_authenticated = False
                    return False
                logging.warning(
                    f"Telegram authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

    def _send_telegram_photo(self, image_path, caption):
        """Send a single photo to Telegram channel"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendPhoto"

            with open(image_path, "rb") as photo:
                files = {"photo": photo}
                data = {
                    "chat_id": self.telegram_channel_id,
                    "caption": caption,
                    "parse_mode": "HTML",
                    "disable_notification": False,  # Enable notifications for important updates
                }

                response = requests.post(url, files=files, data=data)
                response.raise_for_status()

                result = response.json()
                if result.get("ok"):
                    message_id = result["result"]["message_id"]
                    logging.info(
                        f"Sent photo to Telegram channel: message {message_id}"
                    )
                    return True
                else:
                    logging.error(f"Telegram channel photo send failed: {result}")
                    return False

        except Exception as e:
            logging.error(f"Failed to send photo to Telegram channel: {str(e)}")
            return False

    def _send_telegram_media_group(self, image_paths, caption):
        """Send multiple photos as media group to Telegram channel"""
        try:
            url = (
                f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMediaGroup"
            )

            # Prepare media array
            media = []
            files = {}

            for i, image_path in enumerate(image_paths):
                file_key = f"photo{i}"
                files[file_key] = open(image_path, "rb")

                media_item = {"type": "photo", "media": f"attach://{file_key}"}

                # Add caption to first image only
                if i == 0:
                    media_item["caption"] = caption
                    media_item["parse_mode"] = "HTML"

                media.append(media_item)

            data = {
                "chat_id": self.telegram_channel_id,
                "media": json.dumps(media),
                "disable_notification": False,  # Enable notifications for important updates
            }

            try:
                response = requests.post(url, files=files, data=data)
                response.raise_for_status()

                result = response.json()
                if result.get("ok"):
                    message_count = len(result["result"])
                    logging.info(
                        f"Sent media group to Telegram channel with {len(image_paths)} images ({message_count} messages)"
                    )
                    return True
                else:
                    logging.error(f"Telegram channel media group send failed: {result}")
                    return False

            finally:
                # Close all file handles
                for file_handle in files.values():
                    file_handle.close()

        except Exception as e:
            logging.error(f"Failed to send media group to Telegram channel: {str(e)}")
            return False

    def authenticate_pixelfed(self, access_token, max_retries=3):
        """
        Authenticate with Pixelfed using access token
        """
        for attempt in range(max_retries):
            try:
                # Create Pixelfed client (uses same API as Mastodon)
                self.pixelfed_client = Mastodon(
                    access_token=access_token, api_base_url="https://pixelfed.social"
                )

                # Test authentication by getting account info
                account = self.pixelfed_client.me()
                logging.info(
                    f"Successfully authenticated with Pixelfed as @{account['username']}"
                )
                self.pixelfed_authenticated = True
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Pixelfed after {max_retries} attempts: {str(e)}"
                    )
                    self.pixelfed_authenticated = False
                    return False
                logging.warning(
                    f"Pixelfed authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

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

    def authenticate_instagram(self, app_id, app_secret, access_token, max_retries=3):
        """
        Authenticate with Instagram Business API

        Args:
            app_id: Instagram App ID
            app_secret: Instagram App Secret
            access_token: Access token with instagram_basic and instagram_content_publish permissions
        """
        for attempt in range(max_retries):
            try:
                self.instagram_app_id = app_id
                self.instagram_app_secret = app_secret
                self.instagram_access_token = access_token

                # First, try to get token info to understand what type of token we have
                token_info_url = (
                    f"https://graph.facebook.com/v22.0/me?access_token={access_token}"
                )
                token_response = requests.get(token_info_url)

                if token_response.status_code != 200:
                    # If basic token info fails, try with debug_token
                    debug_url = f"https://graph.facebook.com/v22.0/debug_token?input_token={access_token}&access_token={access_token}"
                    debug_response = requests.get(debug_url)

                    if debug_response.status_code != 200:
                        raise Exception(
                            f"Token validation failed. Status: {token_response.status_code}, Response: {token_response.text}"
                        )

                # Method 1: Try to get Instagram Business Account directly from user
                logging.info("Attempting to get Instagram Business Account directly...")
                direct_url = f"https://graph.facebook.com/v22.0/me?fields=instagram_business_account&access_token={access_token}"
                direct_response = requests.get(direct_url)

                instagram_business_account_id = None

                if direct_response.status_code == 200:
                    direct_data = direct_response.json()
                    if "instagram_business_account" in direct_data:
                        instagram_business_account_id = direct_data[
                            "instagram_business_account"
                        ]["id"]
                        logging.info(
                            f"Found Instagram Business Account directly: {instagram_business_account_id}"
                        )

                # Method 2: If direct method fails, try through pages
                if not instagram_business_account_id:
                    logging.info(
                        "Attempting to find Instagram Business Account through Facebook Pages..."
                    )
                    pages_url = f"https://graph.facebook.com/v22.0/me/accounts?access_token={access_token}"
                    pages_response = requests.get(pages_url)

                    if pages_response.status_code == 200:
                        pages_data = pages_response.json()

                        # Look for Instagram Business Account connected to any of the pages
                        for page in pages_data.get("data", []):
                            page_id = page["id"]
                            page_access_token = page["access_token"]

                            # Check if this page has an Instagram Business Account
                            ig_url = f"https://graph.facebook.com/v22.0/{page_id}?fields=instagram_business_account&access_token={page_access_token}"
                            ig_response = requests.get(ig_url)

                            if ig_response.status_code == 200:
                                ig_data = ig_response.json()
                                if "instagram_business_account" in ig_data:
                                    instagram_business_account_id = ig_data[
                                        "instagram_business_account"
                                    ]["id"]
                                    # Update access token to use the page token for Instagram operations
                                    self.instagram_access_token = page_access_token
                                    logging.info(
                                        f"Found Instagram Business Account through page {page_id}: {instagram_business_account_id}"
                                    )
                                    break
                    else:
                        logging.warning(
                            f"Failed to get pages: {pages_response.status_code} - {pages_response.text}"
                        )

                # Method 3: Try alternative Instagram Graph API endpoint
                if not instagram_business_account_id:
                    logging.info("Attempting to use Instagram Graph API directly...")
                    ig_direct_url = f"https://graph.instagram.com/me?fields=id,username&access_token={access_token}"
                    ig_direct_response = requests.get(ig_direct_url)

                    if ig_direct_response.status_code == 200:
                        ig_direct_data = ig_direct_response.json()
                        instagram_business_account_id = ig_direct_data.get("id")
                        if instagram_business_account_id:
                            logging.info(
                                f"Found Instagram account via Instagram Graph API: {instagram_business_account_id}"
                            )
                    else:
                        logging.warning(
                            f"Instagram Graph API failed: {ig_direct_response.status_code} - {ig_direct_response.text}"
                        )

                if not instagram_business_account_id:
                    raise Exception(
                        "Could not find Instagram Business Account. Please ensure:\n"
                        "1. Your Instagram account is converted to a Business account\n"
                        "2. Your Instagram account is connected to a Facebook Page\n"
                        "3. Your access token has the required permissions (instagram_basic, instagram_content_publish)\n"
                        "4. Your app has been approved for Instagram permissions"
                    )

                self.instagram_business_account_id = instagram_business_account_id

                # Test the connection by getting account info
                test_endpoints = [
                    f"https://graph.facebook.com/v22.0/{instagram_business_account_id}?fields=id,username&access_token={self.instagram_access_token}",
                    f"https://graph.instagram.com/{instagram_business_account_id}?fields=id,username&access_token={self.instagram_access_token}",
                ]

                account_info = None
                for test_url in test_endpoints:
                    test_response = requests.get(test_url)
                    if test_response.status_code == 200:
                        account_info = test_response.json()
                        break
                    else:
                        logging.warning(
                            f"Test endpoint failed: {test_url} - {test_response.status_code}"
                        )

                if not account_info:
                    raise Exception(
                        "Could not verify Instagram account access with the provided token"
                    )

                username = account_info.get("username", "Unknown")

                logging.info(
                    f"Successfully authenticated with Instagram Business Account: @{username} (ID: {instagram_business_account_id})"
                )
                self.instagram_authenticated = True
                return True

            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                if hasattr(e, "response") and e.response is not None:
                    try:
                        error_detail = e.response.json()
                        error_msg = f"{error_msg} - Details: {error_detail}"
                    except:
                        error_msg = f"{error_msg} - Response: {e.response.text}"

                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Instagram after {max_retries} attempts: {error_msg}"
                    )
                    self.instagram_authenticated = False
                    return False
                logging.warning(
                    f"Instagram authentication attempt {attempt + 1} failed, retrying... Error: {error_msg}"
                )
                time.sleep(2**attempt)
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Instagram after {max_retries} attempts: {str(e)}"
                    )
                    self.instagram_authenticated = False
                    return False
                logging.warning(
                    f"Instagram authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

    def _validate_facebook_token(self, access_token):
        """
        Validate and get information about the Facebook access token
        """
        try:
            # Get token info
            debug_url = f"https://graph.facebook.com/v22.0/debug_token"
            debug_params = {"input_token": access_token, "access_token": access_token}

            debug_response = requests.get(debug_url, params=debug_params)
            if debug_response.status_code == 200:
                debug_data = debug_response.json()
                token_data = debug_data.get("data", {})

                token_type = token_data.get("type", "unknown")
                app_id = token_data.get("app_id", "unknown")
                is_valid = token_data.get("is_valid", False)
                scopes = token_data.get("scopes", [])

                logging.info(
                    f"Token type: {token_type}, App ID: {app_id}, Valid: {is_valid}"
                )
                logging.info(f"Token scopes: {', '.join(scopes) if scopes else 'None'}")

                if not is_valid:
                    logging.error("Token is not valid")
                    return False

                # Check for required permissions
                required_permissions = ["pages_manage_posts", "pages_read_engagement"]
                missing_permissions = [
                    perm for perm in required_permissions if perm not in scopes
                ]

                if missing_permissions:
                    logging.warning(
                        f"Missing recommended permissions: {', '.join(missing_permissions)}"
                    )

                return True
            else:
                logging.warning(
                    f"Could not validate token: {debug_response.status_code}"
                )
                return True  # Continue anyway

        except Exception as e:
            logging.warning(f"Token validation failed: {str(e)}")
            return True  # Continue anyway

    def authenticate_facebook(self, page_id, page_access_token, max_retries=3):
        """
        Authenticate with Facebook Page API
        """
        # First validate the token
        logging.info("Validating Facebook access token...")
        if not self._validate_facebook_token(page_access_token):
            logging.error("Token validation failed")
            return False

        for attempt in range(max_retries):
            try:
                self.facebook_page_id = page_id
                self.facebook_page_access_token = page_access_token

                # Try different API approaches
                test_endpoints = [
                    # Method 1: Direct page access
                    {
                        "url": f"https://graph.facebook.com/v22.0/{page_id}",
                        "params": {
                            "fields": "id,name",
                            "access_token": page_access_token,
                        },
                    },
                    # Method 2: Through me/accounts if it's a user token
                    {
                        "url": f"https://graph.facebook.com/v22.0/me/accounts",
                        "params": {"access_token": page_access_token},
                    },
                ]

                page_data = None
                successful_method = None

                for i, endpoint in enumerate(test_endpoints, 1):
                    try:
                        response = requests.get(
                            endpoint["url"], params=endpoint["params"]
                        )

                        if response.status_code == 200:
                            data = response.json()

                            if i == 1:  # Direct page access
                                if "id" in data and data["id"] == page_id:
                                    page_data = data
                                    successful_method = f"Direct page access"
                                    break
                            else:  # Through me/accounts
                                pages = data.get("data", [])
                                for page in pages:
                                    if page["id"] == page_id:
                                        page_data = page
                                        # Update token to use page-specific token
                                        self.facebook_page_access_token = page.get(
                                            "access_token", page_access_token
                                        )
                                        successful_method = f"Via me/accounts"
                                        break
                                if page_data:
                                    break
                        else:
                            logging.debug(
                                f"Method {i} failed: {response.status_code} - {response.text[:200]}"
                            )

                    except Exception as e:
                        logging.debug(f"Method {i} error: {str(e)}")
                        continue

                if not page_data:
                    # Get detailed error from the first method
                    response = requests.get(
                        test_endpoints[0]["url"], params=test_endpoints[0]["params"]
                    )
                    if response.status_code != 200:
                        try:
                            error_data = response.json()
                            error_msg = error_data.get("error", {})
                            error_message = error_msg.get("message", "Unknown error")
                            error_code = error_msg.get("code", "Unknown code")

                            if error_code == 190:
                                raise Exception(
                                    f"Invalid access token (Code: {error_code}). Please regenerate your Facebook Page access token."
                                )
                            elif error_code == 100:
                                raise Exception(
                                    f"Invalid page ID (Code: {error_code}). Please check your Facebook Page ID: {page_id}"
                                )
                            else:
                                raise Exception(
                                    f"Facebook API Error - Code: {error_code}, Message: {error_message}"
                                )
                        except ValueError:
                            raise Exception(
                                f"HTTP {response.status_code}: {response.text}"
                            )
                    else:
                        raise Exception(
                            "Could not find or access the specified Facebook page"
                        )

                page_name = page_data.get("name", "Unknown")
                logging.info(
                    f"Successfully authenticated with Facebook Page: {page_name} (ID: {page_id}) using {successful_method}"
                )
                self.facebook_authenticated = True
                return True

            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(
                        f"Failed to authenticate with Facebook after {max_retries} attempts: {str(e)}"
                    )
                    self.facebook_authenticated = False
                    return False
                logging.warning(
                    f"Facebook authentication attempt {attempt + 1} failed, retrying... Error: {str(e)}"
                )
                time.sleep(2**attempt)
        return False

    def post_to_facebook(self, image_paths, doc_url, doc_info=None):
        """
        Post to Facebook Page
        """
        if not self.facebook_authenticated:
            logging.warning("Skipping Facebook post - not authenticated")
            return False

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)

            max_title_length = 1500
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"

            # Create message with proper Facebook formatting
            message = f"{doc_title}\n\nPublished: {pub_date}\n\n{all_hashtags}"

            # Facebook has a character limit, add URL if it fits
            if (
                len(message) + len(doc_url) + 10 <= 8000
            ):  # Facebook has ~8000 char limit
                message += f"\n\nSource: {doc_url}"

            # Process images in chunks (Facebook allows up to 10 images per post)
            for i in range(0, len(image_paths), 10):
                chunk = image_paths[i : i + 10]

                try:
                    # Add delay between posts to respect rate limits
                    if i > 0:
                        time.sleep(3)

                    # Determine message for this chunk
                    chunk_message = (
                        message
                        if i == 0
                        else f"{doc_title} (Part {i//10 + 1})\n\n{all_hashtags}"
                    )

                    if len(chunk) == 1:
                        # Single image post
                        success = self._create_facebook_single_post(
                            chunk[0], chunk_message
                        )
                    else:
                        # Multiple images post
                        success = self._create_facebook_multi_post(chunk, chunk_message)

                    if success:
                        logging.info(f"Successfully posted Facebook chunk {i//10 + 1}")
                    else:
                        logging.error(f"Failed to post Facebook chunk {i//10 + 1}")
                        return False

                except Exception as e:
                    logging.error(
                        f"Failed to process Facebook chunk {i//10 + 1}: {str(e)}"
                    )
                    return False

            logging.info("Successfully posted to Facebook")
            return True

        except Exception as e:
            logging.error(f"Failed to post to Facebook: {str(e)}")
            return False

    def _create_facebook_single_post(self, image_path, message):
        """Create a single image post on Facebook Page by uploading image directly"""
        try:
            url = f"https://graph.facebook.com/v22.0/{self.facebook_page_id}/photos"

            # Upload image directly to Facebook
            with open(image_path, "rb") as image_file:
                files = {"source": image_file}
                data = {
                    "message": message,
                    "access_token": self.facebook_page_access_token,
                }

                response = requests.post(url, files=files, data=data)
                response.raise_for_status()

                result = response.json()
                if "id" in result:
                    logging.info(f"Created Facebook photo post: {result['id']}")
                    return True
                else:
                    logging.error(f"Facebook post creation failed: {result}")
                    return False

        except Exception as e:
            logging.error(f"Failed to create Facebook single post: {str(e)}")
            return False

    def _create_facebook_multi_post(self, image_paths, message):
        """Create a multi-image post on Facebook Page by uploading all images directly"""
        try:
            # First, upload all images and get their IDs
            photo_ids = []

            for i, image_path in enumerate(image_paths):
                try:
                    # Upload each image without publishing
                    url = f"https://graph.facebook.com/v22.0/{self.facebook_page_id}/photos"

                    with open(image_path, "rb") as image_file:
                        files = {"source": image_file}
                        data = {
                            "published": "false",  # Don't publish yet
                            "access_token": self.facebook_page_access_token,
                        }

                        response = requests.post(url, files=files, data=data)
                        response.raise_for_status()

                        result = response.json()
                        if "id" in result:
                            photo_ids.append(result["id"])
                            logging.info(f"Uploaded image {i+1}: {result['id']}")
                        else:
                            logging.warning(f"Failed to upload image {i+1}: {result}")

                except Exception as e:
                    logging.warning(f"Failed to upload image {i+1}: {str(e)}")
                    continue

            if not photo_ids:
                logging.error(
                    "No images were successfully uploaded for Facebook multi-post"
                )
                return False

            # Create the multi-photo post
            url = f"https://graph.facebook.com/v22.0/{self.facebook_page_id}/feed"

            # Prepare attached_media for multi-photo post
            attached_media = []
            for photo_id in photo_ids:
                attached_media.append({"media_fbid": photo_id})

            data = {
                "message": message,
                "attached_media": attached_media,
                "access_token": self.facebook_page_access_token,
            }

            response = requests.post(url, json=data)
            response.raise_for_status()

            result = response.json()
            if "id" in result:
                logging.info(
                    f"Created Facebook multi-image post: {result['id']} with {len(photo_ids)} images"
                )
                return True
            else:
                logging.error(f"Facebook multi-post creation failed: {result}")
                return False

        except Exception as e:
            logging.error(f"Failed to create Facebook multi-post: {str(e)}")
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

    def _prepare_images_for_instagram(self, image_paths):
        """
        Prepare images specifically for Instagram by rotating landscape pages to portrait
        Skip the first page as it's always in portrait mode and doesn't have cropping issues
        """
        instagram_image_paths = []

        for i, image_path in enumerate(image_paths):
            try:
                # Load the image
                with Image.open(image_path) as img:
                    # Skip rotation for the first page (index 0) as it's always portrait
                    if i == 0:
                        # Just copy the first page as-is
                        instagram_path = os.path.join(
                            self.download_dir, f"instagram_page_{i}.jpg"
                        )
                        img.save(instagram_path, "JPEG", quality=95)
                        instagram_image_paths.append(instagram_path)
                        logging.info(f"Kept first page as portrait: {instagram_path}")
                    else:
                        # Check if the image is in landscape mode (width > height)
                        width, height = img.size

                        if width > height:
                            # Rotate landscape image 90 degrees clockwise to make it portrait
                            rotated_img = img.rotate(-90, expand=True)
                            instagram_path = os.path.join(
                                self.download_dir, f"instagram_page_{i}.jpg"
                            )
                            rotated_img.save(instagram_path, "JPEG", quality=95)
                            instagram_image_paths.append(instagram_path)
                            logging.info(
                                f"Rotated landscape page {i} to portrait: {instagram_path}"
                            )
                        else:
                            # Image is already in portrait mode, just copy it
                            instagram_path = os.path.join(
                                self.download_dir, f"instagram_page_{i}.jpg"
                            )
                            img.save(instagram_path, "JPEG", quality=95)
                            instagram_image_paths.append(instagram_path)
                            logging.info(
                                f"Kept portrait page {i} as-is: {instagram_path}"
                            )

            except Exception as e:
                logging.error(
                    f"Error processing image {image_path} for Instagram: {str(e)}"
                )
                # If there's an error, use the original image
                instagram_image_paths.append(image_path)

        return instagram_image_paths

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
            "2024-03-07": "#CanadianGP",
            "2024-03-21": "#AustralianGP",
            "2024-04-04": "#JapaneseGP",
            "2024-04-18": "#ChineseGP",
            "2024-05-02": "#CanadianGP",
            "2024-05-16": "#EmiliaRomagnaGP",
            "2024-05-23": "#CanadianGP",
            "2024-06-06": "#CanadianGP",
            "2024-06-20": "#CanadianGP",
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
            all_tags = ["f1", "formula1", "fia", "CanadianGP"]
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

            max_title_length = 200
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"
            formatted_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

            # Threads has a 500 character limit
            if len(formatted_text) + len(doc_url) + 1 <= 480:
                formatted_text += f"\n\n{doc_url}"

            # Process images in chunks (Threads allows up to 10 images per post)
            root_post_id = None
            parent_post_id = None

            # Add rate limiting to avoid hitting API limits
            import time

            for i in range(0, len(image_paths), 10):
                chunk = image_paths[i : i + 10]

                try:
                    # Add delay between chunks to respect rate limits
                    if i > 0:
                        time.sleep(2)

                    # Determine post text
                    post_text = (
                        formatted_text
                        if i == 0
                        else f"Continued... ({i//10 + 1}/{(len(image_paths) + 9)//10})"
                    )

                    # Create container based on number of images
                    if len(chunk) == 1:
                        container_id = self._create_threads_image_container(
                            chunk[0], post_text, parent_post_id
                        )
                    else:
                        container_id = self._create_threads_carousel_container(
                            chunk, post_text, parent_post_id
                        )

                    if container_id:
                        # Wait a moment before publishing
                        time.sleep(1)

                        # Publish the container
                        post_id = self._publish_threads_container(container_id)
                        if post_id:
                            if not root_post_id:
                                root_post_id = post_id
                            parent_post_id = post_id
                            logging.info(
                                f"Successfully posted chunk {i//10 + 1} to Threads"
                            )
                        else:
                            logging.error(
                                f"Failed to publish Threads container {container_id}"
                            )
                            return False
                    else:
                        logging.error(
                            f"Failed to create Threads container for chunk {i//10 + 1}"
                        )
                        return False

                except Exception as e:
                    logging.error(
                        f"Failed to process Threads chunk {i//10 + 1}: {str(e)}"
                    )
                    return False

            logging.info("Successfully posted to Threads")
            return True

        except Exception as e:
            logging.error(f"Failed to post to Threads: {str(e)}")
            return False

    def post_to_instagram(self, image_paths, doc_url, doc_info=None):
        """
        Post to Instagram using the Instagram Basic Display API
        Instagram supports carousel posts (up to 10 images) and single image posts
        """
        if not self.instagram_authenticated:
            logging.warning("Skipping Instagram post - not authenticated")
            return False

        try:
            # Prepare images specifically for Instagram (rotate landscape pages to portrait)
            instagram_image_paths = self._prepare_images_for_instagram(image_paths)

            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)

            max_title_length = 1500  # Instagram captions should be concise
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            # Instagram hashtags work differently - they should be at the end
            all_hashtags = f"{GLOBAL_HASHTAGS}"

            # Create caption with proper Instagram formatting
            caption = f"{doc_title}\n\nPublished: {pub_date}\n\n{all_hashtags}"

            # Instagram has a 2200 character limit for captions
            if len(caption) + len(doc_url) + 10 <= 2150:  # Leave some buffer
                caption += f"\n\nSource: {doc_url}"

            # Process images in chunks (Instagram allows up to 10 images per carousel)
            for i in range(0, len(instagram_image_paths), 10):
                chunk = instagram_image_paths[i : i + 10]

                try:
                    # Add delay between posts to respect rate limits
                    if i > 0:
                        time.sleep(5)  # Instagram has stricter rate limits

                    # Determine caption for this chunk
                    chunk_caption = (
                        caption
                        if i == 0
                        else f"{doc_title} (Part {i//10 + 1})\n\n{all_hashtags}"
                    )

                    if len(chunk) == 1:
                        # Single image post
                        success = self._create_instagram_single_post(
                            chunk[0], chunk_caption
                        )
                    else:
                        # Carousel post
                        success = self._create_instagram_carousel_post(
                            chunk, chunk_caption
                        )

                    if success:
                        logging.info(f"Successfully posted Instagram chunk {i//10 + 1}")
                    else:
                        logging.error(f"Failed to post Instagram chunk {i//10 + 1}")
                        return False

                except Exception as e:
                    logging.error(
                        f"Failed to process Instagram chunk {i//10 + 1}: {str(e)}"
                    )
                    return False

            # Clean up Instagram-specific image files
            for img_path in instagram_image_paths:
                if img_path.startswith(
                    os.path.join(self.download_dir, "instagram_page_")
                ):
                    try:
                        os.remove(img_path)
                        logging.debug(f"Cleaned up Instagram image: {img_path}")
                    except Exception as e:
                        logging.warning(
                            f"Failed to clean up Instagram image {img_path}: {str(e)}"
                        )

            logging.info("Successfully posted to Instagram")
            return True

        except Exception as e:
            logging.error(f"Failed to post to Instagram: {str(e)}")
            return False

    def _create_instagram_single_post(self, image_path, caption):
        """Create a single image post on Instagram"""
        try:
            # Step 1: Upload image to get a public URL (using Imgur)
            image_url = self._upload_image_to_public_url(image_path)
            if not image_url:
                logging.error("Failed to upload image to public URL for Instagram")
                return False

            # Step 2: Create media container - try different API endpoints
            api_endpoints = [
                f"https://graph.facebook.com/v22.0/{self.instagram_business_account_id}/media",
                f"https://graph.instagram.com/{self.instagram_business_account_id}/media",
            ]

            container_data = {
                "image_url": image_url,
                "caption": caption,
                "access_token": self.instagram_access_token,
            }

            creation_id = None
            for endpoint in api_endpoints:
                try:
                    container_response = requests.post(endpoint, data=container_data)
                    if container_response.status_code == 200:
                        container_result = container_response.json()
                        if "id" in container_result:
                            creation_id = container_result["id"]
                            logging.info(
                                f"Created Instagram media container: {creation_id} via {endpoint}"
                            )
                            break
                    else:
                        logging.warning(
                            f"Endpoint {endpoint} failed: {container_response.status_code} - {container_response.text}"
                        )
                except Exception as e:
                    logging.warning(f"Endpoint {endpoint} error: {str(e)}")
                    continue

            if not creation_id:
                logging.error(
                    "Failed to create Instagram media container with any endpoint"
                )
                return False

            # Step 3: Wait for media to be processed
            time.sleep(3)

            # Step 4: Publish the media using shared method
            return self._publish_instagram_media(creation_id)

        except Exception as e:
            logging.error(f"Failed to create Instagram single post: {str(e)}")
            return False

    def _create_instagram_carousel_post(self, image_paths, caption):
        """Create a carousel post on Instagram with multiple images"""
        try:
            # Step 1: Create media containers for each image
            media_ids = []

            # Define API endpoints to try
            media_endpoints = [
                f"https://graph.facebook.com/v22.0/{self.instagram_business_account_id}/media",
                f"https://graph.instagram.com/{self.instagram_business_account_id}/media",
            ]

            for i, image_path in enumerate(image_paths):
                # Upload image to get public URL
                image_url = self._upload_image_to_public_url(image_path)
                if not image_url:
                    logging.warning(f"Skipping image {image_path} - upload failed")
                    continue

                # Create individual media container for carousel item
                container_data = {
                    "image_url": image_url,
                    "is_carousel_item": "true",
                    "access_token": self.instagram_access_token,
                }

                # Try different endpoints for creating carousel items
                item_created = False
                for endpoint in media_endpoints:
                    try:
                        container_response = requests.post(
                            endpoint, data=container_data
                        )
                        if container_response.status_code == 200:
                            container_result = container_response.json()
                            if "id" in container_result:
                                media_ids.append(container_result["id"])
                                logging.info(
                                    f"Created carousel item {i+1}: {container_result['id']} via {endpoint}"
                                )
                                item_created = True
                                break
                        else:
                            logging.warning(
                                f"Carousel item endpoint {endpoint} failed: {container_response.status_code} - {container_response.text}"
                            )
                    except Exception as e:
                        logging.warning(
                            f"Carousel item endpoint {endpoint} error: {str(e)}"
                        )
                        continue

                if not item_created:
                    logging.warning(
                        f"Failed to create carousel item {i+1} with any endpoint"
                    )

                # Small delay between uploads
                time.sleep(1)

            if not media_ids:
                logging.error("No media items were successfully created for carousel")
                return False

            if len(media_ids) < 2:
                logging.warning(
                    f"Only {len(media_ids)} media items created. Instagram carousels require at least 2 items. Converting to single post."
                )
                # If we only have one item, convert to single post
                if len(media_ids) == 1:
                    return self._publish_single_instagram_media(media_ids[0], caption)
                else:
                    return False

            # Step 2: Create carousel container
            carousel_data = {
                "media_type": "CAROUSEL",
                "children": ",".join(media_ids),
                "caption": caption,
                "access_token": self.instagram_access_token,
            }

            # Try different endpoints for creating carousel container
            creation_id = None
            for endpoint in media_endpoints:
                try:
                    carousel_response = requests.post(endpoint, data=carousel_data)
                    if carousel_response.status_code == 200:
                        carousel_result = carousel_response.json()
                        if "id" in carousel_result:
                            creation_id = carousel_result["id"]
                            logging.info(
                                f"Created Instagram carousel container: {creation_id} via {endpoint}"
                            )
                            break
                    else:
                        logging.warning(
                            f"Carousel container endpoint {endpoint} failed: {carousel_response.status_code} - {carousel_response.text}"
                        )
                except Exception as e:
                    logging.warning(
                        f"Carousel container endpoint {endpoint} error: {str(e)}"
                    )
                    continue

            if not creation_id:
                logging.error(
                    "Failed to create Instagram carousel container with any endpoint"
                )
                return False

            # Step 3: Wait for media to be processed
            time.sleep(5)  # Carousel posts need more time to process

            # Step 4: Publish the carousel
            return self._publish_instagram_media(creation_id)

        except Exception as e:
            logging.error(f"Failed to create Instagram carousel post: {str(e)}")
            return False

    def _publish_instagram_media(self, creation_id):
        """Publish Instagram media (single or carousel) with multiple endpoint fallback"""
        try:
            # Try different endpoints for publishing
            publish_endpoints = [
                f"https://graph.facebook.com/v22.0/{self.instagram_business_account_id}/media_publish",
                f"https://graph.instagram.com/{self.instagram_business_account_id}/media_publish",
            ]

            publish_data = {
                "creation_id": creation_id,
                "access_token": self.instagram_access_token,
            }

            for endpoint in publish_endpoints:
                try:
                    publish_response = requests.post(endpoint, data=publish_data)
                    if publish_response.status_code == 200:
                        publish_result = publish_response.json()
                        if "id" in publish_result:
                            logging.info(
                                f"Successfully published Instagram media: {publish_result['id']} via {endpoint}"
                            )
                            return True
                    else:
                        logging.warning(
                            f"Publish endpoint {endpoint} failed: {publish_response.status_code} - {publish_response.text}"
                        )
                except Exception as e:
                    logging.warning(f"Publish endpoint {endpoint} error: {str(e)}")
                    continue

            logging.error(
                f"Failed to publish Instagram media {creation_id} with any endpoint"
            )
            return False

        except Exception as e:
            logging.error(f"Failed to publish Instagram media {creation_id}: {str(e)}")
            return False

    def _publish_single_instagram_media(self, media_id, caption):
        """Publish a single Instagram media item that was created as a carousel item"""
        try:
            # For single items that were created as carousel items, we need to create a new container
            # because carousel items can't be published directly

            # Get the image URL from the carousel item (this is a workaround)
            # Instead, we'll create a new single media container
            logging.info(f"Converting carousel item {media_id} to single post")

            # We can't easily convert a carousel item to a single post, so we'll return False
            # and let the calling function handle this case differently
            logging.warning("Cannot convert carousel item to single post - skipping")
            return False

        except Exception as e:
            logging.error(f"Failed to publish single Instagram media: {str(e)}")
            return False

    def _create_threads_image_container(self, image_path, text, reply_to_id=None):
        """Create a single image container for Threads"""
        try:
            # Upload image to Imgur
            image_url = self._upload_image_to_public_url(image_path)
            if not image_url:
                logging.error("Failed to upload image to public URL")
                return None

            # Create container
            url = f"https://graph.threads.net/v1.0/{self.threads_user_id}/threads"

            data = {
                "media_type": "IMAGE",
                "image_url": image_url,
                "text": text,
                "access_token": self.threads_access_token,
            }

            if reply_to_id:
                data["reply_to_id"] = reply_to_id

            response = requests.post(url, data=data)
            response.raise_for_status()

            result = response.json()
            container_id = result.get("id")

            if container_id:
                logging.info(f"Created Threads image container: {container_id}")

            return container_id

        except Exception as e:
            logging.error(f"Failed to create Threads image container: {str(e)}")
            return None

    def _create_threads_carousel_container(self, image_paths, text, reply_to_id=None):
        """Create a carousel container for Threads with multiple images"""
        try:
            # First, create individual media containers for each image
            children_ids = []

            for i, image_path in enumerate(image_paths):
                image_url = self._upload_image_to_public_url(image_path)
                if not image_url:
                    logging.warning(f"Skipping image {image_path} - upload failed")
                    continue

                # Create individual media container
                url = f"https://graph.threads.net/v1.0/{self.threads_user_id}/threads"
                data = {
                    "media_type": "IMAGE",
                    "image_url": image_url,
                    "is_carousel_item": "true",
                    "access_token": self.threads_access_token,
                }

                response = requests.post(url, data=data)
                response.raise_for_status()

                result = response.json()
                child_id = result.get("id")
                if child_id:
                    children_ids.append(child_id)
                    logging.info(f"Created carousel item {i+1}: {child_id}")

            if not children_ids:
                logging.error("No images were successfully uploaded for carousel")
                return None

            # Create carousel container
            url = f"https://graph.threads.net/v1.0/{self.threads_user_id}/threads"
            data = {
                "media_type": "CAROUSEL",
                "children": ",".join(children_ids),
                "text": text,
                "access_token": self.threads_access_token,
            }

            if reply_to_id:
                data["reply_to_id"] = reply_to_id

            response = requests.post(url, data=data)
            response.raise_for_status()

            result = response.json()
            container_id = result.get("id")

            if container_id:
                logging.info(f"Created Threads carousel container: {container_id}")

            return container_id

        except Exception as e:
            logging.error(f"Failed to create Threads carousel container: {str(e)}")
            return None

    def _publish_threads_container(self, container_id):
        """Publish a Threads container"""
        try:
            url = (
                f"https://graph.threads.net/v1.0/{self.threads_user_id}/threads_publish"
            )
            data = {
                "creation_id": container_id,
                "access_token": self.threads_access_token,
            }

            response = requests.post(url, data=data)
            response.raise_for_status()

            result = response.json()
            post_id = result.get("id")

            if post_id:
                logging.info(f"Published Threads post: {post_id}")

            return post_id

        except Exception as e:
            logging.error(
                f"Failed to publish Threads container {container_id}: {str(e)}"
            )
            return None

    def _upload_image_to_public_url(self, image_path):
        """
        Upload image to Imgur and return public URL
        Uses Imgur's free anonymous upload API
        """
        try:
            # Imgur anonymous upload endpoint
            url = "https://api.imgur.com/3/image"

            # You can use this anonymous client ID for testing
            # For production, get your own from https://api.imgur.com/oauth2/addclient
            client_id = "546c25a59c58ad7"  # Anonymous client ID

            headers = {"Authorization": f"Client-ID {client_id}"}

            # Read and encode image
            with open(image_path, "rb") as f:
                image_data = f.read()

            # Prepare data for upload
            files = {"image": image_data}

            data = {
                "type": "file",
                "title": f"FIA Document - {os.path.basename(image_path)}",
                "description": "Uploaded via FIA Document Handler",
            }

            # Upload to Imgur
            response = requests.post(url, headers=headers, files=files, data=data)
            response.raise_for_status()

            result = response.json()

            if result.get("success"):
                image_url = result["data"]["link"]
                logging.info(f"Successfully uploaded image to Imgur: {image_url}")
                return image_url
            else:
                logging.error(f"Imgur upload failed: {result}")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to upload image to Imgur: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error uploading to Imgur: {str(e)}")
            return None

    def post_to_pixelfed(self, image_paths, doc_url, doc_info=None):
        """
        Post to Pixelfed - optimized for visual content
        Pixelfed supports up to 20 images per post, 2,000 character limit, and doesn't have threading
        """
        if not self.pixelfed_authenticated:
            logging.warning("Skipping Pixelfed post - not authenticated")
            return False

        try:
            doc_title, pub_date = self._parse_document_info(doc_url, doc_info)

            max_title_length = 1500  # Leave room for other content
            if len(doc_title) > max_title_length:
                doc_title = doc_title[: max_title_length - 3] + "..."

            all_hashtags = f"{GLOBAL_HASHTAGS}"
            base_text = f"{doc_title}\nPublished on {pub_date}\n\n{all_hashtags}"

            # Pixelfed has a 2,000 character limit
            if len(base_text) + len(doc_url) + 10 <= 1950:  # Leave some buffer
                base_text += f"\n\nSource: {doc_url}"

            # Process images in chunks of 20 (Pixelfed's limit)
            total_chunks = (len(image_paths) + 19) // 20

            for chunk_index in range(0, len(image_paths), 20):
                chunk = image_paths[chunk_index : chunk_index + 20]
                chunk_number = (chunk_index // 20) + 1

                # Upload images to Pixelfed
                media_ids = []
                for i, img_path in enumerate(chunk):
                    try:
                        with open(img_path, "rb") as f:
                            # Explicitly specify MIME type for JPEG images
                            media = self.pixelfed_client.media_post(
                                f, mime_type="image/jpeg", description=doc_title
                            )
                            media_ids.append(media["id"])
                    except Exception as e:
                        logging.warning(
                            f"Failed to upload image {img_path} to Pixelfed: {str(e)}"
                        )
                        continue

                # Create post text for this chunk
                if total_chunks > 1:
                    chunk_text = f"{base_text}\n\nPart {chunk_number}/{total_chunks}"
                else:
                    chunk_text = base_text

                # Ensure we don't exceed 2,000 character limit
                if len(chunk_text) > 2000:
                    # Truncate the title if needed
                    excess = len(chunk_text) - 2000
                    title_reduction = excess + 50  # Extra buffer

                    if len(doc_title) > title_reduction:
                        truncated_title = (
                            doc_title[: len(doc_title) - title_reduction - 3] + "..."
                        )
                        chunk_text = f"{truncated_title}\nPublished on {pub_date}\n\n{all_hashtags}"

                        if total_chunks > 1:
                            chunk_text += f"\n\nPart {chunk_number}/{total_chunks}"

                        # Try to add URL if it fits
                        if len(chunk_text) + len(doc_url) + 10 <= 2000:
                            chunk_text += f"\n\nSource: {doc_url}"

                # Post the chunk
                if media_ids:
                    status = self.pixelfed_client.status_post(
                        status=chunk_text, media_ids=media_ids
                    )
                    logging.info(
                        f"Successfully posted Pixelfed chunk {chunk_number}/{total_chunks} with {len(media_ids)} images"
                    )
                else:
                    logging.warning(
                        f"No images uploaded for Pixelfed chunk {chunk_number}"
                    )
                    if (
                        chunk_index == 0
                    ):  # Only post text for the first chunk if no images
                        self.pixelfed_client.status_post(status=chunk_text)
                        logging.info("Successfully posted to Pixelfed (text only)")

                # Add delay between chunks to respect rate limits
                if chunk_index + 20 < len(image_paths):
                    time.sleep(3)

            logging.info("Successfully completed Pixelfed posting")
            return True

        except Exception as e:
            logging.error(f"Failed to post to Pixelfed: {str(e)}")
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

        # Post to Instagram
        if self.instagram_authenticated:
            try:
                results["instagram"] = self.post_to_instagram(
                    image_paths, doc_url, doc_info
                )
            except Exception as e:
                logging.error(f"Unexpected error posting to Instagram: {str(e)}")
                results["instagram"] = False
        else:
            results["instagram"] = False

        if self.facebook_authenticated:
            try:
                results["facebook"] = self.post_to_facebook(
                    image_paths, doc_url, doc_info
                )
            except Exception as e:
                logging.error(f"Unexpected error posting to Facebook: {str(e)}")
                results["facebook"] = False
        else:
            results["facebook"] = False

        # Post to Pixelfed
        if self.pixelfed_authenticated:
            try:
                results["pixelfed"] = self.post_to_pixelfed(
                    image_paths, doc_url, doc_info
                )
            except Exception as e:
                logging.error(f"Unexpected error posting to Pixelfed: {str(e)}")
                results["pixelfed"] = False
        else:
            results["pixelfed"] = False

        # Post to Telegram
        if self.telegram_authenticated:
            try:
                results["telegram"] = self.post_to_telegram(
                    image_paths, doc_url, doc_info
                )
            except Exception as e:
                logging.error(f"Unexpected error posting to Telegram: {str(e)}")
                results["telegram"] = False
        else:
            results["telegram"] = False

        # Post to LinkedIn
        if self.linkedin_authenticated:
            try:
                results["linkedin"] = self.post_to_linkedin(
                    image_paths, doc_url, doc_info
                )
            except Exception as e:
                logging.error(f"Unexpected error posting to LinkedIn: {str(e)}")
                results["linkedin"] = False
        else:
            results["linkedin"] = False

        # Post to Tumblr
        if self.tumblr_authenticated:
            try:
                results["tumblr"] = self.post_to_tumblr(image_paths, doc_url, doc_info)
            except Exception as e:
                logging.error(f"Unexpected error posting to Tumblr: {str(e)}")
                results["tumblr"] = False
        else:
            results["tumblr"] = False

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
        # bluesky_password = os.environ.get("BLUESKY_USERNAME")
        bluesky_password = os.environ.get("BLUESKY_PASSWORD")

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

        # mastodon_access_token = os.environ.get("BLUESKY_USERNAME")
        mastodon_access_token = os.environ.get("MASTODON_ACCESS_TOKEN")

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

        # threads_app_id = os.environ.get("BLUESKY_USERNAME")
        # threads_app_secret = os.environ.get("BLUESKY_USERNAME")
        # threads_access_token = os.environ.get("BLUESKY_USERNAME")
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

    # Authenticate with Instagram
    try:
        # instagram_app_id = os.environ.get("BLUESKY_USERNAME")
        # instagram_app_secret = os.environ.get("BLUESKY_USERNAME")
        # instagram_access_token = os.environ.get("BLUESKY_USERNAME")
        instagram_app_id = os.environ.get("INSTAGRAM_APP_ID")
        instagram_app_secret = os.environ.get("INSTAGRAM_APP_SECRET")
        instagram_access_token = os.environ.get("FACEBOOK_PAGE_ACCESS_TOKEN")

        if instagram_app_id and instagram_app_secret and instagram_access_token:
            auth_results["instagram"] = handler.authenticate_instagram(
                instagram_app_id,
                instagram_app_secret,
                instagram_access_token,
                max_retries=3,
            )
        else:
            logging.warning("Instagram credentials not found in environment variables")
            auth_results["instagram"] = False
    except Exception as e:
        logging.error(f"Unexpected error during Instagram authentication: {str(e)}")
        auth_results["instagram"] = False

    try:
        facebook_page_id = os.environ.get("FACEBOOK_PAGE_ID")
        # facebook_page_access_token = os.environ.get("FACEBOOK_PAGE_ID")
        facebook_page_access_token = os.environ.get("FACEBOOK_PAGE_ACCESS_TOKEN")

        if facebook_page_id and facebook_page_access_token:
            auth_results["facebook"] = handler.authenticate_facebook(
                facebook_page_id, facebook_page_access_token, max_retries=3
            )
        else:
            logging.warning("Facebook credentials not found in environment variables")
            auth_results["facebook"] = False
    except Exception as e:
        logging.error(f"Unexpected error during Facebook authentication: {str(e)}")
        auth_results["facebook"] = False

    try:
        # pixelfed_access_token = os.environ.get("FACEBOOK_PAGE_ID")
        pixelfed_access_token = os.environ.get("PIXELFED_ACCESS_TOKEN")

        if pixelfed_access_token:
            auth_results["pixelfed"] = handler.authenticate_pixelfed(
                pixelfed_access_token, max_retries=3
            )
        else:
            logging.warning("Pixelfed access token not found in environment variables")
            auth_results["pixelfed"] = False
    except Exception as e:
        logging.error(f"Unexpected error during Pixelfed authentication: {str(e)}")
        auth_results["pixelfed"] = False

    try:
        # telegram_bot_token = os.environ.get("FACEBOOK_PAGE_ID")
        # telegram_channel_id = os.environ.get("FACEBOOK_PAGE_ID")
        telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        telegram_channel_id = os.environ.get("TELEGRAM_CHANNEL_ID")

        if telegram_bot_token and telegram_channel_id:
            auth_results["telegram"] = handler.authenticate_telegram(
                telegram_bot_token, telegram_channel_id, max_retries=3
            )
        else:
            logging.warning("Telegram credentials not found in environment variables")
            auth_results["telegram"] = False
    except Exception as e:
        logging.error(f"Unexpected error during Telegram authentication: {str(e)}")
        auth_results["telegram"] = False

    try:
        linkedin_access_token = os.environ.get("LINKEDIN_ACCESS_TOKEN")
        linkedin_organization_id = os.environ.get("LINKEDIN_ORGANIZATION_ID")

        if linkedin_access_token and linkedin_organization_id:
            auth_results["linkedin"] = handler.authenticate_linkedin(
                linkedin_access_token, linkedin_organization_id, max_retries=3
            )
        else:
            logging.warning("LinkedIn credentials not found in environment variables")
            auth_results["linkedin"] = False
    except Exception as e:
        logging.error(f"Unexpected error during LinkedIn authentication: {str(e)}")
        auth_results["linkedin"] = False

    # Authenticate with Tumblr
    try:
        tumblr_consumer_key = os.environ.get("TUMBLR_CONSUMER_KEY")
        tumblr_consumer_secret = os.environ.get("TUMBLR_CONSUMER_SECRET")
        tumblr_access_token = os.environ.get("TUMBLR_ACCESS_TOKEN")
        tumblr_access_token_secret = os.environ.get("TUMBLR_ACCESS_TOKEN_SECRET")
        tumblr_blog_name = os.environ.get("TUMBLR_BLOG_NAME")

        if all(
            [
                tumblr_consumer_key,
                tumblr_consumer_secret,
                tumblr_access_token,
                tumblr_access_token_secret,
                tumblr_blog_name,
            ]
        ):
            auth_results["tumblr"] = handler.authenticate_tumblr(
                tumblr_consumer_key,
                tumblr_consumer_secret,
                tumblr_access_token,
                tumblr_access_token_secret,
                tumblr_blog_name,
                max_retries=3,
            )
        else:
            logging.warning("Tumblr credentials not found in environment variables")
            auth_results["tumblr"] = False
    except Exception as e:
        logging.error(f"Unexpected error during Tumblr authentication: {str(e)}")
        auth_results["tumblr"] = False

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
