import os
import json
import base64
import requests
import time
import logging
from requests_oauthlib import OAuth1

class TwitterAPI:
    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        """
        Initialize the TwitterAPI with authentication credentials.

        Args:
            consumer_key (str): Your API/Consumer Key
            consumer_secret (str): Your API/Consumer Secret
            access_token (str): Your Access Token
            access_token_secret (str): Your Access Token Secret
        """
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        self.auth = OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)
        self.base_url = "https://api.twitter.com"

    def upload_image_chunked(self, image_path, media_category="tweet_image"):
        """
        Upload an image using the chunked media upload endpoint.

        Args:
            image_path (str): Path to the image file
            media_category (str): Media category (default: tweet_image)

        Returns:
            str: Media ID if successful, None otherwise
        """
        try:
            # Check if file exists
            if not os.path.exists(image_path):
                logging.error(f"Error: File {image_path} does not exist.")
                return None

            # Get file size
            file_size = os.path.getsize(image_path)
            logging.info(f"Uploading file {image_path} with size {file_size} bytes")

            # Step 1: INIT - Initialize the upload
            init_url = f"{self.base_url}/1.1/media/upload.json"
            init_params = {
                "command": "INIT",
                "total_bytes": file_size,
                "media_type": self._get_media_type(image_path),
                "media_category": media_category
            }

            logging.info(f"Initializing upload with params: {init_params}")
            init_response = requests.post(init_url, auth=self.auth, data=init_params)

            if init_response.status_code != 200:
                logging.error(f"Error initializing upload: Status {init_response.status_code}, Response: {init_response.text}")
                return None

            media_id = init_response.json()["media_id_string"]
            logging.info(f"Upload initialized with media_id: {media_id}")

            # Step 2: APPEND - Upload the file in chunks
            chunk_size = 4 * 1024 * 1024  # 4MB chunks
            segment_index = 0

            with open(image_path, "rb") as image_file:
                while True:
                    chunk = image_file.read(chunk_size)
                    if not chunk:
                        break

                    append_url = f"{self.base_url}/1.1/media/upload.json"
                    append_params = {
                        "command": "APPEND",
                        "media_id": media_id,
                        "segment_index": segment_index
                    }

                    files = {"media": chunk}
                    append_response = requests.post(append_url, auth=self.auth, data=append_params, files=files)

                    if append_response.status_code != 204:
                        logging.error(f"Error appending chunk {segment_index}: Status {append_response.status_code}, Response: {append_response.text}")
                        return None

                    segment_index += 1
                    logging.info(f"Uploaded chunk {segment_index} of {image_path}")

            # Step 3: FINALIZE - Complete the upload
            finalize_url = f"{self.base_url}/1.1/media/upload.json"
            finalize_params = {
                "command": "FINALIZE",
                "media_id": media_id
            }

            finalize_response = requests.post(finalize_url, auth=self.auth, data=finalize_params)

            if finalize_response.status_code != 200:
                logging.error(f"Error finalizing upload: Status {finalize_response.status_code}, Response: {finalize_response.text}")
                return None

            finalize_data = finalize_response.json()
            logging.info(f"Upload finalized: {finalize_data}")

            # Check if processing is needed
            if "processing_info" in finalize_data:
                media_id = self._wait_for_processing(media_id, finalize_data["processing_info"])

            logging.info(f"Successfully uploaded image {image_path} with media_id: {media_id}")
            return media_id

        except Exception as e:
            logging.error(f"Exception during image upload for {image_path}: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return None

    def _wait_for_processing(self, media_id, processing_info):
        """
        Wait for media processing to complete.

        Args:
            media_id (str): Media ID
            processing_info (dict): Processing info from FINALIZE response

        Returns:
            str: Media ID if successful, None otherwise
        """
        try:
            state = processing_info.get("state")
            logging.info(f"Media processing state: {state}")

            while state == "pending" or state == "in_progress":
                check_after_secs = processing_info.get("check_after_secs", 1)
                logging.info(f"Media processing in progress. Waiting {check_after_secs} seconds...")
                time.sleep(check_after_secs)

                # Check status
                status_url = f"{self.base_url}/1.1/media/upload.json"
                status_params = {
                    "command": "STATUS",
                    "media_id": media_id
                }

                status_response = requests.get(status_url, auth=self.auth, params=status_params)

                if status_response.status_code != 200:
                    logging.error(f"Error checking media status: Status {status_response.status_code}, Response: {status_response.text}")
                    return None

                processing_info = status_response.json().get("processing_info", {})
                state = processing_info.get("state")
                logging.info(f"Updated processing state: {state}")

                if state == "failed":
                    error_info = processing_info.get("error", {})
                    logging.error(f"Media processing failed: {error_info}")
                    return None

            return media_id

        except Exception as e:
            logging.error(f"Exception during processing wait: {str(e)}")
            return None

    def post_tweet_with_media(self, text, media_ids):
        """
        Post a tweet with media.

        Args:
            text (str): Tweet text
            media_ids (list): List of media IDs to attach

        Returns:
            dict: Tweet data if successful, None otherwise
        """
        try:
            tweet_url = f"{self.base_url}/2/tweets"

            # Convert list of media_ids to list format for API v2
            if isinstance(media_ids, str):
                media_ids = [media_ids]
            elif isinstance(media_ids, list) and len(media_ids) == 1 and "," in media_ids[0]:
                # Handle comma-separated string in list
                media_ids = media_ids[0].split(",")

            payload = {
                "text": text,
                "media": {"media_ids": media_ids}
            }

            headers = {
                "Content-Type": "application/json"
            }

            logging.info(f"Posting tweet with payload: {payload}")
            response = requests.post(
                tweet_url,
                auth=self.auth,
                headers=headers,
                json=payload
            )

            if response.status_code != 201:
                logging.error(f"Error posting tweet: Status {response.status_code}, Response: {response.text}")
                return None

            result = response.json()
            logging.info(f"Tweet posted successfully: {result}")
            return result

        except Exception as e:
            logging.error(f"Exception during tweet posting: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return None

    def _get_media_type(self, file_path):
        """
        Get the media type based on file extension.

        Args:
            file_path (str): Path to the media file

        Returns:
            str: Media type
        """
        extension = os.path.splitext(file_path)[1].lower()

        media_types = {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".mp4": "video/mp4"
        }

        return media_types.get(extension, "application/octet-stream")
