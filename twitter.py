import os
import json
import base64
import requests
import time
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
        # Check if file exists
        if not os.path.exists(image_path):
            print(f"Error: File {image_path} does not exist.")
            return None

        # Get file size
        file_size = os.path.getsize(image_path)

        # Step 1: INIT - Initialize the upload
        init_url = f"{self.base_url}/1.1/media/upload.json"
        init_params = {
            "command": "INIT",
            "total_bytes": file_size,
            "media_type": self._get_media_type(image_path),
            "media_category": media_category
        }

        init_response = requests.post(init_url, auth=self.auth, data=init_params)

        if init_response.status_code != 200:
            print(f"Error initializing upload: {init_response.text}")
            return None

        media_id = init_response.json()["media_id_string"]

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
                    print(f"Error appending chunk {segment_index}: {append_response.text}")
                    return None

                segment_index += 1
                print(f"Uploaded chunk {segment_index} of {image_path}")

        # Step 3: FINALIZE - Complete the upload
        finalize_url = f"{self.base_url}/1.1/media/upload.json"
        finalize_params = {
            "command": "FINALIZE",
            "media_id": media_id
        }

        finalize_response = requests.post(finalize_url, auth=self.auth, data=finalize_params)

        if finalize_response.status_code != 200:
            print(f"Error finalizing upload: {finalize_response.text}")
            return None

        finalize_data = finalize_response.json()

        # Check if processing is needed
        if "processing_info" in finalize_data:
            media_id = self._wait_for_processing(media_id, finalize_data["processing_info"])

        return media_id

    def _wait_for_processing(self, media_id, processing_info):
        """
        Wait for media processing to complete.

        Args:
            media_id (str): Media ID
            processing_info (dict): Processing info from FINALIZE response

        Returns:
            str: Media ID if successful, None otherwise
        """
        state = processing_info.get("state")

        while state == "pending" or state == "in_progress":
            check_after_secs = processing_info.get("check_after_secs", 1)
            print(f"Media processing in progress. Waiting {check_after_secs} seconds...")
            time.sleep(check_after_secs)

            # Check status
            status_url = f"{self.base_url}/1.1/media/upload.json"
            status_params = {
                "command": "STATUS",
                "media_id": media_id
            }

            status_response = requests.get(status_url, auth=self.auth, params=status_params)

            if status_response.status_code != 200:
                print(f"Error checking media status: {status_response.text}")
                return None

            processing_info = status_response.json().get("processing_info", {})
            state = processing_info.get("state")

            if state == "failed":
                print(f"Media processing failed: {processing_info.get('error')}")
                return None

        return media_id

    def post_tweet_with_media(self, text, media_ids):
        """
        Post a tweet with media.

        Args:
            text (str): Tweet text
            media_ids (list): List of media IDs to attach

        Returns:
            dict: Tweet data if successful, None otherwise
        """
        tweet_url = f"{self.base_url}/2/tweets"

        # Convert list of media_ids to comma-separated string if it's a list
        if isinstance(media_ids, list):
            media_ids = ",".join(media_ids)

        payload = {
            "text": text,
            "media": {"media_ids": [media_ids]}
        }

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.post(
            tweet_url,
            auth=self.auth,
            headers=headers,
            json=payload
        )

        if response.status_code != 201:
            print(f"Error posting tweet: {response.text}")
            return None

        return response.json()

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


# def main():
#     # Replace these with your actual credentials
#     consumer_key = "YOUR_CONSUMER_KEY"
#     consumer_secret = "YOUR_CONSUMER_SECRET"
#     access_token = "YOUR_ACCESS_TOKEN"
#     access_token_secret = "YOUR_ACCESS_TOKEN_SECRET"

#     # Initialize the Twitter API client
#     twitter = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)

#     # Example: Upload an image and post a tweet with it
#     image_path = "path/to/your/image.jpg"  # Replace with actual path
#     tweet_text = "Check out this image!"  # Replace with your tweet text

#     # Upload the image
#     media_id = twitter.upload_image_chunked(image_path)

#     if media_id:
#         print(f"Image uploaded successfully with media_id: {media_id}")

#         # Post a tweet with the uploaded image
#         tweet = twitter.post_tweet_with_media(tweet_text, media_id)

#         if tweet:
#             print(f"Tweet posted successfully: {tweet}")
#     else:
#         print("Failed to upload image.")


# if __name__ == "__main__":
#     main()