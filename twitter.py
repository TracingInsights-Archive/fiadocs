import contextlib
import logging
import mimetypes
import time

import requests
import tweepy
from tweepy.auth import OAuth1UserHandler
from tweepy.errors import (
    BadRequest,
    Forbidden,
    HTTPException,
    NotFound,
    TooManyRequests,
    TwitterServerError,
    Unauthorized,
)
from tweepy.utils import list_to_csv

logger = logging.getLogger(__name__)


# A custom v2 client so that we can implement the v2 media upload methods, missing in tweepy.
class PostponeTweepyClientV2(tweepy.Client):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def request(
        self,
        method,
        route,
        params=None,
        json=None,
        files=None,
        data=None,
        headers=None,
    ) -> requests.Response:
        """
        Adapted to accept `files`, `data` and `headers`, and remove `user_auth` flag. Originally:
        https://github.com/tweepy/tweepy/blob/db28c0e84826485755eb7fcef0c30f75395dff5f/tweepy/client.py#L64  # noqa
        """
        host = "https://api.twitter.com"

        if headers is None:
            headers = {}
        headers["User-Agent"] = self.user_agent

        auth = OAuth1UserHandler(
            self.consumer_key,
            self.consumer_secret,
            self.access_token,
            self.access_token_secret,
        )
        auth = auth.apply_auth()

        logger.debug(
            f"Making API request: {method} {host + route}\n"
            f"Parameters: {params}\n"
            f"Headers: {headers}\n"
            f"Body: {json}"
        )

        with self.session.request(
            method,
            host + route,
            params=params,
            json=json,
            headers=headers,
            auth=auth,
            files=files,
            data=data,
        ) as response:
            logger.debug(
                "Received API response: "
                f"{response.status_code} {response.reason}\n"
                f"Headers: {response.headers}\n"
                f"Content: {response.content}"
            )

            if response.status_code == 400:
                raise BadRequest(response)
            if response.status_code == 401:
                raise Unauthorized(response)
            if response.status_code == 403:
                raise Forbidden(response)
            if response.status_code == 404:
                raise NotFound(response)
            if response.status_code == 429:
                if self.wait_on_rate_limit:
                    reset_time = int(response.headers["x-rate-limit-reset"])
                    sleep_time = reset_time - int(time.time()) + 1
                    if sleep_time > 0:
                        logger.warning(
                            f"Rate limit exceeded. Sleeping for {sleep_time} seconds."
                        )
                        time.sleep(sleep_time)
                    return self.request(
                        method, route, params, json, files, data, headers
                    )
                else:
                    raise TooManyRequests(response)
            if response.status_code >= 500:
                raise TwitterServerError(response)
            if not 200 <= response.status_code < 300:
                raise HTTPException(response)

            return response

    def media_upload(
        self,
        filename,
        *,
        file=None,
        chunked=False,
        media_category=None,
        additional_owners=None,
        **kwargs,
    ) -> dict:
        file_type = None
        try:
            import imghdr
        except ModuleNotFoundError:
            # imghdr was removed in Python 3.13
            pass
        else:
            h = None
            if file is not None:
                location = file.tell()
                h = file.read(32)
                file.seek(location)
            file_type = imghdr.what(filename, h=h)
            if file_type is not None:
                file_type = "image/" + file_type
        if file_type is None:
            file_type = mimetypes.guess_type(filename)[0]

        if chunked or file_type.startswith("video/"):
            return self.chunked_upload(
                filename,
                file=file,
                file_type=file_type,
                media_category=media_category,
                additional_owners=additional_owners,
                **kwargs,
            )
        else:
            return self.simple_upload(
                filename,
                file=file,
                media_category=media_category,
                additional_owners=additional_owners,
                **kwargs,
            )

    def simple_upload(
        self,
        filename,
        *,
        file=None,
        media_category=None,
        additional_owners=None,
        **kwargs,
    ) -> dict:
        """
        Simple upload is used for uploading images.

        Returns a dict like this:
        {
            'id': '1899873475323076485',
            'size': 10146,
            'expires_after_secs': 86400,
            'image': {'image_type': 'image/jpeg', 'w': 153, 'h': 164},
        }
        """
        with contextlib.ExitStack() as stack:
            if file is not None:
                files = {"media": (filename, file)}
            else:
                files = {"media": stack.enter_context(open(filename, "rb"))}

            post_data = {}
            if media_category is not None:
                post_data["media_category"] = media_category
            if additional_owners is not None:
                post_data["additional_owners"] = additional_owners

            response = self.request(
                "POST",
                "/2/media/upload",
                json=post_data,
                files=files,
                **kwargs,
            )
            # For simple uploads, the response does not have the 'data' key.
            # The media data as top-level keys instead.
            return response.json()

    def chunked_upload(
        self,
        filename,
        *,
        file=None,
        file_type=None,
        wait_for_async_finalize=True,
        media_category=None,
        additional_owners=None,
        **kwargs,
    ) -> dict:
        """
        Chunked upload is used for uploading videos.

        Returns a dict like this:
        {
            'id': '1899886362481818476',
            'media_key': '7_1899886362481818476',
            'size': 14568664,
            'expires_after_secs': 86398,
            'video': {
                'video_type': 'video/mp4',
            },
            'processing_info': {
                'progress_percent': 100,
                'state': 'succeeded',
            },
        }
        """
        fp = file or open(filename, "rb")

        start = fp.tell()
        fp.seek(0, 2)  # Seek to end of file
        file_size = fp.tell() - start
        fp.seek(start)

        min_chunk_size, remainder = divmod(file_size, 1000)
        min_chunk_size += bool(remainder)

        # Use 1 MiB as default chunk size
        chunk_size = kwargs.pop("chunk_size", 1024 * 1024)
        # Max chunk size is 5 MiB
        chunk_size = max(min(chunk_size, 5 * 1024 * 1024), min_chunk_size)

        segments, remainder = divmod(file_size, chunk_size)
        segments += bool(remainder)

        response = self.chunked_upload_init(
            file_size,
            file_type,
            media_category=media_category,
            additional_owners=additional_owners,
            **kwargs,
        )
        media_data = response.json().get("data", {})
        media_id = media_data.get("id")

        for segment_index in range(segments):
            # The APPEND command returns an empty response body
            self.chunked_upload_append(
                media_id, (filename, fp.read(chunk_size)), segment_index, **kwargs
            )

        fp.close()

        response = self.chunked_upload_finalize(media_id, **kwargs)
        media_data = response.json().get("data", {})
        media_id = media_data.get("id")

        if wait_for_async_finalize and media_data.get("processing_info"):
            while (
                media_data["processing_info"]["state"] in ("pending", "in_progress")
                and "error" not in media_data["processing_info"]
            ):
                time.sleep(media_data["processing_info"]["check_after_secs"])
                response = self.get_media_upload_status(media_id, **kwargs)
                media_data = response.json().get("data", {})

        return media_data

    def chunked_upload_append(
        self, media_id, media, segment_index, **kwargs
    ) -> requests.Response:
        post_data = {
            "command": "APPEND",
            "media_id": media_id,
            "segment_index": segment_index,
        }
        files = {"media": media}
        return self.request(
            "POST", "/2/media/upload", data=post_data, files=files, **kwargs
        )

    def chunked_upload_finalize(self, media_id, **kwargs) -> requests.Response:
        post_data = {
            "command": "FINALIZE",
            "media_id": media_id,
        }

        return self.request(
            "POST",
            "/2/media/upload",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=post_data,
            **kwargs,
        )

    def chunked_upload_init(
        self,
        total_bytes,
        media_type,
        *,
        media_category=None,
        additional_owners=None,
        **kwargs,
    ) -> requests.Response:
        post_data = {
            "command": "INIT",
            "total_bytes": total_bytes,
            "media_type": media_type,
        }
        if media_category is not None:
            post_data["media_category"] = media_category
        if additional_owners is not None:
            post_data["additional_owners"] = list_to_csv(additional_owners)

        return self.request(
            "POST",
            "/2/media/upload",
            data=post_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            **kwargs,
        )

    def get_media_upload_status(self, media_id, **kwargs) -> requests.Response:
        return self.request(
            "GET",
            "/2/media/upload",
            params={
                "command": "STATUS",
                "media_id": media_id,
            },
            **kwargs,
        )

    def create_media_metadata(self, media_id, alt_text, **kwargs) -> requests.Response:
        payload = {
            "id": media_id,
            "metadata": {
                "alt_text": {
                    "text": alt_text,
                }
            },
        }

        return self.request(
            "POST",
            "/2/media/metadata",
            json=payload,
            headers={"Content-Type": "application/json"},
            **kwargs,
        )
