import json
import logging
import os
import subprocess
import time
from io import BytesIO

import praw
import requests
import schedule
from atproto import Client
from PIL import Image

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()],
)

# Update credentials section to use environment variables
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = "FormulaDank_to_Bluesky_Bot/1.0"
BLUESKY_EMAIL = os.environ.get("BLUESKY_EMAIL")
BLUESKY_PASSWORD = os.environ.get("BLUESKY_PASSWORD")

# Initialize Reddit client
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT,
)

# Initialize Bluesky client
bluesky = Client()
bluesky.login(BLUESKY_EMAIL, BLUESKY_PASSWORD)


def clean_filename(url):
    base_name = url.split("?")[0]
    return os.path.basename(base_name)


def convert_gif_to_mp4(gif_path):
    output_path = f"{gif_path[:-4]}.mp4"
    try:
        cmd = [
            "ffmpeg",
            "-i",
            gif_path,
            "-vf",
            "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-y",
            output_path,
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        os.remove(gif_path)
        return output_path
    except subprocess.CalledProcessError:
        return None


def verify_file_size(file_path, max_size_kb=900):
    return os.path.getsize(file_path) <= max_size_kb * 1024


def compress_video(video_path, max_size_kb=900):
    target_size = max_size_kb * 1024
    original_size = os.path.getsize(video_path)

    if original_size <= target_size:
        return True

    target_bitrate = (target_size * 8) // (20 * 1024)
    output_path = f"{video_path}_compressed.mp4"

    try:
        cmd = [
            "ffmpeg",
            "-i",
            video_path,
            "-c:v",
            "libx264",
            "-b:v",
            f"{target_bitrate}k",
            "-preset",
            "veryslow",
            "-crf",
            "35",
            "-vf",
            "scale=iw*0.7:-2",
            "-maxrate",
            f"{target_bitrate}k",
            "-bufsize",
            f"{target_bitrate//2}k",
            "-y",
            output_path,
        ]

        subprocess.run(cmd, check=True, capture_output=True)

        if os.path.exists(output_path):
            compressed_size = os.path.getsize(output_path)
            if compressed_size <= target_size:
                os.remove(video_path)
                os.rename(output_path, video_path)
                return True
            os.remove(output_path)
        return False
    except subprocess.CalledProcessError:
        if os.path.exists(output_path):
            os.remove(output_path)
        return False


def compress_image(image_path, max_size_kb=900):
    img = Image.open(image_path)

    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    quality = 95
    scale = 1.0

    while True:
        if scale < 1.0:
            new_size = tuple(int(dim * scale) for dim in img.size)
            resized_img = img.resize(new_size, Image.Resampling.LANCZOS)
        else:
            resized_img = img

        img_byte_arr = BytesIO()
        resized_img.save(img_byte_arr, format="JPEG", quality=quality, optimize=True)

        if len(img_byte_arr.getvalue()) <= max_size_kb * 1024:
            break

        if quality > 20:
            quality -= 5
        else:
            scale *= 0.75

        if scale < 0.3:
            break

    with open(image_path, "wb") as f:
        f.write(img_byte_arr.getvalue())

    return os.path.getsize(image_path) <= max_size_kb * 1024


def load_posted_ids():
    if os.path.exists("posted_ids.json"):
        with open("posted_ids.json", "r") as f:
            return set(json.load(f))
    return set()


def save_posted_ids(posted_ids):
    with open("posted_ids.json", "w") as f:
        json.dump(list(posted_ids), f)


def download_media(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
        return True
    return False


def get_media_urls(post):
    media_urls = []

    if hasattr(post, "is_gallery") and post.is_gallery:
        for item in post.gallery_data["items"]:
            media_id = item["media_id"]
            media_url = post.media_metadata[media_id]["p"][0]["u"]
            media_url = media_url.split("?")[0].replace("preview", "i")
            media_urls.append(media_url)
    elif hasattr(post, "is_video") and post.is_video:
        if hasattr(post, "media") and post.media:
            if "reddit_video" in post.media:
                media_urls.append(post.media["reddit_video"]["fallback_url"])
    elif hasattr(post, "url") and post.url.endswith(
        (".jpg", ".png", ".gif", ".mp4", ".jpeg", ".webp", ".gifv")
    ):
        media_urls.append(post.url)
    elif post.domain in ["v.redd.it", "youtube.com", "youtu.be"]:
        if hasattr(post, "media") and post.media:
            if "reddit_video" in post.media:
                media_urls.append(post.media["reddit_video"]["fallback_url"])

    return media_urls


def create_bluesky_thread(title, media_paths):
    try:
        parent_post = None
        for i in range(0, len(media_paths), 4):
            chunk = media_paths[i : i + 4]
            images = {"$type": "app.bsky.embed.images", "images": []}

            for media_path in chunk:
                if not verify_file_size(media_path):
                    logging.warning(f"File {media_path} too large after compression")
                    continue

                with open(media_path, "rb") as f:
                    image_data = f.read()
                response = bluesky.upload_blob(image_data)
                images["images"].append({"image": response.blob, "alt": title})

            if i == 0:
                formatted_text = f"{title}\n\n#f1 #formula1 #memes"
            else:
                formatted_text = (
                    f"Continued... ({i//4 + 1}/{(len(media_paths) + 3)//4})"
                )

            facets = []
            if i == 0:
                text_bytes = formatted_text.encode("utf-8")
                for tag in ["f1", "formula1", "memes"]:
                    tag_with_hash = f"#{tag}"
                    tag_pos = formatted_text.find(tag_with_hash)
                    if tag_pos != -1:
                        byte_start = len(formatted_text[:tag_pos].encode("utf-8"))
                        byte_end = len(
                            formatted_text[: tag_pos + len(tag_with_hash)].encode(
                                "utf-8"
                            )
                        )
                        facets.append(
                            {
                                "index": {"byteStart": byte_start, "byteEnd": byte_end},
                                "features": [
                                    {"$type": "app.bsky.richtext.facet#tag", "tag": tag}
                                ],
                            }
                        )

            if parent_post:
                post_result = bluesky.post(
                    text=formatted_text,
                    facets=facets,
                    embed=images,
                    reply_to=parent_post,
                )
            else:
                post_result = bluesky.post(
                    text=formatted_text, facets=facets, embed=images
                )

            parent_post = {
                "root": post_result.uri if i == 0 else parent_post["root"],
                "parent": post_result.uri,
            }

        return True
    except Exception as e:
        logging.error(f"Error creating Bluesky thread: {e}")
        return False


def download_and_process_media(url, filename):
    if download_media(url, filename):
        if filename.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            return compress_image(filename)
        elif filename.lower().endswith(".gif"):
            mp4_path = convert_gif_to_mp4(filename)
            if mp4_path and compress_video(mp4_path):
                return True
            return False
        elif filename.lower().endswith((".mp4", ".mov", ".avi")):
            return compress_video(filename)
        return True
    return False


def check_and_post():
    logging.info("Starting new check for posts")
    posted_ids = load_posted_ids()
    subreddit = reddit.subreddit("formuladank")
    one_hour_ago = time.time() - 4800

    try:
        for post in subreddit.new(limit=10):
            if post.created_utc < one_hour_ago:
                continue

            if post.id not in posted_ids:
                logging.info(f"Found new post: {post.title}")
                media_urls = get_media_urls(post)

                if media_urls:
                    media_files = []
                    for i, url in enumerate(media_urls):
                        clean_url = clean_filename(url)
                        filename = f"temp_{post.id}_{i}{os.path.splitext(clean_url)[1]}"
                        if download_and_process_media(url, filename):
                            media_files.append(filename)

                    if media_files:
                        if create_bluesky_thread(post.title, media_files):
                            logging.info(
                                f"Successfully posted thread to Bluesky: {post.title}"
                            )
                            posted_ids.add(post.id)
                            save_posted_ids(posted_ids)

                        for filename in media_files:
                            if os.path.exists(filename):
                                os.remove(filename)

    except Exception as e:
        logging.error(f"Error in check_and_post: {e}")


def main():
    check_and_post()


if __name__ == "__main__":
    main()
