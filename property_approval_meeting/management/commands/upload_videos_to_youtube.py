import os
import logging
import time
from django.core.management.base import BaseCommand
from ...helpers import youtube_uploader_helper as youtube_helper
from property_approval_meeting.helpers import \
    download_videos_from_google_drive_helper as drive_helper  # To get DOWNLOAD_DIR and VIDEO_MIME_TYPES

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Uploads videos from the local download directory to YouTube as unlisted and not for kids.'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting YouTube video upload process..."))

        # 1. Authenticate with YouTube API
        youtube_service = None
        try:
            youtube_service = youtube_helper.get_youtube_service()
            self.stdout.write(self.style.SUCCESS("Successfully authenticated with YouTube API."))
        except FileNotFoundError as e:
            self.stdout.write(self.style.ERROR(
                f"Authentication failed: {e}. Please ensure 'youtube_credentials.json' is in your helpers directory."))
            return
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"An error occurred during YouTube API authentication: {e}"))
            return

        if not youtube_service:
            self.stdout.write(self.style.ERROR("Could not get YouTube service. Exiting."))
            return

        # 2. Define the directory where downloaded videos are stored
        # Make sure drive_helper.DOWNLOAD_DIR points to where your videos are.
        # If you renamed it for generalized files, update it here.
        DOWNLOAD_DIRECTORY = drive_helper.DOWNLOAD_DIR
        if not os.path.exists(DOWNLOAD_DIRECTORY):
            self.stdout.write(
                self.style.WARNING(f"Download directory '{DOWNLOAD_DIRECTORY}' does not exist. No videos to upload."))
            return

        self.stdout.write(f"Scanning directory: {DOWNLOAD_DIRECTORY} for videos...")
        uploaded_count = 0
        skipped_count = 0

        # 3. Iterate through files in the download directory
        for filename in os.listdir(DOWNLOAD_DIRECTORY):
            file_path = os.path.join(DOWNLOAD_DIRECTORY, filename)

            # Check if it's a file and a video (based on mime types/extensions)
            # You might need to refine this check if you download other file types
            # and only want to upload specific ones.
            # For simplicity, we'll check common video extensions.
            is_video = False
            for video_mime in drive_helper.FILES_TO_DOWNLOAD_MIME_TYPES:
                if video_mime.startswith('video/'):
                    # A basic check to see if the extension matches a known video type
                    if filename.lower().endswith(drive_helper.get_file_extension(video_mime)):
                        is_video = True
                        break

            # If you expanded FILES_TO_DOWNLOAD_MIME_TYPES beyond just videos,
            # you'll need more refined logic here to identify *only* video files for upload.
            # Example:
            # if any(filename.lower().endswith(ext) for ext in ['.mp4', '.avi', '.mov', '.webm', '.mkv']):
            #     is_video = True

            if os.path.isfile(file_path) and is_video:
                self.stdout.write(f"Found video file: {filename}")

                # You might want to store a record of uploaded videos
                # to avoid re-uploading the same video multiple times.
                # For this example, we'll assume we only upload new ones.

                # Prepare video metadata
                video_title = os.path.splitext(filename)[0]  # Use filename without extension as title
                video_description = f"Downloaded from Google Drive. Original filename: {filename}"
                video_tags = ["Google Drive", "Downloaded", "Automation", "Video"]

                # You can customize the category_id.
                # "22" for People & Blogs, "24" for Entertainment, "28" for Science & Technology
                video_category_id = "22"

                self.stdout.write(f"Uploading '{video_title}' to YouTube...")

                upload_response = youtube_helper.upload_video(
                    youtube_service,
                    file_path,
                    title=video_title,
                    description=video_description,
                    tags=video_tags,
                    category_id=video_category_id
                )

                if upload_response:
                    self.stdout.write(
                        self.style.SUCCESS(f"Uploaded '{video_title}'. YouTube ID: {upload_response.get('id')}"))
                    uploaded_count += 1
                    # Optional: Delete the local file after successful upload
                    # os.remove(file_path)
                    # self.stdout.write(f"Deleted local file: {file_path}")
                else:
                    self.stdout.write(self.style.ERROR(f"Failed to upload '{video_title}'."))
            else:
                self.stdout.write(f"Skipping non-video file or directory: {filename}")
                skipped_count += 1

        self.stdout.write(self.style.SUCCESS(f"\nYouTube upload process finished."))
        self.stdout.write(self.style.SUCCESS(f"Total videos uploaded: {uploaded_count}"))
        self.stdout.write(self.style.WARNING(f"Total files skipped (non-video or other): {skipped_count}"))

