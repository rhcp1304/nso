import os
import logging
from django.core.management.base import BaseCommand
from ...helpers import youtube_uploader_helper as youtube_helper
from property_approval_meeting.helpers import \
    download_videos_from_google_drive_helper as drive_helper

logger = logging.getLogger(__name__)

YOUTUBE_UPLOAD_LOG_FILE = os.path.join('youtube_upload_log.txt')

class Command(BaseCommand):
    help = 'Uploads videos from the local download directory to YouTube as unlisted and not for kids.'

    def _log_uploaded_video_link(self, filename, youtube_id):
        """Appends the video filename and YouTube link to a log file."""
        youtube_link = f"https://youtu.be/{youtube_id}"
        log_entry = f"{filename}: {youtube_link}\n"
        try:
            with open(YOUTUBE_UPLOAD_LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(log_entry)
            self.stdout.write(self.style.SUCCESS(f"Logged: {filename} -> {youtube_link}"))
        except IOError as e:
            self.stdout.write(self.style.ERROR(f"Error writing to upload log file {YOUTUBE_UPLOAD_LOG_FILE}: {e}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"An unexpected error occurred while logging: {e}"))


    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting YouTube video upload process..."))
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

        DOWNLOAD_DIRECTORY = 'temp_drive_downloads'
        if not os.path.exists(DOWNLOAD_DIRECTORY):
            self.stdout.write(
                self.style.WARNING(f"Download directory '{DOWNLOAD_DIRECTORY}' does not exist. No videos to upload."))
            return

        self.stdout.write(f"Scanning directory: {DOWNLOAD_DIRECTORY} for videos...")
        uploaded_count = 0
        skipped_count = 0

        for filename in os.listdir(DOWNLOAD_DIRECTORY):
            file_path = os.path.join(DOWNLOAD_DIRECTORY, filename)
            is_video = False
            for video_mime in drive_helper.FILES_TO_DOWNLOAD_MIME_TYPES:
                if video_mime.startswith('video/'):
                    if filename.lower().endswith(drive_helper.get_file_extension(video_mime)):
                        is_video = True
                        break

            if os.path.isfile(file_path) and is_video:
                self.stdout.write(f"Found video file: {filename}")
                video_title = os.path.splitext(filename)[0]
                video_description = f"Downloaded from Google Drive. Original filename: {filename}"
                video_tags = ["Google Drive", "Downloaded", "Automation", "Video"]
                video_category_id = "22"
                self.stdout.write(f"Uploading '{video_title}' to YouTube...")
                youtube_video_id = youtube_helper.upload_video(
                    youtube_service,
                    file_path,
                    title=video_title,
                    description=video_description,
                    tags=video_tags,
                    category_id=video_category_id
                )
                if youtube_video_id:
                    self.stdout.write(
                        self.style.SUCCESS(f"Uploaded '{video_title}'. YouTube ID: {youtube_video_id}"))
                    self._log_uploaded_video_link(filename, youtube_video_id) # Call the logging method
                    uploaded_count += 1
                else:
                    self.stdout.write(self.style.ERROR(f"Failed to upload '{video_title}'."))
            else:
                self.stdout.write(f"Skipping non-video file or directory: {filename}")
                skipped_count += 1

        self.stdout.write(self.style.SUCCESS(f"\nYouTube upload process finished."))
        self.stdout.write(self.style.SUCCESS(f"Total videos uploaded: {uploaded_count}"))
        self.stdout.write(self.style.WARNING(f"Total files skipped (non-video or other): {skipped_count}"))