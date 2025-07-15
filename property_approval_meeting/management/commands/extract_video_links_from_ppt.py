import os
import re
from django.core.management.base import BaseCommand, CommandError

# Import the DriveHelper from your new module
# Make sure 'your_app' matches the actual name of your Django app
from ...helpers.extract_video_links_from_ppt_helper import DriveHelper


class Command(BaseCommand):
    help = 'Reads the single PPTX from a Google Drive folder, extracts Google Drive video links, and downloads/re-uploads those videos to the same Drive folder.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--google_drive_folder_id',
            type=str,
            help='The ID of the Google Drive folder containing the single PPTX.',
            required=True
        )
        parser.add_argument(
            '--temp_download_dir',
            type=str,
            default='temp_drive_downloads',
            help='Temporary local directory to store downloaded files before re-uploading.'
        )

    def handle(self, *args, **options):
        google_drive_folder_id = options['google_drive_folder_id']
        temp_download_dir = options['temp_download_dir']

        # Initialize DriveHelper, passing Django's stdout and style for consistent logging
        drive_helper = DriveHelper(output_stream=self.stdout.write, style=self.style)

        self.stdout.write(self.style.SUCCESS(f"Starting process for Google Drive folder ID: {google_drive_folder_id}"))

        # --- Ensure temporary directory exists ---
        if not os.path.exists(temp_download_dir):
            os.makedirs(temp_download_dir)
            self.stdout.write(f"Created temporary download directory: '{temp_download_dir}'")
        else:
            self.stdout.write(f"Using existing temporary download directory: '{temp_download_dir}'")

        service = drive_helper.get_authenticated_drive_service()
        if not service:
            raise CommandError("Failed to authenticate with Google Drive API.")

        # Find the single PPTX file in the specified Drive folder
        pptx_drive_id, pptx_file_name = drive_helper.find_pptx_in_drive_folder(service, google_drive_folder_id)

        if not pptx_drive_id:
            raise CommandError(self.style.ERROR(
                f"No PPTX file found in Google Drive folder '{google_drive_folder_id}'. Ensure the folder ID is correct and it contains exactly one .pptx file, or handle multiple PPTXs manually."))

        local_pptx_path = os.path.join(temp_download_dir, pptx_file_name)
        self.stdout.write(f"Downloading PPTX '{pptx_file_name}' (ID: {pptx_drive_id}) from Drive...")
        if not drive_helper.download_file_from_drive(service, pptx_drive_id, local_pptx_path):
            raise CommandError(self.style.ERROR(f"Failed to download PPTX file from Drive: {pptx_file_name}"))
        self.stdout.write(self.style.SUCCESS(f"PPTX downloaded to: {local_pptx_path}"))

        try:
            self.stdout.write("Extracting potential video links from PPTX...")
            extracted_links = drive_helper.extract_all_potential_links_from_last_slide(local_pptx_path)
            self.stdout.write(f"Found {len(extracted_links)} potential links.")

            # Regex for Google Drive file ID extraction (from shared links)
            google_drive_file_id_pattern = re.compile(r'drive\.google\.com/(?:file/d/|uc\?id=)([a-zA-Z0-9_-]+)')

            for link in extracted_links:
                match = google_drive_file_id_pattern.search(link)
                if match:
                    video_drive_id = match.group(1)
                    self.stdout.write(f"\n--- Detected Google Drive video link: {link} (ID: {video_drive_id}) ---")

                    try:
                        # Get file metadata (name, mimeType) from Drive using its ID
                        file_metadata = service.files().get(fileId=video_drive_id, fields='name,mimeType').execute()
                        video_name = file_metadata.get('name', f"unknown_video_{video_drive_id}")
                        video_mime_type = file_metadata.get('mimeType', 'application/octet-stream')

                        local_video_path = os.path.join(temp_download_dir, video_name)

                        # Handle potential duplicate filenames in the local temp directory
                        counter = 1
                        original_local_video_path = local_video_path
                        while os.path.exists(local_video_path):
                            name_part, ext_part = os.path.splitext(original_local_video_path)
                            local_video_path = f"{name_part}_{counter}{ext_part}"
                            counter += 1

                        self.stdout.write(f"Downloading Google Drive video '{video_name}' to {local_video_path}...")

                        if drive_helper.download_file_from_drive(service, video_drive_id, local_video_path):
                            self.stdout.write(self.style.SUCCESS(f"Successfully downloaded: {local_video_path}"))

                            # Basic check for empty or very small files (could indicate corrupted/error page)
                            if os.path.exists(local_video_path) and os.path.getsize(local_video_path) < 1024:
                                self.stdout.write(self.style.WARNING(
                                    f"WARNING: Downloaded video '{local_video_path}' is very small ({os.path.getsize(local_video_path)} bytes). This might indicate an error or permission issue. Skipping upload."))
                                # Optionally, clean up the small, possibly corrupted file:
                                # os.remove(local_video_path)
                                continue  # Skip uploading a likely corrupted file

                            self.stdout.write(
                                f"Uploading '{video_name}' back to Google Drive folder '{google_drive_folder_id}'...")
                            uploaded_file_id = drive_helper.upload_file_to_drive(service, video_name, local_video_path,
                                                                                 video_mime_type,
                                                                                 google_drive_folder_id)
                            if uploaded_file_id:
                                self.stdout.write(self.style.SUCCESS(
                                    f"Successfully uploaded: {video_name} (New Drive ID: {uploaded_file_id})"))
                                # Optionally, remove the local video file after successful upload
                                # os.remove(local_video_path)
                            else:
                                self.stdout.write(self.style.ERROR(f"Failed to upload '{video_name}' to Google Drive."))
                        else:
                            self.stdout.write(self.style.ERROR(f"Failed to download Google Drive video: {link}"))

                    except HttpError as api_error:
                        self.stdout.write(
                            self.style.ERROR(f"Google Drive API error for video link {link}: {api_error}"))
                        self.stdout.write(self.style.ERROR(
                            "Check permissions for the video file or if the video file exists in Drive."))
                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(f"An unexpected error occurred while processing video link {link}: {e}"))
                else:
                    self.stdout.write(
                        f"Skipping non-Google Drive link: {link} (This script only downloads and re-uploads Google Drive videos).")

        finally:
            # --- Cleanup temporary PPTX file ---
            if os.path.exists(local_pptx_path):
                os.remove(local_pptx_path)
                self.stdout.write(f"Cleaned up temporary PPTX file: {local_pptx_path}")

        self.stdout.write(self.style.SUCCESS("Process completed."))