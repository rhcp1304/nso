import os
import re
import logging
import shutil

from django.core.management.base import BaseCommand, CommandError
from googleapiclient.errors import HttpError

from ...helpers.pipeline_helper import (
    IntegratedPipelineHelper,
    FILES_TO_PROCESS_MIME_TYPES,
    get_file_extension,
    TEMP_DOWNLOAD_DIRECTORY
)

logger = logging.getLogger(__name__)

YOUTUBE_UPLOAD_LOG_FILE = os.path.join('youtube_upload_log.txt')

class Command(BaseCommand):
    help = (
        'Orchestrates the process of creating a Drive folder structure from a PPTX, '
        'moving the PPTX, extracting Google Drive video links, downloading, '
        're-uploading to the new Drive folder, and finally uploading to YouTube.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--ppt_file_id',
            type=str,
            help='The Google Drive File ID of the PowerPoint presentation to process.',
            required=True
        )
        parser.add_argument(
            '--parent_drive_folder_id',
            type=str,
            help='The Google Drive File ID of the top-level parent folder where the Zone/Market folders will be created.',
            required=True
        )
        parser.add_argument(
            '--temp_download_dir',
            type=str,
            default=TEMP_DOWNLOAD_DIRECTORY,
            help='Temporary local directory to store downloaded files.'
        )
        parser.add_argument(
            '--cleanup_temp_dir',
            action='store_true',
            help='Removes the temporary download directory and its contents after the process completes. (Use with caution)'
        )

    def _log_uploaded_video_link(self, filename, youtube_id):
        """Appends the video filename and YouTube link to a log file."""
        youtube_link = f"https://www.youtube.com/watch?v={youtube_id}" # Correct YouTube watch link format
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
        ppt_file_id = options['ppt_file_id']
        parent_drive_folder_id = options['parent_drive_folder_id']
        temp_download_dir = options['temp_download_dir']
        cleanup_temp_dir = options['cleanup_temp_dir']

        helper = IntegratedPipelineHelper(output_stream=self.stdout.write, style=self.style)

        self.stdout.write(self.style.SUCCESS(
            f"Starting integrated processing for PPTX ID '{ppt_file_id}' "
            f"in parent folder '{parent_drive_folder_id}'."
        ))

        # --- Setup Temporary Download Directory ---
        if not os.path.exists(temp_download_dir):
            os.makedirs(temp_download_dir)
            self.stdout.write(f"Created temporary download directory: '{temp_download_dir}'")
        else:
            self.stdout.write(f"Using existing temporary download directory: '{temp_download_dir}'")

        drive_service = None
        youtube_service = None
        local_pptx_path = None
        market_folder_id = None # Will store the ID of the final market folder

        try:
            # --- PART 1: Authenticate and Download PPTX ---
            self.stdout.write(self.style.SUCCESS("\n--- PART 1: Authenticating and Initial PPTX Download ---"))
            drive_service = helper.get_authenticated_drive_service()
            if not drive_service:
                raise CommandError("Failed to authenticate with Google Drive API. Exiting.")

            # Get PPTX metadata to determine its original name
            try:
                pptx_metadata = drive_service.files().get(fileId=ppt_file_id, fields='name,mimeType').execute()
                pptx_file_name = pptx_metadata.get('name')
                if not pptx_file_name or not pptx_file_name.lower().endswith('.pptx'):
                    raise CommandError(f"File ID '{ppt_file_id}' is not a valid PPTX file or name could not be retrieved.")
            except HttpError as e:
                raise CommandError(f"Could not retrieve metadata for PPTX ID '{ppt_file_id}': {e}. Check ID and permissions.")

            local_pptx_path = os.path.join(temp_download_dir, pptx_file_name)
            self.stdout.write(f"Downloading PPTX '{pptx_file_name}' (ID: {ppt_file_id}) from Drive...")
            if not helper.download_file_from_drive(drive_service, ppt_file_id, local_pptx_path):
                raise CommandError(self.style.ERROR(f"Failed to download PPTX file: {pptx_file_name}"))
            self.stdout.write(self.style.SUCCESS(f"PPTX downloaded to: {local_pptx_path}"))

            # --- PART 2: Create Drive Folder Structure & Move PPTX ---
            self.stdout.write(self.style.SUCCESS("\n--- PART 2: Creating Drive Folder Structure and Moving PPTX ---"))
            market_name, zone_name = helper.get_market_and_zone_name_from_ppt(local_pptx_path)

            if not market_name:
                raise CommandError("Could not extract Market Name from the PPTX. Cannot create folder structure.")

            target_parent_for_market = parent_drive_folder_id
            if zone_name:
                self.stdout.write(f"Extracted Market Name: {market_name}")
                self.stdout.write(f"Extracted Zone Name: {zone_name}")
                zone_folder_id = helper.find_or_create_folder(drive_service, zone_name, parent_drive_folder_id)
                if zone_folder_id:
                    target_parent_for_market = zone_folder_id
                else:
                    self.stdout.write(self.style.WARNING(
                        f"Failed to find or create Zone folder '{zone_name}'. Market folder will be created directly under parent."
                    ))
            else:
                self.stdout.write("Could not extract zone name. Creating market folder directly under parent.")

            market_folder_id = helper.find_or_create_folder(drive_service, market_name, target_parent_for_market)
            if not market_folder_id:
                raise CommandError(f"Failed to find or create Market folder '{market_name}'. Aborting.")

            self.stdout.write(f"Moving PPTX (ID: {ppt_file_id}) to new market folder (ID: {market_folder_id})...")
            if not helper.move_file_to_folder(drive_service, ppt_file_id, market_folder_id):
                self.stdout.write(self.style.WARNING(f"WARNING: Failed to move PPTX file {pptx_file_name}. Continuing, but check Drive manually."))
            else:
                self.stdout.write(self.style.SUCCESS(f"PPTX successfully moved to '{market_name}' folder."))


            # --- PART 3: Process Videos (Download, Re-upload to new Drive folder, YouTube Upload) ---
            self.stdout.write(self.style.SUCCESS("\n--- PART 3: Processing Videos ---"))

            market_name_prefix_raw = helper.get_market_name_prefix_for_videos(local_pptx_path)
            prefix_for_filename = ""
            if market_name_prefix_raw:
                prefix_for_filename = f"{re.sub(r'[\\/:*?"<>|]', '', market_name_prefix_raw).strip()} "
                self.stdout.write(f"Using market name prefix for videos: '{prefix_for_filename}'")
            else:
                self.stdout.write("No valid market name prefix found or extracted for videos.")

            self.stdout.write("Extracting potential video links and associated names from PPTX...")
            extracted_links_with_names = helper.extract_all_potential_links_from_last_slide(local_pptx_path)
            self.stdout.write(f"Found {len(extracted_links_with_names)} potential links.")
            google_drive_file_id_pattern = re.compile(r'drive\.google\.com/(?:file/d/|uc\?id=)([a-zA-Z0-9_-]+)')

            downloaded_video_count = 0
            re_uploaded_to_drive_count = 0
            uploaded_to_youtube_count = 0
            skipped_from_youtube_upload_count = 0

            youtube_service = helper.get_youtube_service()
            if not youtube_service:
                self.stdout.write(self.style.WARNING("Failed to authenticate with YouTube API. Skipping YouTube uploads."))

            for item in extracted_links_with_names:
                link = item['link']
                suggested_name = item['name']

                match = google_drive_file_id_pattern.search(link)
                if match:
                    video_drive_id = match.group(1)
                    self.stdout.write(f"\n--- Detected Google Drive video link: {link} (ID: {video_drive_id}) ---")

                    try:
                        file_metadata = drive_service.files().get(fileId=video_drive_id, fields='name,mimeType').execute()
                        original_video_name_from_drive = file_metadata.get('name', f"unknown_video_{video_drive_id}")
                        video_mime_type = file_metadata.get('mimeType', 'application/octet-stream')

                        # Determine base name for the file, prioritizing suggested_name
                        base_name_for_file = ""
                        original_name_from_drive_without_ext, ext_from_drive = os.path.splitext(original_video_name_from_drive)
                        if suggested_name and suggested_name.strip():
                            base_name_for_file = re.sub(r'[\\/:*?"<>|]', '', suggested_name).strip()
                        else:
                            base_name_for_file = re.sub(r'[\\/:*?"<>|]', '', original_name_from_drive_without_ext).strip()

                        final_video_name = f"{prefix_for_filename}{base_name_for_file}{ext_from_drive if ext_from_drive else get_file_extension(video_mime_type)}"
                        local_video_path = os.path.join(temp_download_dir, final_video_name)

                        # Handle potential filename collisions by appending a counter
                        counter = 1
                        original_local_video_path_base, original_local_video_path_ext = os.path.splitext(local_video_path)
                        while os.path.exists(local_video_path):
                            local_video_path = f"{original_local_video_path_base}_{counter}{original_local_video_path_ext}"
                            counter += 1

                        self.stdout.write(f"Downloading Google Drive video '{final_video_name}' to {local_video_path}...")
                        if helper.download_file_from_drive(drive_service, video_drive_id, local_video_path):
                            downloaded_video_count += 1
                            self.stdout.write(self.style.SUCCESS(f"Successfully downloaded: {local_video_path}"))

                            if os.path.exists(local_video_path) and os.path.getsize(local_video_path) < 1024: # Check for very small files
                                self.stdout.write(self.style.WARNING(
                                    f"WARNING: Downloaded video '{local_video_path}' is very small ({os.path.getsize(local_video_path)} bytes). "
                                    "This might indicate an error or permission issue. Skipping re-upload to Drive and YouTube."
                                ))
                                continue

                            # Re-upload to the NEWLY CREATED/FOUND market folder
                            self.stdout.write(
                                f"Uploading '{final_video_name}' back to NEW Google Drive folder '{market_folder_id}'...")
                            uploaded_file_id = helper.upload_file_to_drive(drive_service, final_video_name,
                                                                                 local_video_path,
                                                                                 video_mime_type,
                                                                                 market_folder_id) # Use market_folder_id
                            if uploaded_file_id:
                                re_uploaded_to_drive_count += 1
                                self.stdout.write(self.style.SUCCESS(
                                    f"Successfully re-uploaded: {final_video_name} (New Drive ID: {uploaded_file_id})"))
                            else:
                                self.stdout.write(
                                    self.style.ERROR(f"Failed to re-upload '{final_video_name}' to Google Drive."))

                            # Upload to YouTube
                            if youtube_service:
                                self.stdout.write(f"Uploading '{final_video_name}' to YouTube...")
                                video_title = market_name+ os.path.splitext(final_video_name)[0]
                                youtube_video_id = helper.upload_video_to_youtube(
                                    youtube_service,
                                    local_video_path,
                                    title=video_title,
                                )
                                if youtube_video_id:
                                    self._log_uploaded_video_link(final_video_name, youtube_video_id)
                                    uploaded_to_youtube_count += 1
                                else:
                                    self.stdout.write(self.style.ERROR(f"Failed to upload '{final_video_name}' to YouTube."))
                            else:
                                self.stdout.write(self.style.WARNING("YouTube service not available. Skipping YouTube upload."))
                        else:
                            self.stdout.write(self.style.ERROR(f"Failed to download Google Drive video: {link}"))

                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(f"An error occurred while processing Google Drive video link {link}: {e}"))
                        if isinstance(e, HttpError):
                            self.stdout.write(self.style.ERROR(
                                "Check permissions for the video file or if the video file exists in Drive."))
                        logger.exception(f"Error processing Google Drive video link: {link}")
                else:
                    self.stdout.write(
                        f"Skipping non-Google Drive link: {link} (This script only processes Google Drive videos).")

            self.stdout.write(self.style.SUCCESS(f"\n--- PART 3 Summary (Video Processing) ---"))
            self.stdout.write(self.style.SUCCESS(f"Total videos downloaded from Drive: {downloaded_video_count}"))
            self.stdout.write(self.style.SUCCESS(f"Total videos re-uploaded to Drive (to new folder): {re_uploaded_to_drive_count}"))
            self.stdout.write(self.style.SUCCESS(f"Total videos uploaded to YouTube: {uploaded_to_youtube_count}"))
            self.stdout.write(self.style.WARNING(f"Total files skipped for YouTube upload (non-video or YouTube auth issues): {skipped_from_youtube_upload_count}"))

            self.stdout.write(self.style.SUCCESS("\nOverall process completed successfully."))

        except CommandError as ce:
            self.stdout.write(self.style.ERROR(f"Process aborted: {ce}"))
            logger.error(f"CommandError: {ce}")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"An unexpected critical error occurred: {e}"))
            logger.exception("Critical error in process_ppt_videos_and_folders command.")
        finally:
            # --- Cleanup ---
            self.stdout.write("\n--- Cleanup ---")
            if local_pptx_path and os.path.exists(local_pptx_path):
                try:
                    os.remove(local_pptx_path)
                    self.stdout.write(f"Cleaned up temporary PPTX file: {local_pptx_path}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error cleaning up PPTX file {local_pptx_path}: {e}"))
                    logger.error(f"Error cleaning up PPTX file: {e}")

            if cleanup_temp_dir and os.path.exists(temp_download_dir):
                self.stdout.write(f"Cleaning up temporary download directory: '{temp_download_dir}'...")
                try:
                    shutil.rmtree(temp_download_dir)
                    self.stdout.write(self.style.SUCCESS(f"Successfully removed '{temp_download_dir}' and its contents."))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error cleaning up temporary directory '{temp_download_dir}': {e}"))
                    logger.error(f"Error cleaning up temporary directory: {e}")
            elif not cleanup_temp_dir:
                self.stdout.write(f"Temporary directory '{temp_download_dir}' retained as per --cleanup_temp_dir option.")
            self.stdout.write("Cleanup phase completed.")