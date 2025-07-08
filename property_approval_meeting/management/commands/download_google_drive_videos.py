from django.core.management.base import BaseCommand
import logging
import os, time
from ...helpers import download_videos_from_google_drive_helper as helper

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'This is a utility management command for downloading google drive videos'

    def handle(self, *args, **options):
        service = helper.get_drive_service()
        os.makedirs(helper.DOWNLOAD_DIR, exist_ok=True)
        SHARED_FOLDER_ID = '1jA3P54-AVViTAMpziKJ-SFRuRgvNP-vh'
        if SHARED_FOLDER_ID == 'YOUR_SHARED_FOLDER_ID_HERE' or not SHARED_FOLDER_ID:
            self.stdout.write(self.style.ERROR(
                "Error: Please update 'SHARED_FOLDER_ID' in 'download_google_drive_videos.py' "
                "with the actual ID of the shared Google Drive folder."
            ))
            return

        self.stdout.write(self.style.WARNING("Performing a full recursive scan and download every time."))
        helper.is_file_in_folder_hierarchy.cache_clear()

        try:
            initial_start_page_response = service.changes().getStartPageToken(supportsAllDrives=True).execute()
            new_token_for_next_run = initial_start_page_response.get('startPageToken')

            self.stdout.write(
                self.style.SUCCESS(f"Initiating full recursive scan and download for folder: {SHARED_FOLDER_ID}"))

            video_mime_types = [m for m in helper.FILES_TO_DOWNLOAD_MIME_TYPES]
            if not video_mime_types:
                self.stdout.write(
                    self.style.WARNING("No video MIME types configured in helper. Skipping full video scan."))
                helper.save_last_change_token(new_token_for_next_run)
                return

            folders_to_scan = [SHARED_FOLDER_ID]
            scanned_folders = set()
            downloaded_count_initial = 0

            while folders_to_scan:
                current_folder_id = folders_to_scan.pop(0)
                if current_folder_id in scanned_folders:
                    self.stdout.write(f"  Skipping already scanned folder ID: {current_folder_id}")
                    continue
                scanned_folders.add(current_folder_id)

                self.stdout.write(f"Scanning contents of folder ID: {current_folder_id}")

                temp_page_token = None
                while True:
                    try:
                        q_filter = f"'{current_folder_id}' in parents and (mimeType='application/vnd.google-apps.folder' or ({' or '.join([f'mimeType="{m}"' for m in video_mime_types])})) and trashed=false"

                        folder_contents_response = service.files().list(
                            q=q_filter,
                            spaces='drive',
                            fields='nextPageToken, files(id, name, mimeType, parents)',
                            pageToken=temp_page_token,
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                        ).execute()

                        found_items_on_page = folder_contents_response.get('files', [])
                        if not found_items_on_page:
                            self.stdout.write(f"  No new items found in folder ID: {current_folder_id} on this page.")

                        for item in found_items_on_page:
                            item_id = item.get('id')
                            item_name = item.get('name')
                            item_mime_type = item.get('mimeType')

                            if item_mime_type == 'application/vnd.google-apps.folder':
                                folders_to_scan.append(item_id)  # Add subfolder to queue for scanning
                                self.stdout.write(
                                    f"  Found subfolder: '{item_name}' (ID: {item_id}). Adding to scan queue.")
                            elif item_mime_type in video_mime_types:
                                self.stdout.write(
                                    f"  Found existing video: '{item_name}' (ID: {item_id}, Type: {item_mime_type}). Downloading...")
                                helper.download_file(service, item_id, item_name, helper.DOWNLOAD_DIR, item_mime_type)
                                downloaded_count_initial += 1

                        temp_page_token = folder_contents_response.get('nextPageToken', None)
                        if temp_page_token is None:
                            break
                    except HttpError as error:
                        self.stdout.write(self.style.ERROR(
                            f"An API error occurred during initial scan of folder {current_folder_id}: {error}"))
                        break
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(
                            f"An unexpected error occurred during initial scan of folder {current_folder_id}: {e}"))
                        break

            self.stdout.write(self.style.SUCCESS(
                f"Full recursive scan complete. Downloaded {downloaded_count_initial} existing videos."))

            helper.save_last_change_token(new_token_for_next_run)
            self.stdout.write(self.style.SUCCESS(
                f"Updated last change token to: {new_token_for_next_run} (for potential future incremental runs)."))

            return

        except helper.HttpError as error:
            self.stdout.write(self.style.ERROR(f"An API error occurred during full scan setup: {error}"))
            return
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"An unexpected error occurred during full scan setup: {e}"))
            return

