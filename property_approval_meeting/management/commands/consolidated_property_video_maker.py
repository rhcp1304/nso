import shutil

from django.core.management.base import BaseCommand
import logging
import os

from googleapiclient.errors import HttpError

from ...helpers import download_videos_from_google_drive_helper as gdh, convert_ppt_to_video_helper as cph, video_merger_helper as vmh

logger = logging.getLogger(__name__)

class ConversionError(Exception):
    pass

class Command(BaseCommand):
    help = 'This is a utility management command for testing purpose'

    def handle(self, *args, **options):
        SHARED_FOLDER_ID_LIST = ['1knpkyLuVkpTWNf_841qU4yfik5gdwqNN', '1yPby7fgqcHjCHJd9q4fNBZjIdD4qqCtw',
                                 '16ZxMgaq_ne5u2M4j0jTj_X2mQfNf2g5J']
        folder_path = r"C:\Users\Ankit.Anand\PycharmProjects\nso\downloaded_videos"

        for SHARED_FOLDER_ID in SHARED_FOLDER_ID_LIST:
            shutil.rmtree(folder_path)
            service = gdh.get_drive_service()
            os.makedirs(gdh.DOWNLOAD_DIR, exist_ok=True)
            gdh.is_file_in_folder_hierarchy.cache_clear()
            try:
                initial_start_page_response = service.changes().getStartPageToken(supportsAllDrives=True).execute()
                new_token_for_next_run = initial_start_page_response.get('startPageToken')
                self.stdout.write(
                    self.style.SUCCESS(f"Initiating full recursive scan and download for folder: {SHARED_FOLDER_ID}"))

                video_mime_types = [m for m in gdh.FILES_TO_DOWNLOAD_MIME_TYPES]
                if not video_mime_types:
                    self.stdout.write(
                        self.style.WARNING("No video MIME types configured in helper. Skipping full video scan."))
                    gdh.save_last_change_token(new_token_for_next_run)
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
                                self.stdout.write(
                                    f"  No new items found in folder ID: {current_folder_id} on this page.")
                            for item in found_items_on_page:
                                item_id = item.get('id')
                                item_name = item.get('name')
                                item_mime_type = item.get('mimeType')

                                if item_mime_type == 'application/vnd.google-apps.folder':
                                    folders_to_scan.append(item_id)
                                    self.stdout.write(
                                        f"  Found subfolder: '{item_name}' (ID: {item_id}). Adding to scan queue.")
                                elif item_mime_type in video_mime_types:
                                    self.stdout.write(
                                        f"  Found existing video: '{item_name}' (ID: {item_id}, Type: {item_mime_type}). Downloading...")
                                    gdh.download_file(service, item_id, item_name, gdh.DOWNLOAD_DIR, item_mime_type)
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
                gdh.save_last_change_token(new_token_for_next_run)
                self.stdout.write(self.style.SUCCESS(
                    f"Updated last change token to: {new_token_for_next_run} (for potential future incremental runs)."))
            except gdh.HttpError as error:
                self.stdout.write(self.style.ERROR(f"An API error occurred during full scan setup: {error}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"An unexpected error occurred during full scan setup: {e}"))

            ppt_path = ""
            filenames = get_filenames_in_folder(folder_path)
            print("----------------------------")
            print(filenames)
            for f in filenames:
                if ".pptx" in f:
                    ppt_path = f
                    break
            print(ppt_path)
            output_video_path = r"C:\Users\Ankit.Anand\PycharmProjects\nso\downloaded_videos\0000_ppt_vid.mp4"

            cph.convert_pptx_to_video(
                ppt_path=ppt_path,
                output_video_path=output_video_path,
                stdout=self.stdout,
                style=self.style
            )
            self.stdout.write(self.style.SUCCESS("Overall video conversion process completed successfully."))

            input_folder = r"C:\Users\Ankit.Anand\PycharmProjects\nso\downloaded_videos"
            output_name = "merged_sample_vid_"+SHARED_FOLDER_ID+".mp4"

            self.stdout.write(f"Starting video merge process for folder: {input_folder}")
            self.stdout.write(f"Output video will be named: {output_name}")

            if vmh.merge_videos_in_folder(input_folder, output_name):
                self.stdout.write(self.style.SUCCESS("\nVideo merging completed successfully!"))


def get_filenames_in_folder(folder_path):
    full_paths = []
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path):
            full_paths.append(item_path)  # Append the full path (item_path) instead of just the filename (item)
    return full_paths