import os
import logging
import shutil
from datetime import datetime
import sys

# Removed 'time' as it's not needed for a single run

# Ensure the directory containing your helper modules is in the Python path
script_dir = os.path.dirname(__file__)
sys.path.append(script_dir)

# Import your helper functions
try:
    from convert_ppt_to_video_helper import convert_pptx_to_video, ConversionError
    from video_merger_helper import merge_videos_in_folder
    from download_videos_from_google_drive_helper import (
        get_drive_service, download_file,
        # get_last_change_token, save_last_change_token, # No longer needed for full scan
        is_file_in_folder_hierarchy, FILES_TO_DOWNLOAD_MIME_TYPES,
        DOWNLOAD_DIR  # Still useful for context, though its internal use is in helper
    )
    from googleapiclient.errors import HttpError
except ImportError as e:
    print(
        f"Error importing helper modules. Make sure all three helper files are in the same directory or accessible in PYTHONPATH.")
    print(f"Details: {e}")
    sys.exit(1)

# --- Configuration ---
# Google Drive IDs of the parent folders you want to monitor.
# Replace with your actual Google Drive Folder IDs.
# Example: ['1aB2cDEfGhIJKLMN_OPQRSTUvWxYZA', '2bC3dEFgHiJKLMN_OpqRSTUvwXYZab']
# If you want to monitor files directly in 'My Drive' root, you can often use 'root' as an ID.
GOOGLE_DRIVE_MONITOR_FOLDER_IDS = [
    "1e2GWlwWaVqDVUQsp790UU017s94zRsnw"
    # "YOUR_GOOGLE_DRIVE_FOLDER_ID_1",
    # "YOUR_GOOGLE_DRIVE_FOLDER_ID_2",
    # ... add more folder IDs here
]
# IMPORTANT: You MUST configure GOOGLE_DRIVE_MONITOR_FOLDER_IDS with your actual folder IDs.
# If this list is empty, the script will exit.

OUTPUT_DIRECTORY = "merged_output_videos"
# Base temporary directory where files from Drive will be downloaded and processed
TEMP_DOWNLOAD_AND_PROCESS_BASE_DIR = "temp_drive_processing"

# PPTX to Video settings (passed to convert_pptx_to_video)
SLIDE_DURATION_SECONDS = 5
VIDEO_RESOLUTION = '1920x1080'
VIDEO_FRAME_RATE = 30

# File extensions to look for (based on what we download)
PPTX_EXTENSIONS = ('.pptx',)  # Only look for actual pptx files locally
VIDEO_EXTENSIONS = ('.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv')  # Only look for actual video files locally

# Logging setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("master_video_maker.log"),
                        logging.StreamHandler(sys.stdout)
                    ])
logger = logging.getLogger(__name__)


# --- Dummy Style for _stdout.write() outside Django context ---
class _DummyStyle:
    def SUCCESS(self, msg): return msg

    def WARNING(self, msg): return msg

    def ERROR(self, msg): return msg


_style = _DummyStyle()


# Dummy stdout for convert_pptx_to_video (it requires a .write() method)
class _DummyStdout:
    def write(self, msg, ending='\n'):
        logger.info(msg.strip())  # Direct print messages from helpers to logger


# Instantiate dummy objects to pass to helper functions
dummy_stdout = _DummyStdout()


def ensure_directory(path):
    """Ensures a directory exists."""
    os.makedirs(path, exist_ok=True)
    logger.info(f"Ensured directory exists: {path}")


def get_drive_folder_name(service, folder_id):
    """Fetches the name of a Google Drive folder."""
    try:
        folder = service.files().get(fileId=folder_id, fields='name').execute()
        return folder.get('name', folder_id)
    except HttpError as error:
        logger.error(f"Could not get name for folder ID {folder_id}: {error}")
        return folder_id  # Fallback to ID if name can't be fetched


def process_downloaded_folder(local_folder_path: str, drive_folder_name: str):
    """
    Processes a single locally downloaded folder: converts PPTX, collects videos, merges them.
    """
    logger.info(f"\n--- Processing downloaded content from Drive folder: '{drive_folder_name}' ---")

    # Step 1: Identify PPTX and other videos in the local downloaded folder
    pptx_file = None
    other_video_files = []

    for item in os.listdir(local_folder_path):
        item_path = os.path.join(local_folder_path, item)
        if os.path.isfile(item_path):
            if item.lower().endswith(PPTX_EXTENSIONS):
                if pptx_file:
                    logger.warning(
                        f"Multiple PPTX files found in '{drive_folder_name}' local temp. Using '{os.path.basename(pptx_file)}' and skipping '{item}'.")
                else:
                    pptx_file = item_path
            elif item.lower().endswith(VIDEO_EXTENSIONS):
                other_video_files.append(item_path)

    other_video_files.sort()  # Ensure consistent order for merging

    # Path for the converted PPTX video (will be within the same local_folder_path for convenience)
    converted_pptx_video_path = None

    # Define an output file name for the final merged video
    # Use the Drive folder name as part of the output file name
    output_merged_video_name = f"{drive_folder_name}_merged_video.mp4"
    output_merged_video_path = os.path.join(OUTPUT_DIRECTORY, output_merged_video_name)

    try:
        # Step 2: Convert PPTX to video if found
        if pptx_file:
            logger.info(f"PPTX file found: '{os.path.basename(pptx_file)}'. Converting to video...")
            converted_pptx_video_path = os.path.join(local_folder_path,
                                                     f"{os.path.splitext(os.path.basename(pptx_file))[0]}.mp4")

            try:
                convert_pptx_to_video(
                    ppt_path=pptx_file,
                    output_video_path=converted_pptx_video_path,
                    slide_duration_seconds=SLIDE_DURATION_SECONDS,
                    resolution=VIDEO_RESOLUTION,
                    frame_rate=VIDEO_FRAME_RATE,
                    stdout=dummy_stdout,
                    style=_style
                )
                logger.info(_style.SUCCESS(
                    f"Successfully converted PPTX to video: '{os.path.basename(converted_pptx_video_path)}'"))
            except ConversionError as e:
                logger.error(_style.ERROR(f"Failed to convert PPTX '{os.path.basename(pptx_file)}' to video: {e}"))
                converted_pptx_video_path = None  # Mark as failed
            except Exception as e:
                logger.error(_style.ERROR(
                    f"An unexpected error occurred during PPTX conversion for '{os.path.basename(pptx_file)}': {e}"))
                converted_pptx_video_path = None  # Mark as failed
        videos_to_merge_paths = []
        if converted_pptx_video_path and os.path.exists(converted_pptx_video_path):
            videos_to_merge_paths.append(converted_pptx_video_path)
            logger.info(f"Added converted PPTX video to merge list: {os.path.basename(converted_pptx_video_path)}")

        if other_video_files:
            logger.info(f"Adding {len(other_video_files)} existing video files to merge list.")
            videos_to_merge_paths.extend(other_video_files)

        if not videos_to_merge_paths:
            logger.warning(f"No videos (or valid converted PPTX) found for merging in '{drive_folder_name}'. Skipping.")
            return False

        # Step 4: Merge videos using the local_folder_path as input
        logger.info(f"Merging videos for Drive folder '{drive_folder_name}'...")
        merge_success = merge_videos_in_folder(
            input_folder=local_folder_path,  # Use the directly downloaded folder
            output_filename=output_merged_video_path
        )

        if merge_success:
            logger.info(
                _style.SUCCESS(f"Successfully merged video for '{drive_folder_name}' to '{output_merged_video_path}'"))
            return True
        else:
            logger.error(
                _style.ERROR(f"Failed to merge videos for Drive folder '{drive_folder_name}'. Check logs for details."))
            return False

    except Exception as e:
        logger.error(_style.ERROR(
            f"An unexpected error occurred while processing downloaded content for '{drive_folder_name}': {e}"),
            exc_info=True)
        return False


def main():
    logger.info("--- Starting Google Drive Video Maker (Full Scan Mode) ---")
    if not GOOGLE_DRIVE_MONITOR_FOLDER_IDS:
        logger.error(
            "ERROR: GOOGLE_DRIVE_MONITOR_FOLDER_IDS is empty. Please configure the Google Drive folder IDs you want to monitor in the script.")
        sys.exit(1)

    ensure_directory(OUTPUT_DIRECTORY)
    ensure_directory(TEMP_DOWNLOAD_AND_PROCESS_BASE_DIR)

    drive_service = None
    try:
        drive_service = get_drive_service()
    except (FileNotFoundError, Exception) as e:
        logger.critical(f"Failed to authenticate with Google Drive: {e}")
        logger.critical("Please ensure 'credentials.json' is correctly placed and accessible.")
        sys.exit(1)

    # --- Start of Full Scan Logic ---
    logger.info("Performing a full scan of specified Google Drive folders.")

    # Dictionary to hold files that need processing, grouped by their direct parent folder on Drive
    # This will simulate the "folder-by-folder" processing logic
    folders_to_process_data = {}  # {drive_folder_id: {file_id: file_metadata}}

    for monitor_folder_id in GOOGLE_DRIVE_MONITOR_FOLDER_IDS:
        logger.info(f"Scanning Google Drive folder ID: {monitor_folder_id}")
        query = f"'{monitor_folder_id}' in parents and trashed = false"

        # Add MIME type filtering to the query for efficiency
        mime_type_query_parts = [f"mimeType = '{mt}'" for mt in FILES_TO_DOWNLOAD_MIME_TYPES]
        mime_type_query = f"({' or '.join(mime_type_query_parts)})"
        query += f" and {mime_type_query}"

        # List all files and folders directly within this monitor_folder_id
        # We need to distinguish between files and sub-folders
        results = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType, parents, mimeType)",
            spaces='drive'
        ).execute()

        items = results.get('files', [])

        if not items:
            logger.info(f"No relevant files found directly in Google Drive folder ID: {monitor_folder_id}")
            continue

        for item in items:
            file_id = item.get('id')
            file_name = item.get('name')
            mime_type = item.get('mimeType')
            parents = item.get('parents', [])

            # Check if this file's direct parent is the current monitor_folder_id
            # This ensures we are only processing direct children for this iteration
            if monitor_folder_id in parents:
                if monitor_folder_id not in folders_to_process_data:
                    folders_to_process_data[monitor_folder_id] = {}
                folders_to_process_data[monitor_folder_id][file_id] = item
                logger.debug(f"Found file '{file_name}' (ID: {file_id}, MIME: {mime_type}) in {monitor_folder_id}")
            else:
                logger.debug(
                    f"File '{file_name}' (ID: {file_id}) is not a direct child of {monitor_folder_id}. Skipping for this scan.")

    # Process each identified folder
    folders_processed_count = 0
    folders_successful_count = 0
    for drive_folder_id, files_in_folder in folders_to_process_data.items():
        folders_processed_count += 1
        drive_folder_name = get_drive_folder_name(drive_service, drive_folder_id)

        # Sanitize folder name for local path
        safe_drive_folder_name = drive_folder_name.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":",
                                                                                                                  "_")  # Added colon for Windows paths
        local_temp_folder = os.path.join(TEMP_DOWNLOAD_AND_PROCESS_BASE_DIR,
                                         f"{safe_drive_folder_name}_{drive_folder_id}")
        ensure_directory(local_temp_folder)
        logger.info(
            f"Downloading files for Drive folder '{drive_folder_name}' (ID: {drive_folder_id}) to '{local_temp_folder}'...")

        downloaded_any_file = False
        for file_id, file_metadata in files_in_folder.items():
            # Pass the local_temp_folder for download destination
            download_file(drive_service, file_id, file_metadata['name'], local_temp_folder,
                          file_metadata['mimeType'])
            downloaded_any_file = True

        if downloaded_any_file:
            if process_downloaded_folder(local_temp_folder, drive_folder_name):
                logger.info(_style.SUCCESS(
                    f"Successfully processed and merged video for Drive folder: '{drive_folder_name}'"))
                folders_successful_count += 1
            else:
                logger.error(_style.ERROR(
                    f"Failed to process and merge video for Drive folder: '{drive_folder_name}'"))
        else:
            logger.info(
                f"No relevant files to download or process in Drive folder '{drive_folder_name}'.")

        # Cleanup the temporary download folder for this Drive folder
        if os.path.exists(local_temp_folder):
            try:
                shutil.rmtree(local_temp_folder)
                logger.info(f"Cleaned up local temporary download folder: {local_temp_folder}")
            except Exception as e:
                logger.warning(
                    f"Failed to clean up local temporary download folder '{local_temp_folder}': {e}")

    logger.info("\n--- Google Drive Video Maker (Full Scan) Summary ---")
    logger.info(f"Total Drive folders identified for processing: {len(GOOGLE_DRIVE_MONITOR_FOLDER_IDS)}")
    logger.info(f"Actual Drive folders with relevant files processed: {folders_processed_count}")
    logger.info(f"Folders successfully processed and merged: {folders_successful_count}")
    logger.info(f"Merged videos saved to: '{OUTPUT_DIRECTORY}'")
    logger.info(f"Check 'master_video_maker.log' for detailed output.")
    logger.info("--- Google Drive Video Maker (Full Scan) Finished ---")

    # Final cleanup of the base temp directory if it's empty
    try:
        if os.path.exists(TEMP_DOWNLOAD_AND_PROCESS_BASE_DIR) and not os.listdir(TEMP_DOWNLOAD_AND_PROCESS_BASE_DIR):
            os.rmdir(TEMP_DOWNLOAD_AND_PROCESS_BASE_DIR)
            logger.info(f"Cleaned up empty base temporary directory: {TEMP_DOWNLOAD_AND_PROCESS_BASE_DIR}")
    except OSError as e:
        logger.warning(f"Could not remove empty base temporary directory '{TEMP_DOWNLOAD_AND_PROCESS_BASE_DIR}': {e}")


if __name__ == "__main__":
    main()