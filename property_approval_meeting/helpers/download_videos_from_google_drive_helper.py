import os
import io
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import logging
from functools import lru_cache  # For caching API responses to improve performance

logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DOWNLOAD_DIR = 'downloaded_videos'
POLLING_INTERVAL_SECONDS = 300
LAST_CHANGE_TOKEN_FILE = 'last_change_token.pkl'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, 'credentials.json')
TOKEN_FILE_PATH = os.path.join(BASE_DIR, 'token.pickle')
LAST_CHANGE_TOKEN_FULL_PATH = os.path.join(BASE_DIR, LAST_CHANGE_TOKEN_FILE)

FILES_TO_DOWNLOAD_MIME_TYPES = [
    # Video MIME types
    'video/mp4',
    'video/x-msvideo',  # .avi
    'video/x-flv',  # .flv
    'video/quicktime',  # .mov
    'video/webm',
    'video/3gpp',
    'video/mpeg',
    'video/x-matroska',  # .mkv
    # PowerPoint MIME type
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',  # .pptx (for native .pptx files)
    # Common document/image types (if you want to download them directly)
    'application/pdf',
    'application/msword',  # .doc
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # .docx
    'application/vnd.ms-excel',  # .xls
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # .xlsx
    'text/plain',  # .txt
    'image/jpeg',  # .jpg, .jpeg
    'image/png',  # .png
    # Google Workspace native MIME types (require EXPORT, handled in download_file)
    'application/vnd.google-apps.document',  # Google Docs (exports to .docx)
    'application/vnd.google-apps.presentation',  # Google Slides (exports to .pptx)
    'application/vnd.google-apps.spreadsheet',  # Google Sheets (exports to .xlsx)
    'application/vnd.google-apps.drawing',  # Google Drawings (exports to .png or pdf)
]


def get_drive_service():
    """Authenticates and returns a Google Drive API service object."""
    creds = None
    if os.path.exists(TOKEN_FILE_PATH):
        try:
            with open(TOKEN_FILE_PATH, 'rb') as token:
                creds = pickle.load(token)
            logger.info("Loaded Drive API credentials from token file.")
        except Exception as e:
            logger.warning(f"Could not load Drive API token: {e}. Will re-authenticate.")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Drive API credentials expired, refreshing...")
            creds.refresh(Request())
        else:
            logger.info(f"Initiating new Drive API authentication flow using {CREDENTIALS_FILE}...")
            if not os.path.exists(CREDENTIALS_FILE):
                logger.error(f"Drive credentials file not found: {CREDENTIALS_FILE}")
                raise FileNotFoundError(
                    f"Drive credentials file not found at {CREDENTIALS_FILE}. Please ensure it's there.")

            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        try:
            with open(TOKEN_FILE_PATH, 'wb') as token:
                pickle.dump(creds, token)
            logger.info(f"Drive API credentials saved to {TOKEN_FILE_PATH}.")
        except Exception as e:
            logger.error(f"Failed to save Drive API token: {e}")

    return build('drive', 'v3', credentials=creds)


def download_file(service, file_id, file_name_base, destination_folder, mime_type):
    """
    Downloads a file from Google Drive.
    Handles direct download for common files and export for Google Workspace native formats.

    Args:
        service: Authenticated Google Drive API service object.
        file_id (str): The ID of the file to download.
        file_name_base (str): The base name of the file (without extension, as returned by Drive API).
        destination_folder (str): The local folder to save the file into.
        mime_type (str): The MIME type of the file, used to determine download method and extension.
    """

    # Determine the final file extension based on MIME type
    final_extension = get_file_extension(mime_type)

    # Construct the full file name with the correct extension
    # Use os.path.splitext to ensure we don't double-add extensions if file_name_base already has one
    name_without_ext = os.path.splitext(file_name_base)[0]
    final_file_name = name_without_ext + final_extension

    filepath = os.path.join(destination_folder, final_file_name)

    if os.path.exists(filepath):
        logger.info(f"Skipping download for '{final_file_name}': File already exists locally.")
        return

    logger.info(f"Downloading '{final_file_name}' (ID: {file_id})...")
    try:
        request = None
        if mime_type.startswith('application/vnd.google-apps'):
            # Google Workspace native files need to be exported
            export_mime_type = ''
            if mime_type == 'application/vnd.google-apps.document':
                export_mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'  # .docx
            elif mime_type == 'application/vnd.google-apps.presentation':
                export_mime_type = 'application/vnd.openxmlformats-officedocument.presentationml.presentation'  # .pptx
            elif mime_type == 'application/vnd.google-apps.spreadsheet':
                export_mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'  # .xlsx
            elif mime_type == 'application/vnd.google-apps.drawing':
                export_mime_type = 'image/png'  # or 'image/jpeg', 'application/pdf'

            if export_mime_type:
                logger.info(f"Exporting Google native file '{file_name_base}' to {export_mime_type}...")
                request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
                # Note: final_file_name already has the correct extension based on export_mime_type
            else:
                logger.warning(
                    f"Unsupported Google native app type for export: {mime_type} for file: {file_name_base}. Skipping.")
                return  # Skip if no suitable export MIME type is found
        else:
            # Direct download for other file types
            request = service.files().get_media(fileId=file_id)

        if request is None:  # Should not happen if logic is correct, but as a safeguard
            logger.error(f"Download request could not be created for {file_name_base} ({mime_type}).")
            return

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logger.info(f"Download progress: {int(status.progress() * 100)}%")

        fh.seek(0)
        with open(filepath, 'wb') as f:
            f.write(fh.read())
        logger.info(f"Successfully downloaded '{final_file_name}' to '{filepath}'")
    except HttpError as error:
        logger.error(f"An error occurred during download for '{final_file_name}': {error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during download for '{final_file_name}': {e}")


def get_last_change_token():
    """Loads the last recorded change token from a file."""
    if os.path.exists(LAST_CHANGE_TOKEN_FULL_PATH):
        try:
            with open(LAST_CHANGE_TOKEN_FULL_PATH, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading last change token: {e}")
            return None
    return None


def save_last_change_token(token):
    """Saves the current change token to a file."""
    try:
        with open(LAST_CHANGE_TOKEN_FULL_PATH, 'wb') as f:
            pickle.dump(token, f)
    except Exception as e:
        logger.error(f"Error saving last change token: {e}")


# --- New recursive parent check function ---
@lru_cache(maxsize=None)  # Cache results to avoid repeated API calls for the same folder
def is_file_in_folder_hierarchy(service, file_id, target_folder_id):
    """
    Recursively checks if a given file or folder is within the hierarchy of a target folder.
    This function will trace parent folders up the tree until it finds the target_folder_id
    or reaches the root.
    """
    if file_id == target_folder_id:
        return True  # The file/folder itself is the target folder

    try:
        # Fetch the file's metadata, specifically its parents
        file_metadata = service.files().get(
            fileId=file_id,
            fields='parents'
        ).execute()

        parents = file_metadata.get('parents', [])

        if not parents:
            return False  # Reached root without finding target_folder_id

        # Check if the target_folder_id is among the direct parents
        if target_folder_id in parents:
            return True

        # Recursively check each parent's hierarchy
        for parent_id in parents:
            if is_file_in_folder_hierarchy(service, parent_id, target_folder_id):
                return True

        return False  # Not found in this branch

    except HttpError as e:
        if e.resp.status == 404:
            logger.warning(
                f"File ID {file_id} not found in Drive. May have been deleted or access lost. Assuming not in hierarchy.")
            return False
        logger.error(f"HTTP Error checking hierarchy for {file_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking hierarchy for {file_id}: {e}")
        return False


# --- MODIFIED: Comprehensive get_file_extension mapping ---
def get_file_extension(mime_type):
    """Guesses file extension based on MIME type."""
    # Video extensions
    if 'video/mp4' in mime_type: return '.mp4'
    if 'video/x-msvideo' in mime_type: return '.avi'
    if 'video/x-flv' in mime_type: return '.flv'
    if 'video/quicktime' in mime_type: return '.mov'
    if 'video/webm' in mime_type: return '.webm'
    if 'video/3gpp' in mime_type: return '.3gp'
    if 'video/mpeg' in mime_type: return '.mpeg'
    if 'video/x-matroska' in mime_type: return '.mkv'

    # Document/Image extensions
    if 'application/vnd.openxmlformats-officedocument.presentationml.presentation' in mime_type: return '.pptx'
    if 'application/pdf' in mime_type: return '.pdf'
    if 'application/msword' in mime_type: return '.doc'
    if 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in mime_type: return '.docx'
    if 'application/vnd.ms-excel' in mime_type: return '.xls'
    if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in mime_type: return '.xlsx'
    if 'text/plain' in mime_type: return '.txt'
    if 'image/jpeg' in mime_type: return '.jpg'
    if 'image/png' in mime_type: return '.png'
    if 'image/gif' in mime_type: return '.gif'

    # Mappings for *exported* Google native formats (these are the MIME types they EXPORT to)
    if mime_type == 'application/vnd.google-apps.document': return '.docx'
    if mime_type == 'application/vnd.google-apps.presentation': return '.pptx'
    if mime_type == 'application/vnd.google-apps.spreadsheet': return '.xlsx'
    if mime_type == 'application/vnd.google-apps.drawing': return '.png'  # Common export for drawings

    logger.warning(f"No known file extension for MIME type: {mime_type}. Returning empty string.")
    return ''  # Return empty string if no common extension found
