import os
import io
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import logging
from functools import lru_cache

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
    # 'video/mp4',
    # 'video/x-msvideo',
    # 'video/x-flv',
    # 'video/quicktime',
    # 'video/webm',
    # 'video/3gpp',
    # 'video/mpeg',
    # 'video/x-matroska',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.google-apps.presentation',
]


def get_drive_service():
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
    final_extension = get_file_extension(mime_type)
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
            export_mime_type = ''
            if mime_type == 'application/vnd.google-apps.presentation':
                export_mime_type = 'application/vnd.openxmlformats-officedocument.presentationml.presentation'  # .pptx

            if export_mime_type:
                logger.info(f"Exporting Google native file '{file_name_base}' to {export_mime_type}...")
                request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
            else:
                logger.warning(
                    f"Unsupported Google native app type for export: {mime_type} for file: {file_name_base}. Skipping.")
                return
        else:
            request = service.files().get_media(fileId=file_id)

        if request is None:
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
    if os.path.exists(LAST_CHANGE_TOKEN_FULL_PATH):
        try:
            with open(LAST_CHANGE_TOKEN_FULL_PATH, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading last change token: {e}")
            return None
    return None


def save_last_change_token(token):
    try:
        with open(LAST_CHANGE_TOKEN_FULL_PATH, 'wb') as f:
            pickle.dump(token, f)
    except Exception as e:
        logger.error(f"Error saving last change token: {e}")


@lru_cache(maxsize=None)
def is_file_in_folder_hierarchy(service, file_id, target_folder_id):
    if file_id == target_folder_id:
        return True
    try:
        file_metadata = service.files().get(
            fileId=file_id,
            fields='parents'
        ).execute()
        parents = file_metadata.get('parents', [])
        if not parents:
            return False
        if target_folder_id in parents:
            return True
        for parent_id in parents:
            if is_file_in_folder_hierarchy(service, parent_id, target_folder_id):
                return True
        return False
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


def get_file_extension(mime_type):
    if 'video/mp4' in mime_type: return '.mp4'
    if 'video/x-msvideo' in mime_type: return '.avi'
    if 'video/x-flv' in mime_type: return '.flv'
    if 'video/quicktime' in mime_type: return '.mov'
    if 'video/webm' in mime_type: return '.webm'
    if 'video/3gpp' in mime_type: return '.3gp'
    if 'video/mpeg' in mime_type: return '.mpeg'
    if 'video/x-matroska' in mime_type: return '.mkv'
    if 'application/vnd.openxmlformats-officedocument.presentationml.presentation' in mime_type: return '.pptx'
    if mime_type == 'application/vnd.google-apps.presentation': return '.pptx'
    logger.warning(f"No known file extension for MIME type: {mime_type}. Returning empty string.")
    return ''
