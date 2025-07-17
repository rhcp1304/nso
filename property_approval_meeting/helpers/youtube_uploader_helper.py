import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import logging

logger = logging.getLogger(__name__)

YOUTUBE_SCOPES = [
    'https://www.googleapis.com/auth/youtube.upload',
    'https://www.googleapis.com/auth/youtube.readonly'
]

YOUTUBE_CREDENTIALS_FILE = 'youtube_credentials.json'
YOUTUBE_TOKEN_FILE = 'youtube_token.pickle'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
YOUTUBE_CREDENTIALS_PATH = os.path.join(BASE_DIR, YOUTUBE_CREDENTIALS_FILE)
YOUTUBE_TOKEN_PATH = os.path.join(BASE_DIR, YOUTUBE_TOKEN_FILE)


def get_youtube_service():
    creds = None
    if os.path.exists(YOUTUBE_TOKEN_PATH):
        try:
            with open(YOUTUBE_TOKEN_PATH, 'rb') as token:
                creds = pickle.load(token)
            logger.info("Loaded YouTube API credentials from token file.")
        except Exception as e:
            logger.warning(f"Could not load YouTube API token: {e}. Will re-authenticate.")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("YouTube API credentials expired, refreshing...")
            creds.refresh(Request())
        else:
            logger.info(f"Initiating new YouTube API authentication flow using {YOUTUBE_CREDENTIALS_PATH}...")
            if not os.path.exists(YOUTUBE_CREDENTIALS_PATH):
                logger.error(f"YouTube credentials file not found: {YOUTUBE_CREDENTIALS_PATH}")
                raise FileNotFoundError(
                    f"YouTube credentials file not found at {YOUTUBE_CREDENTIALS_PATH}. Please ensure it's there.")

            flow = InstalledAppFlow.from_client_secrets_file(YOUTUBE_CREDENTIALS_PATH, YOUTUBE_SCOPES)
            creds = flow.run_local_server(port=0)

        try:
            with open(YOUTUBE_TOKEN_PATH, 'wb') as token:
                pickle.dump(creds, token)
            logger.info(f"YouTube API credentials saved to {YOUTUBE_TOKEN_PATH}.")
        except Exception as e:
            logger.error(f"Failed to save YouTube API token: {e}")

    return build('youtube', 'v3', credentials=creds)


def upload_video(youtube_service, file_path, title, description="", tags=None, category_id="22"):
    if not os.path.exists(file_path):
        logger.error(f"Video file not found: {file_path}")
        return None

    body = {
        'snippet': {
            'title': title,
            'description': description,
            'tags': tags if tags else [],
            'categoryId': category_id
        },
        'status': {
            'privacyStatus': 'unlisted',  # 'public', 'private', 'unlisted'
            'selfDeclaredMadeForKids': False
        }
    }

    media_body = MediaFileUpload(file_path, chunksize=-1, resumable=True)

    try:
        request = youtube_service.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media_body
        )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                logger.info(f"Upload progress for '{title}': {int(status.progress() * 100)}%")

        logger.info(f"Successfully uploaded video: '{title}' (Video ID: {response.get('id')})")
        return response

    except HttpError as e:
        logger.error(f"An HTTP error {e.resp.status} occurred during upload of '{title}': {e.content.decode()}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during upload of '{title}': {e}")
        return None

