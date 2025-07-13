import os
import mimetypes
import pickle
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE_PATH = os.path.join(BASE_DIR, 'credentials.json')
TOKEN_FILE_PATH = os.path.join(BASE_DIR, 'token.json')  # Expected token.json path
TOKEN_PICKLE_PATH = os.path.join(BASE_DIR, 'token.pickle')  # Path for legacy .pickle file

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive']


def get_authenticated_service():
    creds = None
    if os.path.exists(TOKEN_FILE_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE_PATH, SCOPES)
            print(f"Loaded credentials from '{TOKEN_FILE_PATH}'.")
        except json.JSONDecodeError:
            print(
                f"Warning: '{TOKEN_FILE_PATH}' is corrupted or not valid JSON. Deleting and attempting re-authentication.")
            os.remove(TOKEN_FILE_PATH)
            creds = None
        except Exception as e:
            print(f"Error loading credentials from '{TOKEN_FILE_PATH}': {e}. Attempting re-authentication.")
            creds = None

    if not creds and os.path.exists(TOKEN_PICKLE_PATH):
        try:
            with open(TOKEN_PICKLE_PATH, 'rb') as token_pickle_file:
                creds = pickle.load(token_pickle_file)
            print(f"Loaded credentials from legacy '{TOKEN_PICKLE_PATH}'.")
            if creds:
                try:
                    with open(TOKEN_FILE_PATH, 'w') as token_json_file:
                        token_json_file.write(creds.to_json())
                    print(f"Converted and saved credentials to '{TOKEN_FILE_PATH}' for future use.")
                except Exception as e:
                    print(
                        f"Warning: Could not save credentials to '{TOKEN_FILE_PATH}': {e}. Continuing with in-memory creds.")

        except (pickle.UnpicklingError, EOFError, IOError) as e:
            print(
                f"Warning: '{TOKEN_PICKLE_PATH}' is corrupted or unreadable: {e}. Deleting and attempting re-authentication.")
            if os.path.exists(TOKEN_PICKLE_PATH):
                os.remove(TOKEN_PICKLE_PATH)
            creds = None
        except Exception as e:
            print(f"Error loading credentials from '{TOKEN_PICKLE_PATH}': {e}. Attempting re-authentication.")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                print("Refreshed expired access token.")
            except Exception as e:
                print(f"Error refreshing token: {e}. Forcing full re-authentication.")
                creds = None
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CREDENTIALS_FILE_PATH, SCOPES)
                print(f"Opening browser for authentication. Ensure '{CREDENTIALS_FILE_PATH}' is correct.")
                creds = flow.run_local_server(port=0)
            except FileNotFoundError:
                print(f"Critical Error: '{CREDENTIALS_FILE_PATH}' not found. Please ensure it's at the specified path.")
                return None
            except Exception as e:
                print(
                    f"Error during OAuth flow: {e}. Please check your internet connection or Google Cloud project setup.")
                return None

        if creds:
            try:
                with open(TOKEN_FILE_PATH, 'w') as token:
                    token.write(creds.to_json())
                print(f"New credentials saved to '{TOKEN_FILE_PATH}'.")
            except Exception as e:
                print(f"Warning: Could not save new credentials to '{TOKEN_FILE_PATH}': {e}.")

    if creds:
        try:
            service = build('drive', 'v3', credentials=creds)
            print("Google Drive API service built successfully.")
            return service
        except HttpError as error:
            print(f"An HTTP error occurred while building the service: {error}")
            return None
    return None


def find_drive_folder(service, folder_name: str) -> str | None:
    try:
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = service.files().list(
            q=query,
            spaces='drive',
            corpus='user',
            fields="files(id, name)"
        ).execute()

        items = results.get('files', [])
        if items:
            print(f"Found folder '{folder_name}' with ID: {items[0]['id']}")
            return items[0]['id']
        else:
            print(
                f"Folder '{folder_name}' not found on Google Drive (including shared folders). Please ensure the name is exact and you have access.")
            return None
    except HttpError as error:
        print(f"An HTTP error occurred while searching for folder: {error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while searching for folder: {e}")
        return None


def upload_file_to_drive_folder(service, local_file_path: str, folder_id: str) -> str | None:
    if not os.path.exists(local_file_path):
        print(f"Error: Local file '{local_file_path}' not found.")
        return None

    file_name = os.path.basename(local_file_path)
    mime_type, _ = mimetypes.guess_type(local_file_path)
    if mime_type is None:
        mime_type = 'application/octet-stream'

    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(local_file_path, mimetype=mime_type, resumable=True)

    try:
        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name'
        ).execute()

        print(f"Successfully uploaded '{uploaded_file.get('name')}' (ID: {uploaded_file.get('id')}) to Google Drive.")
        return uploaded_file.get('id')
    except HttpError as error:
        print(f"An HTTP error occurred during file upload: {error}")
        print("Please check file permissions on Google Drive or the folder ID.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during file upload: {e}")
        return None

