import os
import re
import requests
import subprocess
from urllib.parse import urlparse, parse_qs
import io
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']  # Read-only access for downloading
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GDRIVE_CREDENTIALS_FILE = os.path.join(BASE_DIR, 'credentials.json')
GDRIVE_TOKEN_FILE = os.path.join(BASE_DIR, 'token.json')


def get_gdrive_authenticated_service():
    creds = None
    if os.path.exists(GDRIVE_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(GDRIVE_TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                print("Refreshed expired Google Drive access token.")
            except Exception as e:
                print(f"Error refreshing Google Drive token: {e}. Forcing full re-authentication.")
                creds = None
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    GDRIVE_CREDENTIALS_FILE, SCOPES)
                print(
                    f"Opening browser for Google Drive authentication. Ensure '{GDRIVE_CREDENTIALS_FILE}' is present.")
                creds = flow.run_local_server(port=0)
            except FileNotFoundError:
                print(f"Error: '{GDRIVE_CREDENTIALS_FILE}' not found. Cannot authenticate Google Drive.")
                return None
            except Exception as e:
                print(f"Error during Google Drive OAuth flow: {e}. Check internet/setup.")
                return None

        if creds:
            with open(GDRIVE_TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
            print(f"New Google Drive credentials saved to '{GDRIVE_TOKEN_FILE}'.")

    if creds:
        try:
            service = build('drive', 'v3', credentials=creds)
            return service
        except HttpError as error:
            print(f"An HTTP error occurred while building Google Drive service: {error}")
            return None
    return None


def download_youtube_vimeo_etc(url: str, output_dir: str):
    print(f"  Attempting to download with yt-dlp: {url}")
    try:
        command = ['yt-dlp', url, '-o', '%(title)s.%(ext)s', '-P', output_dir, '--no-part']
        subprocess.run(command, check=True)
        print(f"  Successfully downloaded: {url}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Error downloading {url} with yt-dlp: {e}")
        print(f"  yt-dlp output: {e.output}")
    except FileNotFoundError:
        print("  Error: yt-dlp command not found. Please ensure yt-dlp is installed and in your system's PATH.")
    except Exception as e:
        print(f"  An unexpected error occurred during yt-dlp download: {e}")
    return False


def download_google_drive_video(service, url: str, output_dir: str):
    print(f"  Attempting to download Google Drive video: {url}")
    parsed_url = urlparse(url)
    file_id = None

    if 'drive.google.com' in parsed_url.netloc:
        if '/file/d/' in parsed_url.path:
            match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', parsed_url.path)
            if match:
                file_id = match.group(1)
        elif '/open?id=' in parsed_url.query:
            query_params = parse_qs(parsed_url.query)
            if 'id' in query_params:
                file_id = query_params['id'][0]
        elif '/folders/' in parsed_url.path:
            print("  Warning: This looks like a Google Drive folder link, not a file link. Skipping.")
            return False

    if not file_id:
        print(f"  Could not extract Google Drive file ID from URL: {url}")
        return False

    if not service:
        print("  Google Drive API service not authenticated. Skipping Google Drive download.")
        return False

    try:
        file_metadata = service.files().get(fileId=file_id, fields='name, mimeType').execute()
        file_name = file_metadata.get('name')

        if not file_name:
            print(f"  Could not get filename for Google Drive ID: {file_id}. Skipping.")
            return False

        destination_path = os.path.join(output_dir, file_name)

        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(destination_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False

        while done is False:
            status, done = downloader.next_chunk()
            if status:
                print(f"  Download Progress ({file_name}): {int(status.progress() * 100)}%")
        print(f"  Successfully downloaded Google Drive file: {file_name}")
        return True
    except HttpError as error:
        print(f"  Error downloading Google Drive file {file_id}: {error}")
        if error.resp.status == 404:
            print("  Error: Google Drive file not found or you don't have permission to access it.")
        elif error.resp.status == 403:
            print("  Error: Google Drive permission denied. Ensure the file is shared or your account has access.")
    except Exception as e:
        print(f"  An unexpected error occurred during Google Drive download: {e}")
    return False


def download_generic_video(url: str, output_dir: str):
    print(f"  Attempting to download generic video: {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        if 'Content-Disposition' in response.headers:
            fname_match = re.search(r'filename\*?=(?:UTF-8\'\')?([^;]+)', response.headers['Content-Disposition'])
            if fname_match:
                filename = fname_match.group(1).strip('\'"')
            else:
                filename = os.path.basename(urlparse(url).path)
        else:
            filename = os.path.basename(urlparse(url).path)

        if not filename or '.' not in filename:
            filename = f"downloaded_video_{os.urandom(4).hex()}.mp4"  # Fallback filename

        destination_path = os.path.join(output_dir, filename)

        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  Successfully downloaded generic video to: {destination_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  Error downloading generic video from {url}: {e}")
    except Exception as e:
        print(f"  An unexpected error occurred during generic download: {e}")
    return False