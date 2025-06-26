import os
import re
import requests
import subprocess
from urllib.parse import urlparse, parse_qs
import io

# Google API specific imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# --- Configuration for Google Drive API ---
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']  # Read-only access for downloading
# Paths for Google API credentials (relative to this script's directory)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GDRIVE_CREDENTIALS_FILE = os.path.join(BASE_DIR, 'credentials.json')
GDRIVE_TOKEN_FILE = os.path.join(BASE_DIR, 'token.json')


def get_gdrive_authenticated_service():
    """
    Authenticates with the Google Drive API using OAuth 2.0.
    Looks for token.json, then falls back to credentials.json to initiate OAuth flow.
    """
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
    """
    Downloads video using yt-dlp for YouTube, Vimeo, and other supported sites.
    """
    print(f"  Attempting to download with yt-dlp: {url}")
    try:
        # -o: output template (%(title)s.%(ext)s) saves with original title and extension
        # -P: parent directory (output_dir)
        # --no-part: download directly, no temporary .part files
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
    """
    Downloads a Google Drive video using the Google Drive API.
    Extracts file ID from common Google Drive URL formats.
    """
    print(f"  Attempting to download Google Drive video: {url}")
    parsed_url = urlparse(url)
    file_id = None

    # Handle common Google Drive URL patterns
    if 'drive.google.com' in parsed_url.netloc:
        if '/file/d/' in parsed_url.path:
            # e.g., https://drive.google.com/file/d/FILE_ID/view
            match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', parsed_url.path)
            if match:
                file_id = match.group(1)
        elif '/open?id=' in parsed_url.query:
            # e.g., https://drive.google.com/open?id=FILE_ID
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
        # Get file metadata to determine filename and mimeType
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
    """
    Downloads a video from a direct URL using requests.
    Attempts to derive filename from URL.
    """
    print(f"  Attempting to download generic video: {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Try to get filename from Content-Disposition header, then from URL path
        if 'Content-Disposition' in response.headers:
            fname_match = re.search(r'filename\*?=(?:UTF-8\'\')?([^;]+)', response.headers['Content-Disposition'])
            if fname_match:
                filename = fname_match.group(1).strip('\'"')
            else:
                filename = os.path.basename(urlparse(url).path)
        else:
            filename = os.path.basename(urlparse(url).path)

        # Basic cleanup for filename if it's too generic or empty
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


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Download videos from a list of URLs in a text file."
    )
    parser.add_argument(
        '--url_list_file',
        type=str,
        required=True,
        help="Path to the text file containing one URL per line."
    )
    parser.add_argument(
        '--output_dir',
        type=str,
        default='downloads',  # Default output directory
        help="Directory to save downloaded videos. Will be created if it doesn't exist."
    )

    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    print(f"Saving videos to: {os.path.abspath(args.output_dir)}")

    # Initialize Google Drive service once if needed
    gdrive_service = None
    if os.path.exists(GDRIVE_CREDENTIALS_FILE):
        print("\nAttempting to authenticate for Google Drive...")
        gdrive_service = get_gdrive_authenticated_service()
        if not gdrive_service:
            print("Google Drive API authentication failed. Google Drive links will not be downloaded.")
        else:
            print("Google Drive API service ready.")
    else:
        print("No 'credentials.json' found for Google Drive API. Google Drive links will not be downloaded.")

    # Read URLs from the input file
    urls_to_download = []
    try:
        with open(args.url_list_file, 'r', encoding='utf-8') as f:
            urls_to_download = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: URL list file not found at '{args.url_list_file}'")
        return
    except Exception as e:
        print(f"Error reading URL list file: {e}")
        return

    if not urls_to_download:
        print("No URLs found in the provided file.")
        return

    print(f"\nFound {len(urls_to_download)} URLs to process.")
    successful_downloads = 0

    for i, url in enumerate(urls_to_download):
        print(f"\n--- Processing URL {i + 1}/{len(urls_to_download)}: {url} ---")
        downloaded = False
        parsed_url = urlparse(url)

        # Determine download method based on URL domain
        if "youtube.com" in parsed_url.netloc or "youtu.be" in parsed_url.netloc or \
                "vimeo.com" in parsed_url.netloc or "dailymotion.com" in parsed_url.netloc:
            downloaded = download_youtube_vimeo_etc(url, args.output_dir)
        elif "drive.google.com" in parsed_url.netloc:
            if gdrive_service:
                downloaded = download_google_drive_video(gdrive_service, url, args.output_dir)
            else:
                print("  Skipping Google Drive link: Google Drive API not authenticated.")
        else:
            # Fallback for generic direct video links (e.g., .mp4, .mov, .webm)
            # You might want to refine this check
            if any(ext in parsed_url.path.lower() for ext in ['.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv']):
                downloaded = download_generic_video(url, args.output_dir)
            else:
                print(
                    f"  Warning: URL not recognized as a supported video platform or direct video link. Skipping: {url}")

        if downloaded:
            successful_downloads += 1

    print(f"\n--- Download Summary ---")
    print(f"Total URLs processed: {len(urls_to_download)}")
    print(f"Successful downloads: {successful_downloads}")
    print(f"Failed downloads: {len(urls_to_download) - successful_downloads}")
    print("------------------------")


if __name__ == "__main__":
    main()
