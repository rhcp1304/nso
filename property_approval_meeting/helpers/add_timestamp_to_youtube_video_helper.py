from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_SECRETS_FILE = os.path.join(BASE_DIR, 'youtube_credentials.json')
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"


def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_local_server(port=0)
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


def add_timestamps_to_video(video_id, new_timestamps):
    youtube = get_authenticated_service()
    try:
        list_response = youtube.videos().list(
            id=video_id,
            part="snippet"
        ).execute()

        if not list_response.get("items"):
            print("Video not found.")
            return False

        video_item = list_response["items"][0]
        video_snippet = video_item["snippet"]
        current_description = video_snippet.get("description", "")
        print("--- Current Description ---")
        print(current_description)
        timestamps_text = "\n\n" + "\n".join(new_timestamps)
        updated_description = current_description + timestamps_text
        video_snippet["description"] = updated_description
        update_request = youtube.videos().update(
            part="snippet",
            body={
                "id": video_id,
                "snippet": video_snippet
            }
        )
        update_response = update_request.execute()
        print("\n--- Update Successful ---")
        print("Video title:", update_response["snippet"]["title"])
        print("New description begins with:", update_response["snippet"]["description"][:100], "...")
        return True

    except HttpError as e:
        print(f"An HTTP error occurred: {e.resp.status}")
        print(f"Details: {e.content.decode()}")
        return False

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False