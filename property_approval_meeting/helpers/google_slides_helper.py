import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import uuid

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/presentations'
]

def authenticate_google_api_user(client_secret_path, token_path):
    creds = None
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            print("Loaded credentials from token.json.")
        except Exception as e:
            print(f"Error loading token.json: {e}. Re-authenticating.")
            creds = None

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing access token...")
            try:
                creds.refresh(InstalledAppFlow.from_client_secrets_file(
                    client_secret_path, SCOPES).credentials)
                print("Access token refreshed.")
            except Exception as e:
                print(f"Error refreshing token: {e}. Re-authenticating.")
                creds = None
        if not creds or not creds.valid:
            print(f"Opening browser for authentication. Please authorize the application.")
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secret_path, SCOPES
            )
            creds = flow.run_local_server(port=0)
            print("Authentication successful.")
        # Save the credentials for the next run
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
            print(f"Credentials saved to {token_path}")

    try:
        drive_service = build('drive', 'v3', credentials=creds)
        slides_service = build('slides', 'v1', credentials=creds)
        print("Successfully built Google API services.")
        return drive_service, slides_service
    except Exception as e:
        print(f"Failed to build API services: {e}")
        return None, None


def create_slides_from_folder(folder_id, client_secret_path, token_path):
    drive_service, slides_service = authenticate_google_api_user(client_secret_path, token_path)
    if not drive_service or not slides_service:
        return None

    ppt_file_id = None
    google_slides_id = None
    video_files = []
    presentation_title = "Generated Presentation"

    try:
        # List files in the specified Google Drive folder
        print(f"Listing files in Google Drive folder: {folder_id}...")
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="files(id, name, mimeType)"
        ).execute()
        items = results.get('files', [])

        if not items:
            print(f"No files found in folder: {folder_id}")
            return None

        # Categorize files: find PPT/Google Slides and videos
        for item in items:
            mime_type = item['mimeType']
            if mime_type in [
                'application/vnd.openxmlformats-officedocument.presentationml.presentation',  # .pptx
                'application/vnd.ms-powerpoint'  # .ppt
            ]:
                ppt_file_id = item['id']
                presentation_title = item['name'].replace('.pptx', '').replace('.ppt', '') + " (Converted)"
                print(f"Found PowerPoint file: '{item['name']}' (ID: {item['id']})")
            elif mime_type == 'application/vnd.google-apps.presentation':
                google_slides_id = item['id']
                presentation_title = item['name']
                print(f"Found existing Google Slides file: '{item['name']}' (ID: {item['id']})")
            elif mime_type.startswith('video/'):
                video_files.append({'id': item['id'], 'name': item['name']})
                print(f"Found video file: '{item['name']}' (ID: {item['id']})")

        target_presentation_id = None

        if ppt_file_id:
            # Convert PPT to Google Slides
            print(f"Converting PowerPoint '{presentation_title}' to Google Slides...")
            copied_file_metadata = {
                'name': presentation_title,
                'mimeType': 'application/vnd.google-apps.presentation',
                'parents': [folder_id]  # Keep it in the same folder
            }
            copied_file = drive_service.files().copy(
                fileId=ppt_file_id,
                body=copied_file_metadata
            ).execute()
            target_presentation_id = copied_file.get('id')
            print(f"Conversion complete. New Google Slides ID: {target_presentation_id}")
        else:
            print("No PowerPoint or existing Google Slides presentation found in the folder.")
            # Option: Create a new blank presentation if no PPT is found
            print("Creating a new blank Google Slides presentation...")
            new_presentation_body = {
                'title': "New Blank Presentation (Generated)",
                'parents': [folder_id]
            }
            new_presentation = slides_service.presentations().create(body=new_presentation_body).execute()
            target_presentation_id = new_presentation.get('presentationId')
            print(f"Created blank presentation with ID: {target_presentation_id}")

        if not target_presentation_id:
            print("Failed to identify or create a Google Slides presentation.")
            return None

        # Now, add videos to the presentation
        if not video_files:
            print("No video files found to insert.")
            return target_presentation_id

        print(f"Inserting {len(video_files)} videos into the presentation...")
        requests = []
        for video in video_files:
            # Create a new blank slide for each video
            new_slide_object_id = f"slide_{uuid.uuid4().hex}"
            requests.append({
                'createSlide': {
                    'objectId': new_slide_object_id,
                    'insertionIndex': 0  # Inserts at the end (0 means after all existing slides)
                }
            })
            print(f"Added new slide for video: {video['name']}")

            # Insert the video onto the newly created slide
            video_element_id = f"video_{uuid.uuid4().hex}"
            requests.append({
                'createVideo': {
                    'objectId': video_element_id,
                    'source': 'DRIVE',
                    'id': video['id'],
                    'elementProperties': {
                        'pageObjectId': new_slide_object_id,
                        'size': {  # Approximate size for a standard slide (16:9 aspect ratio)
                            'width': {'magnitude': 9144000, 'unit': 'EMU'},  # 10 inches
                            'height': {'magnitude': 5143500, 'unit': 'EMU'}  # ~5.625 inches
                        },
                        'transform': {  # Center the video
                            'scaleX': 1,
                            'scaleY': 1,
                            'translateX': 0,
                            'translateY': 0,
                            'unit': 'EMU'
                        }
                    }
                }
            })
            print(f"Added video '{video['name']}' to its slide.")

        # Execute all batch update requests
        if requests:
            slides_service.presentations().batchUpdate(
                presentationId=target_presentation_id,
                body={'requests': requests}
            ).execute()
            print("All videos inserted successfully.")
        else:
            print("No video insertion requests were generated.")

        print(f"Process complete. Google Slides Presentation ID: {target_presentation_id}")
        return target_presentation_id

    except HttpError as error:
        print(f"An API error occurred: {error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None