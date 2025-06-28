import win32com.client as win32
import os
import time


def export_ppt_to_video(ppt_path: str, output_video_path: str, resolution: str = "1080p", frame_rate: int = 24):
    """
    Exports a PowerPoint presentation to a video file using COM automation.

    Args:
        ppt_path (str): The full path to the input PowerPoint presentation (.pptx or .ppt).
        output_video_path (str): The full path for the output video file (.mp4 or .wmv).
        resolution (str): The desired video resolution. Options:
                          "4K" (3840x2160), "1080p" (1920x1080), "720p" (1280x720),
                          "480p" (852x480). Default is "1080p".
        frame_rate (int): The desired frame rate (e.g., 24, 25, 30, 60). Default is 24.
                          PowerPoint usually supports 24, 25, 30, 60.
    """
    if not os.path.exists(ppt_path):
        print(f"Error: PowerPoint file not found at '{ppt_path}'")
        return False

    # Ensure output directory exists
    output_dir = os.path.dirname(output_video_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # PowerPoint export quality mapping
    # See https://learn.microsoft.com/en-us/office/vba/api/powerpoint.ppmediaresolutions
    resolution_map = {
        "4K": 8,  # ppMediaResolutionType.ppMediaResolution4K
        "1080p": 5,  # ppMediaResolutionType.ppMediaResolutionFullHD
        "720p": 4,  # ppMediaResolutionType.ppMediaResolutionHD
        "480p": 2  # ppMediaResolutionType.ppMediaResolutionStandard (often 852x480)
    }

    # Default to 1080p if resolution is not recognized
    export_resolution = resolution_map.get(resolution.upper(), 5)  # Default to Full HD

    powerpoint = None
    presentation = None
    try:
        powerpoint = win32.Dispatch("PowerPoint.Application")
        powerpoint.Visible = False  # Keep PowerPoint window hidden
        powerpoint.DisplayAlerts = False  # Suppress alerts

        print(f"Opening PowerPoint presentation: {ppt_path}")
        presentation = powerpoint.Presentations.Open(ppt_path, WithWindow=False)

        # Check if export is already in progress from a previous run
        # This is a basic check, actual status tracking is more complex
        if os.path.exists(output_video_path) and os.path.getsize(output_video_path) > 0:
            print(f"Warning: Output file '{output_video_path}' already exists and is non-empty. "
                  "It might be from a previous incomplete export. Deleting and re-exporting.")
            try:
                os.remove(output_video_path)
            except OSError as e:
                print(f"Error: Could not delete existing output file. Please close any programs using it. {e}")
                return False

        print(f"Exporting '{os.path.basename(ppt_path)}' to video at {resolution} ({frame_rate} fps)...")
        # ExportAsFixedFormat method for video export
        # Ref: https://learn.microsoft.com/en-us/office/vba/api/powerpoint.presentation.exportasfixedformat
        # FileFormat parameter for video is 37 (ppFixedFormatType.ppFixedFormatMP4) or 38 (ppFixedFormatType.ppFixedFormatWMV)
        # However, for video, it's often more straightforward to use SaveAs method for direct video export

        # Powerpoint 2013+ supports direct MP4 export
        # https://learn.microsoft.com/en-us/office/vba/api/powerpoint.ppsaveasfiletype
        # ppSaveAsMP4 (39)
        # ppSaveAsWMV (38)

        # The ExportAsFixedFormat is for PDF/XPS. For video, newer versions use SaveAs with specific file types.
        # However, older VBA examples and some documentation might point to ExportAsFixedFormat for video too,
        # which can be confusing.
        # The correct method for video export is typically `CreateVideo`.
        # https://learn.microsoft.com/en-us/office/vba/api/PowerPoint.Presentation.CreateVideo

        # Set video creation parameters
        presentation.CreateVideo(
            FileName=output_video_path,
            UseTimingsAndNarrations=True,  # Use timings set in PPT, or automatic if none
            DefaultSlideDuration=5,  # Default duration for slides without specific timings (seconds)
            VertRes=export_resolution,  # Resolution quality
            FramesPerSecond=frame_rate,  # FPS
            Quality=100  # Not always explicitly used but good to set (percentage)
        )

        # Wait for the video export to complete.
        # This is crucial as CreateVideo is asynchronous.
        # A simple busy-wait loop checking file existence/size increase.
        print("Exporting... This may take a while for large presentations.")
        initial_size = -1
        max_wait_time = 3600  # Maximum 1 hour wait for export to finish
        start_time = time.time()

        while True:
            time.sleep(5)  # Check every 5 seconds
            current_size = os.path.getsize(output_video_path) if os.path.exists(output_video_path) else 0

            if current_size > initial_size and initial_size != -1:
                # File is growing, export is in progress
                initial_size = current_size
                print(f"  Video file growing: {current_size / (1024 * 1024):.2f} MB")
            elif current_size == initial_size and initial_size != -1:
                # File size hasn't changed, export might be complete
                print(f"  Video file size stabilized: {current_size / (1024 * 1024):.2f} MB. Assuming export complete.")
                break
            elif not os.path.exists(output_video_path) and time.time() - start_time > 30:
                # File hasn't even appeared after 30 seconds, something is wrong
                print("Error: Output video file not appearing after 30 seconds. Export might have failed silently.")
                return False
            elif time.time() - start_time > max_wait_time:
                print(f"Error: Video export timed out after {max_wait_time} seconds.")
                return False
            else:
                # First check, or file hasn't appeared yet
                initial_size = current_size
                print(f"  Waiting for video file to start appearing...")

        print(f"Successfully exported '{os.path.basename(ppt_path)}' to '{output_video_path}'")
        return True

    except Exception as e:
        print(f"An error occurred during PowerPoint export: {e}")
        return False
    finally:
        if presentation:
            presentation.Close()
        if powerpoint:
            powerpoint.Quit()
            # Clean up COM objects
            powerpoint = None
            presentation = None
            # Optionally, release COM objects more aggressively (sometimes needed)
            # import pythoncom
            # pythoncom.CoUninitialize()


# --- Example Usage ---
if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_ppt_file = os.path.join(current_dir, "my_presentation.pptx")  # Change this to your PPT file
    output_video_file = os.path.join(current_dir, "output_video.mp4")

    # --- Create a dummy PowerPoint file for testing if it doesn't exist ---
    if not os.path.exists(input_ppt_file):
        print(f"Creating a dummy PowerPoint file at {input_ppt_file} for testing...")
        try:
            powerpoint = win32.Dispatch("PowerPoint.Application")
            powerpoint.Visible = True
            powerpoint.DisplayAlerts = False

            presentation = powerpoint.Presentations.Add()
            slide1 = presentation.Slides.Add(1, 12)  # ppLayoutTitleOnly
            title1 = slide1.Shapes.Title
            title1.TextFrame.TextRange.Text = "Welcome to My Presentation!"
            slide1.Shapes.AddTextbox(1, 100, 200, 400, 50).TextFrame.TextRange.Text = "This is a test slide."

            slide2 = presentation.Slides.Add(2, 12)  # ppLayoutTitleOnly
            title2 = slide2.Shapes.Title
            title2.TextFrame.TextRange.Text = "Second Slide"
            slide2.Shapes.AddTextbox(1, 100, 200, 400, 50).TextFrame.TextRange.Text = "More content here."

            # Set slide duration (optional, but good for video)
            # This is specific to the Slide object, not the overall presentation timing
            # presentation.Slides[0].SlideShowTransition.AdvanceTime = 3 # 3 seconds
            # presentation.Slides[0].SlideShowTransition.AdvanceOnTime = True

            presentation.SaveAs(input_ppt_file)
            print("Dummy PowerPoint created successfully.")
            presentation.Close()
            powerpoint.Quit()
        except Exception as e:
            print(f"Could not create dummy PowerPoint file. Ensure PowerPoint is installed. Error: {e}")
            input_ppt_file = None  # Prevent export attempt if dummy creation failed

    if input_ppt_file and os.path.exists(input_ppt_file):
        print("\n--- Starting PowerPoint to Video Export ---")
        success = export_ppt_to_video(input_ppt_file, output_video_file, resolution="1080p", frame_rate=25)
        if success:
            print("\nPowerPoint to video export completed successfully!")
            print(f"Check output video at: {output_video_file}")
        else:
            print("\nPowerPoint to video export failed.")
    else:
        print("\nSkipping export: No valid input PowerPoint file available.")