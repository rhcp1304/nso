import os
import subprocess

def merge_videos_in_folder(input_folder: str, output_filename: str = "merged_video.mp4") -> bool:
    if not os.path.isdir(input_folder):
        print(f"Error: Input folder '{input_folder}' not found.")
        return False
    video_extensions = ('.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv')
    video_files = []
    for filename in os.listdir(input_folder):
        if filename.lower().endswith(video_extensions):
            video_files.append(os.path.join(input_folder, filename))
    video_files.sort()
    if not video_files:
        print(f"No video files found in '{input_folder}' with extensions {video_extensions}.")
        return False
    print(f"Found {len(video_files)} video files to merge:")
    for f in video_files:
        print(f"- {os.path.basename(f)}")
    list_file_path = os.path.join(input_folder, "ffmpeg_concat_list.txt")
    try:
        with open(list_file_path, "w") as f:
            for video_file in video_files:
                f.write(f"file '{video_file}'\n")
        print(f"Created FFmpeg concat list file: {list_file_path}")
    except Exception as e:
        print(f"Error creating FFmpeg concat list file: {e}")
        return False

    output_path = output_filename
    if not os.path.isabs(output_path) and not os.path.dirname(output_path):
        output_path = os.path.join(os.getcwd(), output_filename)
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"\nMerging and saving video to: {output_path}")

    ffmpeg_command = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",  # Allows files outside current directory, or with special chars
        "-i", list_file_path,
        "-c:v", "libx264",      # Force H.264 video encoding
        "-preset", "medium",    # Balance between speed and file size/quality
        "-crf", "23",           # Constant Rate Factor: 23 is a good general-purpose quality
        "-c:a", "aac",          # Force AAC audio encoding
        "-b:a", "128k",         # Set audio bitrate
        "-pix_fmt", "yuv420p",  # Ensure compatible pixel format
        "-y",                   # Overwrite output file if it exists
        output_path
    ]

    try:
        print(f"Executing FFmpeg command: {' '.join(ffmpeg_command)}")
        process = subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
        print("\nFFmpeg Output (stdout):")
        print(process.stdout)
        print("\nFFmpeg Errors (stderr):")
        print(process.stderr)
        print("\nVideo merging completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during video merging with FFmpeg: {e}")
        print(f"FFmpeg stdout: {e.stdout}")
        print(f"FFmpeg stderr: {e.stderr}")
        print("\nTroubleshooting tips:")
        print("1. Ensure FFmpeg is installed and its 'bin' directory is in your system's PATH (or temporarily set for the session).")
        print("2. Check the FFmpeg stderr output above for specific error messages from FFmpeg itself.")
        print("3. Verify that your input video files are not corrupted and can be played individually.")
        return False
    except FileNotFoundError:
        print("Error: 'ffmpeg' command not found. Please ensure FFmpeg is installed and in your system's PATH.")
        print("If you don't have admin access, remember to set the PATH for your user account or temporarily for the session.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False
    finally:
        if os.path.exists(list_file_path):
            try:
                os.remove(list_file_path)
                print(f"Cleaned up temporary list file: {list_file_path}")
            except Exception as e:
                print(f"Warning: Could not remove temporary list file '{list_file_path}': {e}")

if __name__ == "__main__":
    test_input_folder = "test_input_videos"
    if not os.path.exists(test_input_folder):
        os.makedirs(test_input_folder)
        print(f"Created dummy input folder: {test_input_folder}")

    try:
        print("Creating dummy video 1 (3 seconds)...")
        subprocess.run(["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=3:s=640x480:r=25", "-c:v", "libx264", "-pix_fmt", "yuv420p", os.path.join(test_input_folder, "video1.mp4")], check=True, capture_output=True)
        print("Creating dummy video 2 (3 seconds)...")
        subprocess.run(["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=3:s=640x480:r=25", "-c:v", "libx264", "-pix_fmt", "yuv420p", os.path.join(test_input_folder, "video2.mp4")], check=True, capture_output=True)
        print("Dummy video files created in 'test_input_videos'.")
    except subprocess.CalledProcessError as e:
        print(f"Could not create dummy videos. Ensure FFmpeg is installed and in your PATH. Error: {e.stderr.decode()}")
        print("Please ensure you can run 'ffmpeg -version' in your terminal.")
        exit() # Exit if dummy videos can't be created to avoid further errors

    success = merge_videos_in_folder(test_input_folder, "output_merged_reencoded_final.mp4")

    if success:
        print(f"\nSuccessfully created output_merged_reencoded_final.mp4 in {os.getcwd()}")
        print("Try playing this file with a robust media player like VLC.")
    else:
        print("\nVideo merging failed. Check the error messages above for details.")

    print(f"\nCleaning up dummy folder '{test_input_folder}'...")
    for f in os.listdir(test_input_folder):
        os.remove(os.path.join(test_input_folder, f))
    os.rmdir(test_input_folder)
    print("Cleanup complete.")
