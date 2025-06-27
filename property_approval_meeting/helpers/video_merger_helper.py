import os
import subprocess
import json
import shutil  # For robust directory removal


def get_video_stream_info(video_path: str):
    """
    Gets essential video and audio stream information using ffprobe.
    Returns (video_info_dict, audio_info_dict) or (None, None) if probing fails.
    """
    try:
        # Probe video stream
        video_command = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",  # Select the first video stream
            "-show_entries", "stream=codec_name,width,height,avg_frame_rate,pix_fmt",
            "-of", "json",
            video_path
        ]
        video_probe_result = subprocess.run(video_command, check=True, capture_output=True, text=True)
        video_info = json.loads(video_probe_result.stdout)
        video_stream = video_info['streams'][0] if video_info and 'streams' in video_info else None

        # Probe audio stream
        audio_command = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "a:0",  # Select the first audio stream
            "-show_entries", "stream=codec_name,sample_rate,channels",
            "-of", "json",
            video_path
        ]
        audio_probe_result = subprocess.run(audio_command, check=True, capture_output=True, text=True)
        audio_info = json.loads(audio_probe_result.stdout)
        audio_stream = audio_info['streams'][0] if audio_info and 'streams' in audio_info else None

        return video_stream, audio_stream
    except (subprocess.CalledProcessError, json.JSONDecodeError, IndexError) as e:
        print(f"Warning: Could not get stream info for '{os.path.basename(video_path)}'. Error: {e}")
        print(f"  FFprobe stdout: {video_probe_result.stdout if 'video_probe_result' in locals() else ''}")
        print(f"  FFprobe stderr: {video_probe_result.stderr if 'video_probe_result' in locals() else ''}")
        return None, None
    except FileNotFoundError:
        print(
            "Error: 'ffprobe' command not found. Please ensure FFmpeg (which includes ffprobe) is installed and in your system's PATH.")
        return None, None
    except Exception as e:
        print(f"An unexpected error occurred during ffprobe for '{os.path.basename(video_path)}': {e}")
        return None, None


def merge_videos_in_folder(input_folder: str, output_filename: str = "merged_video.mp4") -> bool:
    if not os.path.isdir(input_folder):
        print(f"Error: Input folder '{input_folder}' not found.")
        return False

    video_extensions = ('.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv')
    video_files = []
    for filename in os.listdir(input_folder):
        if filename.lower().endswith(video_extensions):
            video_files.append(os.path.join(input_folder, filename))
    video_files.sort()  # Ensure consistent order

    if not video_files:
        print(f"No video files found in '{input_folder}' with extensions {video_extensions}.")
        return False

    print(f"Found {len(video_files)} video files to merge:")
    for f in video_files:
        print(f"- {os.path.basename(f)}")

    # --- Define Target Normalization Profile ---
    # These are common, widely compatible settings. Adjust as needed for your specific use case.
    TARGET_WIDTH = 1280
    TARGET_HEIGHT = 720
    TARGET_FPS = 30
    TARGET_PIX_FMT = 'yuv420p'
    TARGET_VIDEO_CODEC = 'libx264'
    TARGET_AUDIO_CODEC = 'aac'
    TARGET_AUDIO_SAMPLE_RATE = '48000'  # Hz
    TARGET_AUDIO_CHANNELS = 2  # Stereo

    temp_normalized_folder = os.path.join(input_folder, "temp_normalized_videos")
    if os.path.exists(temp_normalized_folder):
        shutil.rmtree(temp_normalized_folder)  # Clean up previous temp folder if it exists
    os.makedirs(temp_normalized_folder)
    print(f"\nCreated temporary normalization folder: {temp_normalized_folder}")

    normalized_video_paths = []
    print("\nStarting video normalization process...")

    for original_file_path in video_files:
        base_name = os.path.basename(original_file_path)
        temp_output_file_path = os.path.join(temp_normalized_folder, f"normalized_{base_name}")

        video_info, audio_info = get_video_stream_info(original_file_path)

        # Assume normalization is needed if info is missing or doesn't match target
        needs_normalization = True
        if video_info and audio_info:
            try:
                # Convert frame rate string "num/den" to float for comparison
                current_fps_str = video_info.get('avg_frame_rate', '0/1')
                current_fps = float(current_fps_str.split('/')[0]) / float(
                    current_fps_str.split('/')[1]) if '/' in current_fps_str else float(current_fps_str)

                # Check if current video matches target profile (allowing for minor FPS float discrepancies)
                if (video_info.get('codec_name') == TARGET_VIDEO_CODEC and
                        int(video_info.get('width', 0)) == TARGET_WIDTH and
                        int(video_info.get('height', 0)) == TARGET_HEIGHT and
                        video_info.get('pix_fmt') == TARGET_PIX_FMT and
                        abs(current_fps - TARGET_FPS) < 0.001 and  # Compare float FPS
                        audio_info.get('codec_name') == TARGET_AUDIO_CODEC and
                        audio_info.get('sample_rate') == TARGET_AUDIO_SAMPLE_RATE and
                        audio_info.get('channels') == TARGET_AUDIO_CHANNELS):
                    needs_normalization = False
            except (ValueError, ZeroDivisionError, TypeError) as e:
                print(f"  Warning: Error parsing stream info for '{base_name}': {e}. Will normalize.")
                needs_normalization = True
            except Exception as e:
                print(f"  An unexpected error occurred during info check for '{base_name}': {e}. Will normalize.")
                needs_normalization = True

        if needs_normalization:
            print(f"  Normalizing '{base_name}'...")
            # Video filter for scaling, padding, and FPS
            # scale: Resizes to target, force_original_aspect_ratio=decrease ensures it fits without stretching
            # pad: Adds black bars to fill the target resolution if aspect ratio changed
            # fps: Sets the output frame rate
            normalize_command = [
                "ffmpeg",
                "-i", original_file_path,
                "-c:v", TARGET_VIDEO_CODEC,
                "-preset", "medium",
                "-crf", "23",
                "-vf",
                f"scale={TARGET_WIDTH}:{TARGET_HEIGHT}:force_original_aspect_ratio=decrease,pad={TARGET_WIDTH}:{TARGET_HEIGHT}:(ow-iw)/2:(oh-ih)/2,fps={TARGET_FPS}",
                "-c:a", TARGET_AUDIO_CODEC,
                "-b:a", "128k",  # Consistent audio bitrate
                "-ar", TARGET_AUDIO_SAMPLE_RATE,
                "-ac", str(TARGET_AUDIO_CHANNELS),
                "-pix_fmt", TARGET_PIX_FMT,
                "-y",  # Overwrite if temp file exists
                temp_output_file_path
            ]
            try:
                subprocess.run(normalize_command, check=True, capture_output=True, text=True)
                normalized_video_paths.append(temp_output_file_path)
                print(f"    Normalized to: {os.path.basename(temp_output_file_path)}")
            except subprocess.CalledProcessError as e:
                print(f"    ERROR normalizing '{base_name}':")
                print(f"    FFmpeg stdout: {e.stdout}")
                print(f"    FFmpeg stderr: {e.stderr}")
                print("    Skipping this file due to normalization error.")
                # Decide if you want to fail the whole merge or skip problematic files
                return False  # Fail the whole merge if one normalization fails
        else:
            print(f"  '{base_name}' is already normalized. Using original file.")
            normalized_video_paths.append(original_file_path)

    if not normalized_video_paths:
        print("No videos successfully normalized or found to merge.")
        return False

    # Create concat list using the normalized (or original compatible) video paths
    list_file_path = os.path.join(input_folder, "ffmpeg_concat_list.txt")
    try:
        with open(list_file_path, "w") as f:
            for video_file in normalized_video_paths:
                f.write(f"file '{video_file}'\n")
        print(f"\nCreated FFmpeg concat list file: {list_file_path}")
    except Exception as e:
        print(f"Error creating FFmpeg concat list file: {e}")
        return False

    output_path = output_filename
    if not os.path.isabs(output_path) and not os.path.dirname(output_path):
        output_path = os.path.join(os.getcwd(), output_filename)
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"\nMerging and saving final video to: {output_path}")

    # Final merge command (re-encoding to ensure consistent output quality)
    ffmpeg_command = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file_path,
        "-c:v", TARGET_VIDEO_CODEC,
        "-preset", "medium",
        "-crf", "23",
        "-c:a", TARGET_AUDIO_CODEC,
        "-b:a", "128k",
        "-pix_fmt", TARGET_PIX_FMT,
        "-y",
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
        print(f"An error occurred during final video merging with FFmpeg: {e}")
        print(f"FFmpeg stdout: {e.stdout}")
        print(f"FFmpeg stderr: {e.stderr}")
        print("\nTroubleshooting tips:")
        print("1. Ensure FFmpeg is installed and its 'bin' directory is in your system's PATH.")
        print("2. Check the FFmpeg stderr output above for specific error messages from FFmpeg itself.")
        print("3. Verify that your original input video files are not corrupted and can be played individually.")
        return False
    except FileNotFoundError:
        print(
            "Error: 'ffmpeg' or 'ffprobe' command not found. Please ensure FFmpeg is installed and in your system's PATH.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False
    finally:
        # Cleanup temporary files and folders
        if os.path.exists(list_file_path):
            try:
                os.remove(list_file_path)
                print(f"Cleaned up temporary concat list file: {list_file_path}")
            except Exception as e:
                print(f"Warning: Could not remove temporary list file '{list_file_path}': {e}")
        if os.path.exists(temp_normalized_folder):
            try:
                shutil.rmtree(temp_normalized_folder)
                print(f"Cleaned up temporary normalization folder: {temp_normalized_folder}")
            except Exception as e:
                print(f"Warning: Could not remove temporary folder '{temp_normalized_folder}': {e}")


if __name__ == "__main__":
    test_input_folder = "test_input_videos"
    output_file = "output_merged_reencoded_final.mp4"

    # Clean up previous test runs
    if os.path.exists(test_input_folder):
        shutil.rmtree(test_input_folder)
    if os.path.exists(output_file):
        os.remove(output_file)

    os.makedirs(test_input_folder)
    print(f"Created dummy input folder: {test_input_folder}")

    try:
        print("\nCreating dummy video 1 (3 seconds, 640x480, 25fps)...")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=3:s=640x480:r=25", "-c:v", "libx264", "-pix_fmt",
             "yuv420p", "-an", os.path.join(test_input_folder, "video1_sd_25fps.mp4")], check=True, capture_output=True)

        print("\nCreating dummy video 2 (3 seconds, 1920x1080, 30fps)...")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=3:s=1920x1080:r=30", "-c:v", "libx264", "-pix_fmt",
             "yuv420p", "-an", os.path.join(test_input_folder, "video2_hd_30fps.mp4")], check=True, capture_output=True)

        print("\nCreating dummy video 3 (3 seconds, 800x600, 20fps, different audio)...")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=3:s=800x600:r=20,sine=d=3", "-c:v", "libx264", "-pix_fmt",
             "yuv420p", "-c:a", "aac", "-ar", "44100", "-ac", "1",
             os.path.join(test_input_folder, "video3_custom_audio.mp4")], check=True, capture_output=True)

        print("\nDummy video files created in 'test_input_videos'.")
    except subprocess.CalledProcessError as e:
        print(f"Could not create dummy videos. Ensure FFmpeg is installed and in your PATH. Error: {e.stderr.decode()}")
        print("Please ensure you can run 'ffmpeg -version' in your terminal.")
        exit(1)  # Exit if dummy videos can't be created to avoid further errors

    success = merge_videos_in_folder(test_input_folder, output_file)

    if success:
        print(f"\nSuccessfully created {output_file} in {os.getcwd()}")
        print("Try playing this file with a robust media player like VLC.")
    else:
        print(f"\nVideo merging failed. Check the error messages above for details.")

    print(f"\nFinal cleanup of dummy folder '{test_input_folder}'...")
    if os.path.exists(test_input_folder):
        shutil.rmtree(test_input_folder)
    print("Cleanup complete.")
