import os
import subprocess
import json
import shutil
import concurrent.futures
import time

# --- FFmpeg Optimization Settings ---
# Adjust these values to prioritize speed.
# Presets: ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow, placebo.
# Faster presets reduce encoding time but may result in larger file sizes or slightly lower quality.
ENCODING_PRESET = "veryfast"
ENCODING_CRF = 28

# --- Multiprocessing Settings ---
# Set the maximum number of worker processes.
# IMPORTANT: Given your low available RAM (1.18 GB out of 16 GB),
# starting with a low number like 2 or 3 is highly recommended.
# If you have more RAM available, you can increase this closer to os.cpu_count().
MAX_WORKERS = 5

# --- Timeout Settings ---
# Maximum time (in seconds) to wait for ffprobe to complete for a single file.
FFPROBE_TIMEOUT = 120 # Increased from 60 to 120 seconds (2 minutes)
# Maximum time (in seconds) to wait for ffmpeg normalization to complete for a single file.
# Adjust based on average video length and complexity. 3600s = 1 hour.
FFMPEG_NORMALIZATION_TIMEOUT = 3600 # Increased from 600 to 3600 seconds (1 hour)
# Maximum time (in seconds) to wait for the final ffmpeg merge to complete.
FFMPEG_MERGE_TIMEOUT = 600 # Increased from 300 to 600 seconds (10 minutes)

# --- Failed Files Log Settings ---
FAILED_FILES_LOG = "unmerged_videos_log.txt"

def log_failed_video(video_path: str, reason: str):
    """Appends the path of a failed video and the reason to a log file."""
    with open(FAILED_FILES_LOG, "a") as f:
        f.write(f"{video_path} | Reason: {reason}\n")

def get_video_stream_info(video_path: str):
    """
    Gets essential video and audio stream information using ffprobe.
    Returns (video_info_dict, audio_info_dict) or (None, None) if probing fails.
    Audio stream can be None if no audio track is found.
    """
    video_probe_result = None
    audio_probe_result = None
    video_stream = None
    audio_stream = None

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
        video_probe_result = subprocess.run(video_command, check=True, capture_output=True, text=True, timeout=FFPROBE_TIMEOUT)
        video_info = json.loads(video_probe_result.stdout)
        video_stream = video_info['streams'][0] if video_info and 'streams' in video_info else None

        # Probe audio stream (allow it to be absent)
        audio_command = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "a:0",  # Select the first audio stream
            "-show_entries", "stream=codec_name,sample_rate,channels",
            "-of", "json",
            video_path
        ]
        audio_probe_result = subprocess.run(audio_command, check=True, capture_output=True, text=True, timeout=FFPROBE_TIMEOUT)
        audio_info = json.loads(audio_probe_result.stdout)
        # Check if 'streams' list is not empty before trying to access index 0
        audio_stream = audio_info['streams'][0] if audio_info and 'streams' in audio_info and len(audio_info['streams']) > 0 else None

        if not video_stream:
            reason = f"Could not find a video stream in '{os.path.basename(video_path)}'."
            print(f"Warning: {reason}")
            log_failed_video(video_path, reason)
            return None, None

        return video_stream, audio_stream
    except subprocess.TimeoutExpired:
        reason = f"ffprobe timed out after {FFPROBE_TIMEOUT}s for '{os.path.basename(video_path)}'"
        print(f"Warning: {reason}. It might be corrupted or too large to probe quickly.")
        log_failed_video(video_path, reason)
        return None, None
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        reason = f"Could not get stream info for '{os.path.basename(video_path)}'. Error: {e}"
        print(f"Warning: {reason}")
        if video_probe_result:
            print(f"  FFprobe stdout (video): {video_probe_result.stdout}")
            print(f"  FFprobe stderr (video): {video_probe_result.stderr}")
        if audio_probe_result:
            print(f"  FFprobe stdout (audio): {audio_probe_result.stdout}")
            print(f"  FFprobe stderr (audio): {audio_probe_result.stderr}")
        log_failed_video(video_path, reason)
        return None, None
    except FileNotFoundError:
        reason = "'ffprobe' command not found. Please ensure FFmpeg is installed and in your system's PATH."
        print(f"Error: {reason}")
        log_failed_video(video_path, reason)
        return None, None
    except Exception as e:
        reason = f"An unexpected error occurred during ffprobe for '{os.path.basename(video_path)}': {e}"
        print(f"An unexpected error occurred during ffprobe for '{os.path.basename(video_path)}': {e}")
        log_failed_video(video_path, reason)
        return None, None

def _normalize_single_video(original_file_path: str, temp_normalized_folder: str,
                            target_width: int, target_height: int, target_fps: int,
                            target_pix_fmt: str, target_video_codec: str,
                            target_audio_codec: str, target_audio_sample_rate: str,
                            target_audio_channels: int, encoding_preset: str, encoding_crf: int) -> str | None:
    """
    Normalizes a single video file to a target profile.
    Returns the path to the normalized video or None if normalization fails.
    If normalization fails, it logs the reason to the failed files log.
    """
    base_name = os.path.basename(original_file_path)
    temp_output_file_path = os.path.join(temp_normalized_folder, f"normalized_{base_name}")

    video_info, audio_info = get_video_stream_info(original_file_path)

    # If video_info is None, it means get_video_stream_info already logged an error and returned None.
    # We cannot proceed without video info.
    if video_info is None:
        return None

    needs_normalization = True
    # Check if current video matches target profile (allowing for minor FPS float discrepancies)
    # This check is for skipping normalization if already compliant.
    # The normalization command itself ensures the output is compliant.
    try:
        current_fps_str = video_info.get('avg_frame_rate', '0/1')
        current_fps = float(current_fps_str.split('/')[0]) / float(
            current_fps_str.split('/')[1]) if '/' in current_fps_str else float(current_fps_str)

        # Check video properties
        video_compliant = (
            video_info.get('codec_name') == target_video_codec and
            int(video_info.get('width', 0)) == target_width and
            int(video_info.get('height', 0)) == target_height and
            video_info.get('pix_fmt') == target_pix_fmt and
            abs(current_fps - target_fps) < 0.001
        )

        # Check audio properties only if audio_info is present
        audio_compliant = True # Assume compliant if no audio or if audio matches
        if audio_info:
            audio_compliant = (
                audio_info.get('codec_name') == target_audio_codec and
                audio_info.get('sample_rate') == target_audio_sample_rate and
                audio_info.get('channels') == target_audio_channels
            )
        else: # If no audio_info, it means source has no audio, so target should also have no audio (via -an)
            # This logic is handled by the ffmpeg command construction below.
            # For the 'needs_normalization' check, if source has no audio, and we're going to use -an, it's 'compliant' in that regard.
            pass # No specific check needed here, the command will handle it.

        if video_compliant and audio_compliant:
            needs_normalization = False
    except (ValueError, ZeroDivisionError, TypeError, AttributeError) as e:
        print(f"  Warning: Error parsing stream info for '{base_name}': {e}. Will normalize.")
        needs_normalization = True
    except Exception as e:
        print(f"  An unexpected error occurred during info check for '{base_name}': {e}. Will normalize.")
        needs_normalization = True

    if needs_normalization:
        print(f"  Normalizing '{base_name}'...")
        normalize_command = [
            "ffmpeg",
            "-i", original_file_path,
            "-c:v", target_video_codec,
            "-preset", encoding_preset,
            "-crf", str(encoding_crf),
            "-vf",
            f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2,fps={target_fps}",
            "-pix_fmt", target_pix_fmt,
            "-y",
            temp_output_file_path
        ]

        # Conditionally add audio parameters
        if audio_info:
            normalize_command.extend([
                "-c:a", target_audio_codec,
                "-b:a", "128k",
                "-ar", target_audio_sample_rate,
                "-ac", str(target_audio_channels),
            ])
        else:
            normalize_command.append("-an") # No audio stream in source, so output without audio

        try:
            subprocess.run(normalize_command, check=True, capture_output=True, text=True, timeout=FFMPEG_NORMALIZATION_TIMEOUT)
            print(f"    Normalized to: {os.path.basename(temp_output_file_path)}")
            return temp_output_file_path
        except subprocess.TimeoutExpired:
            reason = f"FFmpeg normalization timed out after {FFMPEG_NORMALIZATION_TIMEOUT}s"
            print(f"    ERROR: {reason} for '{base_name}'. Skipping this file.")
            log_failed_video(original_file_path, reason)
            return None
        except subprocess.CalledProcessError as e:
            reason = f"FFmpeg normalization failed. Error: {e.stderr.strip()}"
            print(f"    ERROR normalizing '{base_name}':")
            print(f"    FFmpeg stdout: {e.stdout}")
            print(f"    FFmpeg stderr: {e.stderr}")
            print(f"    Skipping this file due to normalization error.")
            log_failed_video(original_file_path, reason)
            return None
        except FileNotFoundError:
            reason = "'ffmpeg' command not found"
            print(f"Error: {reason}. Please ensure FFmpeg is installed and in your system's PATH.")
            log_failed_video(original_file_path, reason)
            return None
        except Exception as e:
            reason = f"An unexpected error occurred during normalization of '{base_name}': {e}"
            print(f"An unexpected error occurred during normalization of '{base_name}': {e}")
            log_failed_video(original_file_path, reason)
            return None
    else:
        print(f"  '{base_name}' is already normalized. Using original file.")
        return original_file_path

def _remove_temp_folder_robustly(path, retries=5, delay=1):
    """Attempts to remove a directory, retrying if it fails due to OS errors."""
    if not os.path.exists(path):
        return

    for i in range(retries):
        try:
            shutil.rmtree(path)
            print(f"Cleaned up temporary folder: {path}")
            return
        except OSError as e:
            print(f"Warning: Attempt {i+1}/{retries} to remove temporary folder '{path}' failed: {e}")
            print("  This often indicates file locks. Waiting and retrying...")
            time.sleep(delay)
            delay *= 1.5
    print(f"Error: Could not remove temporary folder '{path}' after {retries} attempts. Please delete it manually.")
    print("  You may need to close any applications that might be holding files in this folder,")
    print("  or restart your system to release file locks.")

def merge_videos_in_folder(input_folder: str, output_filename: str = "merged_video.mp4") -> bool:
    global ENCODING_PRESET, ENCODING_CRF, MAX_WORKERS, FFPROBE_TIMEOUT, FFMPEG_NORMALIZATION_TIMEOUT, FFMPEG_MERGE_TIMEOUT

    # Clear the log file at the beginning of each merge operation
    if os.path.exists(FAILED_FILES_LOG):
        os.remove(FAILED_FILES_LOG)
    print(f"\nPrevious '{FAILED_FILES_LOG}' cleared.")

    if not os.path.isdir(input_folder):
        reason = f"Input folder '{input_folder}' not found."
        print(f"Error: {reason}")
        log_failed_video(input_folder, reason)
        return False

    video_extensions = ('.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv')
    video_files = []
    for filename in os.listdir(input_folder):
        if filename.lower().endswith(video_extensions):
            video_files.append(os.path.join(input_folder, filename))
    video_files.sort()

    if not video_files:
        reason = f"No video files found in '{input_folder}' with extensions {video_extensions}."
        print(f"No video files found in '{input_folder}' with extensions {video_extensions}.")
        log_failed_video(input_folder, reason)
        return False

    print(f"Found {len(video_files)} video files to merge:")
    for f in video_files:
        print(f"- {os.path.basename(f)}")

    TARGET_WIDTH = 1280
    TARGET_HEIGHT = 720
    TARGET_FPS = 30
    TARGET_PIX_FMT = 'yuv420p'
    TARGET_VIDEO_CODEC = 'libx264'
    TARGET_AUDIO_CODEC = 'aac'
    TARGET_AUDIO_SAMPLE_RATE = '48000'
    TARGET_AUDIO_CHANNELS = 2

    temp_normalized_folder = os.path.join(input_folder, "temp_normalized_videos")
    _remove_temp_folder_robustly(temp_normalized_folder)
    os.makedirs(temp_normalized_folder)
    print(f"\nCreated temporary normalization folder: {temp_normalized_folder}")

    normalized_video_paths = []
    print("\nStarting video normalization process (parallelized)...")
    print(f"Using FFmpeg Preset: '{ENCODING_PRESET}' and CRF: {ENCODING_CRF} for faster encoding.")
    print(f"Using {MAX_WORKERS} worker processes for normalization.")

    success = False
    list_file_path = None
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_video = {
                executor.submit(_normalize_single_video, original_file_path, temp_normalized_folder,
                                TARGET_WIDTH, TARGET_HEIGHT, TARGET_FPS, TARGET_PIX_FMT,
                                TARGET_VIDEO_CODEC, TARGET_AUDIO_CODEC, TARGET_AUDIO_SAMPLE_RATE,
                                TARGET_AUDIO_CHANNELS, ENCODING_PRESET, ENCODING_CRF): original_file_path
                for original_file_path in video_files
            }

            for future in concurrent.futures.as_completed(future_to_video):
                original_file_path = future_to_video[future]
                try:
                    normalized_path = future.result()
                    if normalized_path:
                        normalized_video_paths.append(normalized_path)
                    # If normalized_path is None, _normalize_single_video already logged the failure
                except Exception as exc:
                    reason = f"Unexpected multiprocessing error: {exc}"
                    print(f"  Unexpected error during normalization of '{os.path.basename(original_file_path)}': {exc}")
                    log_failed_video(original_file_path, reason)

        normalized_video_paths.sort()

        if not normalized_video_paths:
            reason = "No videos successfully normalized for merging."
            print(reason + " Aborting merge.")
            return False

        list_file_path = os.path.join(input_folder, "ffmpeg_concat_list.txt")
        try:
            with open(list_file_path, "w") as f:
                for video_file in normalized_video_paths:
                    f.write(f"file '{video_file}'\n")
            print(f"\nCreated FFmpeg concat list file: {list_file_path}")
        except Exception as e:
            reason = f"Error creating FFmpeg concat list file: {e}"
            print(reason)
            log_failed_video("N/A", reason)
            return False

        output_path = output_filename
        if not os.path.isabs(output_path) and not os.path.dirname(output_path):
            output_path = os.path.join(os.getcwd(), output_filename)
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print(f"\nMerging and saving final video to: {output_path}")

        ffmpeg_command = [
            "ffmpeg",
            "-f", "concat",
            "-safe", "0",
            "-i", list_file_path,
            "-c", "copy",
            "-y",
            output_path
        ]

        try:
            print(f"Executing FFmpeg command: {' '.join(ffmpeg_command)}")
            process = subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True, timeout=FFMPEG_MERGE_TIMEOUT)
            print("\nFFmpeg Output (stdout):")
            print(process.stdout)
            print("\nFFmpeg Errors (stderr):")
            print(process.stderr)
            print("\nVideo merging completed successfully!")
            success = True
        except subprocess.TimeoutExpired:
            reason = f"Final FFmpeg merge timed out after {FFMPEG_MERGE_TIMEOUT} seconds."
            print(f"ERROR: {reason} The process was terminated.")
            log_failed_video(f"Merged output: {output_path}", reason)
            success = False
        except subprocess.CalledProcessError as e:
            reason = f"An error occurred during final video merging with FFmpeg: {e.stderr.strip()}"
            print(f"An error occurred during final video merging with FFmpeg: {e}")
            print(f"FFmpeg stdout: {e.stdout}")
            print(f"FFmpeg stderr: {e.stderr}")
            print("\nTroubleshooting tips for final merge:")
            print("1. Ensure FFmpeg is installed and its 'bin' directory is in your system's PATH.")
            print("2. Check the FFmpeg stderr output above for specific error messages from FFmpeg itself.")
            print("3. Verify that the normalized temporary video files are not corrupted.")
            log_failed_video(f"Merged output: {output_path}", reason)
            success = False
        except FileNotFoundError:
            reason = "'ffmpeg' command not found."
            print(f"Error: {reason}. Please ensure FFmpeg is installed and in your system's PATH.")
            log_failed_video(f"Merged output: {output_path}", reason)
            success = False
        except Exception as e:
            reason = f"An unexpected error occurred during final merge: {e}"
            print(f"An unexpected error occurred during final merge: {e}")
            log_failed_video(f"Merged output: {output_path}", reason)
            success = False
    except KeyboardInterrupt:
        print("\nProcess interrupted by user (Ctrl+C). Attempting graceful shutdown and cleanup...")
        log_failed_video("Process Interrupted", "User cancelled operation")
        success = False
    finally:
        if list_file_path and os.path.exists(list_file_path):
            try:
                os.remove(list_file_path)
                print(f"Cleaned up temporary concat list file: {list_file_path}")
            except Exception as e:
                print(f"Warning: Could not remove temporary list file '{list_file_path}': {e}")
        _remove_temp_folder_robustly(temp_normalized_folder)
    return success


if __name__ == "__main__":
    print("\n--- IMPORTANT: System Resources ---")
    print("If you experience hangs or crashes, ensure you have sufficient available RAM.")
    print("Close other memory-intensive applications before running this script.")
    # This RAM warning is based on a previous context. Please verify your current RAM.
    print("Your system currently shows low available physical memory (1.18 GB out of 16.0 GB).")
    print("This could be a contributing factor to hangs or slow performance.")
    print("Consider reducing MAX_WORKERS further if issues persist.")
    print("-----------------------------------\n")

    test_input_folder = "test_input_videos"
    output_file = "output_merged_reencoded_final.mp4"

    if os.path.exists(test_input_folder):
        _remove_temp_folder_robustly(test_input_folder)
    if os.path.exists(output_file):
        try:
            os.remove(output_file)
            print(f"Cleaned up previous output file: {output_file}")
        except OSError as e:
            print(f"Warning: Could not remove previous output file '{output_file}': {e}")
            print("  You may need to close any media players or applications holding this file.")

    os.makedirs(test_input_folder, exist_ok=True)
    print(f"Created dummy input folder: {test_input_folder}")

    try:
        print("\nCreating dummy video 1 (3 seconds, 640x480, 25fps)...")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=3:s=640x480:r=25", "-c:v", "libx264", "-pix_fmt",
             "yuv420p", "-an", os.path.join(test_input_folder, "video1_sd_25fps.mp4")], check=True, capture_output=True, text=True, timeout=30)

        print("\nCreating dummy video 2 (3 seconds, 1920x1080, 30fps)...")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=3:s=1920x1080:r=30", "-c:v", "libx264", "-pix_fmt",
             "yuv420p", "-an", os.path.join(test_input_folder, "video2_hd_30fps.mp4")], check=True, capture_output=True, text=True, timeout=30)

        print("\nCreating dummy video 3 (3 seconds, 800x600, 20fps, different audio)...")
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=3:s=800x600:r=20,sine=d=3", "-c:v", "libx264", "-pix_fmt",
             "yuv420p", "-c:a", "aac", "-ar", "44100", "-ac", "1",
             os.path.join(test_input_folder, "video3_custom_audio.mp4")], check=True, capture_output=True, text=True, timeout=30)

        # --- For testing purposes, uncomment to create a dummy corrupted file ---
        # print("\nCreating dummy corrupted video file for testing error logging...")
        # corrupted_file_path = os.path.join(test_input_folder, "corrupted_video.mp4")
        # with open(corrupted_file_path, "wb") as f:
        #     f.write(os.urandom(1024 * 100)) # 100 KB of random data - not a valid MP4
        # print(f"Dummy corrupted video created at: {corrupted_file_path}")

        # --- For testing purposes, uncomment to create a dummy video with no audio ---
        # print("\nCreating dummy video with no audio for testing...")
        # no_audio_file_path = os.path.join(test_input_folder, "video_no_audio.mp4")
        # subprocess.run(
        #     ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=d=5:s=1280x720:r=25", "-c:v", "libx264", "-pix_fmt",
        #      "yuv420p", "-an", no_audio_file_path], check=True, capture_output=True, text=True, timeout=30)
        # print(f"Dummy video with no audio created at: {no_audio_file_path}")


        print("\nDummy video files created in 'test_input_videos'.")
    except subprocess.TimeoutExpired as e:
        print(f"Could not create dummy videos because an FFmpeg command timed out. Error: {e}")
        print("Please ensure FFmpeg is installed and in your PATH, and your system has enough resources.")
        exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Could not create dummy videos. Ensure FFmpeg is installed and in your PATH. Error: {e.stderr}")
        print("Please ensure you can run 'ffmpeg -version' in your terminal.")
        exit(1)
    except FileNotFoundError:
        print("Error: 'ffmpeg' command not found. Please ensure FFmpeg is installed and in your system's PATH.")
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during dummy video creation: {e}")
        exit(1)

    try:
        success = merge_videos_in_folder(test_input_folder, output_file)

        if success:
            print(f"\nSuccessfully created {output_file} in {os.getcwd()}")
            print("Try playing this file with a robust media player like VLC.")
        else:
            print(f"\nVideo merging failed. Check the console output and '{FAILED_FILES_LOG}' for details.")
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    finally:
        print(f"\nFinal cleanup of dummy folder '{test_input_folder}'...")
        _remove_temp_folder_robustly(test_input_folder)
        if os.path.exists(output_file):
            try:
                os.remove(output_file)
                print(f"Removed potentially incomplete output file: {output_file}")
            except OSError as e:
                print(f"Warning: Could not remove output file '{output_file}' during final cleanup: {e}")
        print("Cleanup complete.")