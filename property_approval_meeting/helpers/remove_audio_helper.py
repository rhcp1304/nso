import subprocess
import os

def remove_audio_from_video(input_video_path, output_video_path):
    FIXED_FFMPEG_PATH = "C:\\Users\\Ankit.Anand\\Downloads\\ffmpeg\\ffmpeg\\bin\\ffmpeg.exe"
    ffmpeg_cmd_name = FIXED_FFMPEG_PATH
    try:
        subprocess.run([ffmpeg_cmd_name, '-version'], check=True, capture_output=True, text=True)
    except FileNotFoundError:
        print(f"Error: FFmpeg executable not found at the fixed path: '{ffmpeg_cmd_name}'")
        print("Please ensure the 'FIXED_FFMPEG_PATH' in video_processing/utils.py is correct and points to your FFmpeg executable.")
        print("You can download FFmpeg from: https://ffmpeg.org/download.html")
        return False
    except subprocess.CalledProcessError as e:
        print(f"Error checking FFmpeg version using '{ffmpeg_cmd_name}': {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while checking FFmpeg: {e}")
        return False

    if not os.path.exists(input_video_path):
        print(f"Error: Input video file not found at '{input_video_path}'")
        return False
    ffmpeg_command = [
        ffmpeg_cmd_name,
        '-i', input_video_path,
        '-c:v', 'copy',
        '-an',
        '-y',
        output_video_path
    ]

    print(f"Executing FFmpeg command: {' '.join(ffmpeg_command)}")

    try:
        # Run the FFmpeg command
        process = subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
        print("FFmpeg output:")
        print(process.stdout)
        print(process.stderr)
        print(f"Successfully removed audio. Output saved to: {output_video_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error during FFmpeg execution: {e}")
        print(f"Command failed with exit code {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False