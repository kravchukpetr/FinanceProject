import shutil
import os
from datetime import date, datetime, timedelta

LOG_DIR = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/")


def rm_old_logs():
    """
    Function delete logs folder for dates older then week but not the last one
    """

    # Define the path to your logs directory
    logs_dir = LOG_DIR + '/logs/'

    # Get the current date
    current_date = datetime.now()

    # Calculate the date 7 days ago
    week_ago = current_date - timedelta(days=7)

    # Get all subdirectories in the logs directory
    subdirs = [d for d in os.listdir(logs_dir) if os.path.isdir(os.path.join(logs_dir, d))]

    # Sort subdirectories by date (newest, first)
    subdirs.sort(reverse=True)

    # Keep track of folders to delete
    folders_to_delete = []

    for subdir in subdirs[1:]:  # Skip the first (most recent) folder
        try:
            # Parse the folder name as a date
            folder_date = datetime.strptime(subdir, "%Y%m%d")

            # If the folder is older than a week, add it to the list
            if folder_date < week_ago:
                folders_to_delete.append(subdir)
        except ValueError:
            # If the folder name doesn't match the expected format, skip it
            print(f"Skipping folder with invalid name format: {subdir}")

    # Delete the old folders
    for folder in folders_to_delete:
        folder_path = os.path.join(logs_dir, folder)
        try:
            shutil.rmtree(folder_path)
            print(f"Deleted folder: {folder}")
        except Exception as e:
            print(f"Error deleting folder {folder}: {e}")

    print("Cleanup completed.")