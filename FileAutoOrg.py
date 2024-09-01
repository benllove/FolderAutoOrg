import os
import re
import shutil
import logging
import argparse
import win32serviceutil
import win32service
import win32event
import configparser
from time import time, sleep
from collections import Counter
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Hardcoded path to the config.ini file
CONFIG_INI_PATH = "C:\\Users\\benlo\\Documents\\DekstopAutoOrg\\configDownloads.ini"

# Read the config.ini to get the target folder, log file path, and ExtensionsToFolder mappings
config_ini = configparser.ConfigParser()
config_ini.read(CONFIG_INI_PATH)
TARGET_FOLDER = os.path.expanduser(config_ini.get('Paths', 'target_folder_path'))
LOG_FILE_PATH = os.path.expanduser(config_ini.get('Paths', 'log_file_path'))

# Load ExtensionsToFolder mappings
EXTENSION_TO_FOLDER = {
    section: [ext.strip() for ext in extensions.split(',')]
    for section, extensions in config_ini.items('ExtensionsToFolder')
}

# Load IgnoredExtensions
IGNORED_EXTENSIONS = [ext.strip() for ext in config_ini.get('IgnoredExtensions', 'extensions').split(',')]

# Default values
PROTECTED_FOLDER_NAMES = list(EXTENSION_TO_FOLDER.keys())
MISC_FOLDER = 'Misc'  # Add a Misc folder for unrecognized extensions

class FileMoverHandler(FileSystemEventHandler):
    def __init__(self, debug_mode=False):
        self.debug_mode = debug_mode
        self.last_config_reload_time = 0

    def on_created(self, event):
        self.process_event(event)

    def on_modified(self, event):
        self.process_event(event)

    def on_moved(self, event):
        self.process_event(event)

    def process_event(self, event):
        try:
            # Ignore the log file and any protected folders
            if not self.debug_mode and (LOG_FILE_PATH in event.src_path or self.is_in_protected_folder(event.src_path)):
                return

            if event.is_directory:
                # Handle newly added folder
                if not self.is_in_protected_folder(event.src_path):
                    self.handle_new_folder(event.src_path)
            else:
                # Handle newly added or modified files
                file_path = event.src_path
                if not self.should_ignore_file(file_path):
                    self.log_transaction(f"Processing file: {file_path}")
                    self.move_file(file_path)
                else:
                    self.log_transaction(f"Ignored file: {file_path}")
        except Exception as e:
            self.log_transaction(f"Error processing file or directory {event.src_path}: {e}")

    def is_in_protected_folder(self, path):
        # Check if the folder name (case-insensitive) is in the list of protected folder names or is the Misc folder
        folder_name = os.path.basename(path).lower()
        protected_folders = [folder.lower() for folder in PROTECTED_FOLDER_NAMES]
        return folder_name in protected_folders or folder_name == MISC_FOLDER.lower()

    def should_ignore_file(self, file_path):
        # Check if the file's extension is in the ignored extensions list
        _, extension = os.path.splitext(file_path)
        return extension.lower() in IGNORED_EXTENSIONS

    def move_file(self, file_path):
        _, extension = os.path.splitext(file_path)
        extension = extension.lower()

        # Retry parameters
        max_retries = 5
        retry_delay = 1  # in seconds

        # Retry loop
        for attempt in range(max_retries):
            try:
                # Wait until the file is fully written by checking its size
                initial_size = os.path.getsize(file_path)
                sleep(0.5)
                if initial_size == os.path.getsize(file_path):
                    break
            except FileNotFoundError:
                self.log_transaction(f"Retry {attempt + 1}/{max_retries}: File not found {file_path}. Retrying...")
                sleep(retry_delay)
        else:
            # If we exhaust the retries, log an error and return
            self.log_transaction(f"File {file_path} could not be processed after {max_retries} retries.")
            return

        # Determine the correct folder based on the file extension
        destination_folder = None
        for folder, extensions in EXTENSION_TO_FOLDER.items():
            if extension in extensions:
                destination_folder = folder
                break

        # If no recognized extension, move to Misc folder
        if not destination_folder:
            destination_folder = MISC_FOLDER

        # Move the file to the appropriate folder
        try:
            self.log_transaction(f"Moving file {file_path} to {destination_folder}")
            self.move_to_folder(file_path, destination_folder)
        except OSError as e:
            if e.errno == 13:  # Permission denied
                self.log_transaction(f"Access denied when trying to move file: {file_path}. Skipping...")
            else:
                self.log_transaction(f"Error moving file {file_path}: {e}")
                raise  # Re-raise if it's a different error


    def handle_new_folder(self, folder_path):
        # Scan the folder and its subfolders for file types
        self.log_transaction(f"Processing folder: {folder_path}")
        file_extensions = []

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                _, extension = os.path.splitext(file)
                extension = extension.lower()
                if not self.should_ignore_file(file):
                    file_extensions.append(extension)

        if file_extensions:
            # Find the most common file extension in the folder and subfolders
            most_common_extension, _ = Counter(file_extensions).most_common(1)[0]

            # Determine the destination folder based on the most common extension
            destination_folder = None
            for folder, extensions in EXTENSION_TO_FOLDER.items():
                if most_common_extension in extensions:
                    destination_folder = folder
                    break

            # If no recognized extension, move the folder to the Misc folder
            if not destination_folder:
                destination_folder = MISC_FOLDER

            # Move the entire folder
            self.move_folder_to_type_folder(folder_path, destination_folder)

    def move_folder_to_type_folder(self, folder_path, folder):
        destination_folder = os.path.join(TARGET_FOLDER, folder)

        # Create destination folder if it doesn't exist
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        # Move the entire folder
        folder_name = os.path.basename(folder_path)
        destination_path = os.path.join(destination_folder, folder_name)

        # Move the folder to the destination
        try:
            shutil.move(folder_path, destination_path)
            self.log_transaction(f"Moved folder {folder_path} to {destination_path}")
        except OSError as e:
            if e.errno == 13:  # Permission denied
                self.log_transaction(f"Access denied when trying to move folder: {folder_path}. Skipping...")
            else:
                self.log_transaction(f"Error moving folder {folder_path}: {e}")
                raise

    def move_to_folder(self, file_path, folder):
        destination_folder = os.path.join(TARGET_FOLDER, folder)

        # Create destination folder if it doesn't exist
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        destination_path = os.path.join(destination_folder, os.path.basename(file_path))
        try:
            shutil.move(file_path, destination_path)
            self.log_transaction(f"Moved file {file_path} to {destination_path}")
        except OSError as e:
            if e.errno == 13:  # Permission denied
                self.log_transaction(f"Access denied when trying to move file: {file_path}. Skipping...")
            else:
                self.log_transaction(f"Error moving file {file_path}: {e}")
                raise

    def log_transaction(self, message):
        if self.debug_mode:
            print(message)
        else:
            logging.info(message)

def setup_logging():
    logging.basicConfig(filename=LOG_FILE_PATH, level=logging.INFO,
                        format='%(asctime)s - %(message)s')
    logging.info("Service started and logging initiated.")

def scan_existing_files(handler):
    # Scan the TARGET_FOLDER for existing files and process them
    for root, dirs, files in os.walk(TARGET_FOLDER):
        # Skip processing of protected folders
        dirs[:] = [d for d in dirs if not handler.is_in_protected_folder(d)]
        
        for directory in dirs:
            dir_path = os.path.join(root, directory)
            if not handler.is_in_protected_folder(directory):
                handler.log_transaction(f"Processing existing folder: {dir_path}")
                handler.handle_new_folder(dir_path)

        for file in files:
            file_path = os.path.join(root, file)
            if not handler.is_in_protected_folder(os.path.dirname(file_path)) and not handler.should_ignore_file(file_path):
                handler.log_transaction(f"Processing existing file: {file_path}")
                handler.move_file(file_path)
            else:
                handler.log_transaction(f"Ignored file: {file_path}")

def run_debug_mode():
    try:
        setup_logging()
        event_handler = FileMoverHandler(debug_mode=True)
        scan_existing_files(event_handler)
        observer = Observer()
        observer.schedule(event_handler, path=TARGET_FOLDER, recursive=False)
        observer.start()
        print(f"Running in debug mode... Watching {TARGET_FOLDER}. Press Ctrl+C to stop.")
        try:
            while True:
                pass
        except KeyboardInterrupt:
            observer.stop()
            observer.join()
            print("Debug mode stopped.")
    except Exception as e:
        print(f"Failed to start due to: {e}")
        return

class DownloadsFileMoverService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'DownloadsFileMoverService'
    _svc_display_name_ = 'Downloads File Mover Service'
    _svc_description_ = 'Monitors Downloads folder and moves files based on extension to designated folders.'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.observer = None
        try:
            setup_logging()
        except Exception as e:
            logging.error(f"Service failed to start due to: {e}")
            self.SvcStop()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        if self.observer:
            self.observer.stop()
            self.observer.join()
        logging.info("Service stopped.")

    def SvcDoRun(self):
        try:
            logging.info("Service started.")
            event_handler = FileMoverHandler()
            scan_existing_files(event_handler)
            self.observer = Observer()
            self.observer.schedule(event_handler, path=TARGET_FOLDER, recursive=False)
            self.observer.start()
            win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
        except Exception as e:
            logging.error(f"Service failed due to: {e}")
            self.SvcStop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download File Mover Service")
    parser.add_argument('--debug', action='store_true', help="Run the script in debug mode")
    args, unknown = parser.parse_known_args()

    if args.debug:
        run_debug_mode()
    else:
        # Handle service commands
        win32serviceutil.HandleCommandLine(DownloadsFileMoverService, argv=[__file__] + unknown)
