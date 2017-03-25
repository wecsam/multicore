#!python3
# This script runs a command on all files in a folder.
# The file path is passed as the first argument.
# Up to args_parsed.max_concurrent instances can be running at the same time.
# If a file is created before the script exits, then it will also be processed.
import argparse, os, pickle, shlex, signal, subprocess, threading
from _pickle import UnpicklingError
FILES_PROCESSED_PICKLE_FILENAME = "ls_xargs_concurrent_files_processed.pickle"
def unpickle_safe(*args, safe_return, error_msg=None, **kwargs):
    '''
    Calls pickle.load, but if UnpicklingError or EOFError is thrown, returns safe_return.
    '''
    try:
        result = pickle.load(*args, **kwargs)
    except (UnpicklingError, EOFError):
        if error_msg is not None:
            print(error_msg)
        return safe_return
    return result
# Trap the interrupt signal so that it can terminate all threads.
keep_processing = True
def discontinue_processing(signal, frame):
    global keep_processing
    print("<Caught termination signal. Will exit when all currently running commands exit.>")
    keep_processing = False
    return 0
signal.signal(signal.SIGINT, discontinue_processing)
signal.signal(signal.SIGTERM, discontinue_processing)
# Up to args_parsed.max_concurrent of the following thread will run at the
# same time. Each thread starts an instance of the command, waits for it to
# complete, and then repeats.
class RunCommand(threading.Thread):
    def files_lock_acquire(self):
        files_lock.acquire()
    def files_lock_release(self):
        files_lock.release()
        files_updated.set()
    def run(self):
        global keep_processing
        while keep_processing:
            # Check whether there are any files to process.
            did_scan = False
            try:
                self.files_lock_acquire()
                if not files_to_process:
                    did_scan = True
                    # There are no files to process! Scan the folder for any new
                    # files that may have appeared since the last scan.
                    files_to_process.update(set(filter(
                        lambda filename: os.path.isfile(os.path.join(args_parsed.directory, filename)),
                        os.listdir(args_parsed.directory)
                    )) - files_processed)
                    # If there are no more files to process, then exit this thread.
                    # There is no need to tell other threads that there are no more
                    # files because new files could be added by the time that they
                    # finish. The main thread is responsible for starting another
                    # thread if more files are added after this thread has exited.
                    if not files_to_process:
                        self.files_lock_release()
                        break
                # Get one item to process and add it to the set of files that have
                # been processed.
                filename = files_to_process.pop()
                files_processed.add(filename)
            finally:
                self.files_lock_release()
            # Print the filename.
            with print_lock:
                if did_scan:
                    print("Scanned for new files.")
                print("Processing file:", filename)
            # Spawn the command and wait for it to complete.
            try:
                subprocess.Popen(command + [os.path.join(args_parsed.directory, filename)], creationflags=subprocess.CREATE_NEW_CONSOLE).wait()
            except FileNotFoundError:
                keep_processing = False
                self.files_lock_acquire()
                files_to_process.clear()
                self.files_lock_release()
                with print_lock:
                    print("Error: While executing the command, the file could not be found.")
# Read command line arguments.
arg_parser = argparse.ArgumentParser(
    description="Runs a command on all files in a folder. The file path is appended to the command. Commands are spawned in new console windows. Up to MAX_CONCURRENT instances of the command can run at the same time. If a file is created before the script exits, then it will also be processed."
)
arg_parser.add_argument("directory", help="the directory whose files will be passed to the command")
arg_parser.add_argument("command", help="the command to which each file path will be appended")
arg_parser.add_argument("-n", "--max-concurrent", type=int, default=4, help="up to this number of instances of the command will be running at the same time")
arg_parser.add_argument("-p", "--pickle", action="store_true", help="load and save the set of the files already processed in a Python pickle in the same directory as the files")
args_parsed = arg_parser.parse_args()
command = shlex.split(args_parsed.command, posix=False)
if os.path.isdir(args_parsed.directory):
    # Keep track of the files to process and the files that have been processed.
    # These sets should hold filenames as strings.
    # We also need a lock for the two sets.
    files_lock = threading.Lock()
    files_to_process = set()
    files_processed = set()
    # If the user added the --pickle option and the pickle file exists, read files_processed from it.
    if args_parsed.pickle:
        print("Reading pickle file...")
        files_processed_pickle_path = os.path.join(args_parsed.directory, FILES_PROCESSED_PICKLE_FILENAME)
        if os.path.isfile(files_processed_pickle_path):
            try:
                with open(files_processed_pickle_path, "rb") as f:
                    files_processed = unpickle_safe(f, safe_return=set(), error_msg="Warning: pickle file is corrupted!")
            except OSError as e:
                print("Warning: cannot open pickle file for reading!")
                print(e)
            try:
                # Save a backup of the pickle file.
                files_processed_pickle_path_bak = files_processed_pickle_path + ".bak"
                if(os.path.isfile(files_processed_pickle_path_bak)):
                    os.remove(files_processed_pickle_path_bak)
                os.rename(files_processed_pickle_path, files_processed_pickle_path_bak)
            except OSError as e:
                print("Warning: unable to save backup of pickle file!")
                print(e)
        # Prevent the running of the command on the pickle file.
        files_processed.add(FILES_PROCESSED_PICKLE_FILENAME)
        files_processed.add(FILES_PROCESSED_PICKLE_FILENAME + ".bak")
    # This event should be set when files_to_process is updated. It lets the
    # main thread start up new threads if necessary.
    files_updated = threading.Event()
    # Also have a lock for printing to stdout.
    print_lock = threading.Lock()
    # Create a list of args_parsed.max_concurrent threads.
    print("Starting monitoring threads...")
    threads = [RunCommand() for i in range(args_parsed.max_concurrent)]
    if threads:
        # Start the threads.
        for thread in threads:
            thread.start()
        # When the file sets are updated, check whether any threads have exited.
        while keep_processing:
            # Wait for the file sets to be updated.
            files_updated.wait()
            files_updated.clear()
            # Update the list of threads to exclude threads that have exited.
            threads = [thread for thread in threads if thread.is_alive()]
            # Do stuff with the file lists.
            pickle_error = None
            with files_lock:
                # Get the number of files to process.
                len_files_to_process = len(files_to_process)
                # If the user specified the --pickle option, dump files_processed to the pickle file.
                if args_parsed.pickle:
                    try:
                        with open(files_processed_pickle_path, "wb") as f:
                            pickle.dump(files_processed, f)
                    except OSError as e:
                        pickle_error = e
            if pickle_error is not None:
                with print_lock:
                    print("Warning: cannot open pickle file for writing!")
                    print(pickle_error)
            # If there are files to process, start new threads.
            if len_files_to_process:
                # If there are already args_parsed.max_concurrent threads
                # running, then the following range is empty and the loop
                # is skipped.
                for i in range(len(threads), args_parsed.max_concurrent):
                    thread = RunCommand()
                    thread.start()
                    threads.append(thread)
            elif not threads:
                # If there are no threads running, then exit.
                break
    else:
        print("Error: For --max-concurrent, the value must be at least 1.")
else:
    print("Error:", args_parsed.directory, "is not a directory.")
