#!python3
# This script runs a command on all files in a folder.
# The file path is passed as the first argument.
# Up to args_parsed.max_concurrent instances can be running at the same time.
# If a file is created before the script exits, then it will also be processed.
import argparse, os, shlex, signal, subprocess, threading
arg_parser = argparse.ArgumentParser(
    description="Runs a command on all files in a folder. The file path is appended to the command. Commands are spawned in new console windows. Up to MAX_CONCURRENT instances of the command can run at the same time. If a file is created before the script exits, then it will also be processed."
)
arg_parser.add_argument("directory", help="the directory whose files will be passed to the command")
arg_parser.add_argument("command", help="the command to which each file path will be appended")
arg_parser.add_argument("-n", "--max-concurrent", type=int, default=4, help="up to this number of instances of the command will be running at the same time")
args_parsed = arg_parser.parse_args()
command = shlex.split(args_parsed.command, posix=(os.name == "posix"))
# Keep track of the files to process and the files that have been processed.
# These sets should hold filenames as strings.
# We also need a lock for the two sets.
files_lock = threading.Lock()
files_to_process = set()
files_processed = set()
# This event should be set when files_to_process is updated. It lets the
# main thread start up new threads if necessary.
files_updated = threading.Event()
# Also have a lock for printing to stdout.
print_lock = threading.Lock()
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
        while keep_processing:
            # Check whether there are any files to process.
            did_scan = False
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
            self.files_lock_release()
            # Print the filename.
            print_lock.acquire()
            if did_scan:
                print("Scanned for new files.")
            print("Processing file:", filename)
            print_lock.release()
            # Spawn the command and wait for it to complete.
            subprocess.Popen(command + [os.path.join(args_parsed.directory, filename)], creationflags=subprocess.CREATE_NEW_CONSOLE).wait()
if os.path.isdir(args_parsed.directory):
    # Create a list of args_parsed.max_concurrent threads.
    print("Starting monitoring threads...")
    threads = [RunCommand() for i in range(args_parsed.max_concurrent)]
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
        # Get and print the number of files to process.
        files_lock.acquire()
        len_files_to_process = len(files_to_process)
        files_lock.release()
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
    print("Error:", args_parsed.directory, "is not a directory.")
