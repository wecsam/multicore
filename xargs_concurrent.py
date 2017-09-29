#!/usr/bin/env python3
import argparse, collections, json, multiprocessing.pool, os, shlex, subprocess, sys

processes = None
command = None
creationflags = subprocess.CREATE_NEW_CONSOLE

def parse_arguments():
    global command, creationflags, processes
    parser = argparse.ArgumentParser(
        usage="%(prog)s [--num-processes=N] command [initial options]"
    )
    parser.add_argument("--num-processes", type=int, default=os.cpu_count())
    parser.add_argument("--no-new-console", action="store_true")
    # Find the first argument (besides sys.argv[0]) that does not start with a hyphen.
    try:
        command_start = 1
        while sys.argv[command_start][0:1] == "-":
            command_start += 1
    except IndexError:
        print("Error: no command was found.")
        parser.print_help()
        sys.exit()
    else:
        command = sys.argv[command_start:]
    # Parse the arguments that were meant for this script (and not each subprocess).
    parsed = parser.parse_args(sys.argv[1:command_start])
    # Find the number of processes.
    processes = parsed.num_processes
    # Check whether we should open a new console window for each subprocess.
    if parsed.no_new_console:
        creationflags = 0

def start_command(command_arg):
    global command, creationflags
    return subprocess.Popen(command + shlex.split(command_arg), creationflags=creationflags).wait()

if __name__ == "__main__":
    parse_arguments()
    with multiprocessing.pool.ThreadPool(processes) as pool:
        # Print a JSON object of the number of times that each return value occurred.
        json.dump(
            collections.Counter(pool.imap_unordered(start_command, sys.stdin)),
            sys.stdout,
            sort_keys=True,
            indent=4,
            separators=(',', ': ')
        )
