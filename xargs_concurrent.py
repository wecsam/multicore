#!/usr/bin/env python3
import argparse, collections, json, multiprocessing.pool, os, shlex, subprocess, sys

processes = None
command = None
creationflags = subprocess.CREATE_NEW_CONSOLE

def parse_arguments():
    global command, creationflags, processes
    parser = argparse.ArgumentParser(
        description=
            "This script takes each line in standard input, appends it to the "
            "specified command, and runs it. Once all commands have completed, "
            "the number of occurrences of each return value is printed in JSON "
            "format."
    )
    parser.add_argument(
        "--num-processes", "-n",
        type=int,
        default=os.cpu_count(),
        metavar="N",
        help=
            "By default, the number of parallel processes that are spawned how "
            "many parallel processes to spawn is limited to the number of CPUs "
            "of the computer. Use this switch to override this number."
    )
    parser.add_argument(
        "--no-new-console", "-p",
        action="store_true",
        help=
            "By default, each command is spawned in its own window. Specify this "
            "switch to use only this console window."
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help=
            "Specify the command to run and its initial arguments. Each line in "
            "standard input is appended to this text, and the whole string is "
            "treated as the command."
    )
    parsed = parser.parse_args()
    processes = parsed.num_processes
    if parsed.no_new_console:
        creationflags = 0
    command = parsed.command
    if not len(command):
        parser.error("no command was specified")

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
