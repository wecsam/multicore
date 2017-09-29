#!/usr/bin/env python3
import argparse, collections, json, multiprocessing.pool, os, shlex, subprocess, sys

processes = None
command = None

def parse_arguments():
    global processes, command
    parser = argparse.ArgumentParser(
        usage="%(prog)s [--num-processes=N] command [initial options]"
    )
    parser.add_argument("--num-processes", type=int, default=os.cpu_count())
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
    # Find the number of processes.
    processes = parser.parse_args(sys.argv[1:command_start]).num_processes

def start_command(command_arg):
    global command
    return subprocess.Popen(command + shlex.split(command_arg), creationflags=subprocess.CREATE_NEW_CONSOLE).wait()

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
