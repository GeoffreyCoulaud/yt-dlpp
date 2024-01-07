import argparse
import sys
import subprocess
import json
import curses
import itertools
import fcntl
import os


def get_videos_from_playlist(pl_uri):
    command = ("yt-dlp", "--flat-playlist", "--dump-single-json", pl_uri)
    proc = subprocess.run(command, capture_output=True, text=True)
    response = json.loads(proc.stdout)
    uris = []
    for video in response.get("entries", dict()):
        uri = video.get("url", None)
        if uri is None:
            continue
        uris.append(uri)
    return uris


def get_video_download_args(argv):
    if argv[0] in ("python", "py", "python3"):
        start = 2  # Skip "python something.py"
    else:
        start = 1  # Skip the executable name
    # TODO allowlist what can be passed as args
    args = argv.copy()[start:-1]
    return args


def download_video(uri, args):
    args = ("yt-dlp", "--quiet", "--progress", *args, uri)
    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8"
    )
    # HACK Set stderr to non-blocking, works only on POSIX
    flags = fcntl.fcntl(proc.stderr, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(proc.stderr, fcntl.F_SETFL, flags)
    return proc


def monitor_downloads_curses(screen, processes):
    screen.clear()
    polls = list(itertools.repeat(None, len(processes)))
    line_lengths = list(itertools.repeat(0, len(processes)))
    ended = 0
    while ended < len(processes):
        # Display process status
        for i, proc in enumerate(processes):
            # Get state
            if polls[i] is not None:
                # Already finished
                line = f"Finished with code {polls[i]}.\n"
            else:
                # Relay proc stderr to parent stderr (non-blocking)
                # HACK non-blocking, works only on POSIX
                try:
                    print(proc.stder.readline(), file=sys.stderr)
                except:
                    pass
                # Get line from stdout (keeps newlines)
                line = proc.stdout.readline()
                if len(line) == 0:
                    # EOF, just finished
                    polls[i] = proc.poll()
                    ended += 1
                    continue
            # Display state
            line = f"[Video {i}] {line}"
            padded_line = line.ljust(line_lengths[i], " ")
            screen.addstr(i, 0, padded_line)
            line_lengths[i] = len(line)
        screen.refresh()


def main():
    # Get video uris from playlist
    video_uris = get_videos_from_playlist(sys.argv[-1])

    # Get video download args
    args = get_video_download_args(sys.argv)

    # Download in subprocesses
    processes = []
    for uri in video_uris:
        proc = download_video(uri, args)
        processes.append(proc)

    # Report on progress per video
    try:
        curses.wrapper(monitor_downloads_curses, processes)
    except KeyboardInterrupt:
        for proc in processes:
            proc.terminate()
        print("Aborted by user.", file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
