import time
# from watchdog.observers.polling import PollingObserver as Observer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path


class CustomEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            print(f"Directory created: {Path(event.src_path).resolve()}")
        else:
            print(f"File created: {Path(event.src_path).resolve()}")

    def on_modified(self, event):
        if event.is_directory:
            print(f"Directory modified: {Path(event.src_path).resolve()}")
        else:
            print(f"File modified: {Path(event.src_path).resolve()}")

    def on_deleted(self, event):
        if event.is_directory:
            print(f"Directory deleted: {Path(event.src_path).resolve()}")
        else:
            print(f"File deleted: {Path(event.src_path).resolve()}")


def main(directory_to_monitor):
    event_handler = CustomEventHandler()
    observer = Observer()
    observer.schedule(event_handler, directory_to_monitor, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main("/home/pi/500GB/data/")
    # main("/home/mark/Musik/")
