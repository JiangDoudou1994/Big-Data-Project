import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class MyHandler(FileSystemEventHandler):

    def on_modified(self, event):
        if event.src_path.endswith('py'):
            print "source file %s changed!" % event.src_path
            os.system('python -m unittest discover -v -s tests -p "*_test.py"')

if __name__ == "__main__":
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
