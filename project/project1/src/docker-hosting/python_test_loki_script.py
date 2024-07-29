import logging as log
import time

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def main():
    while True:
        log.info("This is a log message")
        time.sleep(5)

if __name__ == "__main__":
    main()
