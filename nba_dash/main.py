import logging
from scripts.initial_setup import initial_setup
from scripts.update import run_update
from scheduler import start_scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    choice = input("Enter '1' for initial setup, '2' for update, '3' to start scheduler: ")
    if choice == '1':
        initial_setup()
    elif choice == '2':
        run_update()
    elif choice == '3':
        start_scheduler()
    else:
        logger.error("Invalid choice. Please enter '1', '2', or '3'.")

if __name__ == "__main__":
    main()