import logging
from scripts.initial_setup import initial_setup
from scripts.update import run_update

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    choice = input("Enter '1' for initial setup, '2' for update: ")
    if choice == '1':
        initial_setup()
    elif choice == '2':
        run_update()
    else:
        logger.error("Invalid choice. Please enter '1' or '2'.")

if __name__ == "__main__":
    main()