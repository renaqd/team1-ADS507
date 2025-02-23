from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from scripts.update import run_update
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_scheduler():
    scheduler = BackgroundScheduler()
    
    # Schedule the update to run every day at 2:00 AM
    scheduler.add_job(
        run_update,
        trigger=CronTrigger(hour=2, minute=0),
        id='daily_update',
        name='Run daily update at 2 AM',
        replace_existing=True)
    
    # Schedule a weekly update every Sunday at 1:00 AM
    scheduler.add_job(
        run_update,
        trigger=CronTrigger(day_of_week='sun', hour=1, minute=0),
        id='weekly_update',
        name='Run weekly update on Sunday at 1 AM',
        replace_existing=True)

    scheduler.start()
    logger.info("Scheduler started")

if __name__ == "__main__":
    start_scheduler()
    
    # Keep the script running
    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        pass