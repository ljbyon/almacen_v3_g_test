import logging
import time
from datetime import datetime
import os

def setup_file_logging():
    if not os.path.exists("logs"):
        os.makedirs("logs")
        print("📁 Created logs folder")
    
    log_filename = f"logs/booking_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"📝 Logging started - saving to: {log_filename}")
    
    return logger

# Test the logging
logger = setup_file_logging()
logger.info("🧪 Testing logging setup...")
logger.info("📝 This should appear in both terminal and log file")
logger.warning("⚠️ This is a warning message")
logger.error("❌ This is an error message")
logger.info("✅ Logging test completed")

print("✅ Test completed! Check logs folder for the log file.")