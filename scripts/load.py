from lib.generic.logger import INFO, DEBUG, setup_logging
import os

log = os.path.join(os.getcwd(), "logs","load.log")
setup_logging(abs_log_file_path=log)
INFO("Info message")
DEBUG("Debug message")