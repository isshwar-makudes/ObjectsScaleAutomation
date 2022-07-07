import time

from components.controller_main import ControllerMain
from lib.generic.logger import DEBUG, INFO

def main():
  c = ControllerMain()
  c.setup()
  c.prepare()
  c.start_workload()
  INFO("Started the workload")
  time.sleep(60*10)
  INFO("Start compaction thread")
  c.start_compaction_async()
  INFO("Completed 10 min run time. Starting error injection")
  c.notify_clients_about_error_injection(wait_for_acknowledgement=True, timeout=300)
  c.start_error_injection()


if __name__ == "__main__":
  main()