import argparse

from lib.generic.parent import Parent
from lib.generic.logger import setup_logging, INFO, DEBUG

class ClientMain(Parent):
  def run(self):
  """
  starting point to execute this script
  """
  parsed_args = self.parse_arguments()
  configure_logging(parsed_args)
  print_details(parsed_args)
  runner = APITestsRunner(parsed_args=parsed_args)
  runner.run()


def parse_arguments(self):
  """
  Parses command line arguments

  Returns: Parsed args as object

  Raises: argparse.ArgumentError exception
  """
  parser = argparse.ArgumentParser(description="The entrypoint process to start load in a client")
  parser.add_argument("-c", "--config_file",
                      help='Config file path in the local client',
                      type=str, required=False)
  parser.add_argument("-l", "--log_level",
                      help='Log messages whose severity >= given level would be logged',
                      type=str, required=False, default="DEBUG")
  parsed_args = parser.parse_args()
  if parsed_args.debug:
    nulog.set_level(nulog.logging.DEBUG)
  if not parsed_args.list and not \
    (parsed_args.endpoint_url and
     parsed_args.access_key and parsed_args.secret_key):
    raise argparse.ArgumentError(
      argument=None,
      message="Atleast one of endpoint_url, access_key and secret_key is "
              "missing. When --list is not specified, all three arguments "
              "are mandatory")
  return parsed_args

def configure_logging(parsed_args):
  """Configures log dir for this run

  Args:
    parsed_args(object): Parsed args
  """
  log_dir = parsed_args.log_dir
  if not log_dir:
    log_dir = os.path.join(os.getcwd(), "api_suite", "logs")
  log_dir = os.path.join(log_dir, strftime("%Y%m%d_%H%M%S"))
  os.environ["LOG_DIR"] = log_dir
  if not os.path.exists(log_dir):
    os.makedirs(log_dir)
  nulog.configure(log_dir=log_dir)

def print_details(parsed_args):
  """
  Print the details related to the run

  Args:
    parsed_args(object): Parsed args
  """
  INFO("Log directory: %s" % os.environ["LOG_DIR"])
  INFO("Process pid: %s" % os.getpid())
  INFO("Arguments passed:\n%s" % pformat(parsed_args))



if __name__ == "__main__":
  ClientMain().run()