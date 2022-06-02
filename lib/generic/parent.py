from lib.generic.logger import INFO, DEBUG
from lib.generic.dynamic_execution import DynamicExecution

class Parent(object):

  class WORK_TYPE(object):
    THREAD = "thread"
    PROCESS = "process"

  def __init__(self, thread_opts=None, process_opts=None):
    self._thread_manager = DynamicExecution(**thread_opts)
    self._process_opts = process_opts

  def add_work_async(self, run_using=WORK_TYPE.THREAD, **kwargs):
    if run_using == WORK_TYPE.THREAD:
      self._thread_manager.add(**kwargs)
    elif run_using == WORK_TYPE.PROCESS:
      pass
    else:
      raise Exception("Invalid value '%s' specified for 'run_using'" % run_using)

  def wait_for_completion(timeout=300, check_for_failures=True)
    pass

  @property
  def thread_manager(self):
    return self._thread_manager