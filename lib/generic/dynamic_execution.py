from time import sleep, time
from threading import Thread, RLock
from Queue import Queue
from copy import deepcopy

from lib.generic.logger import ERROR, INFO
from lib.utils import get_random_string

class DynamicExecution():
  default_quotas = {"generic": {"perc":100},
                    "read": {"perc":0},
                    "write": {"perc":0},
                    "delete": {"perc":0}
                   }
  def __init__(self, threadpoolsize=10, timeout=7200, quotas=None):
    self._poolsize = threadpoolsize
    self._timeout = timeout
    if not quotas:
      quotas = deepcopy(self.default_quotas)
    self._quotas = quotas
    self._queues = {}
    self._time_to_exit = False
    self._workers = []
    self._results = {}
    self._lock = RLock()
    self._init_workers()

  def add(self, target, custom=None, **kwargs):
    timeout = kwargs.pop("timeout", self._timeout)
    quota_type = kwargs.pop("quota_type")
    queue = self._queues[quota_type]
    work_id = get_random_string()
    while timeout > 0:
      if self._time_to_exit:
        INFO("Exit is called, marking %s as no-op, Type : %s"
             %(target, quota_type))
        return
      try:
        kwargs["__work_id"] = work_id
        task = {"target":target, "kwargs":kwargs}
        queue.put(task, timeout=1)
        self._results[work_id] = {"task": task, "custom": custom}
        return
      except:
        pass
      timeout -= 1
    raise Exception("Timeout after %s, while adding task : %s, Type : %s"
                    %(self._timeout, target, quota_type))

  def complete_run(self, wait_time=1, timeout=600):
    INFO("Complete run is called. Setting EXIT-MARKER=TRUE in DynamicExecution")
    self._time_to_exit = True
    stime = time()
    while (len(self._workers) > 0) or (time()-stime > 600):
      INFO("Number of active workers : %s"%(self._workers))
      for workerthread in self._workers:
        if not workerthread.is_active:
          self._workers.pop(workerthread)
      sleep(wait_time)

  def wait_for_all_worker_to_be_free(self, wait_time=1, timeout=300):
    etime = time()+timeout
    while etime-time() > 0:
      inactive_workers = 0
      for workerthread in self._workers:
        if not workerthread.is_active:
          inactive_workers += 1
      print ("Number of workers : %s, Inactive : %s"
             %(len(self._workers), inactive_workers))
      if inactive_workers == len(self._workers):
        return True
      sleep(wait_time)
    WARN("Not all workers became free. Total/Active/Inactive workers : %s / %s "
         "/ %s"%(size, size-len(inactive_workers), len(inactive_workers)))
    return False

  def _stop_all_workers(self, quota_type=None):
    quota_types = [quota_type]
    if not quota_type:
      quota_types = self._quotas.keys()
    INFO("Stopping all the workers : %s"%(quota_types))
    for quota_type in quota_types:
      for thrd in self._quotas[quota_type]["threads"]:
        if thrd.isAlive():
          try:
            INFO("Stopping worker : %s"%(thrd.name))
            thrd._Thread__stop()
          except Exception as err:
            ERROR("Failed to stop worker : %s, Error : %s"%(thrd.name, err))

  def get_active_workers(self, quota_type=None, verbose=False):
    res = {}
    quota_types = [quota_type]
    if not quota_type:
      quota_types = self._quotas.keys()
    for quota_type in quota_types:
      count = 0
      for thrd in self._quotas[quota_type]["threads"]:
        if thrd.isAlive():
          if verbose:
            INFO("Worker : %s, busy with %s job"
                 %(thrd.name, thrd._Thread__target.__name__))
          count += 1
      res[quota_type] = count
    return res

  def size(self):
    return self._poolsize

  def consume_task_result(self, work_id):
    return del(self._results[work_id])

  def consume_results(self, check_results=True):
    all_results = self._results
    self._results = {}
    if check_results:
      for work_id, result in all_results.iteritems():
        if result.get("exception", None):
          raise Exception("Work %s has failed: %s" % (work_id, result))
    return all_results

  def _assign_workers(self):
    total_workers = 0
    for key in self._quotas.keys():
      num_workers = qlength = 0
      quota = self._quotas[key]
      if quota.get("perc"):
        num_workers = (quota["perc"] * self._poolsize)/100
        if num_workers < 1:
          num_workers = 1
        qlength = num_workers*2
      elif quota.get("num_workers"):
        num_workers = quota.get("num_workers")
        qlength = num_workers
      else:
        ERROR("Unknown workload")
        continue
      total_workers = total_workers + num_workers
      quota["num_workers"] = num_workers
      self._quotas[key] = quota
      INFO("Creating queue for workload : %s, with quota : %s, Queue : %s"
           %(key, quota, qlength))
      queue = Queue(qlength)
      self._queues[key] = queue
    free_threads =  self._poolsize - total_workers
    INFO("Total workers created : %s, Queue : %s"
         %(total_workers, total_workers*2))
    if free_threads > 0:
      #TODO add free threads to generic pool
      pass

  def _get_task(self, worker_type, timeout=None):
    stime = time()
    while not self._time_to_exit:
      if self._queues[worker_type].empty():
        sleep(0.1)
        continue
      try:
        with self._lock:
          task = self._queues[worker_type].get(timeout=0.1)
          return task
      except Exception as err:
        ERROR("Failed to get item from Queue : %s, Since : %s secs"
              %(worker_type, time()-stime))
        if timeout and (stime-time() > timeout):
          raise Exception("Timeout")
        continue
    while not self._queues[worker_type].empty():
      INFO("Emptying workload:%s Queue"%(worker_type))
      try:
        self._queues[worker_type].get(timeout=1)
      except Exception as err:
        ERROR("Failed to empty Queue : %s. Error : %s" %(worker_type, err))
        continue

  def _init_workers(self):
    self._assign_workers()
    for quota_type in self._quotas.keys():
      INFO("Creating workers for : %s workload, with args : %s"
           %(quota_type, self._quotas[quota_type]))
      num_workers = self._quotas[quota_type]["num_workers"]
      threads = []
      for i in range(num_workers):
        worker_name = "%sWorkerThread %s"%(quota_type.title(), i)
        wthread = self._start_worker(quota_type, worker_name, True)
        threads.append(wthread)
      self._quotas[quota_type]["threads"] = threads

  def _start_worker(self, quota_type, worker_name, daemon, retries=3):
    while retries > 0:
      retries -= 1
      try:
        initworker = worker(self, quota_type, worker_name)
        wthread = Thread(target=initworker.execute)
        wthread.name =  worker_name
        wthread.daemon = daemon
        wthread.start()
        return wthread
      except KeyError as kerr:
        ERROR("Failed to start the worker %s, Retries left : %s, Error : %s"
              %(worker_name, retries, err.message))
        if retries == 0:
          raise

class worker(object):
  def __init__(self, manager, worker_type, worker_name):
    self._manager = manager
    self._type = worker_type
    self._is_active = True
    self._name = worker_name
    INFO("Initializing worker %s, for workload : %s"%(self._name, self._type))

  def execute(self):
    while not self._manager._time_to_exit:
      task = self._get_task()
      if not task:
        continue
      self._execute_task(task)
    self._is_active = False
    INFO("Exit is called. Exiting worker %s, type %s"%(self._name, self._type))

  def is_active(self):
    return self._is_active

  def _execute_task(self, task):
    target = task.pop("target")
    kwargs = task.pop("kwargs")
    work_id = kwargs.pop("__work_id")
    result = self._manager._results[work_id]
    try:
      result["ret_value"] = target(**kwargs)
    except Exception as err:
      result["exception"] = err
      ERROR("Worker %s (type : %s), hit exception while executing task : %s,"
            "Err : (%s) %s, Trace : %s"%(self._name, self._type, target,
                                          type(err), err, format_exc()))
      self._is_active = False
      if not self._manager._time_to_exit:
        raise

  def _get_task(self):
    try:
      return self._manager._get_task(self._type)
    except Exception as err:
      if "Timeout" in err.message:
        return None
      ERROR("Worker %s (type : %s), hit exception while getting task : %s"
            %(self._name, self._type, err))
      self._is_active = False
      raise
