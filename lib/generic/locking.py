import threading
import time
import random

LOCK_TIMEOUT = 60
STEP = 5

class ReadWriteLock(object):
  """ A lock object that allows many simultaneous "read locks", but
  only one "write lock." """

  def __init__(self):
    self._read_ready = threading.Condition(threading.Lock())
    self._readers = 0

  def acquire_read(self):
    """ Acquire a read lock. Blocks only if a thread has
    acquired the write lock. """
    self.acquire_lock()
    try:
        self._readers += 1
    finally:
        self.release_lock()

  def release_read(self):
    """ Release a read lock. """
    self.acquire_lock()
    try:
        self._readers -= 1
        if not self._readers:
            self._read_ready.notifyAll()
    finally:
        self.release_lock()

  def acquire_write(self):
    """ Acquire a write lock. Blocks until there are no
    acquired read or write locks. """
    self.acquire_lock()
    while self._readers > 0:
        self._read_ready.wait()

  def release_write(self):
    """ Release a write lock. """
    self.release_lock()

  def acquire_lock(self, timeout=LOCK_TIMEOUT):
    attempts = 0
    end_time = time.time() + timeout
    while time.time() < end_time:
      if self._read_ready.acquire(False): # non-blocking lock
        return True
      else:
        wait_interval = random.random() * STEP
        # sleep for a random time before attempting again so
        # that not all threads try to acquire and sleep at the same time
        time.sleep(wait_interval)
    else:
      # Timeout is hit
      raise Exception("Lock couldnt be acquired within timeout %s" % LOCK_TIMEOUT)

  def release_lock(self):
    self._read_ready.release()
