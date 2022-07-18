from datetime import datetime

from time import sleep, time
from lib.constants import Constants
from lib.generic.logger import ERROR, INFO, DEBUG
from lib.system_interations.pod import POD

class ATLAS():
  def __init__(self, pcobj, mspcluster,
               namespace=Constants.MSP_DEFAULT_NAMESPACE):
    self._pcobj = pcobj
    self._msp = mspcluster
    self._namespace = namespace
    self._master_pod = None
    self._atlas_master = None
    self._init_atlas_pods()

  @property
  def namespace(self):
    return self._namespace

  @property
  def service_name(self):
    return Constants.OBJECTS_ATLAS_SERVICE

  def is_master_changed(self):
    if not self._atlas_master:
      self.get_atlas_master()
      return False
    current_master = self.get_atlas_master()
    for podname, details in self._init_details.iteritems():
      if details["is_master"]:
        if (current_master["name"] != podname or
            current_master["incarnation_id"] != details["incarnation_id"]):
          ERROR("ATLAS master is changed. Old : %s, Current : %s"
                %(details, current_master))
          return True
    return False

  def get_atlas_master(self, refresh=True):
    master_changed = False
    pod = None
    if self._atlas_master:
      pod = self._allpods[self._atlas_master["name"]]
      details = self._get_pod_details(pod)
      if details["is_master"]:
        return self._atlas_master
      else:
        master_changed = True
    if master_changed:
      ERROR("ATLAS master changed. Old : %s"%(pod.name))
      if not refresh:
        return self._atlas_master
    self._atlas_master = self._get_atlas_master()
    return self._atlas_master

  def initiate_scan(self, scan_type, wait_for_completion=True,
                        timeout=Constants.ATLAS_DEFAULT_SCAN_TIMEOUT,
                        wait_for_ongoing_scan_to_finish=True):
    self.get_atlas_master()
    if wait_for_ongoing_scan_to_finish:
      self._wait_for_scan_to_finish(timeout=timeout)
    cmd = 'links -dump %s'%Constants.ATLAS_FULL_SCAN_TRIGGER_PAGE
    if scan_type == Constants.ATLAS_PARTIAL_SCAN:
      cmd = 'links -dump %s'%Constants.ATLAS_PARTIAL_SCAN_TRIGGER_PAGE
    elif scan_type == Constants.ATLAS_SELECTIVE_SCAN:
      cmd = 'links -dump %s'%Constants.ATLAS_SELECTIVE_SCAN_TRIGGER_PAGE
    pod = self._allpods[self._atlas_master["name"]]
    INFO("Initiating %s scan, cmd : %s on %s"%(scan_type, cmd, pod.name))
    res = pod.exec_cmd(cmd)
    sleep(10)
    is_scan_running = self._is_atlas_scan_running()
    if not is_scan_running:
      raise Exception("%s scan is invoked successfully but no scan is found on"
                      " atlas page"%cmd)
    if is_scan_running.lower() != "%s scan"%scan_type:
      ERROR("%s scan is invoked successfully but %s running scan "
                      "found on ATLAS page"%(scan_type, is_scan_running))
    if not wait_for_completion:
      return "%s scan"%scan_type
    self._wait_for_scan_to_finish(timeout=timeout, scan_type=scan_type)
    if self.is_master_changed():
      raise Exception("ATLAS Master changed during or after %s scan"%scan_type)

  def initiate_partial_scan(self, wait_for_completion=True,
                         timeout=Constants.ATLAS_DEFAULT_SCAN_TIMEOUT,
                         wait_for_ongoing_scan_to_finish=True):
    return self.initiate_scan(Constants.ATLAS_PARTIAL_SCAN, wait_for_completion,
                              timeout, wait_for_ongoing_scan_to_finish)

  def initiate_full_scan(self, wait_for_completion=True,
                         timeout=Constants.ATLAS_DEFAULT_SCAN_TIMEOUT,
                         wait_for_ongoing_scan_to_finish=True):
    return self.initiate_scan(Constants.ATLAS_FULL_SCAN, wait_for_completion,
                              timeout, wait_for_ongoing_scan_to_finish)

  def _get_atlas_master(self):
    timeout = 300
    etime = time()+timeout
    while etime-time() > 0:
      for pod, podinst in self._allpods.iteritems():
        details = self._get_pod_details(podinst)
        if details["is_master"]:
          self._atlas_master = details
          return self._atlas_master
      ERROR("ATLAS Master not found. Retrying in 30s")
      sleep(30)
    raise Exception("Failed to find ATLAS master POD")

  def _get_pod_details(self, pod):
    res = pod.exec_cmd("links -dump http:0:7103".split(" "))
    if not res:
      ERROR("Failed to execute command in %s"%pod.name)
    res = [i.rstrip() for i in res.split("\n")[0:15]]
    timeformat='%Y%m%d-%H:%M:%S-GMT'
    pod_details = {"is_master":False, "name":pod.name}
    for line in res:
      if "Start time" in line:
        pod_start_time = [i.rstrip("|").rstrip() for i in line.split()][-1]
        pod_start_time = "".join(pod_start_time.partition('GMT')[0:2])
        pod_details["pod_start_time"] = datetime.strptime(pod_start_time,
                                                        timeformat)
        continue
      if "Uptime" in line and "Master" not in line:
        pod_details["pod_uptime"]= [i.rstrip("|").strip()
                                    for i in line.split("  ")][-1]
        continue
      if "Atlas master" in line:
        pod_details["is_master"] = True
        continue
      if "Master Start" in line:
        master_start_time = [i.rstrip("|").rstrip() for i in line.split()][-1]
        master_start_time = "".join(master_start_time.partition('GMT')[0:2])
        pod_details["master_start_time"] = datetime.strptime(master_start_time,
                                                           timeformat)
        continue
      if "Master uptime" in line:
        pod_details["master_uptime"] = [i.rstrip("|").strip()
                                      for i in line.split("  ")][-1]
        continue
      if "Incarnation id" in line:
        pod_details["incarnation_id"] = [i.rstrip("|").rstrip()
                                      for i in line.split()][-1]
        continue
    return pod_details

  def _init_atlas_pods(self):
    healthy_pods, unhealthy_pods = self._msp.get_all_pods_for_service(
                            self.service_name, self.namespace)
    if unhealthy_pods:
      ERROR("Found Unhealthy ATLAS PODs : %s"%unhealthy_pods)
    if not healthy_pods:
      ERROR("No healthy ATLAS PODs found")
    self._allpods = {}
    for podname in healthy_pods:
      self._allpods[podname] = POD(self._msp, podname, self.namespace)
    self._init_details = {podname:self._get_pod_details(podinst)
                             for podname, podinst in self._allpods.iteritems()}
    self.get_atlas_master()
    INFO("Current ATLAS Master : %s"%(self._atlas_master))

  def _is_atlas_scan_running(self):
    master = self._allpods[self._atlas_master["name"]]
    res = master.exec_cmd("links -dump http:0:7103")
    start_tracking = False
    for line in res.split("\n"):
      if "Atlas Jobs" in line:
        start_tracking = True
        continue
      if start_tracking:
        if Constants.ATLAS_SCAN_RUNNING_STATUS in line:
          res = [i for i in line.split("|") if "scan" in i.lower()
                 or "selective" in i.lower() or "partial" in i.lower()
                 or "full" in i.lower()]
          if not res:
            ERROR("Could not find scan details in : %s"%line)
            continue
          return res[0].rstrip()
    if not start_tracking:
      ERROR("Failed to get the atlas scan details from %s. Is Master Changed : "
            "%s"%(master.name, self.is_master_changed()))
    return False

  def _wait_for_scan_to_finish(self, timeout, scan_type=None):
    current_scan = self._is_atlas_scan_running()
    if not current_scan:
      DEBUG("No ATLAS scan is running")
      return
    if scan_type and scan_type not in current_scan.lower():
      ERROR("Expecting ATLAS scan %s to be running but found %s"
                      %(scan_type, current_scan))
    INFO("ATLAS is running %s scan. Scan timeout : %s"%(current_scan, timeout))
    stime = datetime.now()
    etime =  time()+timeout
    timespent = 0
    expected_scan = "" if not scan_type else "(Expected : %s)"%scan_type
    while etime-time() > 0:
      current_scan = self._is_atlas_scan_running()
      scan_time = datetime.now()-stime
      if not current_scan:
        INFO("ATLAS scan finished in %s"%(scan_time))
        return
      if timespent%300 == 0:
        DEBUG("ATLAS Scan %s%s is running since %s"
              %(current_scan, expected_scan, scan_time))
      else:
        print("ATLAS Scan %s%s is running since %s"
              %(current_scan, expected_scan, scan_time))
      sleep(30)
      timespent += 30
    raise Exception("Timeout after %s secs, while waiting for ATLAS scan %s%s "
              "to finish."%(datetime.now()-stime, current_scan, expected_scan))