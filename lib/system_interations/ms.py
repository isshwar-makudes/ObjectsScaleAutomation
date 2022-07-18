from lib.constants import Constants
from lib.generic.logger import ERROR
from lib.system_interations.pod import POD

class MS():
  def __init__(self, pcobj, mspcluster,
               namespace=Constants.MSP_DEFAULT_NAMESPACE):
    self._pcobj = pcobj
    self._msp = mspcluster
    self._namespace = namespace
    self._init_ms_pods()

  @property
  def namespace(self):
    return self._namespace

  @property
  def service_name(self):
    return Constants.OBJECTS_MS_SERVICE

  def get_compaction_stats(self):
    res = {}
    for ms in self.get_all_ms_pods():
      bg_tasks = self._get_bg_task_manager_page(ms)
      res[ms] = self._get_compaction_status(bg_tasks)
    return res

  def _get_bg_task_manager_page(self, podname):
    cmd = "links -dump http:0:7102/poseidon_db/bg_task_manager"
    return self._allpods[podname].exec_cmd(cmd.split(" "))

  def _get_compaction_status(self, bg_tasks):
    active_compaction_marker = False
    pending_compaction_marker = False
    res = {"active_compactions":[],
           "pending_compactions":[],
           "compaction_in_progress":False}
    if not bg_tasks.strip():
      return res
    for line in bg_tasks.strip().split("\n"):
      if "Compaction in progress".lower() in line.lower(
            ) and "true" in line.lower():
        res["compaction_in_progress"] = True
      if "Active Compactions".lower() in line.lower():
        active_compaction_marker = True
      if active_compaction_marker:
        if pending_compaction_marker:
          active_compaction_marker = False
      if "Pending Compactions In Queue".lower() in line.lower():
        pending_compaction_marker = True
      if pending_compaction_marker:
        if "Active Flush Tasks".lower() in line.lower():
          pending_compaction_marker = False
      for cf in self._column_families:
        if cf in line:
          if active_compaction_marker:
            res["active_compactions"].append(line)
          if pending_compaction_marker:
            res["pending_compactions"].append(line)
    return res

  def _init_ms_pods(self):
    self._column_families = [Constants.POSEIDON_BUCKETINFO_MAP,
      Constants.KPOSEIDON_BUCKETINDEX_MAP, Constants.POSEIDON_BUCKETLIST_MAP,
      Constants.POSEIDON_VDISKINFO_MAP, Constants.POSEIDON_OPENVDISKINFO_MAP,
      Constants.POSEIDON_REGIONINFO_MAP, Constants.POSEIDON_BUCKETSTATS_MAP,
      Constants.POSEIDON_BUCKET_REPLICATIONINFO_MAP,
      Constants.POSEIDON_OBJECT_REPLICATIONINFO_MAP,
      Constants.POSEIDON_OBJECT_TAGINFO_MAP, Constants.POSEIDON_VNODESTATS_MAP,
      Constants.POSEIDON_DIRECTORYINFO_MAP,
      Constants.POSEIDON_LIFECYCLE_POLICYINFO_MAP,
      Constants.POSEIDON_GLOBAL_REGIONINFO_MAP,
      Constants.POSEIDON_BUCKET_TAGINFO_MAP]
    healthy_pods, unhealthy_pods = self._msp.get_all_pods_for_service(
                            self.service_name, self.namespace)
    if unhealthy_pods:
      ERROR("Found Unhealthy MS PODs : %s"%unhealthy_pods)
    if not healthy_pods:
      ERROR("No healthy MS PODs found")

    self._allpods = {}
    for podname in healthy_pods+unhealthy_pods:
      self._allpods[podname] = POD(self._msp, podname, self.namespace)

  def get_all_ms_pods(self):
    return self._allpods.keys()

  def is_bucket_list_compaction_running(self):
    cstats = self.get_compaction_stats()
    for ms, stats in cstats.iteritems():
      for active_compactions in stats["active_compactions"]:
        if Constants.POSEIDON_BUCKETLIST_MAP in active_compactions:
          return True, cstats
      for pending_compactions in stats["pending_compactions"]:
        if Constants.POSEIDON_BUCKETLIST_MAP in pending_compactions:
          return True, cstats
    return False, cstats