from json import loads
from lib.generic.logger import INFO, ERROR

class Mspctl():
  def __init__(self, pcobj):
    self._pc = pcobj
    self._version = None
    self._mspctl = "/usr/local/nutanix/cluster/bin/mspctl"
    self.get_mspctl_version()
    #self.get_cluster_version()

  def get_cluster_version(self):
    cmd = "%s controller version -o json"%(self._mspctl)
    if self._version == "1.0":
      cmd = "%s controller_version"%(self._mspctl)
    self.cluster_version = self._pc.exec_cmd(cmd, retry=0)
    #INFO("MSP Cluster Version : %s"%self.cluster_version)

  def get_mspctl_version(self):
    if self._version:
      return self._version
    cmd = "%s cluster_list"%self._mspctl
    try:
      self._pc.exec_cmd(cmd, retry=0)
      self._version = "1.0"
    except Exception as err:
      ERROR("Failed to mspctl cluster_list. Error  :%s"%(err))
      self._version = "2.0"
    INFO("Mspctl version is : %s"%(self._version))

  def get_list(self, output_format="json"):
    cmd = "%s cluster list -o json"%self._mspctl
    if self._version == "1.0":
      cmd = "%s cluster_list"%self._mspctl
    return loads(self._pc.exec_cmd(cmd))

  def cluster_destroy(self, cluster_uuid):
    cmd = "%s cluster delete -f --confirm=False %s"%(self._mspctl, cluster_uuid)
    if self._version == "1.0":
      cmd = "%s destroy -u %s -f"%(self._mspctl, cluster_uuid)
    self._pc.exec_cmd(cmd)

  def get_kubeconfig(self, cluster_uuid):
    cmd = "%s cluster kubeconfig %s"%(self._mspctl, cluster_uuid)
    if self._version == "1.0":
      cmd = "%s kubeconfig -u %s"%(self._mspctl, cluster_uuid)
    return self._pc.exec_cmd(cmd)

  def get_status(self, cluster_uuid, output_format="json"):
    cmd = "%s cluster get %s -o json"%(self._mspctl, cluster_uuid)
    if self._version == "1.0":
      cmd = "%s  status -u %s"%(self._mspctl, cluster_uuid)
    return loads(self._pc.exec_cmd(cmd))

  def disable_ha_drs_flags(self):
    cmd = "%s controller flag set skip_ha_check true;"%(self._mspctl)
    cmd += "%s controller flag set disable_memory_thick_prov_esx"%(self._mspctl)
    cmd += " true; %s controller flag set skip_drs_check true;"%(self._mspctl)
    if self._version == "1.0":
      cmd = "%s flag -a set -f disable_memory_thick_prov_esx="%(self._mspctl)
      cmd += "true;%s flag -a set -f skip_ha_check=true;"%(self._mspctl)
      cmd += "%s flag -a set -f skip_drs_check=true"%(self._mspctl)
    INFO("Disabling msp flags : %s"%(cmd))
    self._pc.exec_cmd(cmd)