
class VCenter(object):
  def __init__(self, vcenterhost, user, pwd, dcname, clustername,  port):
    from pyVmomi import vim
    from pyVim import connect
    self._host, self._user, self._pwd, self._port = vcenterhost, user, pwd, port
    self._dcname, self._clustername = dcname, clustername
    self._si = connect.SmartConnectNoSSL(host=vcenterhost, user=user, pwd=pwd,
                                         port=port)
    #Connect to vCenter. Exit on failure.
    atexit.register(connect.Disconnect, self._si)
    self._content = self._si.RetrieveContent()
    self._children = self._content.rootFolder.childEntity
    self._dcmor = self.get_datacenter_mor(dcname)
    self._cluster = self.get_cluster_mor(clustername)

  def get_datacenter_mor(self, dcname):
    for child in self._children:
      if str(child.name) in dcname and dcname in str(child.name):
        return child
    raise Exception("Failed to find datacenter - {0}".format(dcname))

  def get_cluster_mor(self, clustername):
    for cluster in self._dcmor.hostFolder.childEntity:
      if cluster.name == clustername:
        return cluster
    raise Exception("Failed to find clustername - {0}".format(clustername))

  def get_all_hosts_from_cluster(self, hostip=None):
    if not hostip:
      return self._cluster.host
    for host in self._cluster.host:
      if host.name == hostip:
        return host
    raise Exception("Failed to find host - {0}".format(hostip))

  def add_host_portgroup(self, vswitch_name, portgroup_name, vlan_id):
    for host in self.get_all_hosts_from_cluster():
      portgroup_spec = vim.host.PortGroup.Specification()
      portgroup_spec.vswitchName = vswitch_name
      portgroup_spec.name = portgroup_name
      portgroup_spec.vlanId = int(vlan_id)
      network_policy = vim.host.NetworkPolicy()
      network_policy.security = vim.host.NetworkPolicy.SecurityPolicy()
      portgroup_spec.policy = network_policy
      task=host.configManager.networkSystem.AddPortGroup(portgroup_spec)
      self.wait_for_tasks(task)

  def wait_for_tasks(self, task, timeout=600):
    if not task:
      return
    etime = time()+timeout
    msg = ""
    while etime - time() > 0:
      try:
        qtime=task.info.queueTime
        stime=task.info.startTime
        ctime=task.info.completeTime
        taskprogress = task.info.progress
      except:
        taskprogress=None
      msg = "Task Entity Name - %s : Task Name - %s,  Task State - %s, "\
            "TaskProgress=%s"%(task.info.entityName, task.info.descriptionId,
                               task.info.state,taskprogress)
      if task.info.state == 'success':
        return task.info.state
      elif task.info.state == 'error':
        raise Exception("%s, failed with error : - %s, Complete Error  : %s"
              %(msg, task.info.error.msg, task.info.error))
      sleep(10)
    raise Exception("Task failed with error : Timeout, Task Details : %s"%(msg))

  '''
  Get datastore managed object reference object.
  '''
  def get_datastoremor(self, dcmor, datastorename="datastorename"):
    datastore_folder_mor = dcmor.datastoreFolder
    for datastore in datastore_folder_mor.childEntity:
      if datastore.name == datastorename:
        return datastore
    return None

  '''
  Get network managed object reference
  '''
  def get_networkmor(self, host, networkname='VM Network'):
    for network in host.network:
      if network.name == networkname:
        return network
    return None
