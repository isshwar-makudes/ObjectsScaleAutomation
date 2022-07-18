from time import time, sleep
from json import loads
from requests import get
from datetime import datetime
from re import findall

from lib.generic.utils import remote_exec, execute_rest_api_call
from lib.generic.logger import INFO, DEBUG, ERROR

class AOSCluster():
  def __init__(self, peip, adminuser, adminpass, username="nutanix",
               password="nutanix/4u"):
    self._peip = peip
    self._adminuser = adminuser
    self._adminpass = adminpass
    self._user = username
    self._pass = password
    self._pe_url = "https://%s:9440"%self._peip

  def get_storage_pool_usage(self, name=None):
    url = "%s/PrismGateway/services/rest/v1/storage_pools/"%self._pe_url
    res = execute_rest_api_call(url=url, request_method=get,
                                            data=None, retry=5)
    res = loads(res.content)
    usage = 0
    for entity in res["entities"]:
      usage += int(entity["usageStats"]["storage.usage_bytes"])
    return usage

  def curator_master(self, timeout=600):
    etime = time()+timeout
    while etime-time() > 0:
      try:
        return self._get_curator_master()
      except Exception as err:
        print("Failed to get curator master. Err : %s"%err)
      sleep(30)

  def initiate_full_scan(self, wait_for_completion=True, timeout=24*3600,
                      wait_for_ongoing_scan_to_finish=True):
    if wait_for_ongoing_scan_to_finish:
      self._wait_for_ongoing_curator_scan(timeout)
    curator_master = self.curator_master()
    cmd = "links -dump http://%s:2010/master/api/"%(curator_master)
    cmd += "client/StartCuratorTasks?task_type=2"
    out, err = self._exec(cmd, curator_master)
    INFO("Started curator full scan.Cmd : %s,Out : %s,Err : %s"%(cmd, out, err))
    sleep(30)
    if not self._is_curator_scan_running():
      raise Exception("Curator fullscan is invoked but not found on curator")
    if not wait_for_completion:
      return
    stime = datetime.now()
    self._wait_for_ongoing_curator_scan(timeout)
    INFO("Curator full scan completed in %s"%(datetime.now()-stime))

  def _wait_for_ongoing_curator_scan(self, timeout):
    if not self._is_curator_scan_running():
      return
    etime = time() + timeout
    INFO("Waiting for ongoing curator scan to finish")
    timespent = 0
    scan_fetch_failed_time = 0
    while etime-time() > 0:
      try:
        if not self._is_curator_scan_running():
          INFO("Curator scan completed in %s secs"%timespent)
          return
        scan_fetch_failed_time = 0
      except Exception as err:
        ERROR("Failed to get curator scan status. Err : %s"%err)
        scan_fetch_failed_time += 30
      if timespent%300 == 0:
        DEBUG("Curator scan is running since : %s secs"%timespent)
      else:
        print("Curator scan is running since : %s secs"%timespent)
      if scan_fetch_failed_time == 1800:
        raise Exception("Not able to get scan status from last 30+ mins")
      sleep(30)
      timespent += 30
    raise Exception("Hit timeout after %s secs, while waiting for ongoing "
                    "curator scan to finish"%timeout)

  def _get_curator_master(self):
    cmd = 'links -dump http:0:2010 | grep Master'
    out, err = self._exec(cmd)
    if "Curator Master" in out:
      ip = findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", out)
      if ip:
        return ip[0].strip()
    if "Master Start" in out:
      return self._peip
    raise Exception("Failed to find curator master")

  def _is_curator_scan_running(self, retry=10, interval=30):
    curator_master = self.curator_master()
    cmd = "links -dump http://%s:2010 | grep Running"%curator_master
    while retry > 0:
      try:
        out, err = self._exec(cmd, curator_master)
      except Exception as err:
        ERROR("Failed to get curator scan status. Error : %s, Retry : %s"
              %(err, retry))
        if retry <= 0:
          raise
        retry -= 1
        sleep(interval)
        continue
      if "Running" in out:
        return True
      return False

  def create_vm_from_image(self, vmname, vmimage, image_source_url,
                           storage_ctr, network, num_clones, memory_in_mb,
                           num_vcpus, num_cores_per_vcpu, network_vlan):
    if not self._is_vm_exists(vmname):
      self.create_network(network, network_vlan)
      self.create_image(vmimage, image_source_url, container=storage_ctr)
    res = self.create_vm(vmname, vmimage, network, memory_in_mb, num_vcpus,
                         num_cores_per_vcpu)
    vms = {}
    vms_created = self.clone_vm(vmname, vmname, num_clones)
    for vm in vms_created:
      vms[vm] = {}
    self.power_on("%s-"%vmname)
    vms = self.get_vm_details(vms.keys())
    INFO("VMs created : %s"%vms)
    return self.get_vm_details(vms.keys())
    """
    for vm in vms.keys():
      vms[vm] = {"ip":self.get_ip(vm)}
    INFO("VMs created : %s"%vms)
    return vms
    """

  def create_vm(self, vmname, vmimage, network, memory_in_mb, num_vcpus,
                num_cores_per_vcpu):
    if self._is_vm_exists(vmname):
      return True
    INFO("Creating VM %s"%(vmname))
    cmd = "vm.create %s memory=%sM num_vcpus=%s "%(vmname, memory_in_mb,
                                                   num_vcpus)
    cmd += "num_cores_per_vcpu=%s"%(num_cores_per_vcpu)
    self._acli(cmd)
    self.disk_add(vmname, vmimage)
    self.nic_add(vmname, network)

  def create_image(self, imagename, source_url, image_type="kDiskImage",
                  container='NutanixManagementShare', wait_for_completion=True):
    if self._image_exists(imagename):
      return self.get_image(imagename)
    INFO("Creating VM Image %s:%s"%(imagename, source_url))
    cmd = "image.create %s container=%s image_type=%s source_url=%s wait=%s"%(
              imagename, container, image_type, source_url, wait_for_completion)
    self._acli(cmd)
    return self.get_image(imagename)

  def create_network(self, networkname, vlan):
    if self._is_network_exists(networkname):
      return self.get_network(networkname)
    INFO("Creating Network:vlanid %s:%s"%(networkname, vlan))
    cmd = "net.create %s vlan=%s"%(networkname, vlan)
    self._acli(cmd)
    return self.get_network(networkname)

  def get_network(self, network):
    return self._acli("net.get %s"%network)

  def _is_network_exists(self, network):
    try:
      self.get_network(network)
      return True
    except:
      return False

  def get_image(self, imagename):
    return self._acli("image.get %s"%imagename)

  def _image_exists(self, imagename):
    try:
      self._acli("image.get %s"%imagename)
      return True
    except:
      return False

  def get_ip(self, vmname, timeout=600, get_all_details=False):
    end_time = time()+timeout
    details = None
    while end_time - time() > 0:
      details = {}
      try:
        details = self.get_vm(vmname)
      except Exception as err:
        print("Failed to get VM details : %s, Err : %s"%(vmname, err))
      if "ipAddresses" not in details or not details['ipAddresses'
                                          ] or len(details['ipAddresses']) < 1:
        print("Waiting for %s to get ip. Current Details : %s"%(
                                            vmname, details.get("ipAddresses")))
        sleep(30)
        continue
      if get_all_details:
        return details
      return details['ipAddresses'][0]
    raise Exception("Hit timeout after %s secs while waiting for VM IP. VM : %s"
                    ", Details : %s"%(timeout, vmname, details))

  def power_on(self, vmname):
    cmd = "vm.on %s*"%vmname
    return self._acli(cmd)

  def power_reset(self, vmprefix=None, vmname=None):
    cmd = "vm.reset %s"%vmname
    if vmprefix:
      cmd = "vm.reset %s*"%vmprefix
    return self._acli(cmd)

  def clone_vm(self, vmname, vmprefix, num_clones=1):
    new_names = [vmprefix]
    if num_clones > 1:
      new_names = ["%s-%s"%(vmprefix, i) for i in range(num_clones)]
    vms_to_clone = self._bind_to_existing_vms(new_names)
    if vms_to_clone:
      new_vms = ",".join(vms_to_clone)
      INFO("Cloning VM %s from %s"%(new_vms, vmname))
      cmd = "vm.clone %s clone_from_vm=%s"%(new_vms, vmname)
      self._acli(cmd)
    return new_names

  def get_vm_details(self, vmnames):
    vmdetails = {}
    for vm in self.get_vm():
      vmdetails[vm['vmName']] = vm
    existing_vms = vmdetails.keys()
    res = {}
    for new_vmname in vmnames:
      if new_vmname in existing_vms:
        res[new_vmname] = vmdetails[new_vmname]
      else:
        res[new_vmname] = self.get_ip(new_vmname, get_all_details=True)
    #small hack to make sure existing code does not break
    vms = {}
    for vm in res.keys():
      print("%s : %s"%(vm, res[vm]['ipAddresses']))
      if not res[vm]['ipAddresses']:
        vmip = self.get_ip(vm)
        vms[vm] = {'ip': vmip}
        continue
      vms[vm] = {'ip': res[vm]['ipAddresses'][0]}
    return vms

  def _bind_to_existing_vms(self, vmnames):
    vms_to_clone = []
    existing_vms = [i['vmName'] for i in self.get_vm()]
    for new_vmname in vmnames:
      if new_vmname in existing_vms:
        print("VM %s alredy exists, binding "%new_vmname)
        continue
      vms_to_clone.append(new_vmname)
    if vms_to_clone:
      print("VM to clone : %s"%vms_to_clone)
    return vms_to_clone

  def nic_add(self, vmname, network):
    cmd = "vm.nic_create %s network=%s"%(vmname, network)
    return self._acli(cmd)

  def disk_add(self, vmname, vmimage):
    cmd = "vm.disk_create %s clone_from_image=%s"%(vmname, vmimage)
    return self._acli(cmd)

  def get_vm(self, vmname=None):
    url = "https://%s:9440/PrismGateway/services/rest/v1/vms"%self._peip
    res = execute_rest_api_call(url=url, request_method=get,
                  data=None, username=self._adminuser, password=self._adminpass)
    res = loads(res.content)
    if not vmname:
      return res["entities"]
    for vm in res['entities']:
      if vm['vmName'] == vmname:
        return vm
    raise Exception("VM %s not found"%vmname)

  def _is_vm_exists(self, vmname):
    try:
      self.get_vm(vmname)
      return True
    except:
      return False

  def _exec(self, cmd, peip=None):
    if not peip:
      peip = self._peip
    output, errors = remote_exec(peip, cmd)
    if output is None and errors is None:
      raise Exception("Output and Errors are returned as None. Cmd : %s, "
                      "PE : %s"%(cmd, peip))
    if output: output.rstrip()
    if errors: errors.rstrip()
    return output, errors

  def _acli(self, cmd):
    # TO much hack here. Go REST route
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli -o json %s"%(cmd)
    out, errors = remote_exec(self._peip, cmd)
    if errors:
      raise Exception("Failed to execute %s on %s, Err : %s (out : %s)"
                      %(cmd, self._peip, errors, out))
    outputs = out.rstrip().split("\n")
    for line in outputs:
      if not line.strip():
        continue
      output = loads(line.strip())
      if isinstance(output['status'], str) and output["status"
                                                          ].rstrip().isdigit():
        output["status"] = int(output["status"])
      if not isinstance(output['status'], int):
        ERROR("cmd : %s, out : %s, err : %s"%(cmd, output, errors))
      if output["status"] != 0:
        raise Exception("Failed to execute %s on %s, Err : %s (out : %s)"
                        %(cmd, self._peip, errors, out))
    res = [i.rstrip() for i in outputs if i]
    if not res[-1]:
      ERROR("Output is empty, Cmd : %s, Out : %s, RawOut : %s, OutLIst : %s"
            %(cmd, res, out.rstrip(), out.split("\n")))
    return loads(res[-1]) if res[-1] else {}