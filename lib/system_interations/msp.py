from time import time, sleep
from kubernetes import client, config, stream
from os import remove

from lib.constants import Constants
from lib.generic.logger import ERROR, INFO
from lib.generic.utils import write_to_file

class MSP():
  def __init__(self, pcobject):
    self._pc = pcobject
    self.msp_cluster_details = self._pc.get_msp_cluster_details()
    #self.power_on_everything()
    self.k8handle = self._load_kubeconfig()

  def get_all_pods_for_service(self, service_name, namespace=None):
    if not namespace: namespace = Constants.MSP_DEFAULT_NAMESPACE
    unhealthy_pods, healthy_pods = self.get_unhealthy_pods(namespace)
    return (
        [pod for pod in healthy_pods.keys() if pod.startswith(service_name)],
        [pod for pod in unhealthy_pods.keys() if pod.startswith(service_name)])

  def get_all_pods(self):
    all_pods = {}
    for service in self.k8handle.list_pod_for_all_namespaces(watch=False).items:
      if service.metadata.namespace not in all_pods:
        all_pods[service.metadata.namespace] = {"services":{}}
      all_pods[service.metadata.namespace][
                  "services"][service.metadata.name] = {"state":service.status}
    return all_pods

  def get_unhealthy_pods(self, namespace):
    all_pods = self.get_all_pods()
    unhealthy_pods = {}
    healthy_pods = {}
    for service, metadata in all_pods[namespace]["services"].iteritems():
      if service.startswith("objects-browser"):
        continue
      if Constants.MSP_POD_RUNNING_STATE not in metadata["state"].phase:
        unhealthy_pods[service] = metadata["state"].phase
      else:
        healthy_pods[service] = metadata["state"].phase
    return unhealthy_pods, healthy_pods

  def power_on_everything(self):
    for vm in self.msp_cluster_details["vm_names"]:
      try:
        self._pc.on(vm)
      except:
        pass
      try:
        self._pc.resume(vm)
      except:
        pass
    self._pc.start_cvm_cluster()

  def _load_kubeconfig(self):
    kfilename = "kf"+str(time())
    write_to_file(self.msp_cluster_details["kubeconfig"].strip(),
                        kfilename, "w")
    config.load_kube_config(config_file=kfilename)
    configuration = client.Configuration()
    configuration.verify_ssl=False
    configuration.debug = False
    client.Configuration.set_default(configuration)
    k8handle = client.CoreV1Api()
    remove(kfilename)
    return k8handle

  def execute_command_in_pod(self, pod_name, cmd, namespace=None, retry=3,
                            delay=30):
    if not namespace: namespace=Constants.MSP_DEFAULT_NAMESPACE
    if isinstance(cmd, str):
      cmd = cmd.split()
    if isinstance(cmd, str):
      cmd = cmd.split()
    while retry >0:
      try:
        return stream.stream(self.k8handle.connect_get_namespaced_pod_exec, pod_name,
                       namespace, command=cmd, stderr=True, stdin=False,
                       stdout=True, tty=False)
      except Exception as err:
        retry -= 1
        ERROR("Hit error while executing %s on %s. Error  :%s"
              %(cmd, pod_name, err))
        if retry == 0: raise
        sleep(delay)

  def restart_pod(self, podnames, namespace):
    if isinstance(podnames, str): podnames = [podnames]
    for pod in podnames:
      INFO("Initiating POD Restart : %s / %s"%(namespace, pod))
      self.k8handle.delete_namespaced_pod(pod, namespace, async_req=False)


  def get_worker_ips(self):
    pass

  def get_ssh_script(self):
    pass

  def get_registry_node(self):
    pass

  def restart_etcd(self, random_node=True, all_nodes=False):
    pass

  def restart_registry(self):
    pass

  def restart_iam(self):
    pass