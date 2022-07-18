class POD():
  def __init__(self, mspcluster, podname, namespace):
    self._msp = mspcluster
    self._name = podname
    self._namespace = namespace

  @property
  def name(self):
    return self._name

  @property
  def namespace(self):
    return self._namespace

  def exec_cmd(self, cmd):
    return self._msp.execute_command_in_pod(cmd=cmd, pod_name=self._name,
                                            namespace=self._namespace)

  def restart(self):
    return self._msp.restart_pod(self._name, self._namespace)

  def delete(self):
    return self._msp.restart_pod(self._name, self._namespace)