
class SSHCommandExecutionFailed(Exception):
  def __init__(self, msg, out, err)
    super(SSHCommandExecutionFailed, self).__init__(msg)
    self.output = out
    self.outerr = err
