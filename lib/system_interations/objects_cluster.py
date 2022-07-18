from lib.system_interations.atlas import ATLAS
from lib.system_interations.ms import MS

class ObjectsCluster():
  def __init__(self, pcobj, mspcluster):
    self._msp = mspcluster
    self._pc = pcobj
    self._init_components()

  def _init_components(self):
    self.atlas = ATLAS(self._pc, self._msp)
    self.ms = MS(self._pc, self._msp)