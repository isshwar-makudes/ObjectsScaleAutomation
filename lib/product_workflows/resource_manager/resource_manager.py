from lib.generic.logger import INFO
from lib.product_workflows.objects import PCObjects
from lib.product_workflows.resource_manager.vcenter import VCenter

class ResourceManager(object):

  def __init__(self, args):
    # TODO: send only needed args
    self.args = args
    self._pcobj = None
    self._aoscluster = None

  def create_or_bind_oss(self):
    args = self.args
    self._pcobj = PCObjects(args["pcip"], args["pcuser"], args["pcpass"],
                      args["objects_name"], args["endpoint"], args["peip"])
    vip, dsip = args["pe_vip"], args["pe_dsip"]
    if not args["skip_aos_setup_post_rdm_deployment"]:
      vip, dsip = self._pcobj.configure_vip_dsip(peip=pe, vip=args["pe_vip"],
                                         dsip=args["pe_dsip"])
      INFO("VIP/DSIP/DeploymentsIDs : --pe_vip %s --pe_dsip %s --deployment_id "
           "%s --deployments %s --skip_aos_setup_post_rdm_deployment true"
           %(vip,dsip, deployment_id, " ".join(deployments)))
      self._pcobj.configure_dns_ntp(pe)
      self._pcobj.configure_dns_ntp(pc)
      if args["hypervisor_type"] == "ahv":
        self._pcobj.create_network(args["internal_network"], args["vlan_id"],
                             args["network_prefix"], args["start_ip"],
                             args["end_ip"])
      else:
        vcenter = VCenter(args["vcenter_ip"], args["vcenter_user"],
                          args["vcenter_pwd"], args["vcenter_dc"],
                          args["vcenter_cluster"], args["vcenter_port"])
        vcenter.add_host_portgroup("vSwitch0", args["internal_network"],
                                   args["vlan_id"])
        self._pcobj.register_to_vcenter(vcenter)
        self._pcobj.create_esxi_network(vcenter, args["internal_network"],
                                  args["netmask"], args["gateway"],
                                  args["start_ip"], args["end_ip"])
    args["peip"] = vip
    num_nodes_list = args["num_nodes"]
    # Pick one value
    args["num_nodes"] = choice(num_nodes_list)
    INFO("Deploying Objects")
    # Handles bind as well
    self._pcobj.deploy(**args)
    # Reset to what was given originally
    args["num_nodes"] = num_nodes_list

    # Fetch endpoint url
    # TODO: Both http and https version needed?
    args["endpoint_url"] = ",".join(["http://%s,https://%s"%(i, i)
                                 for i in self._pcobj.get_objects_endpoint(get_all=True)])
    
    # TODO: applicable?
    if args.get("secondary_objects_name"):
      INFO("Replacing deployment details with secondary objects cluster names")
      args["objects_name"] = args["secondary_objects_name"]
      args["client_ips"] = args["secondary_client_ips"]
      args["internal_ips"] = args["secondary_internal_ips"]
      args["endpoint_url"] = args["secondary_endpoint_url"]

    return args

  def create_vms(self):
    INFO("Deploying %s clients for remote execution" % self.args["num_remote_clients"])
    if not self._aoscluster:
      self._aoscluster = AOSCluster(self.args["remote_clients_peip"],
                              self.args["remote_clients_pe_admin_user"],
                              self.args["remote_clients_pe_admin_password"])
    clients = self._aoscluster.create_vm_from_image(
                self.args["remote_clients_vmname"],
                self.args["remote_clients_vmimage"],
                self.args["remote_clients_vmimage_url"],
                self.args["remote_clients_storage_ctr"],
                self.args["remote_clients_network_name"],
                self.args["num_remote_clients"],
                self.args["remote_clients_memory"],
                self.args["remote_clients_vcpu"],
                self.args["remote_clients_cores_per_vcpu"],
                self.args["remote_clients_network_vlanid"])
    client_ips = [details['ip'] for vm, details in clients.iteritems()]
    return client_ips

  def create_or_bind_iam_user(self):
    pass