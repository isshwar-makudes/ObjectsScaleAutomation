import socket

from requests import get, post, put, delete
from json import dumps, loads
from random import choice
from time import time, sleep
from base64 import b64decode
from os import system
from uuid import uuid4
from ssl import get_server_certificate
from traceback import format_exc

from lib.generic.logger import INFO, WARN, ERROR, DEBUG
from lib.generic.utils import remote_exec, get_vip_dsip
from lib.system_interations.mspctl import Mspctl

class PC():
  def __init__(self, pcip, username, password, objects_name, endpoint_url,
               peip=None):
    self.pcip = pcip
    self.peip = peip
    self._mspctl = None
    self.uuid = None
    self.username = username
    self.password = password
    self.objects_name = objects_name
    self._endpoint_ip = endpoint_url.split("/")[-1]
    self._pc_url = "https://%s:9440"%(self.pcip)
    self._oss_url = "%s/oss/api/nutanix/v3/objectstores"%(self._pc_url)
    self._oss_groups_url = "%s/oss/api/nutanix/v3/groups"%(self._pc_url)
    self.is_objects_service_enabled()
    self._init_cluster_properties()
    self.msp_uuid=None
    self._lcm = Upgrade(self)
    self.uuid = self.objects_uuid

  @property
  def objects_uuid(self):
    if self.uuid:
      return self.uuid
    if not self.is_objects_service_enabled():
      WARN("Objects service not enabled on %s"%self.pcip)
      return None
    self.uuid = self._get_objects_uuid()
    return self.uuid

  @property
  def state(self):
    if not self._state:
      self._get_objects_uuid()
    return self._state

  def whitelist_nfs_client(self, client_ip):
    payload = {"nfs_allowed_client_cidr_list":[{"cidr":client_ip}]}
    url = "%s/%s/nfs/allowed_clients"%(self._oss_url, self.objects_uuid)
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))

  def configure_notification_endpoint(self, syslog_endpoint=None,
                                      nats_endpoint=None, kafka_endpoint=None):
    current_config = self.get_bucket_notification_config()
    INFO("Current notification endpoint config : %s"%current_config)
    payload = {"api_version": "3.0"}
    payload = self._get_syslog_notification_config(payload, current_config,
                                                   syslog_endpoint)
    payload = self._get_nats_notification_config(payload, current_config,
                                                 nats_endpoint)
    payload = self._get_kafka_notification_config(payload, current_config,
                                                 kafka_endpoint)
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/notifications"%(
            self._pc_url, self.objects_uuid)
    INFO("Enabling notification endpoint, payload : %s, Url : %s"
         %(payload, url))
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    res = res.json()
    msg = ""
    if syslog_endpoint and 'syslog' not in res:
      msg += "Syslog endpoint not found in res, "
    if nats_endpoint and 'nats_streaming' not in res:
      msg += "Nats endpoint not found in res, "
    if kafka_endpoint and 'kafka' not in res:
      msg += "Kafka endpoint not found in res, "
    if msg: ERROR("%s in res : %s"%(msg, res))
    INFO("Post notification endpoint configuration : %s"
         %self.get_bucket_notification_config())
    return res

  def _get_kafka_notification_config(self, payload, current_config,
                                      kafka_endpoint):
    if kafka_endpoint: kafka_endpoint = kafka_endpoint.split(",")
    kafkaendpt = current_config.get("kafka", {})
    if kafkaendpt:
      kafkaendpt = current_config.get("kafka", {}).get("endpoint")
    res = False
    if kafkaendpt and kafka_endpoint:
      for endpt in kafka_endpoint:
        if endpt not in [str(i) for i in kafkaendpt]:
          res = True
    else:
      res = True
    if res:
      payload['kafka'] = {
          "disable_success_write":False,
          "enable_failed_write":False,
          "endpoint":kafka_endpoint
        }
    else:
      INFO("Skipping configuring kafka endpoint ( %s ) since its already "
           "configured or not provided"%kafka_endpoint)
      if current_config.get("kafka", {}):
        payload['kafka'] = current_config["kafka"]
    return payload

  def _get_nats_notification_config(self, payload, current_config,
                                      nats_endpoint):
    if nats_endpoint: nats_endpoint = nats_endpoint.split(",")
    natsendpt = current_config.get("syslog", {})
    if natsendpt:
      natsendpt = current_config.get("syslog", {}).get("endpoint")
    res = False
    if natsendpt and nats_endpoint:
      for endpt in nats_endpoint:
        if endpt not in [str(i) for i in natsendpt]:
          res = True
    if res:
      payload['nats_streaming'] = {
          "disable_success_write":False,
          "enable_failed_write":False,
          "endpoint":nats_endpoint,
          "cluster_id":"test"
        }
    else:
      INFO("Skipping configuring nats endpoint ( %s ) since its already "
           "configured or not provided"%nats_endpoint)
      if current_config.get("nats_streaming", {}):
        payload['nats_streaming'] = current_config["nats_streaming"]
    return payload

  def _get_syslog_notification_config(self, payload, current_config,
                                      syslog_endpoint):
    if syslog_endpoint: syslog_endpoint = syslog_endpoint.split(",")
    sysendpt = current_config.get("syslog")
    if sysendpt:
      sysendpt = current_config.get("syslog").get("endpoint")
    INFO("Preparing syslog endpoint : %s. Current : %s"
         %(syslog_endpoint, sysendpt))
    res = False
    if sysendpt and syslog_endpoint:
      for endpt in syslog_endpoint:
        if endpt not in [str(i) for i in sysendpt]:
          res = True
    if res:
      payload['syslog'] = {
          "endpoint":syslog_endpoint
        }
    else:
      INFO("Skipping configuring syslog endpoint ( %s ) since its already "
           "configured or not provided"%syslog_endpoint)
      if current_config.get("syslog", {}):
        payload['syslog'] = current_config["syslog"]
    return payload

  def enable_bucket_notification(self, bucket, kafka=False, syslog=True,
                                 nats=False):
    current_config = self.get_bucket_notification_config()
    if kafka:
      self.enable_kafka_bucket_notification(bucket, current_config)
    if syslog:
      self.enable_syslog_bucket_notification(bucket, current_config)
    if nats:
      self.enable_nats_bucket_notification(bucket, current_config)

  def enable_nats_bucket_notification(self, bucket, current_config):
    if not current_config.get("nats_streaming"):
      INFO("Nats notification endpoint not found : %s, skipping update"
           %current_config)
      return
    payload = {"api_version": "3.0",
               "notification_rule":{"data_events":["CREATE_ALL", "ACCESS_ALL",
                                                     "REMOVE_ALL",
                                                     "CREATE_ALL_ERROR",
                                                     "ACCESS_ALL_ERROR",
                                                     "REMOVE_ALL_ERROR"],
                                    "nats_streaming_enabled":True}}
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/"%self._pc_url
    url += "%s/buckets/%s/notification_rules/"%(self.objects_uuid, bucket)
    INFO("Enabling Nats notification config on %s, payload : %s, Url : %s"
         %(bucket, payload, url))
    self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))

  def enable_syslog_bucket_notification(self, bucket, current_config):
    if not current_config.get("syslog"):
      INFO("Syslog notification endpoint not found : %s, skipping update"
           %current_config)
      return
    self.delete_syslog_bucket_notification(bucket)
    payload = {"api_version": "3.0",
               "notification_rule":{"data_events":["CREATE_ALL", "ACCESS_ALL",
                                                     "REMOVE_ALL",
                                                     "CREATE_ALL_ERROR",
                                                     "ACCESS_ALL_ERROR",
                                                     "REMOVE_ALL_ERROR"],
                                    "syslog_enabled":True}}
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/"%self._pc_url
    url += "%s/buckets/%s/notification_rules/"%(self.objects_uuid, bucket)
    INFO("Enabling syslog notification config on %s, payload : %s, Url : %s"
         %(bucket, payload, url))
    self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))

  def delete_syslog_bucket_notification(self, bucket):
    payload = {
                "entity_type": "bucket_notification_rule",
                "filter_criteria": "bucket_name==%s"%bucket,
                "group_member_sort_attribute": "entity_id",
                "group_member_attributes": [
                  {
                    "attribute": "entity_id"
                  },
                  {
                    "attribute": "syslog_enabled"
                  }
                ]
              }
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/groups"%(self._pc_url,
                                                              self.objects_uuid)
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    res = res.json()
    if not("group_results" in res and res["group_results"]):
      return
    eid = res['group_results'][0]['entity_results'][0]['entity_id']
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/"%self._pc_url
    url += "%s/buckets/%s/notification_rules/%s"%(self.objects_uuid, bucket, eid)
    INFO("Deleting current syslog config on %s is : %s, Url : %s"
         %(bucket, res, url))
    self._execute_rest_api_call(url=url, request_method=delete, data=None)

  def enable_kafka_bucket_notification(self, bucket, current_config):
    if not current_config.get("kafka"):
      INFO("Kafka notification endpoint not found : %s, skipping kafka update"
           %current_config)
      return
    payload = {"api_version": "3.0",
               "notification_rule":{"data_events":["CREATE_ALL", "ACCESS_ALL",
                                                     "REMOVE_ALL",
                                                     "CREATE_ALL_ERROR",
                                                     "ACCESS_ALL_ERROR",
                                                     "REMOVE_ALL_ERROR"],
                                    "kafka_enabled":True}}
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/"%self._pc_url
    url += "%s/buckets/%s/notification_rules/"%(self.objects_uuid, bucket)
    INFO("Enabling kafka notification config on %s, payload : %s, Url : %s"
         %(bucket, payload, url))
    self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))

  def get_bucket_notification_config(self):
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/notifications"%(
                                              self._pc_url, self.objects_uuid)
    res = self._execute_rest_api_call(url=url, request_method=get, data=None,
                                      retry=3)
    return res.json()

  def enable_no_squash(self, bucketname, uid=0, gid=0, dir_perm='rwxr-xr-x',
                       file_perm='rw-r--r--', squash='NoSquash'):
    url = "%s/%s/buckets/%s"%(self._oss_url, self.objects_uuid, bucketname)
    payload = {'api_version': '3.0',
               'metadata':{'kind':"bucket"},
               'spec': {'description': '',
                'name': bucketname,
                'resources': {'features': [],
                 'nfs_configuration': {'owner': {'GID': gid, 'UID': uid},
                  'permissions': {'directory': dir_perm, 'file': file_perm},
                  'readonly': False,
                  'squash': squash}}}}
    res = self._execute_rest_api_call(url=url, request_method=put,
                                      data=dumps(payload))
    return loads(res.content)

  def update_storage_capacity(self, total_size_in_gb):
    #https://<PC>/oss/api/nutanix/v3/objectstores/<UUID>/update-total-capacity
    #POST
    pass

  def get_replication_endpoints(self, az="Local AZ"):
    url = "%s/oss/pc_proxy/groups"%(self._pc_url)
    payload = {"entity_type":"availability_zone",
               "group_member_attributes":[{"attribute":"name"}],
               }
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    res = loads(res.content)
    details = self._extract_cluster_details(res)
    if not az:
      return details
    for azname, azdetails in details.iteritems():
      if azdetails["name"] == az:
        return azdetails

  def add_replication_rule(self, bucket_name, repl_cluster):
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/buckets/%s/"\
          "replication_rule"%(self._pc_url, self.objects_uuid, bucket_name)
    payload = {"api_version":"3.0",
               "metadata":{"kind":"bucket_replication",
                           "source_bucket_name":bucket_name},
                "spec":{"source_oss_fqdn":"%s.%s"%(self.cluster_name,
                                                   self.cluster_domain),
                        "source_oss_uuid":self.cluster_uuid,
                        "target_bucket_name":repl_cluster["bucket"],
                        "target_endpoint_list":repl_cluster["endpoint_list"],
                        "target_oss_fqdn":repl_cluster["oss_fqdn"],
                        "target_oss_uuid":repl_cluster["oss_uuid"],
                        "target_pc":"Local AZ",
                        "status":True}}
    res = self._execute_rest_api_call(url=url, request_method=put,
                                      data=dumps(payload), retry=3)
    return loads(res.content)

  def get_tiering_endpoints(self, endpoint_name=None):
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/groups"%(self._pc_url,
                                                              self.objects_uuid)
    payload = {"entity_type":"endpoint",
             "group_member_attributes" :[{"attribute":"entity_id"},
                                        {"attribute":"endpoint_name"},
                                        {"attribute":"tiering_usage_bytes"},
                                        {"attribute":"tiering_eligible_bytes"},
                                        {"attribute":"s3_endpoint_type"},
                                        {"attribute":"s3_data_store_uri"},
                                        {"attribute":"endpoint_type"}]}
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    res = loads(res.content)
    details = self._extract_cluster_details(res)
    if not endpoint_name:
      return details
    for eid, endpoint_details in details.iteritems():
      if endpoint_details["endpoint_name"] == endpoint_name:
        return endpoint_details

  def add_tiering_endpoint(self, endpoint_name, endpoint_ip, bucket_name,
                           access_key, secret_key, bind=True):
    if bind:
      res = self.get_tiering_endpoints(endpoint_name)
      if res:
        return res
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/endpoint_proxy/"\
          "endpoint"%(self._pc_url, self.objects_uuid)
    endpoint_url = "%s.%s"%(bucket_name, endpoint_ip)
    payload = {"api_version":"3.0",
               "kind":"endpoint",
               "endpoint":{
                            "endpoint_type":"s3",
                            "endpoint_name":endpoint_name,
                            "s3_endpoint":{"access_key":access_key,
                                           "secret_key":secret_key,
                                           "data_store_uri":endpoint_url
                                           }
                          }
              }
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    return loads(res.content)


  def add_multicluster(self, timeout=900, **kwargs):
    if self.state != "COMPLETE":
      ERROR("%s is in %s state, skipping multicluster addition"
            %(self.objects_name, self.state))
      return "Skipped : Cluster in %s state"%(self.state)
    available_clusters = self.get_clusters_for_multicluster()
    if not available_clusters:
      return "Skipped : No cluster available to add"
    cluster_name = kwargs.get("cluster_name")
    cluster = available_clusters[choice(available_clusters.keys())]
    if cluster_name:
      cluster = available_clusters[cluster_name]
    cluster_name = cluster["cluster_name"]
    cluster_vip = cluster["external_ip_address"]
    cluster_uuid = self._pe_cluster_uuid(cluster_vip)[0]
    payload = {"api_version":"3.0",
               "multicluster":{"pe_uuid":{"uuid":cluster_uuid,
                                          "kind":"cluster"},
                               "data_services_ip":cluster_vip,
                               "pe_cluster_name":cluster_name,
                               "max_usage_pct":kwargs.get("pct", 100)}}
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/multicluster"\
            %(self._pc_url, self.objects_uuid)
    INFO("Multicluster : Adding %s (Cluster IP : %s) to %s, URL : %s"
          %(cluster_name, cluster_vip, self.objects_name, url))

    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    stime = time()
    DEBUG("Verifying if multicluster operation was successful")
    while time()-stime < timeout:
      status = self._get_multiclusters()
      DEBUG("Get Multiclusters output (%s status : %s) : %s"
            %(status, cluster_name, status[cluster_name]["state"]))
      if status[cluster_name]["state"] in ["COMPLETE", "MC_COMPLETE"]:
        return status[cluster_name]["state"]
      sleep(30)
    raise Exception("Hit timeout after %s seconds, while adding cluster %s, "
                    "to %s"%(timeout, cluster_name, self.objects_name))

  def clean_up_all_vgs(self, container_name, vgs=None):
    """
    cmd = "source /etc/profile; sh /home/nutanix/tmp/clan"
    output, errors = Utils.remote_exec(self.peip, cmd)
    """
    if not vgs:
      vgs = self.get_vgs_from_container_v3(container_name)
    for vgname, vg in vgs.iteritems():
      if vg.get("attachment_list"):
        INFO("Detaching vg from client : %s"%(str(vg)))
        for client in vg["attachment_list"]:
          self.detach_vg_from_client(vg["uuid"], client["iscsi_initiator_name"],
                                     client["client_uuid"])
      INFO("Deleting vg from cluster :  %s"%(vg["uuid"]))
      self.delete_vg_v2(vg["uuid"])

  def delete_vg_v2(self, vg_uuid):
    url = "https://%s:9440/PrismGateway/services/rest/v2.0/"\
          "volume_groups/%s"%(self.peip, vg_uuid)
    res = self._execute_rest_api_call(url=url, request_method=delete,
                                      data=None)
    return loads(res.content)

  def detach_vg_from_client(self, vg_uuid, client_initiator_name, client_uuid):
    url = "https://%s:9440/PrismGateway/services/rest/v2.0/"\
          "volume_groups/%s/close"%(self.peip, vg_uuid)
    payload = {
                "iscsi_client": {
                  "client_address":client_initiator_name,
                  "uuid": client_uuid
                  },
                "operation": "DETACH",
                "uuid": vg_uuid
                }
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    return loads(res.content)

  def get_iam_user_uuid(self, email=False):
    res = self.get_all_iam_users()
    for user in res["users"]:
      if email == user["username"]:
        return user

  def create_iam_user(self, username, email, recreate=False):
    if recreate:
      userdetails = self.get_iam_user_uuid(email)
      if userdetails:
        self.remove_iam_user(userdetails["uuid"])
    url = "%s/oss/iam_proxy/buckets_access_keys"%(self._pc_url)
    payload = {"users":[{
                      "type":"external",
                      "username":"%s"%(email),
                      "display_name":"%s"%(username)
                      }]}
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    res = loads(res.content)
    if "users" in res.keys():
      usrkey = res["users"][0]["buckets_access_keys"][0]
      return {
              "secret":str(usrkey["secret_access_key"]),
              "access":str(usrkey["access_key_id"]),
              "uuid" : str(res["users"][0]["uuid"])
            }
    return {"access":str(res["access_keys"][0]["access_key_id"]),
            "uuid":str(res["access_keys"][0].get("user_id")).strip(),
            "secret":str(res["access_keys"][0]["secret_access_key"])}

  def get_objects_alerts(self):
    if self.state != "COMPLETE":
      ERROR("%s is in %s state, alerts can not be fetched"
            %(self.objects_name, self.state))
      return {}
    if not self.uuid:
      self.uuid = self._get_objects_uuid()
    url = "%s/oss/api/nutanix/v3/objectstores/%s/groups"%(self._pc_url,
                                                          self.uuid)
    payload = {
                "entity_type": "alert",
                "group_member_sort_attribute": "name",
                "group_member_sort_order": "ASCENDING",
                "group_member_count": 60,
                "group_member_offset": 0,
                "filter_criteria": "(state==active,state==suppressed,"\
                                   "state==silence_pending,state==inactive);"\
                                   "(severity==critical,severity==warning,"\
                                   "severity==info);customer_visible==true",
                "group_member_attributes": [
                  {"attribute": "name"},
                  {"attribute": "description"},
                  {"attribute": "severity"},
                  {"attribute": "create_time"},
                  {"attribute": "resolved_time"},
                  {"attribute": "state"},
                  {"attribute": "labels"},
                  {"attribute": "customer_visible"}
                ]
              }
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    res = loads(res.content)
    res =  self._extract_cluster_details(res)
    return res

  def delete(self, timeout=900, **kwargs):
    """
    Request Method: DELETE
    """
    self.uuid = self._get_objects_uuid()
    if not self.uuid:
      ERROR("%s not found"%(self.objects_name))
      return
    if self._state == "COMPLETE":
      try:
        self.delete_bucket()
      except Exception as err:
        ERROR("Failed to delete ObjectsBrowser. Error : %s"%(err))
    services_to_restart = kwargs.pop("services_to_restart_during_delete")
    INFO("Deleting %s with uuid %s"%(self.objects_name, self.uuid))
    if self._get_objects_details()["state"] != "DELETING":
      url = "%s/%s"%(self._oss_url, self.uuid)
      res = self._execute_rest_api_call(url=url, request_method=delete,
                                        data='{}')
      if res.status_code > 300:
        ERROR("Error while deleting %s (uuid %s). PC : %s , Error : %s"
              %(self.objects_name, self.uuid, self.pcip, res.content))
        raise Exception("Failed to delete Objects : %s"%(self.objects_name))
    services_restarted = []
    stime = time()
    while time()-stime < timeout:
      status = self._get_all_objects_clusters()
      if self.objects_name not in status:
        INFO("%s is deleted successfully"%(self.objects_name))
        return services_restarted
      if "ERROR" in status[self.objects_name]["state"]:
        raise Exception("Failed to delete %s, Error : %s"%(self.objects_name,
                        status[self.objects_name]["error_message_list"]))
      INFO("%s in %s state for %s secs, Status : %s"%(self.objects_name,
           status[self.objects_name]["state"], time()-stime,
           status[self.objects_name]))
      sleep(60)
      if services_to_restart:
        service = services_to_restart.pop(0)
        service_name, perc = service.split(":")
        INFO("Restarting service : %s, During Task : Delete"%(service_name))
        self._restart_service(service_name)
        services_restarted.append(service)
    raise Exception("Hit timeout : %s, failed to delete %s"
                    %(timeout, self.objects_name))

  def delete_bucket(self, bucket_name="objectsbrowser"):
    url = "%s/%s/buckets/%s"%(self._oss_url, self.uuid, bucket_name)
    INFO("Deleting bucket : %s, URL : %s"%(bucket_name, url))
    res = self._execute_rest_api_call(url=url, request_method=delete,
                                      data='{}')
    if res.status_code > 300:
      ERROR("Error while deleting %s (uuid %s). PC : %s , Error : %s"
            %(bucket_name, self.uuid, self.pcip, res.content))

  def deploy(self, **kwargs):
    num_loop = kwargs.pop("num_loop_deploments")
    self._deploy(**kwargs)
    self.uuid = self._get_objects_uuid()
    if not kwargs["skip_cert_validation"]:
      self._validate_certs()

  def destroy_msp_cluster(self):
    self.get_mspcluster_uuid()
    self._mspctl.cluster_destroy(self.msp_uuid)
    INFO("MSP Cluster Destroyed : %s"%(self.msp_uuid))

  def exec_cmd(self, cmd, retry=3, debug=False):
    if debug: INFO("Executing %s on PC"%(cmd))
    out, err = remote_exec(self.pcip, cmd, retry=retry)
    if err:
      raise Exception("Hit error while executing %s on %s"%(err, self.pcip))
    return out.strip()

  def get_vgs_from_container_v2(self, container_name=None):
    url = "https://%s:9440/api/nutanix/v2.0/volume_groups"%(self.peip)
    res = self._execute_rest_api_call(url=url, request_method=get, data=None)
    res = loads(res.content)
    if not container_name:
      return res
    container_uuid = self.get_storage_container_uuid_v2(container_name)
    vg_mapping = {}
    for vg in res['entities']:
      struuid = None
      if "disk_list" not in vg:
        continue
      for disk in vg["disk_list"]:
        if disk["storage_container_uuid"]  == container_uuid:
          vg_mapping[vg["name"]] = {
                        "storage_container_uuid":disk["storage_container_uuid"],
                        "vmdisk_uuid":disk["vmdisk_uuid"],
                        "attachment_list":vg.get("attachment_list"),
                        "iscsi_target":vg["iscsi_target"],
                        "uuid":vg["uuid"]}
        break
    return vg_mapping


  def get_storage_container_uuid_v2(self, name):
    strs = self.get_storage_container_v2()
    for strname in strs["entities"]:
      if strname["name"] == name:
        return strname["storage_container_uuid"]

  def get_storage_container_v2(self):
    url = "https://%s:9440/api/nutanix/v2.0/storage_containers"%(self.peip)
    res = self._execute_rest_api_call(url=url, request_method=get, data=None)
    return loads(res.content)

  def get_all_iam_users(self, offset=0, length=10000):
    url = "%s/oss/iam_proxy/users?offset=%s&length=%s"\
          %(self._pc_url, offset, length)
    res = self._execute_rest_api_call(url=url, request_method=get, data=None)
    return loads(res.content)

  def get_bucket_info(self, buckets=None, start_offset=0, end_offset=10000,
                      paginated_list=True, page_size=60):
    if not paginated_list:
      return self._get_bucket_info(buckets, start_offset, end_offset)
    instance_stats =  self.get_instance_stats()
    num_buckets = int(instance_stats["num_buckets"])
    num_pages = num_buckets/page_size + (0 if num_buckets % page_size == 0
                                         else 1)
    start_offset=0
    bucketdata={}
    for page in range(num_pages):
      res = self._get_bucket_info(buckets, start_offset=start_offset,
                                  end_offset=page_size)
      bucketdata.update(res)
      start_offset += page_size
    return bucketdata

  def get_bucket_properties(self, bucket):
    url = "https://%s:9440/oss/api/nutanix/v3/objectstores/%s/buckets/%s"\
          %(self.pcip, self.uuid, bucket)
    payload = {'entity_type': 'bucket',
               'group_member_attributes': [{'attribute': 'name'},
                                           {'attribute': 'storage_usage_bytes'},
                                           {'attribute': 'object_count'}],
                'group_member_sort_attribute': 'name'}
    res = self._execute_rest_api_call(url=url, request_method=get,
                                      data=dumps(payload))
    result = {bucket:{}}
    res = loads(res.content)
    bucket_status = res["status"]["resources"]
    bucket_spec = res["spec"]["resources"]
    version_state = "disabled"
    if "VERSIONING" in bucket_status.get("featurs",[]):
      version_state = "enabled"
      if bucket_status.get("versioning_suspended"):
        version_state = "suspended"
    worm_retention = bucket_status.get("worm_retention_days", 0)
    expiry = bucket_spec.get("retention_duration_days", 0)
    version_expiry = bucket_spec.get("delete_object_after_days", 0)

    result = {
              bucket:{
                "versioning":version_state,
                "worm_days":worm_retention,
                "expiry_days":expiry,
                "version_expiry_days":version_expiry,
                "bucket_size":bucket_status.get("total_size_bytes"),
                "objcount":bucket_status.get("total_size_bytes")
              }
            }
    return result

  def get_bucket_shared_users(self, bucket):
    """GET list of all users sharing bucket
    """
    url = "%s/%s/groups"%(self._oss_url, self.uuid)
    payload ={
              "entity_type": "bucket",
              "entity_ids":[bucket],
              'group_member_attributes':[{'attribute': 'name',
                                          'attribute':'buckets_share'}]
            }
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    return loads(res.content)

  def get_certs_from_pc(self):
    url = "%s/%s/client_self_signed_cert"%(self._oss_url, self.objects_uuid)
    res = self._execute_rest_api_call(url=url, request_method=get, data='{}')
    certs = loads(res.content)
    return {"ca":b64decode(certs["ca"]),
            "client_cert":b64decode(certs["client_cert"]),
            "client_key":b64decode(certs["client_key"])}

  def download_objects_certs(self):
    if not self.cluster_version.startswith("3.2"):
      return self.get_certs_from_pc()
    url = "%s/%s/download_ca_cert"%(self._oss_url, self.objects_uuid)
    res = self._execute_rest_api_call(url=url, request_method=get, data='{}')
    certs = loads(res.content)
    return {"ca":b64decode(certs["ca"])}

  def get_clusters_for_multicluster(self):
    payload = {"entity_type":"cluster",
               "group_member_count":1000,
               "group_member_attributes":[{"attribute":"cluster_name"},
                                        {"attribute":"storage.capacity_bytes"},
                                          {"attribute":"storage.usage_bytes"},
                                          {"attribute":"external_ip_address"},
                                          {"attribute":"storage.free_bytes"},
                                    {"attribute":"storage.logical_usage_bytes"},
                                          {"attribute":"version"},
                                          {"attribute":"hypervisor_types"}],
                "group_member_sort_attribute":"cluster_name",
                "group_member_sort_order":"ASCENDING"}
    url = "%s/oss/pc_proxy/groups"%(self._pc_url)
    res = self._execute_rest_api_call(url=url,
                                request_method=post, data=dumps(payload))
    res = loads(res.content)
    result = {}
    current_cluster = self._get_pe_name().keys()[0]
    for i in res["group_results"][0]["entity_results"]:
      tmp = {}
      for j in i["data"]:
        if not j["values"]:
          continue
        tmp[j["name"]] = j["values"][0]["values"][0]
      if tmp["cluster_name"] == current_cluster:
        continue
      if "storage.capacity_bytes" not in tmp:
        WARN("Filtering cluster without storage capacity : %s"%(tmp))
        continue
      result[tmp["cluster_name"]] = tmp
    DEBUG("Available clusters for multicluster addition : %s"%(result))
    return result

  def get_cvmips(self, peip=None):
    if not peip:
      peip = self.peip
    cmd= "source /etc/profile; /usr/local/nutanix/cluster/bin/svmips"
    output, errors = remote_exec(peip, cmd)
    return output.strip().split(" ")

  def get_hostips(self):
    cmd= "source /etc/profile; /usr/local/nutanix/cluster/bin/hostips"
    output, errors = remote_exec(self.peip, cmd)
    return output.strip().split(" ")

  def delete_egroups(self, num_egroups_to_delete, wait_for_data_recovery,
                     hostip=None):
    if not hostip:
      hostip = choice(self.get_cvmips())
    if not self._ctr_id:
      cmd = 'source /etc/profile; /home/nutanix/prism/cli/ncli ctr ls | grep '\
            'objectsdc -B2 | grep Id | cut -d":" -f4'
      output, errors = remote_exec(hostip, cmd)
      self._ctr_id = output.strip()
    #TODO - find egroup from any of the vdisk from ctrid

  def get_instance_stats(self):
    res = None
    url="https://%s:9440/oss/api/nutanix/v3/groups"%(self.pcip)
    payload = {'entity_type': 'objectstore',
               'group_member_attributes': [{'attribute': 'name'},
                                           {'attribute': 'usage_bytes'},
                                           {'attribute': 'num_buckets'},
                                           {'attribute': 'num_objects'}]}
    try:
      res = self._execute_rest_api_call(url=url, request_method=post,
                                        data=dumps(payload))
    except Exception as err:
      return res
    res = loads(res.content)
    result = {}
    for i in res["group_results"][0]["entity_results"]:
      tmp = {}
      for j in i["data"]:
        tmp[j["name"]] = j["values"][0]["values"][0]
      result[tmp["name"]] = tmp
      if self.objects_name == tmp["name"]:
        return tmp

  def get_msp_cluster_details(self, include_vm_details=False):
    self.get_mspcluster_uuid()
    kubeconfig = self._mspctl.get_kubeconfig(self.msp_uuid)
    status = self._mspctl.get_status(self.msp_uuid)
    msp_vms = None
    if include_vm_details:
      msp_vms = self._get_objects_vms(self.peip)
    version = self._mspctl.get_cluster_version()
    cluster_list = self._mspctl.get_list()
    res = {"kubeconfig":kubeconfig, "status":status, "vm_names":msp_vms,
           "cluster_version":version, "cluster_list":cluster_list}
    #INFO("MSP Cluster Details : %s"%(res))
    return res

  def get_mspcluster_uuid(self, force=True):
    cluster_list = self._mspctl.get_list()
    for cluster in cluster_list:
      INFO("MSP Cluster Found : %s (uuid %s), Looking for %s"
           %(cluster.get("cluster_name"), cluster["cluster_uuid"],
             self.objects_name))
      if cluster.get("cluster_name") and \
            cluster.get("cluster_name").strip() == self.objects_name:
        self.msp_uuid = cluster["cluster_uuid"]
    if not self.msp_uuid and force:
      raise Exception("MSP cluster %s not found"%(self.objects_name))
    return self.msp_uuid

  def get_objects_endpoint(self, get_all=False):
    res = self._get_objects_details()
    INFO("%s : Instance Info - %s"%(self.objects_name, res))
    if get_all:
      return res.get("client_access_network_ip_used_list")
    return str(choice(res.get("client_access_network_ip_used_list")))

  def get_svmips(self):
    cmd= "source /etc/profile; /usr/local/nutanix/cluster/bin/svmips"
    output, errors = remote_exec(self.pcip, cmd)
    return output.strip()

  def get_users_to_uuid_mapping(self, offset=0, length=10000):
    users = self.get_all_iam_users(offset, length)
    if not users:
      return None
    self.users_to_uuid = {}
    for user in users.get("users", []):
      self.users_to_uuid[user["username"]] = user["uuid"]
    return self.users_to_uuid

  def off(self, vmname):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli vm.off %s"%(vmname)
    output, errors = remote_exec(self.peip, cmd)

  def on(self, vmname):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli vm.on %s"%(vmname)
    output, errors = remote_exec(self.peip, cmd)

  def pause(self, vmname):
    cmd ="source /etc/profile; /usr/local/nutanix/bin/acli vm.pause %s"%(vmname)
    output, errors = remote_exec(self.peip, cmd)

  def reboot(self, vmname):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli vm.reboot %s"\
          %(vmname)
    output, errors = remote_exec(self.peip, cmd)

  def reboot_cvm(self, *args, **kwargs):
    cvmip = choice(self.get_cvmips())
    cmd ="source /etc/profile; /usr/bin/sudo reboot > /dev/null 2>&1 &"
    INFO("Initiating CVM reboot %s"%(cvmip))
    remote_exec(cvmip, cmd)
    return cvmip

  def reboot_all_cvm(self, *args, **kwargs):
    cvmips = self.get_cvmips()
    for cvmip in cvmips:
      cmd ="source /etc/profile; /usr/bin/sudo reboot > /dev/null 2>&1 &"
      INFO("Initiating All CVM reboot %s"%(cvmip))
      remote_exec(cvmip, cmd)
    return ",".join(sorted(cvmips))

  def reboot_host(self, *args, **kwargs):
    hostip = choice(self.get_hostips())
    cmd ="source /etc/profile; reboot > /dev/null 2>&1 &"
    INFO("Initiating Host reboot %s"%(hostip))
    remote_exec(hostip, cmd, username="root")
    return hostip

  def reboot_all_host(self, *args, **kwargs):
    hosts = self.get_hostips()
    for hostip in hosts:
      cmd ="source /etc/profile; reboot > /dev/null 2>&1 &"
      INFO("Initiating Host reboot %s"%(hostip))
      remote_exec(hostip, cmd, username="root")
    return ",".join(sorted(hosts))

  def rebootall(self, vmname):
    cmd ="source /etc/profile; /usr/local/nutanix/bin/acli vm.reboot %s*"\
         %(vmname.split("-")[0])
    remote_exec(self.peip, cmd)

  def remove_iam_user(self, uuid):
    url = "%s/oss/iam_proxy/users/%s"%(self._pc_url, uuid)
    INFO("Removing user with uuid : %s"%(uuid))
    self._execute_rest_api_call(url=url,request_method=delete,data='{}')

  def remove_share(self, bucket, iamuser):
    url = "%s/%s/buckets/%s/remove_share"%(self._oss_url, self.uuid, bucket)
    payload ={
              "entity_type": "bucket",
              "name": bucket,
              "bucket_permissions": [
                {
                  "username": iamuser,
                  "permissions": [
                    "READ",
                    "WRITE"
                  ]
                }
              ]
            }
    return self._execute_rest_api_call(url=url, request_method=put,
                                       data=dumps(payload))

  def replace_certs(self, cert_type, **kwargs):
    if self.state != "COMPLETE":
      ERROR("%s is in %s state, skipping cert replacement"
            %(self.objects_name, self.state))
      return "Skipped : Cluster in %s state"%(self.state), None
    if cert_type != "self_signed":
      ERROR("Unsupported %s type."%(cert_type))
      return None, None
    INFO("Re-generating self signed certs for %s"%(self.objects_name))
    res = self._replace_certs(**kwargs)
    self._validate_certs()
    return cert_type, res

  def configure_dns_ntp(self, hostip, dnsip="10.40.64.16",
                        ntp="ntp.dyn.nutanix.com"):
    cmd = "source /etc/profile;/home/nutanix/prism/cli/ncli cluster "
    cmd += "add-to-name-servers servers=%s;"%(dnsip)
    cmd += "/home/nutanix/prism/cli/ncli cluster add-to-ntp-servers "
    cmd += "servers=%s"%(ntp)
    INFO("Configuring NTP/DNS on %s with cmd : %s"%(hostip, cmd))
    try:
      output, errors = remote_exec(hostip, cmd)
    except Exception as err:
      ERROR("Failed to configure DNS/NTP on %s, cmd : %s, Error : %s"
            %(hostip, cmd, err.message))

  def configure_vip_dsip(self, peip, vip=None, dsip=None):
    self.peip = peip
    current_vip, current_dsip = self.get_cluster_vip_dsip(peip)
    if current_vip and  current_dsip:
      DEBUG("Vip/Dsip is already configured. Current IPs : %s, Given Ips : %s,"
            "Skipping configuration"%((current_vip, current_dsip), (vip, dsip)))
      return current_vip, current_dsip
    if current_vip:
      vip = current_vip
    if current_dsip:
      dsip = current_dsip
    if not vip or not dsip:
      new_vip, new_dsip = self.discover_vip_dsip()
      if not vip:
        vip = new_vip
      if not dsip:
        dsip = new_dsip
    #if not vip or not dsip:
    #  vip, dsip = self.discover_vip_dsip(True)
    cmd = "source /etc/profile;/home/nutanix/prism/cli/ncli cluster edit-params"
    cmd += " external-data-services-ip-address=%s external-ip-address=%s"\
           %(dsip, vip)
    output, errors = remote_exec(self.peip, cmd)
    INFO("Cluster vip/dsip : %s & %s, Result : %s"%(vip, dsip, output.strip()))
    return vip, dsip

  def get_cluster_vip_dsip(self, peip):
    url = "https://%s:9440/api/nutanix/v2.0/cluster/"%(peip)
    res = self._execute_rest_api_call(url=url, request_method=get, data={})
    res = res.json()
    return res["cluster_external_ipaddress"], \
           res["cluster_external_data_services_ipaddress"]

  def discover_vip_dsip(self, use_cluster_vip=False):
    for hostip in self.get_hostips():
      vip, dsip = get_vip_dsip(hostip, use_cluster_vip)
      try:
        socket.inet_aton(dsip)
        socket.inet_aton(vip)
        if self._is_ip_free(dsip) and self._is_ip_free(vip):
          return vip, dsip
      except:
        ERROR("%s : Either/Both Dsip/Vip is invalid : %s/%s"%(hostip,dsip, vip))
    return None, None

  def _is_ip_free(self, ip):
    ret = system("ping  -c1 {0} >& /dev/null".format(ip))
    if ret == 0:
      ERROR("%s is pingable"%(ip))
      return False
    return True

  def create_network(self, network_name, vlan_id, network_prefix, start_ip,
                     end_ip, dnsserver="10.40.64.15"):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli net.create %s "\
          "vlan=%s ip_config=%s;"%(network_name, vlan_id, network_prefix)
    cmd += "source /etc/profile;/usr/local/nutanix/bin/acli net.add_dhcp_pool "\
           "%s start=%s end=%s;"%(network_name, start_ip, end_ip)
    cmd += "/usr/local/nutanix/bin/acli net.update_dhcp_dns %s servers=%s"\
           %(network_name, dnsserver)
    INFO("Creating network with cmd : %s, on host : %s"%(cmd, self.peip))
    output, errors = remote_exec(self.peip, cmd)
    cmd = "source /etc/profile;/usr/local/nutanix/bin/acli net.get "\
          "%s"%(network_name)
    output, errors = remote_exec(self.peip, cmd)
    INFO("Network details after creation : %s"%(output.strip()))

  def create_esxi_network(self, vcenter, network_name, netmask,
                          gateway, start_ip, end_ip, dnsserver="10.40.64.15"):
    uuid = self.register_vcenter_in_objects(vcenter)
    url = "%s/oss/api/nutanix/v3/platform_client/%s/ipam"%(self._pc_url, uuid)
    payload = {"api_version":"1.0", "esx_datacenter":vcenter._dcname,
               "esx_network":network_name,
               "netmask":netmask,
               "gateway":gateway,
               "dns_servers":[dnsserver],
               "ip_ranges":[{"start_ip":start_ip,"end_ip":end_ip}]}
    INFO("Creating ESXi network in Objects. URL : %s, Payload : %s"
         %(url, payload))
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    sleep(10)
    return res

  def register_to_vcenter(self, vcenter, peip=None):
    if not peip:
      peip = self.get_hostips()[0]
    url = "https://%s:9440/PrismGateway/services/rest/v1/management_servers/"\
          "register"%(peip)
    payload = {"adminUsername":vcenter._user,
               "adminPassword":vcenter._pwd,
               "ipAddress":vcenter._host,
               "port":str(vcenter._port)}
    INFO("Registering vCenter to PE. URL : %s, Payload : %s"%(url, payload))
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    sleep(10)
    return res

  def register_vcenter_in_objects(self, vcenter):
    url = "%s/oss/api/nutanix/v3/platform_client"%(self._pc_url)
    payload = {"api_version":"1.0", "password":vcenter._pwd,
               "username":vcenter._user,
               "vcenter_endpoint":vcenter._host}
    INFO("Registering vCenter to Objects.URL : %s, Payload : %s"%(url, payload))
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    sleep(10)
    res = loads(res.content)
    return res["platform_client_uuid"].strip()

  def reset(self, vmname):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli vm.power_cycle %s"%(
                                                                        vmname)
    output, errors = remote_exec(self.peip, cmd)

  def resetall(self, vmname):
    cmd ="source /etc/profile; /usr/local/nutanix/bin/acli vm.reset %s*"%(
                                                          vmname.split("-")[0])
    remote_exec(self.peip, cmd)

  def restart_cassandra(self, *args, **kwargs):
    cvmip = self.stop_cassandra(**kwargs)
    sleep(60)
    self.start_cassandra()
    return cvmip

  def restart_stargate(self, *args, **kwargs):
    cvmip = self.stop_stargate(**kwargs)
    sleep(60)
    self.start_stargate()
    return cvmip

  def restart_zk(self, num=0, interval=30):
    for i in range(num):
      for hostip in self.get_svmips().split(" "):
        cmd = "source /etc/profile; links -dump http://0.0.0.0:9877/h/exit"
        output, errors = remote_exec(hostip, cmd)
      sleep(interval)

  def reset_stargate_storage_gflag(self, interval, gflag_value):
    gcmd = "source /etc/profile; links -dump "
    gcmd += "http:0:2009/h/gflags?stargate_disk_full_pct_threshold"
    for hostip in self.get_cvmips():
      cmd = "%s=%s"%(gcmd, gflag_value)
      INFO("Configuring gflag %s on %s"%(cmd, hostip))
      output, errors = remote_exec(hostip, cmd)
    sleep(interval)
    for hostip in self.get_cvmips():
      cmd = "%s=95"%(gcmd)
      INFO("Resetting gflag to default : %s on %s"%(cmd, hostip))
      output, errors = remote_exec(hostip, cmd)

  def resume(self, vmname):
    cmd="source /etc/profile; /usr/local/nutanix/bin/acli vm.resume %s"%(vmname)
    remote_exec(self.peip, cmd)

  def scale_out(self, capacity=None, timeout=1200,
                services_to_restart_during_scaleout=[]):
    if self.state != "COMPLETE":
      ERROR("%s is in %s state, skipping cert replacement"
            %(self.objects_name, self.state))
      return "Skipped : Cluster in %s state"%(self.state)
    url = "%s/%s/scale-out"%(self._oss_url, self.objects_uuid)
    cluster_details = self._get_objects_details()
    current_nodes =  int(cluster_details["num_msp_workers"])
    if current_nodes < 3:
      WARN("Skipping scale-out, Non-HA cluster. Num Nodes in cluster : %s"
           %(current_nodes))
      return "Skipped : Num Nodes %s < 3"%(current_nodes)
    num_nodes = current_nodes + 1
    payload = {"UUID":self.objects_uuid,
               "total_vcpu_count":num_nodes*10,
               "total_memory_size_mib":num_nodes*32*1024,
               "total_capacity_gib":int(cluster_details["total_capacity_gib"])}
    if capacity:
      payload["total_capacity_gib"] = capacity
    INFO("Scaling out %s with %s parms"%(self.objects_name, payload))
    res = self._execute_rest_api_call(url=url,
                                request_method=post, data=dumps(payload))
    res = self._wait_until_state(operation="Scale-out", timeout=timeout,
                         service_to_restart=services_to_restart_during_scaleout)
    current_nodes = int(self._get_objects_details()["num_msp_workers"])
    if current_nodes != num_nodes:
      raise Exception("Scaleout on %s was successful, but cluster returned less"
                      " node. Expected vs Found : %s vs %s"%(self.objects_name,
                      num_nodes,current_nodes))
    INFO("Scaleout - %s is expanded by one node"%(self.objects_name))
    return res

  def set_quota(self, bucket_name, size):
    pass

  def share_bucket(self, bucket, iamuser):
    url = "%s/%s/buckets/%s/share"%(self._oss_url, self.uuid, bucket)
    payload = {
              "entity_type": "bucket",
              "name": bucket,
              "bucket_permissions": [
                {"username": iamuser,
                 "permissions": ["READ","WRITE"]}
              ]
            }
    return self._execute_rest_api_call(url=url, request_method=put,
                                       data=dumps(payload))

  def shutdown(self, vmname):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli vm.shutdown %s"%(
                                                                         vmname)
    remote_exec(self.peip, cmd)

  def skip_node_check(self):
    for hostip in self.get_svmips().split(" "):
      INFO("Enabling skip node check on : %s"%(hostip))
      cmd = "source /etc/profile; docker exec  aoss_service_manager touch"
      cmd += " /tmp/skip_node_worker_match"
      output, errors = remote_exec(hostip, cmd, retry = 0)
      self._mspctl.disable_ha_drs_flags()
      try:
        cmd="docker cp ~/tmp/delete/poseidon_master "
        cmd += "aoss_service_manager:/svc/config/"
        output, errors = remote_exec(hostip, cmd, retry=0)
      except Exception as err:
        ERROR("Error while updating supervisord & yaml templates : %s"%(err))

  def start_cassandra(self, *args, **kwargs):
    return self.start_cvm_cluster(self.peip)

  def start_cvm_cluster(self, cvmip=None, *args, **kwargs):
    if not cvmip:
      cvmip = choice(self.get_cvmips())
    cmd ="source /etc/profile; /usr/local/nutanix/cluster/bin/cluster start"
    INFO("Initiating Cluster Start on : %s"%(cvmip))
    print("Cluster Start : ", remote_exec(cvmip, cmd, retry=3,
                                                retry_delay=60))
    return cvmip

  def start_stargate(self, *args, **kwargs):
    return self.start_cvm_cluster(self.peip)

  def stop_cassandra(self, *args, **kwargs):
    cvmip = choice(self.get_cvmips())
    cmd ="source /etc/profile; "
    cmd += "/usr/local/nutanix/cluster/bin/genesis stop cassandra"
    INFO("Initiating Cassandra Stop on : %s"%(cvmip))
    remote_exec(cvmip, cmd)
    return cvmip

  def stop_stargate(self, *args, **kwargs):
    cvmip = choice(self.get_cvmips())
    cmd = "source /etc/profile; "
    cmd += "/usr/local/nutanix/cluster/bin/genesis stop stargate"
    INFO("Initiating Stargate Stop on : %s"%(cvmip))
    remote_exec(cvmip, cmd)
    return cvmip

  def lcm_upgrade_objects(self, target_version, objects_manager_version=True,
                          objects_service_version=False,
                          wait_for_completion=True):
    self._lcm.inventory()
    src_version, dst_version = None, None
    if objects_manager_version:
      src_version = self._get_service_manager_version()
      INFO("Upgrading Objects manager from %s to %s"
           %(src_version, target_version))
    else:
      res = self._get_objects_details()
      src_version = str(res["deployment_version"]).strip()
      INFO("Upgrading Objects service from %s to %s"
           %(src_version, target_version))
    try:
      upgraderes = self._lcm.update(target_version, objects_manager_version,
                                    objects_service_version, self.objects_name,
                                    wait_for_completion)
    except Exception as err:
      ERROR("Hit exception : %s, during Objects upgrade."%(err))
      if "Read timed out" not in str(err):
        raise
    if objects_manager_version:
      dst_version = self._get_service_manager_version(10)
      INFO("Post upgrade Objects manager %s"%(dst_version))
    else:
      #Remove below hack once read-timeout is fixed on requests-get
      wait_time = time()+900
      while wait_time - time() > 0:
        try:
          if str(self._get_objects_details()["deployment_version"]
                                                    ) == target_version.strip():
            break
        except Exception as err:
          INFO("Deployment version not found. Err : %s"%(err))
          sleep(30)
      self._wait_until_state(operation="Upgrade", timeout=3600)
      res = self._get_objects_details()
      dst_version = str(res["deployment_version"]).strip()
      INFO("Post upgrade Objects service %s"%(dst_version))
    if dst_version != target_version:
      raise Exception("Failed to upgrade cluster. Expected %s as a deployment "
                      "version but found : %s"%(target_version, dst_version))
    return src_version, dst_version

  def initiate_lcm_inventory(self, wait_for_inventory=True):
    url = "https://%s:9440/PrismGateway/services/rest/v1/genesis"%(self.pcip)
    value = {".oid":"LifeCycleManager",
             ".method":"lcm_framework_rpc",
             ".kwargs":{"method_class":"LcmFramework",
             "method":"perform_inventory",
             "args":["http://download.nutanix.com/lcm/2.0"]}}
    payload = {'value': dumps(value)}
    INFO("Initiating LCM inventory via genesis. URL : %s, Payload : %s"
         %(url, payload))
    resp = self._execute_rest_api_call(url=url, request_method=post,
                                       data=dumps(payload))
    if wait_for_inventory:
      #TODO implement me , call wait for task method from upgrade class
      pass
    return resp.json()

  def upgrade_objects(self, objects_version, rc_version, script_url,
                  manual_upgrade_build_url, lcm_upgrade=True,
                  objects_manager_version=True, objects_service_version=False):
    if lcm_upgrade:
      return self.lcm_upgrade_objects(objects_version, objects_manager_version,
                                 objects_service_version)
    if manual_upgrade_build_url:
      return self._manual_objects_upgrade(manual_upgrade_build_url,
                                          objects_version)
    wget_cmd = "source /etc/profile; /usr/bin/wget  -q -T300 %s"%(script_url)
    INFO("Downloading upgrade script. Command : %s"%(wget_cmd))
    res = None
    try:
      res = remote_exec(self.pcip, wget_cmd, retry=0)
    except Exception as err:
      ERROR("Failed to download upgrade script. Error : %s"%(err.message))
    res = self._get_objects_details()
    src_version = str(res["deployment_version"]).strip()
    upgrade_cmd = "source /etc/profile; /usr/bin/python rc_upgrade.py  "\
                  "-rc %s  -version %s -name %s"%(rc_version, objects_version,
                                                  self.objects_name)
    INFO("Upgrading Objects with cmd : %s"%(upgrade_cmd))
    res = None
    try:
      res = remote_exec(self.pcip, upgrade_cmd, retry=0)
    except Exception as err:
      ERROR("Failed to upgrade cluster : %s, URL : %s, Err : %s, Cmd"
            " : %s, Error : %s"%(self.objects_name, script_url, res,
            upgrade_cmd, err.message))
      raise
    success_str = "Upgrade successfully triggered to version"
    if not (success_str in res[0] or success_str in res[1]):
      raise Exception("Failed to upgrade cluster : %s, URL : %s, Err : %s, Cmd"
                      " : %s"%(self.objects_name, script_url, res, upgrade_cmd))
    self._wait_until_state(operation="ObjectsUpgrade")
    res = self._get_objects_details()
    found_version = str(res["deployment_version"]).strip()
    if found_version != str(objects_version):
      raise Exception("Failed to upgrade cluster. Expected %s as a deployment "
                      "version but found : %s"%(objects_version, found_version))
    INFO("Successfully upgraded %s to %s"%(self.objects_name, found_version))
    return src_version, found_version

  def _manual_objects_upgrade(self, manual_upgrade_build_url,
                              objects_unreleased_version):
    src_version = self._get_service_manager_version()
    pcips = self.get_cvmips(self.pcip)
    for pcip in pcips:
      cmd = "source /etc/profile; /usr/bin/wget  -q -T300 "
      cmd += "%s; sudo mv  aoss_service_manager.tar.xz "%(
                                                      manual_upgrade_build_url)
      cmd += "/home/docker/aoss_service_manager/aoss_service_manager.tar.xz; "
      cmd += "sudo chown nutanix:nutanix /home/docker/"
      cmd += "aoss_service_manager/aoss_service_manager.tar.xz; "
      cmd += "sudo chmod 755 /home/docker/aoss_service_manager/"
      cmd += "aoss_service_manager.tar.xz; "
      cmd += "source /etc/profile; /usr/local/nutanix/cluster/bin/"
      cmd += "genesis stop aoss_service_manager "
      res = None
      INFO("Upgrade : Upgrading service manager on %s to %s, cmd : %s"
           %(pcip, objects_unreleased_version, cmd))
      try:
        res = remote_exec(pcip, cmd, retry=0)
      except Exception as err:
        ERROR("Failed to upgrade cluster : %s, URL : %s, Err : %s, Cmd"
              " : %s, Error : %s"%(self.objects_name, manual_upgrade_build_url,
                                   res, cmd, err.message))
        raise
    sleep(60)
    self.start_cvm_cluster(pcip)
    sleep(180)
    res = self._get_service_manager_version()
    if objects_unreleased_version not in res.strip():
      raise Exception("Upgrade Failed : Expected service manager version : %s, "
                      "but found : %s"%(objects_unreleased_version, res))
    return src_version.strip(), objects_unreleased_version

  def _get_service_manager_version(self, num_retry=3):
    docker_cmd = "/usr/bin/docker inspect  --format '{{json .}}' "
    docker_cmd += "aoss_service_manager"
    src_version = remote_exec(self.pcip, docker_cmd,
                                    retry=num_retry)[0].strip()
    res = loads(src_version.strip())
    return res["Config"]["Image"].split(":")[-1].strip()

  def _restart_service(self, service_name, restart_time=300):
    pcips = self.get_cvmips(self.pcip)
    for pcip in pcips:
      cmd = "source /etc/profile; "
      cmd += "/usr/local/nutanix/cluster/bin/genesis stop %s"%(service_name)
      INFO("Stopping service %s on %s, cmd : %s"%(service_name, pcip, cmd))
      res = remote_exec(pcip, cmd, retry=0)
      self._service_status(service_name, pcip)
    INFO("Starting %s on %s by starting cluster"%(service_name, pcip))
    self.start_cvm_cluster(pcip)
    if "aoss" in service_name or "msp" in service_name:
      for pcip in pcips:
        timeout = time()+restart_time
        while (timeout - time()) > 0:
          res = self.get_docker_ps(pcip)
          if "aoss_service_manager" in res[0] and "msp-controller" in res[0]:
            break
          sleep(30)
    sleep(30)

  def _service_status(self, service_name, cvmip):
    cmd = "source /etc/profile; /usr/local/nutanix/cluster/bin/genesis"
    cmd += " status | grep  %s"%(service_name)
    INFO("Getting service status %s on %s, cmd : %s"%(service_name, cvmip, cmd))
    res = remote_exec(cvmip, cmd, retry=0)
    INFO("%s status on %s : %s"%(service_name, cvmip, res))

  def get_docker_ps(self, cvmip=None):
    if not cvmip:
      cvmip = self.get_cvmips(self.pcip)[0]
    cmd = "source /etc/profile; /usr/bin/docker ps"
    try:
      res = remote_exec(cvmip, cmd, retry=0)
      INFO("Docker ps output from : %s, cmd : %s, res : %s"%(cvmip, cmd, res))
      return res
    except Exception as err:
      ERROR("Failed to get docker ps output on %s, cmd : %s, Error : %s"
            %(cvmip, cmd, err.message))

  def upgrade_cluster(self, upgrade_build_url, pc_upgrade=True,
                      wait_for_upgrade=True, upgrade_timeout=3600):
    hostip = self.peip
    if pc_upgrade:
      hostip = self.pcip
    cluster_info_cmd = "source /etc/profile; " \
                       "/home/nutanix/prism/cli/ncli cluster info"
    cluster_info = remote_exec(hostip, cluster_info_cmd)
    host_type = "PC" if pc_upgrade else "PE"
    INFO("%s Host %s, Build before upgrade : %s"
         %(host_type, hostip, cluster_info))
    cmd = "source /etc/profile; /usr/bin/wget -q -T300 %s"%(upgrade_build_url)
    INFO("Upgrade : Downloading %s on %s, cmd : %s"
         %(upgrade_build_url, hostip, cmd))
    res = remote_exec(hostip, cmd, retry=0)
    INFO("%s Upgrade : Wget res : %s"%(host_type, res))
    cmd = "source /etc/profile; tar xvfz %s;"%(upgrade_build_url.split("/")[-1])
    INFO("%s Upgrade : Untarring build %s on %s, cmd : %s"
         %(host_type, upgrade_build_url, hostip, cmd))
    res = remote_exec(hostip, cmd, retry=0)
    INFO("%s Upgrade : Untar res : %s"%(host_type, res))
    before_version, before_full_version = self.get_build_info(hostip,pc_upgrade)
    cmd = "source /etc/profile;"
    cmd += "/home/nutanix/install/bin/cluster -i install upgrade"
    INFO("%s Upgrade : Upgrading %s to %s, cmd :  %s"
         %(host_type, hostip, upgrade_build_url, cmd))
    res = remote_exec(hostip, cmd, retry=0)
    INFO("%s Upgrade : Upgrade res : %s"%(host_type, res))
    self._wait_for_upgrade(hostip, upgrade_timeout, pc_upgrade)
    cluster_info = remote_exec(hostip, cluster_info_cmd)
    INFO("%s Host %s, Build After upgrade : %s"
         %(host_type, hostip, cluster_info))
    after_version, after_full_version = self.get_build_info(hostip, pc_upgrade)
    msg = "%s HostIP : %s, BuildBefore : %s, BuildAfter : %s"%(
                    host_type, hostip, before_full_version, after_full_version)
    if before_full_version == after_full_version:
      raise Exception("Upgrade Failed : PE uprgade was successful but same "
                      "version reported before and after upgrade.%s"%(msg))
    INFO("Upgrade is successful, %s"%(msg))
    return before_full_version, after_full_version

  def _wait_for_upgrade(self, hostip, timeout, pc_upgrade, **kwargs):
    stime = time()
    cooltime = 0
    while timeout >  time()-stime:
      upgrade_status = "True"
      if not pc_upgrade:
        upgrade_status = self.is_upgrade_in_progress(hostip)
        if not upgrade_status:
          cooltime += 30
        else:
          cooltime = 0
      INFO("%s Upgrade : in progress status : %s, checking again in 30s, Time "
           "elapsed : %s secs, Cooling Time : %s"%("PC" if pc_upgrade else "PE",
           upgrade_status, time()-stime, cooltime))
      if cooltime > 300:
        break
      sleep(30)
    INFO("Upgrade finished on : %s"%(hostip))

  def validate_upgrade(self, hostip, previous_full_version, is_pc):
    current_version, current_full_version = self.get_build_info(hostip, is_pc)
    if previous_full_version == current_full_version:
      return True
    return False

  def get_build_info(self, hostip, is_pc):
    if not is_pc:
      res = self.get_cluster_details_v2(hostip)
      return res.get("version"), res.get("full_version")
    res = self.get_cluster_list_v3(hostip)
    for entity in res["entities"]:
      if entity["status"]["resources"]["config"]["build"]["version"] == "pc":
        ver = entity["status"]["resources"]["config"]["build"]["version"]
        fver = entity["status"]["resources"]["config"]["build"]["full_version"]
        return ver, fver

  def get_cluster_list_v3(self, hostip):
    url = "https://%s:9440/api/nutanix/v3/clusters/list"%(hostip)
    try:
      res = self._execute_rest_api_call(url=url, request_method=post, data={})
      return res.json()
    except Exception as err:
      return {}

  def get_cluster_details_v2(self, hostip):
    try:
      url = "https://%s:9440/PrismGateway/services/rest/v2.0/cluster/"%(hostip)
      res = self._execute_rest_api_call(url=url, request_method=get, data={})
      return res.json()
    except Exception as err:
      return {}

  def is_upgrade_in_progress(self, hostip):
    try:
      return self.get_cluster_details_v2(hostip)["is_upgrade_in_progress"]
    except Exception as err:
      ERROR("Hit %s while getting upgrade status from %s"%(err, hostip))
      return True

  def is_objects_service_enabled(self):
    url = "%s/api/nutanix/v3/services/oss/status"%(self._pc_url)
    print(url)
    res = self._execute_rest_api_call(url=url, request_method=get, data={})
    if res.json()["service_enablement_status"] != "ENABLED":
      return False
    if not self._mspctl:
      self._mspctl = Mspctl(self)
    return True

  def enable_objects_service(self, timeout=600):
    url = "%s/api/nutanix/v3/services/oss"%(self._pc_url)
    INFO("Enabling objects service on %s"%(url))
    self._execute_rest_api_call(url=url, request_method=post,
                      data=dumps({"state": "ENABLE"}))
    stime=time()
    while timeout > time()-stime:
      if self.is_objects_service_enabled():
        INFO("Objects service is enabled on %s"%self.pcip)
        return
      INFO("Objects service is still not enabled on %s, check after 30s. Total "
            "time elapsed : %s"%(self.pcip, int(time()-stime)))
      sleep(30)
    raise Exception("Hit timeout after %s secs while waiting for Objects "
                    "service enablement"%(timeout))

  def _deploy(self, **kwargs):
    if not self.is_objects_service_enabled():
      self.enable_objects_service()
    new_deploy = kwargs.pop("deployment_v2", True)
    smv = self._get_service_manager_version()
    INFO("Service Manager Version : %s"%(smv))
    self.uuid = self._get_objects_uuid()
    #self.peip = kwargs["peip"]
    if kwargs.get("bind", True) and self.uuid:
      INFO("Cluster with name %s, already exits. Binding to it."
           %(self.objects_name))
      res = self._get_objects_details()
      if res["state"] == "PENDING":
        self._wait_until_state()
      res = self._get_objects_details()
      res["bind"] = True
      self._init_cluster_properties()
      if not kwargs["skip_cert_validation"]:
        self._validate_certs(True)
      return res
    self.get_docker_ps()
    deployment_spec = self._get_deployment_spec(**kwargs)
    service_to_restart = kwargs.pop("services_to_restart_during_deploy")
    if new_deploy:
      old_versions = ["1","2","3.0","3.1"]
      for ver in old_versions:
        if smv.startswith(ver):
          WARN("Invoking old deployment API. Aoss version : %s"%(smv))
          return self._deploy_objects_instance(deployment_spec,
                                               service_to_restart)
      INFO("Invoking new deployment API. Aoss version : %s"%(smv))
      return self._deploy_objects_instance_v2(deployment_spec,
                                                service_to_restart, True)
    return self._deploy_objects_instance(deployment_spec, service_to_restart)

  def _get_deployment_spec(self, **kwargs):
    deployment_size = kwargs["num_nodes"]
    cpus = deployment_size*10
    mem = deployment_size * 32 * 1024
    storage_capacity = kwargs["storage_capacity"]
    pe_cluster_uuid, hypervisor_type = self._pe_cluster_uuid(kwargs["peip"])
    client_network = self._get_network_uuid(pe_cluster_uuid,
                                      kwargs["client_network"], hypervisor_type)
    internal_network = self._get_network_uuid(pe_cluster_uuid,
                                    kwargs["internal_network"], hypervisor_type)
    client_ips = kwargs["client_ips"].split(",")
    internal_ips = kwargs["internal_ips"].split(",")
    domain_name = kwargs["domain_name"]
    deployment_spec = {
      "api_version": "3.0",
      "metadata": {"kind": "objectstore"},
      "spec": {
        "name": self.objects_name,
        "description": "Deploying Objects Cluster",
        "resources": {
          "domain": domain_name,
          "aggregate_resources": {
            "total_vcpu_count": cpus,
            "total_memory_size_mib": mem,
            "total_capacity_gib": storage_capacity
          },
          "cluster_reference": {
            "kind": "cluster", "uuid": pe_cluster_uuid
          },
          "buckets_infra_network_reference": {
            "kind": "subnet", "uuid": internal_network
          },
          "client_access_network_reference": {
            "kind": "subnet", "uuid": client_network
          },
          "buckets_infra_network_dns": internal_ips[0],
          "buckets_infra_network_vip": internal_ips[1],
          "client_access_network_ip_list": client_ips
        }
      }
    }
    INFO("Deploying Objects cluster with spec : %s"%(deployment_spec))
    return deployment_spec

  def _deploy_objects_instance(self, deployment_spec, service_to_restart):
    res = self._execute_rest_api_call(url=self._oss_url, request_method=post,
                                      data=dumps(deployment_spec))
    rservices = self._wait_until_state(service_to_restart=service_to_restart)
    self.uuid = self._get_objects_uuid()
    res = self._get_objects_details()
    self._init_cluster_properties()
    res["bind"] = False
    self._validate_certs()
    res["restarted_services"] = rservices
    return res

  def _deploy_objects_instance_v2(self, deployment_spec, service_to_restart,
                                  ignore_deployment_prechecks_failures=False):
    self._failed_prechecks = []
    deploy_uuid = self._save_deployment(deployment_spec)
    INFO("Saved deployment with UUID : %s"%(deploy_uuid))
    token = self._get_token(deploy_uuid)
    INFO("Generated new token : %s"%(token))
    sleep(1)
    INFO("Starting Precheck with UUID & Token : %s & %s"%(deploy_uuid, token))
    self._start_precheck(deploy_uuid, token)
    INFO("Starting Deployment with UUID & Token : %s & %s"%(deploy_uuid, token))
    url = "%s/%s?token=%s&deploy=true"%(self._oss_url, deploy_uuid, token)
    res = self._execute_rest_api_call(url=url, request_method=put,
                                      data=dumps(deployment_spec))
    rservices = self._wait_until_state(service_to_restart=service_to_restart)
    if self._failed_prechecks and not ignore_deployment_prechecks_failures:
      raise Exception("Deployment succeeded even though %s prechecks failed"
                      %(self._failed_prechecks))
    self.uuid = self._get_objects_uuid()
    res = self._get_objects_details()
    res["bind"] = False
    self._validate_certs()
    res["restarted_services"] = rservices
    return res

  def _start_precheck(self, uuid, token, wait_for_precheck=True):
    url = "%s/inspector/%s?token=%s"%(self._oss_url, uuid, token)
    self._execute_rest_api_call(url=url, request_method=post, data='{}')
    sleep(1)
    res = None
    if wait_for_precheck:
      while True:
        try:
          res = self._get_precheck_status(uuid, token)
          status = res["running_status"]["running"]
          self._process_precheck_results(res)
          if not status:
            self._process_precheck_results(res)
            break
        except Exception as err:
          ERROR("Failed to get precheck status.ERROR : %s, Res : %s"%(err, res))
        sleep(60)
    INFO("Precheck final status : %s"%(self._get_precheck_status(uuid, token)))

  def _process_precheck_results(self, precheck_res):
    if not precheck_res or not precheck_res["plugins_status"]:
      return
    res = "\n"
    for j in precheck_res["plugins_status"]:
      if "subcheck_results" not in j:
        continue
      if "results" not in j["subcheck_results"]:
        continue
      for i in j["subcheck_results"]["results"]:
        check_name = i["metadata"]["name"]
        res += "%s%s: %s\n"%(check_name, " "*(60-len(check_name)),i["status"])
        if i["status"].upper() not in ["PENDING", "PASS", "RUNNING"]:
          if check_name not in self._failed_prechecks:
            INFO("Precheck Failed : %s"%(check_name))
          self._failed_prechecks.append(check_name)
    INFO("Precheck test results : %s"%(res))

  def _get_precheck_status(self, uuid, token):
    url = "%s/inspector/%s?token=%s"%(self._oss_url, uuid, token)
    res = self._execute_rest_api_call(url=url, request_method=get, data='{}')
    return res.json()

  def _save_deployment(self, deployment_spec):
    url = "%s?save=true"%(self._oss_url)
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(deployment_spec))
    res = res.json()
    INFO("Deployment saved result : %s"%(res))
    return res["metadata"]["uuid"]

  def _get_token(self, uuid, num=0):
    url = "%s/%s?get_new_token=true"%(self._oss_url, uuid)
    res = self._execute_rest_api_call(url=url, request_method=get, data='{}')
    res = res.json()
    INFO("New Token result : %s"%(res))
    if not res.get("token") and num < 3:
      sleep(30)
      num += 1
      return self._get_token(uuid, num)
    return res["token"]

  def _execute_rest_api_call(self, url, request_method,  data, timeout=120,
                             retry=0, retry_delay=30):
    params = {"url":url, "headers":{"Content-Type": "application/json"},
             "auth":(self.username, self.password), "verify":False,
             "timeout":timeout}
    if data is not None:
      if isinstance(data, dict):
        params["data"]=dumps(data)
      else:
        params["data"]=str(data)
    retry += 1
    msg = ""
    while retry > 0:
      res = request_method(**params)
      if res.status_code < 300:
        return res
      msg = "Error while executing %s (uuid %s).\nPC : %s (Objects name : %s)"\
            "\nError : %s (Code:%s),\nURL : %s\npayload : %s"%(
              request_method.__name__,
              self.uuid, self.pcip, self.objects_name, res.content,
              res.status_code, url, params["data"])
      ERROR(msg)
      retry -= 1
      if retry > 0 :sleep(retry_delay)
      continue
    raise Exception(msg)

  def _extract_cluster_details(self, res):
    results = {}
    if not res.get("group_results"):
      return results
    if not res["group_results"][0]["entity_results"]:
      return results
    for i in res["group_results"][0]["entity_results"]:
      tmp = {"entity_id":i["entity_id"]}
      for data in i["data"]:
        if isinstance(data["values"][0]["values"], list):
          tmp[data["name"]] = data["values"][0]["values"][0]
          if data["name"] == "client_access_network_ip_used_list":
            tmp[data["name"]] = data["values"][0]["values"]
        else:
          tmp[data["name"]] = data["values"][0]["values"]
      if "name" in tmp:
        results[tmp["name"]] = tmp
      else:
        results[uuid4().hex] = tmp
    return results

  def _get_ahv_network_uuid(self, cluster_uuid, network_name):
    url = "%s/api/nutanix/v3/subnets/list"%(self._pc_url)
    res = self._execute_rest_api_call(url=url, request_method=post, data='{}')
    network_details = {}
    for network in loads(res.content)["entities"]:
      DEBUG("Cluster UUID %s, NetworkName : %s !!!"
           %(network["status"]["cluster_reference"]["uuid"],
             network["status"]["name"]))
      if network["status"]["cluster_reference"]["uuid"] != cluster_uuid:
        continue
      network_details[network["status"]["name"]] = {
                                            "uuid":network["metadata"]["uuid"]}
    for name, details in network_details.iteritems():
      if name == network_name:
        return details["uuid"]
    raise Exception("PE %s, Network %s not found"%(cluster_uuid, network_name))

  def _get_bucket_info(self, buckets, start_offset, end_offset):
    url="https://%s:9440/oss/api/nutanix/v3/objectstores/%s/groups"\
        %(self.pcip, self.uuid)
    payload = {"entity_type":"bucket", "group_member_sort_attribute":"name",
               "group_member_sort_order":"ASCENDING",
               "group_member_count":60, "group_member_offset":0,
               "group_member_attributes":[
                          {"attribute":"cors"},
                          {"attribute":"name"},
                          {"attribute":"worm"},
                          {"attribute":"website"},
                          {"attribute":"versioning"},
                          {"attribute":"object_count"},
                          {"attribute":"retention_start"},
                          {"attribute":"suspend_versioning"},
                          {"attribute":"storage_usage_bytes"},
                          {"attribute":"retention_duration_days"},
                          {"attribute":"bucket_notification_state"},
                          {"attribute":"inbound_replication_status"},
                          {"attribute":"outbound_replication_status"}]}
    #Quick hack to make it configurable from API side.
    if start_offset >0 or end_offset < 10000:
      payload['group_member_count'] = end_offset
      payload['group_member_offset'] = start_offset
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    if not res or res.status_code > 300:
      ERROR("Error while getting bucket info from PC : %s (uuid %s) , Error :"
            " %s, uuid : %s"%(self.pcip, self.uuid, res.content, self.uuid))
      return None
    res = loads(res.content)
    result = {}
    for i in res["group_results"][0]["entity_results"]:
      tmp = {}
      for j in i["data"]:
        tmp[j["name"]] = j["values"][0]["values"][0]
      if buckets and tmp["name"] not in buckets:
        continue
      result[tmp["name"]] = tmp
    return result

  def _get_bucket_stats(self, bucket_names):
    bucketinfo = None
    try:
      bucketinfo = self.get_bucket_info(bucket_names)
    except Exception as err:
      ERROR("Failed to get bucket info from PC. Err : %s"%(err))
      return 0, 0
    if not bucketinfo:
      ERROR("No bucket info found for : %s"%(bucket_names))
      return 0, 0
    total_objects = total_size = 0
    for bucket, stats in bucketinfo.iteritems():
      total_size += int(stats["storage_usage_bytes"])
      total_objects += int(stats["object_count"])
    return total_objects, total_size

  def _get_cluster_details(self):
    url = "%s/api/nutanix/v3/clusters/list"%(self._pc_url)
    res = self._execute_rest_api_call(url=url, request_method=post, data='{}')
    cluster_details = {}
    for cluster in loads(res.content)["entities"]:
      cluster_type = "UNKNOWN"
      nodes = cluster["status"]["resources"].get("nodes")
      if nodes:
        nodes = nodes["hypervisor_server_list"]
        if "vmware" in [i["type"].lower() for i in nodes]:
          cluster_type = "VMWARE"
        elif all([i["type"].lower() == "ahv" for i in nodes]):
          cluster_type = "AHV"
      cluster_details[cluster["status"]["name"]] = {
          "uuid":cluster["metadata"]["uuid"],
          "vip":cluster["status"]["resources"]["network"].get("external_ip"),
          "dsip":cluster["status"]["resources"]["network"]\
                 .get("external_data_services_ip"),
          "type":cluster_type
          }
    return cluster_details

  def _get_esxi_network_uuid(self, cluster_uuid, network_name):
    url = "%s/oss/api/nutanix/v3/platform_client/pe_ipams/%s"%(self._pc_url,
                                                               cluster_uuid)
    res = self._execute_rest_api_call(url=url, request_method=get, data='{}')
    for network in loads(res.content)["pe_ipam_list"]:
      if network["esx_network"] == network_name:
        return network["ipam_name"]
    raise Exception("PE %s, Network %s not found"%(cluster_uuid, network_name))

  def _get_multiclusters(self):
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/groups"%(self._pc_url,
                                                              self.objects_uuid)
    payload = {"entity_type":"multicluster",
               "group_member_sort_attribute":"name",
               "group_member_sort_order":"ASCENDING",
               "group_member_count":20,
               "group_member_offset":0,
               "group_member_attributes":[{"attribute":"name"},
                                          {"attribute":"total_capacity_bytes"},
                                          {"attribute":"available_bytes"},
                                          {"attribute":"state"},
                                          {"attribute":"max_usage_pct"}]}
    res = self._execute_rest_api_call(url=url, request_method=post,
                                      data=dumps(payload))
    res = loads(res.content)
    result = {}
    for i in res["group_results"][0]["entity_results"]:
      tmp = {}
      for j in i["data"]:
        tmp[j["name"]] = j["values"][0]["values"][0]
      result[tmp["name"]] = tmp
    return result

  def _get_network_uuid(self, cluster_uuid, network_name, hypervisor_type):
    if hypervisor_type == "VMWARE":
      return self._get_esxi_network_uuid(cluster_uuid, network_name)
    else:
      return self._get_ahv_network_uuid(cluster_uuid, network_name)

  def get_endpoint_ips(self):
    return self._get_objects_details()["client_access_network_ip_used_list"]

  def num_msp_workers(self):
    details = self._get_objects_details()
    if not details:
      raise Exception("Failed to get the cluster details")
    return int(details["num_msp_workers"])

  def _get_objects_uuid(self):
    clusters = self._get_objects_details()
    if not clusters:
      return None
    self._state = clusters.get("state")
    return clusters.get("entity_id")

  def _get_objects_details(self):
    clusters = self._get_all_objects_clusters()
    if not clusters:
      ERROR("Failed to get Objects Clusters details from PC")
    return clusters.get(self.objects_name)

  @property
  def cluster_name(self):
    if not self._name:
      self._init_cluster_properties()
    return self._name

  @property
  def cluster_state(self):
    if not self._state:
      self._init_cluster_properties()
    return self._state

  @property
  def cluster_version(self):
    if not self._version:
      self._init_cluster_properties()
    return self._version

  @property
  def cluster_domain(self):
    if not self._domain:
      self._init_cluster_properties()
    return self._domain

  @property
  def cluster_uuid(self):
    if not self.uuid:
      self._init_cluster_properties()
    return self.uuid

  def _init_cluster_properties(self):
    cluster = {}
    if self.is_objects_service_enabled():
      cluster = self._get_objects_details()
      if not cluster:
        cluster = {}
    else:
      WARN("Objects is not enabled on %s"%self.pcip)
    self.uuid = cluster.get("entity_id")
    self._state = cluster.get("state")
    self._name = cluster.get("name")
    self._version = cluster.get("deployment_version")
    self._domain = cluster.get("domain")

  def _get_all_objects_clusters(self):
    #get_status = get all details about current objects cluster
    #get_list = get details about all objects cluster
    res = None
    payload = {'entity_type': 'objectstore',
               'group_member_attributes': [
                             {'attribute': 'name'},
                             {"attribute":"client_access_network_ip_list"},
                             {"attribute":"error_message_list"},
                             {"attribute":"client_access_network_ip_used_list"},
                             {"attribute":"percentage_complete"},
                             {"attribute":"total_capacity_gib"},
                             {"attribute":"num_msp_workers"},
                             {"attribute":"deployment_version"},
                             {"attribute":"num_alerts_critical"},
                             {"attribute":"num_alerts_info"},
                             {"attribute":"num_alerts_warning"},
                             {"attribute":"objects_certificate_dns"},
                             {"attribute":"objects_alternate_fqdns"},
                             {"attribute":"deployment_hv_type"},
                             {"attribute":"num_alerts_internal"},
                             {"attribute":"domain"},
                             {"attribute":"state"}],
               'group_member_sort_attribute': 'name'}
    try:
      res = self._execute_rest_api_call(url=self._oss_groups_url,
                                        request_method=post,
                                        data=dumps(payload))
    except Exception as err:
      return res
    res = loads(res.content)
    return self._extract_cluster_details(res)

  def _get_objects_vms(self, peip):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli vm.list | "
    cmd += "grep %s | cut -d\" \" -f1"%(self.objects_name.lower())
    vms, errors = remote_exec(peip, cmd)
    return vms.strip().split("\n")

  def _get_pe_name(self):
    url = "%s/oss/api/nutanix/v3/objectstore_proxy/%s/groups"%(self._pc_url,
                    self.objects_uuid)
    payload = {"entity_type":"multicluster",
               "group_member_count":1000,
              "group_member_attributes":[{"attribute":"name"},
                                         {"attribute":"total_capacity_bytes"},
                                         {"attribute":"uuid"},
                                         {"attribute":"physical_usage_bytes"},
                                         {"attribute":"is_primary"},
                                         {"attribute":"space_usage_bytes"},
                                         {"attribute":"state"},
                                         {"attribute":"pe_uuid"},
                                         {"attribute":"max_usage_pct"},
                                         {"attribute":"available_bytes"}],
              "group_member_sort_attribute":"name",
              "group_member_sort_order":"ASCENDING"}
    res = self._execute_rest_api_call(url=url,
                                request_method=post, data=dumps(payload))
    res = loads(res.content)
    result = {}
    for i in res["group_results"][0]["entity_results"]:
      tmp = {}
      for j in i["data"]:
        tmp[j["name"]] = j["values"][0]["values"][0]
      result[tmp["name"]] = tmp
    return result

  def _pe_cluster_uuid(self, peip, timeout=1200):
    stime = time()
    while timeout > time()-stime:
      clusters = self._get_cluster_details()
      INFO("PC : %s, Looking for cluster with PE : %s,  Clusters Found : %s,"
           "Retring after 30s, Time Elapsed : %s secs"
           %(self.pcip, peip, clusters, int(time()-stime)))
      for name, details in clusters.iteritems():
        if peip == details["vip"] or peip == details["dsip"]:
          return details["uuid"], details["type"]
      sleep(30)
    raise Exception("PE %s, UUID not found in PC : %s, Cluster Found : %s"
                    %(peip, self.pcip, clusters))

  def _replace_certs(self, services_to_restart_during_replace_cert, **kwargs):
    url="%s/%s/regenerate_self_signed_cert"%(self._oss_url, self.objects_uuid)
    res = self._execute_rest_api_call(url=url, request_method=post, data='{}',
                                      timeout=600)
    return self._wait_until_state(operation="Cert_replacement", timeout=600,
                    service_to_restart=services_to_restart_during_replace_cert)

  def _validate_certs(self, skip_if_not_healthy=False):
    if self.state != "COMPLETE" or skip_if_not_healthy:
      ERROR("%s is in %s state, skipping cert validation, skip if not "
            "healthy : %s"%(self.objects_name, self.state, skip_if_not_healthy))
      return
    # TODO: M2Crypto installation through pip is throwing error, ask about this
    import M2Crypto
    cert_from_pc = self.download_objects_certs()
    endpoints = self.get_objects_endpoint(True)
    fqdn = "%s.%s"%(self.objects_name, self._get_objects_details()["domain"])
    INFO("Validating Certs : %s, Endpoint : %s"%(self.objects_name, endpoints))
    for endpoint in endpoints:
      INFO("Validating Certs from %s"%(endpoint))
      cert_from_s3 =  None
      try:
        cert_from_s3 =  get_server_certificate((endpoint, 443,))
      except Exception as err:
        ERROR("Error occured while validating cert for %s. Err : %s, Trace : %s"
              %(endpoint, err, "".join(format_exc())))
        raise
      assert cert_from_pc["ca"] == cert_from_s3, "Cert mismatch - Expected"\
           " vs Found : %s vs %s"%(cert_from_pc["ca"], cert_from_s3)
      x509 = M2Crypto.X509.load_cert_string(cert_from_s3)
      altname = x509.get_ext("subjectAltName")
      cert_dns_names = [str(i.strip()) for i in altname.get_value().split(",")]
      assert len(cert_dns_names) >= 2, "Expected 2 cert for FQDN & wildcard"\
             ".FQDN, but Found : %s"%(cert_dns_names)
      assert ("DNS:%s"%(fqdn) in cert_dns_names and
              "DNS:*.%s"%(fqdn) in cert_dns_names), "Expected "\
              "cert with wildcard but found : %s"%(cert_dns_names)

  def _wait_until_state(self, expected_state="COMPLETE",operation="Deployment",
                        timeout=6000, service_to_restart=[], retry_interval=60):
    stime = time()
    in_progress_states = ["PENDING", "SCALING_OUT", "REPLACING_CERT",
                          "UPGRADING"]
    res = None
    service = None
    service_restarted = []
    while time()-stime < timeout:
      res = self._get_objects_details()
      if res["state"] == expected_state:
        INFO("%s state for %s is %s"%(operation, res, expected_state))
        return service_restarted
      if res["state"] in in_progress_states:
        INFO("Task : %s is in %s state. Time elapsed :  %s secs. State : %s. "
             "Retry in : %s s"%(operation, res["state"], int(time()-stime),
                               res, retry_interval))
        if service_to_restart and not service:
          service = service_to_restart.pop(0)
        if service:
          service_name, perc = service.split(":")
          if int(res["percentage_complete"].strip()) >= int(perc.strip()):
            INFO("Restarting service : %s, During Task : %s, at perc : %s"
                 %(service_name, operation, res["percentage_complete"]))
            self._restart_service(service_name)
            service_restarted.append("%s:%s"%(service,
                                              res["percentage_complete"]))
            service = None
        sleep(retry_interval)
        continue
      if "ERROR" in res["state"]:
        raise Exception("%s state is %s,Error : %s"%(operation, res["state"],
                        res))
      raise Exception("%s state is in unknown state : %s,Error : %s"
                      %(operation, res["state"], res))
    ERROR("Timeout after %s sec : %s is in %s state since %s secs. State : %s."
          %(timeout, operation, res["state"], int(time()-stime), res))
    raise Exception("Timeout %s, during %s : %s"%(timeout, res, operation))