"""
This is just my dirty script to upload few objects. If you need any help, plz
contact me @ anirudh.sonar@nutanix.com

NOTE : This script is not meant for perf measurement. Its not optimized to give
you correct numbers.It can generate some moderate load and thats the purpose of
this script.

Packages required :
  sudo pip install kubernetes
  sudo pip install paramiko
  sudo pip install boto3
  sudo pip install hanging_threads
  sudo pip install poster
  sudo pip install M2Crypto
  sudo pip install argcomplete
  sudo pip install httplib2
  sudo pip install bs4
  sudo pip install dateutil
  sudo pip install psutil
  sudo pip install getpass
  sudo pip install prettytable
  sudo pip install ansi2html
  sudo pip install pytz
  sudo pip install signal
  sudo pip install urllib2
  sudo pip install shutils
"""
import sys, subprocess
from sys import setrecursionlimit, exit, stdout
"""
for package in ["kubernetes", "paramiko", "boto3", "hanging_threads", "poster",
                "argcomplete", "httplib2", "bs4", "pytz", "psutil", "gc"]:
  try:
    __import__(package)
  except:
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
"""

"""
DEBUGGING MEMORY LEAK
mem_top()#limit=10, width=100, sep='\n',
#        refs_format='{num}\t{type} {obj}', bytes_format='{num}\t {obj}',
#        types_format='{num}\t {obj}',
#        verbose_types=[dict, list], verbose_file_name='/tmp/mem_top',)
"""
try:
  from kubernetes.stream import stream
  from kubernetes import client, config
except Exception as err:
  print "ERROR : Failed to load kubernetes packages. Error : %s"%(err)
import atexit,  posix,  logging, socket, json2html

from mem_top import mem_top
import re, shutil, inspect
from dateutil import parser as dateparser
from Queue import Queue
from psutil import Process
from getpass import getpass
from botocore.config import Config
from prettytable import PrettyTable
from ansi2html import Ansi2HTMLConverter
from bs4 import BeautifulSoup, SoupStrainer
from hanging_threads import start_monitoring
from poster.streaminghttp import register_openers
from threading import Thread, RLock, currentThread

from hmac import new
from re import findall
from uuid import uuid4
from httplib2 import Http
from pytz import timezone
from ast import literal_eval
from hashlib import md5, sha1
from math import floor, pow, log
from argparse import ArgumentParser
from argcomplete import autocomplete
from datetime import datetime, timedelta
from json import dumps, load, loads, dump
from signal import signal, SIGINT, SIGKILL
from base64 import encodestring, b64decode
from requests import post, get, put, delete
from boto3 import set_stream_logger, session
from string import digits, letters, hexdigits
from time import sleep, time, strftime, gmtime
from urllib2 import Request, urlopen, HTTPError
from stat import S_ISREG, S_ISDIR, S_IMODE, S_IWOTH
from random import choice, randint, randrange, sample, shuffle
from ssl import _create_unverified_context, get_server_certificate
from paramiko import SSHClient, AutoAddPolicy, MissingHostKeyPolicy
from traceback import format_exc, print_exc, print_stack, print_last, \
        print_list, print_tb, format_stack, format_tb, format_exception_only
from os import getpid, path, kill, SEEK_SET, remove, urandom, getcwd, system, \
               pathconf_names, makedirs, symlink, walk, fsync

"""
#Disable SSL warnings.
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()
"""

logging.VERBOSE=5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: \
                          inst.log(logging.VERBOSE, msg, *args, **kwargs)

logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger("paramiko").setLevel(logging.CRITICAL)
logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("kubernetes").setLevel(logging.CRITICAL)
logging.getLogger("socket").setLevel(logging.CRITICAL)

formatter = logging.Formatter('%(asctime)s (%(threadName)s) %(levelname)s '\
                              '[%(filename)s:%(lineno)d] : %(message)s')

logger = logging.getLogger()
WARN = logger.warn
INFO = logger.info
ERROR = logger.error
DEBUG = logger.debug
VERBOSE = logger.verbose
LOGEXCEPTION = logger.exception

setrecursionlimit(100000)

def logconfig(logfilename, level=1, rotate_logs=False,
              rotation_max_bytes=100*1024*1024, rotation_backup_count=10):
  """Configure python logging module.
  """
  ch = logging.StreamHandler()
  if level == 2:
    logger.setLevel(logging.DEBUG)
    ch.setLevel(logging.DEBUG)
  elif level > 3:
    logger.setLevel(logging.VERBOSE)
    ch.setLevel(logging.VERBOSE)
  else:
    logger.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)
  ch.setFormatter(formatter)
  fh = logging.FileHandler(logfilename)
  fh.setFormatter(formatter)
  logger.addHandler(fh)
  logger.addHandler(ch)
  if rotate_logs:
    rh = RotatingFileHandler(logfilename, maxBytes=rotation_max_bytes,
                                  backupCount=rotation_backup_count)
    logger.addHandler(rh)

def connect(aws=False, addressing_style=None, debug=False, **kwargs):
  """Initialize boto3 client.
  """
  if debug:
    set_stream_logger(name='botocore')
  else:
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
  INFO("Creating boto3 client with args : %s"%(kwargs))
  ip, access, secret = kwargs["ip"], kwargs["access"], kwargs["secret"]
  if (not ip and not aws) or (ip and (not access or not secret)):
    raise Exception("Endpoint URL or creds are missing")
  if not addressing_style:
    if ip.startswith("10") or "//10" in ip:
      addressing_style = "path"
    else:
      addressing_style = "virtual"
  botosession = session.Session()
  mpc = kwargs.get("max_pool_connections", 20)
  if mpc < 20:
    mpc = 20
  s3config = {"connect_timeout" :kwargs.get("connect_timeout", 10),
              "read_timeout":kwargs.get("read_timeout", 60),
              "max_pool_connections":mpc,
              "retries":{'max_attempts': kwargs.get("max_retries", 0)}}
  if addressing_style:
    s3config["s3"] = {"addressing_style":addressing_style}
  config = Config(**s3config)
  params={"config" : config,
          "service_name" : "s3",
          "aws_access_key_id" :  access,
          "aws_secret_access_key" :  secret,
          "use_ssl" : False if not kwargs.get("use_ssl") else kwargs["use_ssl"],
          "verify" : False if not kwargs.get("verify") else kwargs["verify"]}
  if ip:
    params["endpoint_url"] = ip
  s3handle = botosession.client(**params)
  if kwargs.get("validate_endpoint", False):
    INFO("Checking endoing validity")
    s3handle.list_buckets()
  return s3handle

def format_exception():
  exception_list = format_stack()
  exception_list = exception_list[:-2]
  exception_list.extend(format_tb(sys.exc_info()[2]))
  exception_list.extend(format_exception_only(sys.exc_info()[0],
                        sys.exc_info()[1]))

  exception_str = "Traceback (most recent call last):\n"
  exception_str += "".join(exception_list)
  # Removing the last \n
  exception_str = exception_str[:-1]
  return exception_str

def debug_print(msg):
  print datetime.now(), "(%s)"%inspect.stack()[1][3], msg

class UploadObjs(object):
  """Module which can trigger many workloads on S3 endpoint."""

  def __init__(self, endpoint, static_data=False, **kwargs):
    """
    Initilize class with required class params.

    Args:
      endpoint(str): a string of s3 endpoints as coma separated values
      static_data(bool): TBD
      kwargs(dict): all the other arguments required
    """
    self._kwargs = kwargs
    self.instance_uuid = uuid4().hex
    self._init_vars(endpoint, static_data)

  def _init_vars(self, endpoint, static_data):
    """initialize all the variables needed for the object"""
    self._utils = Utils()
    self._static_data = static_data
    self._test_start_time = datetime.now()
    self._endpoints = endpoint.split(",")
    self._lock = RLock()
    self._pid = getpid()
    self._init_bucketinfo_db()
    self._ip = self._utils.get_local_ip()
    self._data, self._stats, self._num_objects = "", {}, -1
    self.rdm, self._logstats = None, None

    self._debug_level = self._kwargs.pop("v")
    self._access = self._kwargs.pop("access")
    self._secret = self._kwargs.pop("secret")
    self.logfile = self._kwargs.pop("logfile")
    self._timeout = self._kwargs.pop("runtime")
    self._iter = self._kwargs.pop("iteration_num")
    self._local_run = self._kwargs.pop("local_run")
    self.logpath = self._kwargs.pop("log_path")+"/"
    self._retry_count = self._kwargs.pop("retry_count")
    self._retry_delay = self._kwargs.pop("retry_delay")
    self._uniq_obj_idn = "_%s_"%self.instance_uuid[0:5]
    self._ignore_errors = self._kwargs.pop("ignore_errors")
    self.fatals , self._s3objs = [], []
    self.nfs_server = self._kwargs.get("nfs_server",
                                       self._endpoints[0].split("/")[-1])
    self._thrd_monitor_interval = self._kwargs.pop("thread_monitoring_interval")
    self._opexecutor = OpExecutor(self, self.logpath+"ERROR")

    self._conditions, self._oss_cluster = None, None
    self._start_time = time()
    self._multipart_objects_map = {}
    self._existing_objects = self._existing_size = 0
    self._execute_api = self._opexecutor._execute_api
    self._local_data = self._task_manager = None, None
    self._objectscluster, self._upgrade_in_progress = None, False

    self._objname_distributor = ObjNameDistributor(self)
    self._archival_server_location = "load/%s/%s/%s"%("generic",
                          self._kwargs["test_start_time"], self.instance_uuid)
    INFO("Instance/Obj UUID : %s / %s"%(self.instance_uuid, self._uniq_obj_idn))
    self._init_db()

  def _init_db(self):
    """initialize the in memory database for all the workloads running"""
    self._webserver = None
    self._instance_num_objs, self._instance_size = 0, 0
    self._total_ops_recorded = {
          "vars":{"stop_writes":False,
                  "expect_failure_during_ei":False,
                  "time_to_exit":False,
                  "execution_completed":False,
                  "force_kill":False,
                  "change_workload_detected":False,
                  "workload_started":False,
                  "auto_deletes_enabled":False,
                  "test_timeout_reached":False,
                  "stop_workload_initiated":False
                  },
          "totalcount": 0,
          "totalfailure": 0,
          "totalfailureignored": 0,
          "expected_failure_count": 0,
          "skipped_old_reads": 0,
          "multipart": {
            "totalongoing": 0,
            "totalcount": 0,
            "totalparts": 0,
            "aborted": {
              "count": 0,
              "size": 0,
              "parts":0
            },
            "totalfinalizedparts": 0
          },
          "object-controller": {"stats": {}},
          "errors": {"err_msgs": {},"err_codes": {}},
          "endpoint_stats": {},
          "backup": {"bucketinfo": {}},
          "workload_profiles": [],
          "active_objects": {},
          "failed_workers": {},
          "nfsstats": {"filesdirs": {"totaldirs": 0,"totalfiles": 0}},
          "misc": {},
          "counts": {},
          "prefix_distribution": {},
          "anomalis": {
            "NfsReadMismatch": {"count": 0},
            "DirFoundInS3List": {"count": 0},
            "SizeChangedForNfsRenameOrCopy": {"count": 0},
            "SrcCopyObjCouldNotLock": {"count": 0},
            "CopySrcObjNotFound": {"count": 0},
            "LookUpAfterDelete": {"count": 0},
            "skipped_expired_reads": {"count": 0},
            "SizeChangedForCopy": {"count": 0},
            "ExpiredObjectFoundInList": {"count": 0},
            "skipped_expired_range_reads": {"count": 0},
          },
          "workload_to_prefix_map": {},
          "TestStatus": {
            "live_workloads":{},
            "status": "Running",
            "start_time": "%s"%(self._test_start_time),
            "client_ip": self._utils.get_local_ip(),
            "uuid": self.instance_uuid,
            "memory_usage": 0,
            "logs":{"logfile":self.logfile, "logpath":self.logpath}
          },
          "opstats":{},
          "compaction_stats":{}
        }

  @property
  def _time_to_exit(self):
    return self._total_ops_recorded["vars"]["time_to_exit"]

  @property
  def _execution_completed(self):
    return self._total_ops_recorded["vars"]["execution_completed"]

  @property
  def _expect_failure_during_ei(self):
    return self._total_ops_recorded["vars"]["expect_failure_during_ei"]

  def is_alive(self):
    return False if self._time_to_exit else True

  def _debug_logging(self, msg, level=0, msg_type="debug"):
    """logs the message with the log level according to argument"""
    if self._debug_level < level:
      return
    if msg_type == Constants.MSG_TYPE_DEBUG:
      DEBUG(msg)
    elif msg_type == Constants.MSG_TYPE_ERROR:
      ERROR(msg)
    elif msg_type == Constants.MSG_TYPE_WARN:
      WARN(msg)

  def capture_tcpdump(self, **kwargs):
    """
    this method is used mainly to track nfs requests it starts the packet
    tracing using tcpdump cmd
    """
    self._utils.start_tcpdump(kwargs["tcpdump_cmd"], self.logpath)

  def _init_bucketinfo_db(self, archive=False):
    """initialize bucket information database"""
    if archive:
      cd = "%s"%(datetime.now())
      self._total_ops_recorded["backup"]["bucketinfo"][cd] = self._bucket_info
    self._bucket_info = {"prefixes" : {},
                         "bucket_names" : [],
                         "list_bucket_map":{},
                         "bucket_to_marker_map":{},
                         "delete_bucket_to_marker_map":{
                            "num_objs_deleted":0,
                            "num_deletemarkers_deleted":0,
                            "total_size":0,
                            "total_time":0,
                            "in_use_buckets":[],
                            "empty_buckets":[]},
                         "list_for_copy" : {},
                         "bucket_map":{}
                         }

  def archive_results(self):
    """archive the stats and db to s3 bucket"""
    try:
      self._archive_results()
    except Exception as err:
      ERROR("Failed to archive results. Err : %s"%err)

  def _archive_results(self):
    """
    archive the stats and db to s3 bucket,
    it will take archival_endpoint_url, archival_access_key, archival_secret_key
    from self._kwargs and push the files using given parameter
    """
    bucket = self._kwargs["archival_bucket_name"]
    if not hasattr(self, "_archival_s3c"):
      INFO("Archival location : %s / %s / %s"
           %(self._kwargs["archival_endpoint_url"], bucket,
             self._archival_server_location))
      self._archival_s3c = connect(ip=self._kwargs["archival_endpoint_url"],
                     access=self._kwargs["archival_access_key"],
                     secret=self._kwargs["archival_secret_key"], max_retries=5)
    for file_to_upload in [{"records.json":self._total_ops_recorded},
                     {"test.log":self.logfile},
                     {"config.json":Constants.STATIC_TESTCONFIG_FILE},
                     {"stats.html":Constants.STATIC_STATS_HTML_FILE},
                     {"stats":Constants.STATIC_STATS_FILE},
                     {"stats.err":Constants.STATIC_ERROR_FILE},
                     {"bucket_info.json":self._bucket_info}]:
      (keyname, body), = file_to_upload.items()
      objname = "%s/%s"%(self._archival_server_location, keyname)
      if isinstance(body, dict):
        body = dumps(self._total_ops_recorded, indent=2, default=str)
      elif path.exists(body) and path.isfile(body):
        try:
          with open(body) as fh:
            body = fh.read()
        except Exception as err:
          ERROR("Failed to read %s. Skipping uplading to archival server : %s"
                %(objname, self._kwargs["archival_endpoint_url"]))
          continue
      DEBUG("Uploading %s to archival server : %s"
            %(objname, self._kwargs["archival_endpoint_url"]))
      try:
        self._archival_s3c.put_object(Bucket=bucket, Key=objname, Body=body)
      except Exception as err:
        ERROR("Failed to upload %s to archival server : %s, err : %s"
              %(objname, self._kwargs["archival_endpoint_url"], err))

  def _pause_workload(self, workload_name, sleep_time=0):
    """
    this method will be called every time when controller thread push new
    workload to worker_queue to check if the given workload is paused or not
    returns whether the workload will be paused or not depending on the
    "auto_deletes_enabled" ans "stop_writes" flags in db

    Args:
      workload_name(str): name of the workload to check
      sleep_time(int): sleep time defines for how long thread should be in sleep
                       state when the workload is paused

    Returns: bool
      True, if workload should be paused, False otherwise
    """
    auto_deletes = self._total_ops_recorded["vars"]["auto_deletes_enabled"]
    stop_writes = self._total_ops_recorded["vars"]["stop_writes"]
    msg = "Workload : %s is Paused, AutoDeletes : %s, StopWrites : %s, Sleep "\
          "Time : %s"%(workload_name.upper(), auto_deletes, stop_writes,
                       sleep_time)
    res = False
    if workload_name in ["copy", "write", "nfsrename", "nfscopy", "nfsvdbench",
                         "nfsoverwrite", "multipart_put", "nfsfio", "nfswrite"]:
      if auto_deletes or stop_writes:
        res = True
    if workload_name in ["read", "nfslist", "nfsread", "read_objects"]:
      if auto_deletes:
        res = True
    if res:
      print msg
      sleep(sleep_time)
    return res

  def copy(self, **kwargs):
    """
    controller thread will call this method, it will generate relevant kwargs
    for copy and will push the actual method and kwargs in the worker queue

    Args:

    """
    del(kwargs)
    srcbucket, srckey, srcsize, srctag, srcversion = None, None, 0, None, None
    refresh_freq = self._kwargs['copy_srcobj_refresh_frequency']
    INFO("Starting COPY workload,  Prefix : %s, Obj size limit : %s, "
         "SrcCopyRefreshRate : %s"
         %(self._total_ops_recorded["workload_to_prefix_map"]["copy"],
           self._kwargs["copy_srcobj_size_limit"], refresh_freq))
    count = 0
    while not self._exit_test("Copy Workload"):
      if self._pause_workload("copy", 10):
        continue

      bucket = choice(self._bucket_info["bucket_names"])
      if count % refresh_freq == 0:
        if srckey:
          self._objname_distributor.may_be_lock_unlock_obj(srcbucket, srckey,
                                               srcsize, Constants.UNLOCK)
        srcbucket, srckey, srcsize, srctag, srcversion = \
            self._objname_distributor.get_object_from_bucket(
            choice(self._total_ops_recorded["workload_to_prefix_map"]["copy"]),
            nfs_workload=False, workload="copy",
            expected_obj_size=self._kwargs.get("copy_srcobj_size_limit"),
            dstbucket=None if refresh_freq > 1 else bucket)
        if not self._objname_distributor.may_be_lock_unlock_obj(srcbucket,
                                              srckey, srcsize, Constants.LOCK):
          self._testobj._total_ops_recorded["anomalis"][
                                      "SrcCopyObjCouldNotLock"]["count"] += 1
          continue
        if srcsize == 0 and refresh_freq > 10:
          refresh_freq = 10 #Hack to avoid too many copy of zero byte objects.
          #Else we will be too busy to copy zero byte objects.
        elif refresh_freq == 10:
          refresh_freq = self._kwargs['copy_srcobj_refresh_frequency']
      keyname = self._objname_distributor.get_dst_obj_for_copy(bucket, count,
                                  srckey, srcsize, srctag, "copy")
      copysrc = {"Bucket": srcbucket, "Key": srckey}
      if srcversion: copysrc["VersionId"] = srcversion
      self._task_manager.add(self._copy_object, quota_type="copy",rsize=srcsize,
               srctag=srctag, read_after_copy=self._kwargs["read_after_copy"],
               CopySource=copysrc, Key=keyname, Bucket=bucket)
      count += 1

  def _get_delete_prefix(self):
    if len(self._workload.keys()) == 1 and self._workload.keys()[0] == "delete":
      return ""
    if self._total_ops_recorded["vars"]["auto_deletes_enabled"]:
      return choice(self._bucket_info["prefixes"]["prefixes"])
    else:
      return choice(self._total_ops_recorded["workload_to_prefix_map"][
                      "delete"])

  def _lock_unlock_bucket_for_delete(self, bucket, lock=False, unlock=False):
    if lock:
      if bucket in self._bucket_info["delete_bucket_to_marker_map"][
                                                              "in_use_buckets"]:
        return False
      with self._lock:
        if bucket not in self._bucket_info["delete_bucket_to_marker_map"][
                                                              "in_use_buckets"]:
          self._bucket_info["delete_bucket_to_marker_map"]["in_use_buckets"
            ].append(bucket)
          return True
      return False
    if unlock:
      if bucket in self._bucket_info["delete_bucket_to_marker_map"][
                                                              "in_use_buckets"]:
        with self._lock:
          if bucket in self._bucket_info["delete_bucket_to_marker_map"][
                                                              "in_use_buckets"]:
            self._bucket_info["delete_bucket_to_marker_map"]["in_use_buckets"
              ].remove(bucket)
            return True
      return False

  def delete(self, **kwargs):
    del(kwargs) #Save memory
    self._delete_empty_bucket = True if len(self._workload.keys()) == 1 and \
                          self._workload.keys()[0] == "delete" else False
    for bucket in self._bucket_info["bucket_names"]:
      if bucket not in self._bucket_info["delete_bucket_to_marker_map"]:
        versioning = self._execute(self._s3ops.s3_get_bucket_versioning,
                     Bucket=bucket).get("Status") in ["Suspended", "Enabled"]
        self._bucket_info["delete_bucket_to_marker_map"][bucket] = {
                "versioning":versioning, "locker":None}
    stime = time()
    self._deleted_objects =  0
    self._deleted_skipped = 0
    self._last_bucket_delete_time = time()
    INFO("Starting DELETE workload, Prefix : %s"
         %self._total_ops_recorded["workload_to_prefix_map"]["delete"])
    while not self._exit_test("Delete Workload"):
      if not self._bucket_info["bucket_names"]:
        self._delete_empty_buckets(self._kwargs["skip_bucket_deletion"])
        self._set_exit_marker("No bucket found to delete")
        continue
      if self._delete_empty_bucket and (time() -
                                        self._last_bucket_delete_time>120):
        self._delete_empty_buckets(self._kwargs["skip_bucket_deletion"])
        self._last_bucket_delete_time = time()
      prefix = self._get_delete_prefix()
      bucket = self._get_bucket_for_delete(
                                      self._kwargs["unique_bucket_for_delete"])
      if not bucket:
        DEBUG("No bucket found to delete object. Num Empty Buckets : %s, "
              "Num In Use buckets : %s. Total buckets : %s"
              %(len(self._bucket_info["delete_bucket_to_marker_map"][
                                      "empty_buckets"]),
              len(self._bucket_info["delete_bucket_to_marker_map"][
                  "in_use_buckets"]), len(self._bucket_info["bucket_names"])))
        sleep(1)
        continue
      if prefix not in self._bucket_info["delete_bucket_to_marker_map"][bucket]:
        self._bucket_info["delete_bucket_to_marker_map"][bucket][prefix] = {
          "version_marker":"", "key_marker":"", "continuation_token":""}
      self._task_manager.add(self._list_and_delete, prefix=prefix,
          quota_type="delete", bucket=bucket)

  def deploy(self, **kwargs):
    num_loop = kwargs.pop("num_loop_deploments")
    num_nodes = kwargs.pop("num_nodes")
    restart_zk = kwargs.pop("restart_zk")
    self._total_ops_recorded["deploy"] = {"num_deploy":1, "deployments":[]}
    if kwargs["manual_upgrade_build_url"]:
      if not self._objectscluster.is_objects_service_enabled():
        self._objectscluster.enable_objects_service()
      self._objectscluster._manual_objects_upgrade(
          kwargs["manual_upgrade_build_url"], kwargs["objects_upgrade_version"])
    while num_loop > self._total_ops_recorded["deploy"]["num_deploy"] and \
                                        not self._exit_test("Deploy Workload"):
      self._objectscluster.restart_zk(restart_zk, 60)
      self._objectscluster.skip_node_check()
      self._deploy_objects(num_nodes, **kwargs)
      print dumps(self._total_ops_recorded["deploy"], indent=2, default=str)
      self._sanity_check(**kwargs)
      self._multicluster_check(**kwargs)
      self._scaleout_check(**kwargs)
      self._replace_certs(**kwargs)
      print dumps(self._total_ops_recorded["deploy"], indent=2, default=str)
      self._objectscluster.restart_zk(restart_zk, 60)
      self._delete_objects(timeout=1800, **kwargs)
      print dumps(self._total_ops_recorded["deploy"], indent=2, default=str)
      self._total_ops_recorded["deploy"]["num_deploy"] += 1
      sleep(kwargs["loop_deploy_delay"])
      INFO("Deployment Num - %s : %s"
           %(self._total_ops_recorded["deploy"]["num_deploy"],
             dumps(self._total_ops_recorded["deploy"], indent=2, default=str)))
    INFO("Final Deployment Result : %s"%(self._total_ops_recorded["deploy"]))
    self._total_ops_recorded["vars"]["execution_completed"] = True

  def read_object(self, etag, **kwargs):
    anomalis = "skipped_expired_reads"
    if "Range" in kwargs: anomalis = "skipped_expired_range_reads"
    if self._is_object_expired(kwargs["Bucket"], kwargs["Key"],anomalis): return
    is_nfs_obj = self._is_nfs_object(kwargs["Key"], etag, kwargs["Bucket"],
                                     kwargs["VersionId"])
    if "-" in etag and not self._kwargs["skip_parts_subblock_integrity_check"] \
        and not is_nfs_obj:
      return self.read_multipart_object(etag, **kwargs)
    skip_integrity_check = kwargs.pop("skip_integrity_check")
    res = self._get_object(**kwargs)
    if not res:
      return
    oc = res.get("ResponseMetadata", {}).get("HTTPHeaders",{}).get("x-ntnx-id")
    stream = res["Body"]
    datahash = md5()
    data_size = 0
    while True:
      tmp = ""
      try:
        tmp = stream.read(1024*1024)
      except Exception as err:
        ERROR("Bucket / Obj : %s / %s, Error : %s, Oc : %s"
              %(kwargs.get("Bucket"), kwargs.get("Key"), err.message, oc))
        raise
      if not tmp:
        break
      elif skip_integrity_check:
        continue
      datahash.update(tmp)
      self._range_read_integrity_check(bucket=kwargs["Bucket"],
                                       objname=kwargs["Key"], data=tmp,
                                       object_offset=data_size)
      data_size += len(tmp)
    #self._head_object(**kwargs)
    if skip_integrity_check:
      return res
    s3md5 = res.get("ETag").strip("\"")
    md5sum = datahash.hexdigest()
    if res["ContentLength"] != data_size:
      raise Exception("%s / %s : DataLen mismatch. Expected vs Found : %s vs %s"
                      ", Oc : %s"%(kwargs["Bucket"], kwargs["Key"],data_size,
                        res["ContentLength"], oc))
    # - will prevent nfs-written or over-written obj etag verification, since
    #is_nfs_obj will prevent false negative is nfs-obj is copied via s3
    if "-" in etag or is_nfs_obj:
      return res
    if s3md5 != md5sum:
      raise Exception("Data Corruption : Bucket/Object : %s / %s(version/len :"
                " %s (%s)), Expected md5 vs found : %s vs %s, Oc : %s"
                %(kwargs["Bucket"], kwargs["Key"], kwargs["VersionId"],
                res["ContentLength"],md5sum, s3md5, oc))
    return res

  def read_multipart_object(self, etag, **kwargs):
    skip_integrity_check = kwargs.pop("skip_integrity_check")
    res = self._get_object(**kwargs)
    if not res:
      return
    partinfo = self.get_object_partinfo(kwargs["Bucket"], kwargs["Key"],
                                  kwargs["VersionId"], res["ETag"].strip("\""))
    oc = res.get("ResponseMetadata", {}).get("HTTPHeaders",{}).get("x-ntnx-id")
    stream = res["Body"]
    datahash = md5()
    data_size = 0
    current_part = partinfo.pop(0)
    current_part_size = current_part["size"]
    current_part_num = current_part["partnum"]
    current_offset = 0
    while True:
      size_to_read = 1024*1024
      if current_part_size < size_to_read:
        #If current_part_size is less than size to read, means we read almost
        #entire data from current part. And less than MB is remaining
        #Just read the remaining data from current part.
        size_to_read = current_part_size
        current_part_size = 0
      else:
        current_part_size -= size_to_read
      current_offset += size_to_read
      if size_to_read == 0:
        #size_to_read = 0 mean, we read current part
        if len(partinfo) > 0:
          current_part = partinfo.pop(0)
          current_part_size = current_part["size"]
          current_part_num = current_part["partnum"]
          #Reset current offset, so we start from offset=0 for current part.
          #This is required since we upload part independenly so every part
          #stamps offset starts with 0
          current_offset = 0
          continue
        else:
          #If no parts in partinfo then we read entire object.
          #So try reading larger than 0 bytes data from stream and expect no
          #data is returned.
          size_to_read = 1024*1024
      tmp = ""
      try:
        tmp = stream.read(size_to_read)
      except Exception as err:
        ERROR("Bucket / Obj : %s / %s, Error : %s, Oc : %s"
              %(kwargs.get("Bucket"), kwargs.get("Key"), err.message, oc))
        raise
      if not tmp:
        break
      elif skip_integrity_check:
        continue
      datahash.update(tmp)
      offset_range = "bytes=%s-%s"%(current_offset-size_to_read, current_offset)
      self._range_read_integrity_check(bucket=kwargs["Bucket"],
                    objname=kwargs["Key"], data=tmp,
                    offset_range=offset_range,
                    etag="DummyEtagMultipartPart%s"%current_part_num,
                    validate_range_read_offset=True, object_offset=data_size)
      data_size += len(tmp)
    if skip_integrity_check:
      return res
    if res["ContentLength"] != data_size:
      raise Exception("%s / %s : DataLen mismatch. Expected vs Found : %s vs %s"
                      ", Oc : %s"%(kwargs["Bucket"], kwargs["Key"],data_size,
                        res["ContentLength"], oc))
    return res

  def get_object_partinfo(self, bucket, objname, versionid, etag):
    num_parts = int(etag.split("-")[-1].strip())
    partinfo = []
    for part in range(1, num_parts+1):
      res = self._head_object(Bucket=bucket, Key=objname, VersionId=versionid,
                              PartNumber=part)
      partinfo.append({"size" : res["ContentLength"], "partnum":part})
    return partinfo

  def get_processed_stats(self):
    return self._logstats.log_stats(self._total_ops_recorded, True, True,
                                  workload_start_time=self._workload_start_time)

  def register_results(self, objects_elk_endpoint):
    ip = self._utils.get_local_ip()
    data = {"stats":self.get_processed_stats(),
            "current_time":strftime("%a, %d %b %Y %X GMT", gmtime()),
            "uuid":self.instance_uuid,
            "client_ip" : ip}
    url = objects_elk_endpoint
    self._pcobj._execute_rest_api_call(request_method=post,
                                       url=url, data=data)

  def get_stats(self):
    return self._total_ops_recorded

  def compact_ms(self, column_families=None, pods=None,
                 randomize_pods=True, wait_for_compaction=True,
                 compaction_timeout=7200, compaction_interval=900, **kwargs):
    if not self._error_injection:
      self._error_injection = ErrorInjection(self, self._objectscluster)
    if not pods:
      pods = self._error_injection.get_all_ms_pods()
    if not column_families:
      column_families = self._error_injection._get_cf_index(None, True)
    self._total_ops_recorded["ms_compaction"] = {}
    for ms in pods:
      self._total_ops_recorded["ms_compaction"][ms] = {}
      for cf in column_families:
        self._total_ops_recorded["ms_compaction"][ms][cf] = {"totalcount":0,
                                              "last" : datetime.now()}
    INFO("Services selected for compaction : %s, CF : %s"
         %(pods, column_families))
    while not self._exit_test("Compact MS"):
      for podname in pods:
        cf = choice(column_families)
        INFO("Initiating %s compaction on %s"%(cf, podname))
        if not self._error_injection.compact_cf(cf, podname):
          ERROR("Failed to initiate %s compaction on %s"%(cf, podname))
        else:
          self._total_ops_recorded["ms_compaction"][podname][cf]["totalcount"
                                                                        ] += 1
          self._total_ops_recorded["ms_compaction"][podname][cf]["last"
                                                              ] = datetime.now()
      if wait_for_compaction:
        self._error_injection.wait_for_ongoing_compaction(compaction_timeout)
      sleep(compaction_interval)

  def inject_errors(self, **kwargs):
    self._error_injection = ErrorInjection(self, self._objectscluster,
                            kwargs.pop("eiops"), kwargs.pop("component_for_ei"))
    self._error_injection.inject_errors(kwargs.pop("ei_interval"))
    if self._time_to_exit and not self._execution_completed:
      ERROR("Exit marker found, leaving cluster in existing state.")
      return
    INFO("PoweringOn/Resuming all the VMs")
    self._error_injection.power_on_everything()

  def list(self, **kwargs):
    del(kwargs)
    INFO("Starting S3LIST workload")
    while not self._exit_test("List Workload"):
      bucket = choice(self._bucket_info["bucket_names"])
      if bucket not in self._bucket_info["list_bucket_map"]:
        self._bucket_info["list_bucket_map"][bucket] = {
                  "vmarker":"", "kmarker":"", "prefix":"", "prefixes":[]}
      try:
        self._task_manager.add(self._list_delimeter, quota_type="list",
          validate_list_against_head=self._kwargs["validate_list_against_head"],
          bucket=bucket,
          prefix=self._bucket_info["list_bucket_map"][bucket]["prefix"],
          marker=self._bucket_info["list_bucket_map"][bucket]["vmarker"])
      except Exception as err:
        if "Timeout while adding task" in err or \
              "Timeout while adding task" in err.message:
          continue

  def log_stats(self, debug_stats, stats_interval, **kwargs):
    debug_stats_interval = kwargs["debug_stats_interval"]
    del(kwargs)
    self._logstats = Stats(self._start_time, debug_stats, self._pid,
                              self._iter)
    if self._objects_cluster_monitor:
      self._existing_objects , self._existing_size = \
        self._objectscluster._get_bucket_stats(self._bucket_info["bucket_names"])
      INFO("Existing Objects Stats, Total Objects/Size : %s, %s"
           %(self._existing_objects , self._existing_size))
    count = 0
    log_debug_stats = True
    while not self._exit_test("Log Stats"):
      log_debug_stats = False
      if count >= debug_stats_interval:
        count = 0
        log_debug_stats = True
      count += stats_interval
      if self._objects_cluster_monitor:
        try:
          self._monitor_bucket_stats()
        except Exception as err:
          ERROR("Failed to get PC stats. Error : (%s) %s"%(type(err), err))
      self._log_stats(log_debug_stats=log_debug_stats)
      sleep(stats_interval)

  def nfslist(self, **kwargs):
    del(kwargs)
    self._total_ops_recorded["misc"].setdefault("nfslist", {"delay" : 0})
    INFO("Starting NFSLIST workload")
    self._bucket_info["nfs_list_bucket_map"] = {"in_use":[]}
    while not self._exit_test("NFS List"):
      if self._pause_workload("nfslist", 10):
        continue
      bucket_to_list = None
      for bucket in self._nfsops._registered_buckets:
        if bucket not in self._bucket_info["nfs_list_bucket_map"]["in_use"]:
          self._bucket_info["nfs_list_bucket_map"]["in_use"].append(bucket)
          bucket_to_list = bucket
          break
      if not bucket_to_list:
        self._total_ops_recorded["misc"]["nfslist"]["delay"] += 1
        if self._total_ops_recorded["misc"]["nfslist"]["delay"] % 30 == 0:
          ERROR("Failed find any bucket for nfslist workload since %s seconds"
                %(self._total_ops_recorded["misc"]["nfslist"]["delay"]))
        sleep(1)
        continue
      self._total_ops_recorded["misc"]["nfslist"]["delay"] = 0
      try:
        self._task_manager.add(self._nfs_list, quota_type="nfslist",
          bucket=bucket_to_list,
          skip_list_validation_vs_s3=self._kwargs["skip_list_validation_vs_s3"])
      except Exception as err:
        if bucket in self._bucket_info["nfs_list_bucket_map"]["in_use"]:
          with self._lock:
            if bucket in self._bucket_info["nfs_list_bucket_map"]["in_use"]:
              self._bucket_info["nfs_list_bucket_map"]["in_use"].remove(bucket)
        if "Timeout while adding task" in err \
              or "Timeout while adding task" in err.message:
          continue

  def _nfs_list(self, bucket, skip_list_validation_vs_s3):
    mntpt = self._total_ops_recorded["nfsops"]["mounts"][bucket]["path"]
    try:
      self._list_nfs_dir(bucket=bucket,
                skip_list_validation_vs_s3=skip_list_validation_vs_s3)
    except Exception as err:
      ERROR("Hit error while nfs listing bucket : %s, Error : %s, Trace : %s"
            %(bucket, err.message, ''.join(format_exc())))
      raise

  def _list_nfs_dir(self, bucket, skip_list_validation_vs_s3):
    if self._exit_test("List Nfs Dir"):
      return {}
    return self._nfsops.nfslist(bucket, skip_list_validation_vs_s3)

  def nfsfio(self, **kwargs):
    del(kwargs)
    self._total_ops_recorded["counts"]["fio"] = {"count":0}
    while not self._exit_test("NFS Fio Workload"):
      if self._pause_workload("nfsfio", 10):
        continue
      self._task_manager.add(self._nfsops.nfsfio, quota_type="nfsfio",
                             filenum=self._file_put_count,
                             io_type=self._kwargs["fio_io_type"],
                             block_size=self._kwargs["fio_write_block_size"],
                             runtime=self._kwargs["fio_runtime"],
                             iodepth=self._kwargs["fio_iodepth"],
                             filesize=self._kwargs["fio_filesize"],
                             buffered_io=self._kwargs["fio_buffered_io"])
      self._total_ops_recorded["counts"]["fio"]["count"] += 1
      self._file_put_count += 1

  def nfsvdbench(self, **kwargs):
    del(kwargs)
    self._total_ops_recorded["counts"]["vdbench"] = {"count":0}
    vdb_out = self._kwargs["vdbench_logs_location"]
    vdb_out = path.join(self.logpath, "vdb") if not vdb_out else vdb_out
    self._utils.local_exec("mkdir -p %s"%(vdb_out))
    while not self._exit_test("Nfs vdbench Workload"):
      if self._pause_workload("nfsvdbench", 10):
        continue
      self._task_manager.add(self._nfsops.nfsvdbench, quota_type="nfsvdbench",
                             filenum=self._file_put_count,
                             io_type=self._kwargs["vdb_io_type"],
                             block_size=self._kwargs["vdb_write_block_size"],
                             runtime=self._kwargs["vdb_runtime"],
                             iodepth=self._kwargs["vdb_iodepth"],
                             filesize=self._kwargs["vdb_filesize"],
                             buffered_io=self._kwargs["vdb_buffered_io_flag"],
                             read_perc=self._kwargs["vdbench_read_perc"],
                             vdb_output_dir=vdb_out,
               vdbench_binary_location=self._kwargs["vdbench_binary_location"])
      self._total_ops_recorded["counts"]["vdbench"]["count"] += 1
      self._file_put_count += 1

  def nfswrite(self, **kwargs):
    del(kwargs)
    self._num_objects = self._kwargs["num_objects"]
    self._is_nfswrite_workload = True
    INFO("Starting NFSWRITE workload with static_data  :%s, Prefixes : %s"
         %(self._static_data, self._bucket_info["prefixes"]["prefixes"]))
    while not self._exit_test("Nfs Write Workload"):
      if self._pause_workload("nfswrite", 10):
        continue
      self._task_manager.add(self._nfsops.nfswrite, quota_type="nfswrite",
                             objnum=self._file_put_count,
                             block_size=self._kwargs["nfs_write_block_size"])
      self._file_put_count += 1

  def nfsread(self, **kwargs):
    del(kwargs)
    INFO("Starting NFSREAD workload with prefix : %s"
         %(self._total_ops_recorded["workload_to_prefix_map"]["nfsread"]))
    self._is_nfsread_workload = True
    while not self._exit_test("NFS Read Workload"):
      if self._pause_workload("nfsread", 10):
        continue
      prefix=choice(
                self._total_ops_recorded["workload_to_prefix_map"]["nfsread"])
      self._task_manager.add(self._list_and_read, quota_type="nfsread",
       bucket=choice(self._total_ops_recorded["nfsops"]["mounts"].keys()),
       objprefix=choice(self._total_ops_recorded["workload_to_prefix_map"
                                         ]["nfsread"]), num_reads_from_list=0,
       skip_integrity_check=self._kwargs["skip_nfs_read_only_integrity_check"],
       skip_zero_file_nfs_read=self._kwargs["skip_zero_file_nfs_read"],
       nfs_read_type=self._kwargs["nfs_read_type"],
       num_nfs_range_reads_per_file=self._kwargs["num_nfs_range_reads_per_file"],
       nfs_range_read_size=self._kwargs["nfs_range_read_size"])

  def nfsrename(self, **kwargs):
    self._nfs_copy_or_rename(self._nfsops.nfsrename, "nfsrename",
      self._total_ops_recorded["workload_to_prefix_map"]["nfsrename"], **kwargs)

  def nfscopy(self, **kwargs):
    self._nfs_copy_or_rename(self._nfsops.nfscopy, "nfscopy",
      self._total_ops_recorded["workload_to_prefix_map"]["nfscopy"], **kwargs)

  def _nfs_copy_or_rename(self, method, quota_type, fileprefix, **kwargs):
    del(kwargs)
    workload_name = method.__name__
    srckey = srcbucket = srctag = srcversion = None
    srcsize = 0
    INFO("Starting %s workload,  Prefix : %s, Src Obj size limit : %s"
         %(workload_name.upper(),
           fileprefix, self._kwargs.get("copy_srcobj_size_limit")))
    count = 0
    while not self._exit_test(workload_name):
      if self._pause_workload(workload_name, 10):
        continue
      srcbucket, srckey, srcsize, srctag, srcversion = \
            self._objname_distributor.get_object_from_bucket(choice(fileprefix),
                              nfs_workload=True, workload=workload_name)
      bucket = choice(self._nfsops._registered_buckets)
      keyname = self._objname_distributor.get_dst_obj_for_copy(bucket, count,
                                      srckey, srcsize, srctag, method.__name__)
      copysrc = {"Bucket": srcbucket, "Key": srckey, "VersionId":srcversion}
      self._task_manager.add(method, quota_type=quota_type,
               rsize=srcsize, srctag=srctag,
               read_after_copy=self._kwargs["read_after_copy"],
               CopySource=copysrc, Key=keyname,Bucket=bucket)
      count += 1

  def nfsoverwrite(self, **kwargs):
    del(kwargs)
    INFO("Starting NFSOVERWRITE workload with Prefix : %s"
         %(self._total_ops_recorded["workload_to_prefix_map"]["nfsoverwrite"]))
    while not self._exit_test("NFS Overwrite Workload"):
      if self._pause_workload("nfsoverwrite", 10):
        continue
      prefix = choice(self._total_ops_recorded["workload_to_prefix_map"][
                      "nfsoverwrite"])
      srcbucket, srckey, srcsize, srctag, srcversion = \
             self._objname_distributor.get_object_from_bucket(prefix,
                                    nfs_workload=True, workload="nfsoverwrite")
      self._task_manager.add(self._nfsops.nfsoverwrite,
               quota_type="nfsoverwrite", bucket=srcbucket, objname=srckey,
               versionid=srcversion, rsize=self._kwargs["size_of_over_write"],
               srctag=srctag,
               num_overwrites=self._kwargs["num_over_writes_per_file"],
               size_of_write=self._kwargs["size_of_over_write"],
               block_size=self._kwargs["over_write_block_size"],
               read_after_nfsoverwrite=self._kwargs["read_after_nfsoverwrite"])

  def nfsdelete(self, **kwargs):
    del(kwargs) #Save memory
    self._total_ops_recorded["misc"].setdefault("nfsdelete", {"delay" : 0})
    self._delete_empty_bucket = False
    self._bucket_info["nfs_buckets"] = {"delete_marker_map": {
                                                  "num_files_deleted":0,
                                                  "total_size":0,
                                                  "total_time":0 },
                                        "in_use_nfs_buckets":[]}
    for bucket in self._nfsops._registered_buckets:
      self._bucket_info["nfs_buckets"]["delete_marker_map"][bucket] = {
                                        "vmarkers":[("", "")], "markers":[""]}
    stime = time()
    INFO("Starting NFSDELETE workload with prefix : %s"
         %self._total_ops_recorded["workload_to_prefix_map"]["nfsdelete"])
    while not self._exit_test("NFS Delete"):
      if not self._nfsops._registered_buckets:
        self._set_exit_marker("No bucket found for nfsdelete workload")
        continue
      if self._total_ops_recorded["vars"]["auto_deletes_enabled"]:
        if not self._is_delete_workload_running:
          prefix = choice(self._bucket_info["prefixes"]["prefixes"])
        else:
          #Just pause deletes if autodelete is enabled n delete workload running
          sleep(10)
          continue
      else:
        prefix = choice(self._total_ops_recorded["workload_to_prefix_map"][
                        "nfsdelete"])
      bucket = self._get_random_nfs_bucket(
                                      self._kwargs["unique_bucket_for_delete"])
      if not bucket:
        sleep(1)
        continue
      self._task_manager.add(self._list_and_nfsdelete, prefix=prefix,
          quota_type="nfsdelete", bucket=bucket,
          skip_head_before_delete=self._kwargs["skip_head_before_delete"],
          skip_head_after_delete=self._kwargs["skip_head_after_delete"],
          skip_read_before_delete=self._kwargs["skip_read_before_delete"])

  def _list_and_nfsdelete(self, bucket, prefix, skip_head_before_delete,
                          skip_head_after_delete, skip_read_before_delete):
    bucket_info = self._bucket_info["nfs_buckets"]["delete_marker_map"][bucket]
    res, marker, istrunc = self._list_objects(bucket=bucket, prefix=prefix,
                             versions=False, marker=bucket_info["markers"][-1])
    self._bucket_info["nfs_buckets"]["delete_marker_map"][
                                                  bucket]["markers"] = [marker]
    if not istrunc:
      self._bucket_info["nfs_buckets"]["delete_marker_map"][
                                                      bucket]["markers"] = [""]
    self._bucket_info["nfs_buckets"]["in_use_nfs_buckets"].remove(bucket)
    if not res.get("Contents"):
      self._total_ops_recorded["misc"]["nfsdelete"]["delay"] += 1
      if self._total_ops_recorded["misc"]["nfsdelete"]["delay"]%30 == 0:
        ERROR("No files (Prefix : %s) found in %s to delete"%(prefix, bucket))
      sleep(1)
      return
    self._total_ops_recorded["misc"]["nfsdelete"]["delay"] = 0
    files_to_delete = []
    self._nfsops.nfsdelete(bucket, res["Contents"], skip_head_before_delete,
                           skip_head_after_delete, skip_read_before_delete)

  def patch_boto_schema_for_endpoint(self):
    """
    Method to update the botocore services.json with new member, "Endpoint"
    in case of Tiering. OSS supports a new parameter "Endpoint" for object
    tiering. If this parameter is not updated in the services.json
    file then the put_bucket_lifecycle_configuration api fails, whenever
    'Endpoint' argument is used.
    """
    from botocore import BOTOCORE_ROOT
    BOTO_SERVICES_FILE = "data/s3/2006-03-01/service-2.json"
    BOTO_ENDPOINT = {"Endpoint":{"type":"string"}}
    BOTO_TRANSITION_MEMBERS = {
        u'Endpoint': {
        u'shape': u'Endpoint',
        u'documentation': u'<p>The S3 storage used to store the tiered object.\
        </p>'}}
    INFO("Updating the boto schema with Endpoint")
    DEBUG("Botocore root directory: {}".format(BOTOCORE_ROOT))
    botocore_services_file = path.join(BOTOCORE_ROOT, BOTO_SERVICES_FILE)
    with open(botocore_services_file) as data:
      s3_services = load(data)
    if 'Endpoint' in s3_services['shapes'] and 'Endpoint' in s3_services[
                                             'shapes']['Transition']['members']:
      INFO("Boto3 %s file is already updated with Endpoint member. Skipping "
           "further update."%(BOTO_SERVICES_FILE))
      return
    s3_services['shapes'].update(BOTO_ENDPOINT)
    s3_services['shapes']['Transition']['members'].update(
      BOTO_TRANSITION_MEMBERS)
    with open(botocore_services_file, 'w') as fp:
      fp.writelines(dumps(s3_services, indent=2, sort_keys=True, default=str))

  def range_read_object(self, **kwargs):
    skip_integrity_check = kwargs.pop("skip_integrity_check")
    #+1 to the range-read-size since byterange counts first offset.
    #E.g - 0-1023 = 1024bytes (i.e 0 is counted here)
    range_read_size = kwargs.pop("range_read_size")+1
    objsize = kwargs.pop("objsize")
    start_offset, end_offset = kwargs["Range"].split("=")[1].split("-")
    size_of_range_read = int(end_offset.strip())-int(start_offset.strip())
    res = self._get_object(objsize=size_of_range_read, **kwargs)
    if not res:
      return
    oc = res.get("ResponseMetadata", {}).get("HTTPHeaders",{}).get("x-ntnx-id")
    stream = res["Body"]
    data_size = 0
    #If range size is greater than 1M then integrity check will start failing,
    #since we are passing 1M data to check integrity of larger size as given
    #in Range variable. So pass Range=bytes value per 1M chunk and keep
    #incrementing start_offset until we read entire data
    read_size = 1024 * 1024
    while True:
      tmp = ""
      try:
        tmp = stream.read(read_size)
      except Exception as err:
        ERROR("Bucket / Obj : %s / %s, Error : %s, Oc : %s"
              %(kwargs.get("Bucket"), kwargs.get("Key"), err.message, oc))
        raise
      if not tmp:
        stream.read(read_size)
        break
      elif skip_integrity_check:
        continue
      end_offset = int(start_offset)+len(tmp)
      self._range_read_integrity_check(bucket=kwargs["Bucket"],
                      objname=kwargs["Key"], data=tmp,
                      offset_range="bytes=%s-%s"%(start_offset, end_offset),
                      etag=res["ETag"], object_offset=data_size)
      data_size += len(tmp)
      start_offset = end_offset
    #ContentLength always returns size of the data returned. At this stage we
    #dont read any data beyond the objsize , so its safe to have to this check.
    if range_read_size != data_size != res["ContentLength"]:
      raise Exception("Bucket/Key : %s / %s, Expected data size vs read data "
                      "size : %s vs %s, Range : %s, objsize  :%s, Oc : %s"
                      %(kwargs["Bucket"], kwargs["Key"], range_read_size,
                      data_size, kwargs["Range"], objsize, oc))
    kwargs["rsize"] = range_read_size
    #self._head_object(**kwargs)
    return res

  def read(self, **kwargs):
    del(kwargs)
    INFO("Starting READ workload with prefix : %s"
         %(self._total_ops_recorded["workload_to_prefix_map"]["read"]))
    while not self._exit_test("Read Workload"):
      if self._pause_workload("read", 10):
        continue
      try:
        self._task_manager.add(self._list_and_read, quota_type="read",
          bucket=choice(self._bucket_info["bucket_names"]),
          objprefix=choice(self._total_ops_recorded["workload_to_prefix_map"][
            "read"]), num_reads_from_list=self._kwargs["num_reads_from_list"],
          skip_integrity_check=self._kwargs["skip_integrity_check"],
          skip_zero_file_nfs_read=None, nfs_read_type=None,
          num_nfs_range_reads_per_file=None, nfs_range_read_size=None)
      except Exception as err:
        if "Timeout while adding task" in err or \
              "Timeout while adding task" in err.message:
          continue

  def _workload_markers(self, **kwargs):
    self._set_workload_markers(**kwargs)
    self._prepare_common_objprefixes()
    self._nfs_workload_running = False
    if (self._is_nfsread_workload or self._is_nfslist_workload or
       self._is_nfscopy_workload or self._is_nfsrename_workload or
       self._is_nfsdelete_workload or self._is_nfswrite_workload or
       self._is_nfs_overwrite_workload or self._is_nfsfio_workload or
       self._is_nfsvdbench_workload):
      self._nfs_workload_running = True
    INFO("Workload : %s, Delete : %s, Copy : %s, Write : %s, NfsRead : %s, Nfs"
         "List : %s"%(self._workload.keys(), self._is_delete_workload_running,
         self._is_copy_workload, self._is_write_workload,
         self._is_nfsread_workload, self._is_nfslist_workload))

  def _set_workload_markers(self, **kwargs):
    self._is_read_workload_running =  "read" in self._workload.keys() or kwargs[
                                                    "is_read_workload_running"]
    self._is_delete_workload_running =  "delete" in self._workload.keys(
                                       ) or kwargs["is_delete_workload_running"]
    self._is_copy_workload =  "copy" in self._workload.keys() or kwargs[
                                                    "is_copy_workload_running"]
    self._is_write_workload =  "write" in self._workload.keys() or kwargs[
                                                    "is_write_workload_running"]
    self._is_nfsread_workload =  "nfsread" in self._workload.keys() or kwargs[
                                                  "is_nfsread_workload_running"]
    self._is_nfslist_workload =  "nfslist" in self._workload.keys() or kwargs[
                                                  "is_nfslist_workload_running"]
    self._is_nfscopy_workload =  "nfscopy" in self._workload.keys() or kwargs[
                                                  "is_nfscopy_workload_running"]
    self._is_nfsrename_workload =  "nfsrename" in self._workload.keys(
                                    ) or kwargs["is_nfsrename_workload_running"]
    self._is_nfsdelete_workload =  "nfsdelete" in self._workload.keys(
                                    ) or kwargs["is_nfsdelete_workload_running"]
    self._is_nfswrite_workload =  "nfswrite" in self._workload.keys() or kwargs[
                                                "is_nfswrite_workload_running"]
    self._is_nfs_overwrite_workload =  "nfsoverwrite" in self._workload.keys(
                              ) or kwargs["is_nfs_overwrite_workload_running"]
    self._is_nfsfio_workload =  "nfsfio" in self._workload.keys(
                                      ) or kwargs["is_nfsfio_workload_running"]
    self._is_nfsvdbench_workload =  "nfsvdbench" in self._workload.keys(
                                  ) or kwargs["is_nfsvdbench_workload_running"]
    self._is_lookup_workload =  "lookup" in self._workload.keys(
                                  ) or kwargs["is_lookup_workload_running"]

  def _configure_monitor_workload(self, **kwargs):
    monitor_workloads = []
    if kwargs["inject_errors"]:
      monitor_workloads.append("inject_errors")
    if kwargs["initiate_periodic_compaction"]:
      monitor_workloads.append("compact_ms")
    if kwargs["capture_tcpdump"]:
      monitor_workloads.append("capture_tcpdump")
    monitor_workloads.append("log_stats")
    if kwargs["stargate_storage_gflag"] < 95:
      monitor_workloads.append("simulate_storage_full")
    if kwargs["pc_upgrade_build_url"] or kwargs["pe_upgrade_build_url"] or \
                kwargs["objects_upgrade_version"] or \
                kwargs["objects_unreleased_version"]:
      monitor_workloads.append("upgrade")
    if kwargs["start_webserver_logging"]:
      monitor_workloads.append("start_webserver_logging")
    return monitor_workloads

  def start_webserver_logging(self, **kwargs):
    port = kwargs.pop("webserver_logging_port")
    del(kwargs)
    while not self._exit_test("Start Webserver Logging"):
      self._webserver = LoadWebserver(self, port=port)
      try:
        self._webserver.run()
      except Exception as err:
        ERROR("Hit exception while running webserver. Err : %s"%err)
      try:
        self._webserver.close()
      except Exception as err:
        print "Failed to close webserver. Err : %s"%err
      sleep(10)

  def start_workload(self):
    self._total_ops_recorded["vars"]["change_workload_detected"] = True
    self._signal_handler_registered = False
    self._num_failures_to_ignore = 0
    kwargs = self._kwargs.copy()
    pcip, pcuser, pcpass, objects_name = kwargs.pop("pcip"), \
          kwargs.pop("pcuser"), kwargs.pop("pcpass"), kwargs.pop("objects_name")
    peip, peuser, pepass,  = kwargs.pop("peip"), kwargs.pop("peuser", "admin"),\
                              kwargs.pop("pepass", "Nutanix.123")
    self._objsizes = kwargs.pop("objsize")
    parallel_threads = kwargs.pop("parallel_threads")
    self._storage_limit = kwargs.pop("storage_limit")
    self._prepare_workload_profiles(kwargs.pop("workload"))
    #shuffle(self._workload_profiles)
    select_random_workload = kwargs.pop("select_random_workload")
    self._nfsops = NFSOps(self, self._endpoints,
                          self._kwargs["enable_s3fs_mount"])
    while self._total_ops_recorded["vars"]["change_workload_detected"]:
      stime = datetime.now()
      workload, kwargs["objsize"] = self._get_workload_profile(
                                                        select_random_workload)
      INFO("Workload to run : %s (objsize : %s)"%(workload, kwargs["objsize"]))
      rec = {"workload":workload, "start_time":stime,
             "objsize":kwargs["objsize"], "end_time":"InProgress",
             "time": "InProgress", "ip":self._ip, "iteration":self._iter,
             "mem_usage":self._utils.get_mem_usage(self._pid),
             "status":Constants.TEST_RUNNING}
      self._total_ops_recorded["workload_profiles"].append(rec)
      self._init_infra(pcip, pcuser, pcpass, objects_name, peip, peuser, pepass)
      monitor_workloads = self._prepare_workload(workload,
                                                 parallel_threads, **kwargs)
      if not self._objectscluster:
        self._prepare_objects_cluster(pcip, pcuser, pcpass, objects_name,
                              peip=peip, peuser=peuser, pepass=pepass, **kwargs)
      try:
        self._start_and_monitor(self._workload.keys(), monitor_workloads,
                                **kwargs)
      except Exception as err:
        DEBUG("Exiting test. Error : %s, Logfile : %s"%(err, self.logfile))
        self._stop_workload("Hit road block")
        self._dump_final_stats()
        rec.update({"end_time":str(datetime.now()),"time":datetime.now()-stime,
                    "status":Constants.TEST_FAILED})
        raise
      etime = datetime.now()
      rec.update({"end_time":str(etime),"time":etime-stime,
                  "status":Constants.TEST_COMPLETED})
      self._total_ops_recorded["workload_profiles"].pop() #Remove latest rec
      self._total_ops_recorded["workload_profiles"].append(rec) #Append res
      if self._total_ops_recorded["vars"]["change_workload_detected"]:
        reset_flag = False
        if not self._time_to_exit:
          WARN("Setting EXIT martker in start_workload due to "
               "change_workload_detected . Will reset it after stopping wl")
          self._total_ops_recorded["vars"]["time_to_exit"] = True
          reset_flag=True
        self._stop_workload("Workload change detected", close_error_fh=False)
        if reset_flag:
          WARN("Resetting EXIT martker to False in start_workload due to "
               "change_workload_detected .")
          self._total_ops_recorded["vars"]["time_to_exit"] = False
      self._init_bucketinfo_db(archive=True)
    self._stop_workload("Hit road block")
    DEBUG("Exiting test : %s"%self.logfile)
    self._dump_final_stats()

  def _prepare_workload_profiles(self, workload):
    self._workload_profiles = workload
    if isinstance(workload, str):
      self._workload_profiles = workload.split("|")

  def _prepare_workload(self, workloads, parallel_threads, **kwargs):
    """Start given workload.
    """
    self._total_ops_recorded["vars"]["time_to_exit"] = False
    self._total_ops_recorded["vars"]["change_workload_detected"] = False
    if self._thrd_monitor_interval > 0:
      start_monitoring(seconds_frozen = self._thrd_monitor_interval if
                       self._thrd_monitor_interval > 0 else 60 )
    self._workload = {}
    wperc = 100/len(workloads)
    for workload in workloads:
      wload = workload.split(":")
      if len(wload) == 1:
        self._workload[wload[0]] = wperc
        continue
      self._workload[wload[0]] = int(wload[1])
    quota = {}
    for workload_type, workload_per in self._workload.iteritems():
      quota[workload_type] = {"perc":workload_per}
    self._exit_marker = self._create_exit_marker()
    self._workload_markers(**kwargs)
    monitor_workloads = self._configure_monitor_workload(**kwargs)
    if "deploy" in self._workload.keys():
      parallel_threads = 1
    self._bucket_info["bucket_properties"] = {"versioned_buckets":0,
                                          "standard_buckets":0,
                                          "worm_buckets":0,
                                          "lifecycle_config_enabled_buckets":0,
                                          "replication_buckets":0,
                                          "total_buckets":0,
                                          "nfs_buckets":0}
    if not("deploy" in self._workload.keys() and len(self._workload.keys())==1):
      quota["monitor"] = {"num_workers":len(monitor_workloads)}
      self._prepare_stage(parallel_threads=parallel_threads, **kwargs)
      for bucketname in self._bucket_info["bucket_names"]:
        self._get_bucket_properties(bucketname)
        self._update_bucket_properties(bucketname,self._bucket_info[bucketname])
      self._prepare_setup_for_write_workload()
    else:
      monitor_workloads = []
    quota["cntr"] = {"num_workers":len(self._workload.keys())}
    self._task_manager = DynamicExecution(parallel_threads, quotas=quota)
    self._total_ops_recorded["TestStatus"]["live_workloads"
                                                            ] = self._workload
    return monitor_workloads

  def _get_bucket_properties(self, bucketname):
    versioning = self._execute(self._s3ops.s3_get_bucket_versioning,
                Bucket=bucketname).get("Status") in ["Suspended", "Enabled"]
    res = self._execute(self._s3ops.s3_get_object_lock_configuration,
        Bucket=bucketname)
    worm = res.get("ObjectLockConfiguration", {}).get("ObjectLockEnabled")
    days = res.get("ObjectLockConfiguration", {}).get("Rule",
                   {}).get("DefaultRetention", {}).get("Days")
    mode = res.get("ObjectLockConfiguration", {}).get("Rule",
                   {}).get("DefaultRetention", {}).get("Mode")
    repl = self._execute(self._s3ops.s3_get_bucket_replication,
                         Bucket=bucketname,
                         ignore_errors=["NoSuchReplicationConfiguration"])
    if repl and "ReplicationConfiguration" in repl:
      repl = repl["ReplicationConfiguration"]["Rules"][0]["Status"]
    else:
      repl = False
    res = self._execute(self._s3ops.s3_get_bucket_lifecycle_configuration,
                        Bucket=bucketname,
                        ignore_errors=["NoSuchLifecycleConfiguration"])
    rules = []
    for rule in res.get("Rules", []):
      if rule.get("Status") != "Enabled" and rule.get("Status") != "Disabled":
        continue
      prefix = rule.get("Filter", {}).get("Prefix")
      tags = []
      if "And" in rule.get("Filter", {}):
        tags = rule["Filter"]["And"].get("Tags")
        prefix = rule["Filter"]["And"].get("Prefix")
      rules.append({"prefix":prefix,
                    "tags":tags,
                    "status":rule.get("Status"),
                    "parts":rule.get("AbortIncompleteMultipartUpload",
                                     {}).get("DaysAfterInitiation"),
                    "expiry":rule.get("Expiration", {}).get("Days"),
                    "versions":rule.get("NoncurrentVersionExpiration",
                                        {}).get("NoncurrentDays")})

    self._bucket_info[bucketname] = {"versioning" : versioning,
                                     "worm": {"mode":mode,
                                              "status":worm,
                                              "days":days},
                                      "expiry":rules,
                                      "replication":repl,
                              "nfs":self._is_nfs_enabled_on_bucket(bucketname)}

  def _update_bucket_properties(self, bucket, bucket_property):
    no_properties = True
    if bucket_property["versioning"]:
      no_properties = False
      self._bucket_info["bucket_properties"]["versioned_buckets"] += 1
    if bucket_property["worm"]["status"] != "Disabled":
      no_properties = False
      self._bucket_info["bucket_properties"]["worm_buckets"] += 1
    if bucket_property["replication"]:
      no_properties = False
      self._bucket_info["bucket_properties"]["replication_buckets"] += 1
    if len(bucket_property["expiry"]) > 0:
      no_properties = False
      self._bucket_info["bucket_properties"][
                                    "lifecycle_config_enabled_buckets"] += 1
    if bucket_property["nfs"]:
      no_properties = False
      self._bucket_info["bucket_properties"]["nfs_buckets"] += 1
    self._bucket_info["bucket_properties"][
                            "live_multipart"] = len(self._multipart_objects_map)
    self._bucket_info["bucket_properties"]["total_buckets"] += 1
    if no_properties:
      self._bucket_info["bucket_properties"]["standard_buckets"] += 1

  def simulate_storage_full(self, stargate_test_initial_delay=900,  **kwargs):
    etime = time()+stargate_test_initial_delay
    while time() < etime:
      if self._exit_test("Simulate Storage Full"):
        return
      sleep(30)
    self._total_ops_recorded["stargate_storage_gflag_change"] = {"count" : 0,
                                  "Last.Recorded.Time":datetime.now()}
    gflag_value = kwargs.pop("stargate_storage_gflag")
    interval = kwargs.pop("stargate_gflag_switch_interval")
    delay = kwargs.pop("storage_test_interval")
    del(kwargs)
    while not self._exit_test("Simulate Storage Full"):
      if self._upgrade_in_progress:
        INFO("Skipping stargate storage test since upgrade in progress. "
             "Retrying after 30secs")
        sleep(30)
        continue
      self._objectscluster.reset_stargate_storage_gflag(interval, gflag_value)
      self._total_ops_recorded["stargate_storage_gflag_change"]["count"] += 1
      self._total_ops_recorded["stargate_storage_gflag_change"]\
        ["Last.Recorded.Time"] = datetime.now() - self._test_start_time
      sleep(delay)

  def simulate_egroup_data_corruption(self, egroup_corruption_initial_delay=900,
                                      **kwargs):
    etime = time()+egroup_corruption_initial_delay
    while  time() < etime:
      if self._exit_test("Simulate Egroup Data Corruption"):
        return
      sleep(30)

    num_egroups_to_delete = kwargs["num_egroups_to_delete"]
    wait_for_data_recovery = kwargs["wait_for_data_recovery"]
    data_corruption_interval = kwargs["data_corruption_interval"]
    cvmips = self._objectscluster.get_cvmips()
    self._total_ops_recorded["num_egroups_deleted"] = {
                                  "hosts":{i:{"count"} for i in cvmips},
                                  "Last.Recorded.Time":datetime.now()}
    while not self._exit_test("Simulate Egroup Data Corruption"):
      host, num_egroups = self._objectscluster.delete_egroups(
                                  num_egroups_to_delete, wait_for_data_recovery)
      self._total_ops_recorded["num_egroups_deleted"]["count"] += num_egroups
      self._total_ops_recorded["num_egroups_deleted"][host]["count"
                                                            ] += num_egroups
      sleep(data_corruption_interval)

  def stop(self, reason="Stopped from REST call"):
    self._stop_workload(reason)
    return True

  def lookup(self, **kwargs):
    """Payload which invokes lookup on random bucket. It lists random bucket
    and invokes object or versioned lookups on every object.
    """
    del(kwargs)
    while not self._exit_test("Lookup Workload"):
      self._task_manager.add(self._list_and_lookup, quota_type="lookup")

  def _list_and_lookup(self):
    """List bucket and invoke lookup on every object found in response.
    """
    res = {}
    bucket, markers = self._get_random_bucket_for_lookup()
    if "vmarker" in markers:
      res, vmarker, kmarker, istrunc = self._list_objects(bucket=bucket,
        versions=True, kmarker=markers["marker"], vmarker=markers["vmarker"])
      if not istrunc:
        vmarker, kmarker = "", ""
      with self._lock:
        self._bucket_info["bucket_map"][bucket]["lookup"]["markers"].update(
          {"version_marker":{"marker":kmarker, "vmarker":vmarker}})
    else:
      res, marker, istrunc = self._list_objects(bucket=bucket, versions=False,
        marker=markers["marker"])
      if not istrunc:
        marker = ""
      with self._lock:
        self._bucket_info["bucket_map"][bucket]["lookup"]["markers"].update(
          {"object_marker":{"marker":marker}})
    self._lookup_all_objects(bucket, res)

  def _lookup_all_objects(self, bucket, objects):
    """Lookup all objects or versions from list
    """
    objs = []
    if "Versions" in objects:
      objs = objects["Versions"]
    elif "Contents" in objects:
      objs = objects.get("Contents", [])
    objects_to_lookup = []
    negative_loopups = 0
    for obj in objs:
      params = {"Bucket":bucket, "Key":obj["Key"],
                "set_exit_marker_on_failure":False}
      if "VersionId" in obj: params.update({"VersionId":obj["VersionId"]})
      objects_to_lookup.append(params)
      objects_to_lookup.append(params)
      objects_to_lookup[-1]["Key"] += "-_-_-_-_-INVALIDKEY"
      negative_loopups += 1
    for obj in objects_to_lookup:
      try:
        self._execute(self._s3ops.s3_lookup, **obj)
      except Exception as err:
        #debug_print("Error while lookup. Err : %s, (%s)"%(err, obj))
        pass
    if negative_loopups > 0:
      cnum = self._total_ops_recorded["s3_lookup"].get("negative_lookups", 0)
      with self._lock:
        self._total_ops_recorded["s3_lookup"]["negative_lookups"] = cnum + negative_loopups

  def _get_random_bucket_for_lookup(self):
    """Get Random bucket and fetch markers from map to proceed.
    Return bucket and its markers, based on random of versioned or regular list
    """
    bucket = choice(self._bucket_info["bucket_names"])
    markers = self._bucket_info["bucket_map"][bucket]["lookup"]["markers"]
    return bucket, markers[choice(markers.keys())]

  def write(self, objsize, **kwargs):
    del(kwargs)
    self._num_objects = self._kwargs["num_objects"]
    self._object_put_count = 0
    stime = time()
    extra_headers = {}
    #For NFS bucket, no other feature is supported, so disable tagging
    if self._kwargs["enable_tagging"]:
      extra_headers["x-amz-tagging"] = Constants.DEFAULT_TAG_FOR_EXPIRY
    INFO("Starting WRITE workload with static_data  :%s, Prefixes : %s"
         %(self._static_data, self._bucket_info["prefixes"]["prefixes"]))
    while not self._exit_test("Write Workload"):
      if self._pause_workload("write", 10):
        continue
      self._task_manager.add(self._put_object, quota_type="write",
                     objnum=self._object_put_count, extra_headers=extra_headers)
      self._object_put_count += 1

  def _prepare_setup_for_write_workload(self):
    self._upload_types = self._kwargs["upload_type"].split(",")
    self._objnames = None
    if self._static_data:
      if self._kwargs["objsize"][-1] < ((1024*1024*20)+1):
        self._generate_static_data(self._kwargs["objsize"][-1])
      else:
        self._static_data = False
        WARN("Turning self._static_data OFF, since objsize is bigger than 1mb")
    self._delimeter_prefix = ""
    self._num_leaf_objects = choice(self._kwargs["num_leaf_objects"])
    #self._prepare_common_objprefixes()

  def _prepare_common_objprefixes(self):
    upref = ""
    if self._kwargs.get("distributed_clients"):
      upref = self._uniq_obj_idn
    delete_prefix = "%s%s"%(Constants.DELETE_WL_DEFAULT_PREFIX, upref)
    nfs_delete_prefix = "%s%s"%(Constants.NFSDELETE_WL_DEFAULT_PREFIX, upref)
    nfs_overwrite = "%s%s"%(Constants.NFSOWRITE_WL_DEFAULT_PREFIX, upref)
    nfs_rename = "%s%s"%(Constants.NFSRENAME_WL_DEFAULT_PREFIX, upref)
    nfs_fio = "%s%s"%(Constants.NFSFIO_WL_DEFAULT_PREFIX, upref)
    nfs_vdbench = "%s%s"%(Constants.NFSVDBENCH_WL_DEFAULT_PREFIX, upref)
    read_prefixes = [self._kwargs["objprefix"]]
    self._total_ops_recorded["workload_to_prefix_map"] = {
                                         "delete":[delete_prefix],
                                         "nfsdelete":[nfs_delete_prefix],
                                         "read":[self._kwargs["objprefix"]],
                                         "nfsread":[self._kwargs["objprefix"]],
                                         "nfsoverwrite":[nfs_overwrite],
                                         "nfsrename":[nfs_rename],
                                         "prefixes_to_ignore":[],
                                         "nfsfio":[nfs_fio],
                                         "nfsvdbench":[nfs_vdbench]}
    self._bucket_info["prefixes"]["prefixes"] = [self._kwargs["objprefix"],
                                                 delete_prefix]
    if self._is_delete_workload_running:
      self._total_ops_recorded["workload_to_prefix_map"]["prefixes_to_ignore"
                                    ].append(delete_prefix)

    if self._is_nfsfio_workload:
      self._total_ops_recorded["workload_to_prefix_map"]["prefixes_to_ignore"
                                ].append(nfs_fio)

    if self._is_nfsvdbench_workload:
      self._total_ops_recorded["workload_to_prefix_map"]["prefixes_to_ignore"
                                ].append(nfs_vdbench)
    if self._is_nfsdelete_workload:
      self._bucket_info["prefixes"]["prefixes"].append(nfs_delete_prefix)
      self._total_ops_recorded["workload_to_prefix_map"]["prefixes_to_ignore"
                                ].append(nfs_delete_prefix)

    if self._is_nfswrite_workload:
      read_prefixes.append(Constants.SPARSE_FILE_DEFAULT_PREFIX)
      read_prefixes.append(Constants.NFSWRITE_WL_DEFAULT_PREFIX)
      self._bucket_info["prefixes"]["prefixes"].append(
                                          Constants.NFSWRITE_WL_DEFAULT_PREFIX)

    if self._is_nfscopy_workload:
      read_prefixes.append(Constants.NFSCOPY_WL_DEFAULT_PREFIX)
      self._bucket_info["prefixes"]["prefixes"].append(
                                            Constants.NFSCOPY_WL_DEFAULT_PREFIX)

    if self._is_nfsrename_workload:
      self._bucket_info["prefixes"]["prefixes"].append(nfs_rename)
      self._total_ops_recorded["workload_to_prefix_map"]["prefixes_to_ignore"
                                 ].append(nfs_rename)

    if self._is_nfs_overwrite_workload:
      self._bucket_info["prefixes"]["prefixes"].append(nfs_overwrite)
      self._total_ops_recorded["workload_to_prefix_map"]["prefixes_to_ignore"
                                 ].append(nfs_overwrite)

    if self._kwargs["set_expiry"]:
      if self._kwargs["expiry_prefix"]:
        read_prefixes.append(self._kwargs["expiry_prefix"])
        self._bucket_info["prefixes"]["prefixes"].append(
                                              self._kwargs["expiry_prefix"])
      if self._kwargs["enable_tagging"] and self._kwargs["tag_expiry_prefix"]:
        self._bucket_info["prefixes"]["prefixes"].append(
                                            self._kwargs["tag_expiry_prefix"])
        read_prefixes.append(self._kwargs["tag_expiry_prefix"])
      if self._kwargs["enable_tiering"] and \
                  self._kwargs["tiering_endpoint"] and \
                  self._kwargs["tiering_prefix"] != "":
         self._bucket_info["prefixes"]["prefixes"] += self._kwargs[
                                                  "tiering_prefix"].split(",")
         read_prefixes += self._kwargs["tiering_prefix"].split(",")

    self._total_ops_recorded["workload_to_prefix_map"]["safe_workloads"] = [
                                        "copy", "write", "nfswrite", "nfscopy"]
    self._total_ops_recorded["workload_to_prefix_map"]["unsafe_workloads"] = [
                          "read", "nfsread", "nfsowrite", "delete", "nfsdelete"]
    self._total_ops_recorded["workload_to_prefix_map"]["read"] = read_prefixes
    self._total_ops_recorded["workload_to_prefix_map"]["nfsread"]= read_prefixes
    self._total_ops_recorded["workload_to_prefix_map"]["nfscopy"]= read_prefixes
    self._total_ops_recorded["workload_to_prefix_map"]["copy"] = read_prefixes
    INFO("Workload Prefixes : %s\nAll Prefixes : %s\nPrefixes to Ignore : %s\n"
         "Safe Workloads : %s\nPrefix To Workload Map : %s\n"
         "Prefix TestUUID : %s"
         %(self._total_ops_recorded["workload_to_prefix_map"],
           self._bucket_info["prefixes"]["prefixes"],self._total_ops_recorded[
                                "workload_to_prefix_map"]["prefixes_to_ignore"],
           self._total_ops_recorded["workload_to_prefix_map"]["safe_workloads"],
           self._total_ops_recorded["workload_to_prefix_map"],
           upref))

  def _configure_darksite_bundle(self, **kwargs):
    pass

  def upgrade(self, pe_upgrade_build_url, pc_upgrade_build_url, upgrade_timeout,
              upgrade_delay, delay_between_upgrade=900,
              read_only_workload_after_upgrade=1800, **kwargs):
    self._configure_darksite_bundle(**kwargs)

    #Invoke inventory using genesis APIs, to make sure LCM upgrades to latest
    #version so we can continue using LCM REST Interface without any prob.
    self._objectscluster.initiate_lcm_inventory()
    while upgrade_delay > 0:
      if self._exit_test("Upgrade"):
        return
      sleep(10)
      upgrade_delay -= 10
    objects_upgrade_version = kwargs["objects_upgrade_version"]
    objects_upgrade_rc_version = kwargs["objects_upgrade_rc_version"]
    objects_upgrade_script_url = kwargs["objects_upgrade_script_url"]
    wait_before_objects_upgrade = kwargs["wait_before_objects_upgrade"]
    objects_unreleased_version = kwargs["objects_unreleased_version"]
    objects_unreleased_rc_version = kwargs["objects_unreleased_rc_version"]
    upgrade_seq = kwargs["upgrade_seq"]
    self._total_ops_recorded["upgrade"] = {}
    INFO("Upgrade sequence : %s"%(upgrade_seq))
    INFO("Setup Info Before Upgrade : %s"%(self._get_setup_info()))
    for cluster_type in upgrade_seq:
      #INFO("Upgrade Entity selected : %s"%(cluster_type))
      if cluster_type == "pc" and pc_upgrade_build_url:
        INFO("Upgrading PC to %s"%(pc_upgrade_build_url))
        self._upgrade_infra(pc_upgrade_build_url, True, upgrade_timeout, 0,
                            delay_between_upgrade)
      elif cluster_type == "pe" and pe_upgrade_build_url:
        INFO("Upgrading PE to %s"%(pe_upgrade_build_url))
        self._upgrade_infra(pe_upgrade_build_url, False, upgrade_timeout,
                             read_only_workload_after_upgrade,
                             delay_between_upgrade)
      elif cluster_type == "objects_released" and objects_upgrade_version:
        INFO("Upgrading Objects to %s"%(objects_upgrade_version))
        self._upgrade_objects(wait_before_objects_upgrade,
                          objects_upgrade_version,
                          objects_upgrade_rc_version,
                          objects_upgrade_script_url,
                          read_only_workload_after_upgrade)
      elif cluster_type == "objects_unreleased" and objects_unreleased_version:
        INFO("Upgrading Objects to %s"%(objects_unreleased_version))
        self._upgrade_objects(wait_before_objects_upgrade,
                          objects_unreleased_version,
                          objects_unreleased_rc_version,
                          objects_upgrade_script_url,
                          read_only_workload_after_upgrade, False,
                          kwargs["manual_upgrade_build_url"], False)
      sleep(delay_between_upgrade)
    INFO("Setup Info After Upgrade : %s"%(self._get_setup_info()))
    while not self._exit_test("Upgrade"):
      sleep(30)

  def _upgrade_infra(self, build_url, pc_upgrade, upgrade_timeout,
                    read_only_workload_after_upgrade,
                    delay_between_upgrade):
    if not build_url:
      return
    builds = build_url if isinstance(build_url, list) else build_url.split(",")
    for build in builds:
      self._upgrade_aos_pc(build_url=build, pc_upgrade=pc_upgrade,
                           upgrade_timeout=upgrade_timeout,
             read_only_workload_after_upgrade=read_only_workload_after_upgrade)
      if len(builds) > 1:
        sleep(delay_between_upgrade)

  def _upgrade_aos_pc(self, build_url, pc_upgrade, upgrade_timeout,
                      read_only_workload_after_upgrade):
    if not build_url:
      return
    stime = time()
    pause_writes = False
    cluster_type = "PC" if pc_upgrade else "PE"
    if cluster_type == "PE":
      pause_writes = True
    INFO("Setting pause_write & upgrade_in_progress flags to True before "
         " %s upgrade"%(cluster_type))
    before = None
    after = None
    if pause_writes:
      self._total_ops_recorded["vars"]["stop_writes"] = True
      self._upgrade_in_progress = True
    try:
      before, after = self._objectscluster.upgrade_cluster(build_url,
                                  pc_upgrade, upgrade_timeout=upgrade_timeout)
    except Exception as err:
      self._set_exit_marker("Failed to upgrade %s to : %s, Error : %s"
                            %(cluster_type, build_url, err.message))
      raise
    if cluster_type.lower() not in self._total_ops_recorded["upgrade"]:
      self._total_ops_recorded["upgrade"][cluster_type.lower()] = []
    self._total_ops_recorded["upgrade"][cluster_type.lower()].append({"builds" :
                                          {"before": before,
                                          "after" : after},
                                          "time" : datetime.now(),
                                          "upgrade_time" : time()-stime})
    if self._is_read_workload_running:
      INFO("read_only_workload_after_upgrade is set to %s, sleep for %s"%(
            read_only_workload_after_upgrade, read_only_workload_after_upgrade))
      sleep(read_only_workload_after_upgrade)
    if pause_writes:
      INFO("%s Upgrade successful. Setting pause_write & upgrade_in_progress"
           "flags to False. Time Took : %s"%(cluster_type, time()-stime))
      self._upgrade_in_progress = False
      self._total_ops_recorded["vars"]["stop_writes"] = False

  def _upgrade_objects(self, wait_before_objects_upgrade, objects_version,
                rc_version, script_url, read_only_workload_after_upgrade,
                released=True, manual_upgrade_build_url=None, lcm_upgrade=True):
    if not objects_version:
      return
    release_type = "released" if released else "unreleased"
    INFO("Wait before upgrade is set to : %s, sleeping till then."
         %(wait_before_objects_upgrade))
    sleep(wait_before_objects_upgrade)
    stime = time()
    INFO("Setting pause_write & upgrade_in_progress flags to True before "
         "objects upgrade to %s version"%(release_type))
    self._total_ops_recorded["vars"]["stop_writes"] = True
    self._upgrade_in_progress = True
    before = None
    after = None
    try:
      before, after = self._objectscluster.upgrade_objects(objects_version,
                        rc_version, script_url, manual_upgrade_build_url,
                        lcm_upgrade, False, True)
    except Exception as err:
      self._set_exit_marker("Failed to upgrade Objects to %s version "
              ": %s/%s, Error : %s"%(release_type, objects_version, rc_version,
                            err.message))
      raise
    if "objects_%s"%(release_type) not in self._total_ops_recorded["upgrade"]:
      self._total_ops_recorded["upgrade"]["objects_%s"%(release_type)] = []
    self._total_ops_recorded["upgrade"]["objects_%s"%(release_type)].append({
                                        "builds" : {"before": before,
                                                    "after" : after},
                                        "time" : datetime.now(),
                                        "upgrade_time" : time()-stime})
    if self._is_read_workload_running:
      INFO("read_only_workload_after_upgrade is set to %s, sleep for %s"%(
            read_only_workload_after_upgrade, read_only_workload_after_upgrade))
      sleep(read_only_workload_after_upgrade)
    INFO("Objects upgrade successful to unreleased version. Re-Setting "
         "pause_write & upgrade_in_progress flags to False. Time Took : %s"
         %(time()-stime))
    self._upgrade_in_progress = False
    self._total_ops_recorded["vars"]["stop_writes"] = False

  def _check_live_workers(self):
    active_workers = self._task_manager.get_active_workers()
    msg = "Live workers : %s"%active_workers
    INFO(msg)
    to_exit = False
    msg = ""
    for workload, workers in active_workers.iteritems():
      if workers < 1:
        msg += "All workers for the workload \"%s\" have died."\
              "Test will exit.\n"%(workload)
        ERROR(msg)
        to_exit = True
    if to_exit:
      self._set_exit_marker(msg.rstrip(), exception=''.join(format_exc()),
                            raise_exception=False)

  def _check_src_obj_for_copy(self, copysrc, srcsize, active_objs,
                              skip_size_chk=False):
    if not self._objname_distributor.may_be_lock_unlock_objs(active_objs,
                                                              Constants.LOCK):
      return False, None
    try:
      if self._is_object_exists(**copysrc):
        srchead = self._head_object(**copysrc)
        #Possibility srcsize returned in list and head is diff. And if objsize
        #is greater than 5G then copy will most likely timeout.Hence below check
        if srchead["ContentLength"] != srcsize:
          ERROR("Size mismatch, reported in list %s vs head %s, for obj %s. "
                "Skip_size_check : %s"%(srcsize, copysrc, skip_size_chk))
          self._total_ops_recorded["anomalis"]["SizeChangedForCopy"][
                                                                  "count"] += 1
          if srchead["ContentLength"] > 5*1024*1024*1024 and not skip_size_chk:
            self._objname_distributor.may_be_lock_unlock_objs(active_objs,
                                                               Constants.UNLOCK)
            return False, srchead
        return True, srchead
      ERROR("Src Object for Copy does not exists. : %s. Skipping Copy"%copysrc)
      self._total_ops_recorded["anomalis"]["CopySrcObjNotFound"]["count"] += 1
      self._objname_distributor.may_be_lock_unlock_objs(active_objs,
                                                         Constants.UNLOCK)
    except Exception as err:
        ERROR("Hit head-object error on src Object for Copy : %s, Err : %s"
              %(copysrc, err))
        self._total_ops_recorded["anomalis"]["CopySrcObjNotFound"]["count"] += 1
        self._objname_distributor.may_be_lock_unlock_objs(active_objs,
                                                           Constants.UNLOCK)
    return False, None

  def _copy_object(self, **kwargs):
    srcsize = kwargs.get("rsize")
    copysrc = kwargs.get("CopySource")
    #active_objs = [{"bucket":kwargs["Bucket"], "objname":kwargs["Key"],
    #                "size":srcsize}, {"bucket":copysrc["Bucket"],
    #                "objname":copysrc["Key"], "size":srcsize, "objtype":"src"}]
    active_objs = [{"bucket":kwargs["Bucket"], "objname":kwargs["Key"],
                    "size":srcsize}]
    srctag = kwargs.pop("srctag").strip("\"")
    read_after_copy = kwargs.pop("read_after_copy")
    copy_obj_tag = copysrc["Key"]
    if len(copysrc["Key"]) > 256:
      tag_key = copysrc["Key"].split("/")
      copy_obj_tag = "%s_LONG-NAME-TRIMMED_%s"%(tag_key[0], tag_key[-1])

    #Tagging is not supported for NFS buckets. So disable it
    if not self._bucket_info[kwargs["Bucket"]]["nfs"]:
      expiry_tag = ""
      if kwargs["Key"].startswith(self._kwargs["tag_expiry_prefix"]):
        expiry_tag = "&%s"%Constants.DEFAULT_TAG_FOR_EXPIRY
      versionid = ""
      if copysrc.get("VersionId"):
        versionid = "&VersionId=%s"%copysrc["VersionId"]
      kwargs["Tagging"] = "Bucket=%s&Key=%s&Size=%s%s&ETag=%s%s"%(
         copysrc["Bucket"], copy_obj_tag, srcsize,versionid, srctag, expiry_tag)
      kwargs["TaggingDirective"] = "REPLACE"

    srcobj_ok, srchead, res = self._execute(self._copy_object_s3,
            copysrc=copysrc, srcsize=srcsize, active_objs=active_objs, **kwargs)
    if not res:
      return
    currentobj = {"Bucket" : kwargs["Bucket"], "Key" : kwargs["Key"],
                  "VersionId" : res.get("VersionId")}
    self._integrity_check_post_copy(active_objs, res, currentobj, copysrc,
                                    srctag, srchead, read_after_copy)

  def _integrity_check_post_copy(self, active_objs, res, currentobj, copysrc,
                                 srctag, srchead, read_after_copy):
    etag = res["CopyObjectResult"]["ETag"].strip("\"")
    if ("-" not in srctag) and etag != srctag:
      msg = "Data Corruption : Current Object : %s, [copy src : %s], Expected "\
            "md5 vs found : %s vs %s"%(currentobj, copysrc, srctag, etag)
      ERROR(msg)
      return self._set_exit_marker(msg, exception=''.join(format_exc()))
    headres = self._head_object(**currentobj)
    #Below check will fail for multipart objs.So skip it for obj with - in etag.
    if ("-" not in srchead["ETag"]) and \
                  (srchead["ETag"].strip("\"") != headres["ETag"].strip("\"")):
      msg = "ETag mismatch betw src object : %s\n-> copied object : %s\n"\
                "Expected md5 vs found : %s vs %s"\
                %(copysrc, currentobj, srchead["ETag"], headres["ETag"])
      ERROR(msg)
      self._set_exit_marker(msg, exception=''.join(format_exc()))
    if srchead["ContentLength"] != headres["ContentLength"]:
      tname = currentThread().name
      msg = "(%s) Size mismatch betw src object : %s\n-> copied object : %s\n"\
                "Expected ContentLength vs found : %s vs %s"\
                %(tname, copysrc, currentobj, srchead["ContentLength"],
                  headres["ContentLength"])
      if self._kwargs["ignore_nfs_size_errs_vs_s3"] and (
         self._is_nfs_object(currentobj["Key"], headres["ETag"].strip("\""))
          or self._is_nfs_object(copysrc["Key"], srchead["ETag"].strip("\""))):
        ERROR(msg+", Ignoring and Turning off read_after_copy, it may fail")
        read_after_copy = False
      else:
        self._total_ops_recorded["failed_workers"][tname] = msg
        self._set_exit_marker(msg, exception=''.join(format_exc()))
        raise Exception(msg)
    if read_after_copy:
      self._execute(self._read_after_copy, src_obj=copysrc, dst_obj=currentobj,
                    objsize=srchead["ContentLength"])
    self._objname_distributor.may_be_lock_unlock_objs(active_objs,
                                                       Constants.UNLOCK)

  def _copy_object_s3(self, copysrc, srcsize, active_objs, **kwargs):
    srcobj_ok, srchead = self._check_src_obj_for_copy(copysrc, srcsize,
                                                      active_objs)
    res = {}
    if not srcobj_ok:
      return None, None, res
    kwargs.update({"rsize":srcsize, "set_exit_marker_on_failure":False})
    try:
      res = self._execute(self._s3ops.s3_copy_object, **kwargs)
    except Exception as err:
      ERROR("Copy Object failed. Src Head : %s (Previous Size : %s)"
            %(srchead, srcsize))
      self._objname_distributor.may_be_lock_unlock_objs(active_objs,
                                                       Constants.UNLOCK)
      raise
    return srcobj_ok, srchead, res

  def _read_after_copy(self, src_obj, dst_obj, objsize):
    if self._is_object_expired(src_obj["Bucket"], src_obj["Key"]):
      WARN("Src %s (Dst : %s) seems to be expired. Skipping post copy integrity"
           " check"%(src_obj, dst_obj))
      return
    srcget = self._execute(self._get_object, objsize=objsize, **src_obj)
    if not srcget:
      ERROR("Src object %s, seems to be expired. Skipping read-after-copy"
            %(src_obj))
    srcdata = srcget["Body"]
    curdata = self._execute(self._get_object, objsize=objsize,**dst_obj)["Body"]
    pointer = 0
    while True:
      srctmpdata = srcdata.read(1024*1024)
      curtmpdata = curdata.read(1024*1024)
      pointer += 1
      if srctmpdata != curtmpdata:
        msg = "Data Corruption : Current Object : %s, [copy src : %s], "\
              "Data @ pointer :%s, Expected vs Found : %s vs %s"\
              %(dst_obj, src_obj, pointer*1024*1024, srctmpdata, curtmpdata)
        ERROR(msg)
        self._set_exit_marker(msg, exception=''.join(format_exc()))
      if not srctmpdata and not curtmpdata:
        break

  def _create_bucket(self, bucket, enable_versioning, enable_nfs, **kwargs):
    s3c = self._s3ops.s3obj
    cb = CreateBucket(self._s3ops.s3_get_endpoint_host(s3c), self._access,
                      self._secret, bucket, s3c, testobj=self,
                      pcobj=self._objectscluster)
    if self._is_bucket_exists(bucket):
      if not self._is_nfs_enabled_on_bucket(bucket):
        cb.enable_bucket_notification()
      return
    enable_nfs = choice([True, False]) if enable_nfs == "random" else enable_nfs
    repl = False
    if enable_versioning == "random":
      enable_versioning = choice([True, True, False])
    if not enable_nfs and kwargs["repl_cluster"]:
      repl = choice([True, False])
    if enable_nfs:
      repl = False
      enable_versioning = False
      kwargs["set_expiry"] = False
    INFO("Creating bucket : %s, NFS-Access : %s, Repl : %s, Versioning : %s, "\
         "Expiry : %s"%(bucket, enable_nfs, repl, enable_versioning,
                        kwargs["set_expiry"]))
    self._execute(self.requests_create_bucket, cb=cb, bucket=bucket,
                  enable_nfs=enable_nfs, set_expiry=kwargs["set_expiry"])
    if not self._is_nfs_enabled_on_bucket:
      cb.enable_bucket_notification()
    suspend_versioning = False
    if enable_versioning:
      suspend_versioning = choice([True, False])
      INFO("Enabling versioning on bucket : %s"%(bucket))
      self._execute(self._s3ops.s3_put_bucket_versioning, Bucket=bucket,
                    VersioningConfiguration={"Status":"Enabled"})
      if suspend_versioning:
        INFO("Suspending versioning on bucket : %s"%(bucket))
        self._execute(self._s3ops.s3_put_bucket_versioning, Bucket=bucket,
                      VersioningConfiguration={"Status":"Suspended"})
    if repl:
      self.enable_replication_relationship(bucket, enable_versioning,
                                    suspend_versioning, kwargs["repl_cluster"],
                                    kwargs["create_new_bucket_for_replication"])
    return bucket

  def requests_create_bucket(self, cb, bucket, enable_nfs, set_expiry):
    if self._is_bucket_exists(bucket):
      return
    return cb.create_bucket(enable_nfs=enable_nfs, set_expiry=set_expiry)

  def configure_notification_endpoint(self):
    if not self._kwargs["notification_endpoint"
        ] or not self._objectscluster:
      return
    config = self._kwargs["notification_endpoint"]
    self._execute(self._objectscluster.configure_notification_endpoint,
                  syslog_endpoint=config.get('syslog_endpoint'),
                  nats_endpoint=config.get('nats_endpoint'),
                  kafka_endpoint=config.get('kafka_endpoint'))

  def enable_replication_relationship(self, bucket, enable_versioning,
                          suspend_versioning,  repl_cluster, create_new_bucket):
      replication_bucket = dict(repl_cluster)
      self._create_availability_zone(replication_bucket["pcip"],
                    replication_bucket["pcuser"], replication_bucket["pcpass"])
      repl = replication_bucket["bucket"]
      replv = replication_bucket.pop("versioned_bucket")
      replvs = replication_bucket.pop("suspended_versioned_bucket")
      ip = "http://%s"%replication_bucket["endpoint_list"][0]
      repl_bucket = "%s-%s"%(bucket, int(time()))
      if enable_versioning:
        if suspend_versioning:
          replication_bucket["bucket"] = replvs
          repl_bucket = "%s-%s"%(replvs, repl_bucket)
        else:
          replication_bucket["bucket"] = replv
          repl_bucket = "%s-%s"%(replv, repl_bucket)
      else:
        repl_bucket = "%s-%s"%(repl, repl_bucket)
      if create_new_bucket:
        self._create_bucket_on_destination(ip, replication_bucket['access_key'],
                                           replication_bucket["secret_key"],
                                           repl_bucket, enable_versioning,
                                           suspend_versioning)
        replication_bucket["bucket"] = repl_bucket
      #self._objectscluster.add_replication_rule(bucket, replication_bucket)
      self._enable_bucket_replication(bucket, replication_bucket)

  def _create_availability_zone(self, remote_pcip, remote_pcuser, remote_pcpass):
    pass

  def _enable_bucket_replication(self, bucket, replication_bucket):
    rule = {
            "Role": "",
            "Rules": [{
                "Destination": {
                  "Bucket": '%s:%s'%(replication_bucket["bucket"],
                                     replication_bucket["oss_fqdn"])
                },
                "Status": "Enabled"
              }
            ]
          }
    INFO("Enabling replication for bucket : %s, Replication Cluster : %s"
         %(bucket, rule))
    self._execute(self._s3ops.s3_put_bucket_replication, Bucket=bucket,
                  ReplicationConfiguration=rule)

  def _create_bucket_on_destination(self, endpoint, access, secret, bucket,
                            enable_versioning=False, suspend_versioning=False):
    INFO("Creating dest bucket for replication : %s@%s"%(bucket, endpoint))
    s3c = connect(ip=endpoint, access=access, secret=secret)
    cb = CreateBucket(endpoint, access, secret, bucket, s3c)
    cb.create_bucket(False, False)
    if enable_versioning or suspend_versioning:
      self._execute(s3c.put_bucket_versioning, Bucket=bucket,
                    VersioningConfiguration={"Status":"Enabled"})
    if suspend_versioning:
      self._execute(s3c.put_bucket_versioning, Bucket=bucket,
                    VersioningConfiguration={"Status":"Suspended"})
    rule = {
            "Expiration": {"Days":3},
            "NoncurrentVersionExpiration":{"NoncurrentDays":3},
            "AbortIncompleteMultipartUpload":{"DaysAfterInitiation":3},
            "Status": "Enabled",
            "Filter" :{"Prefix":""}
          }
    INFO("Setting expiry rule on destination bucket : %s"%(rule))
    s3c.put_bucket_lifecycle_configuration(Bucket=bucket,
                  LifecycleConfiguration={"Rules":[rule]})

  def _create_exit_marker(self):
    filename = "/tmp/%s_exit_marker.txt"%(time())
    INFO("Exit marker to exit program : %s"%(filename))
    return filename

  def _delete_bucket(self, bucket):
    if bucket not in self._bucket_info[
                                "delete_bucket_to_marker_map"]["empty_buckets"]:
      return bucket
    if self._is_bucket_exists(bucket):
      INFO("Deleting bucket : %s"%(bucket))
      try:
        self._execute(self._s3ops.s3_delete_bucket, Bucket=bucket,
                      set_exit_marker_on_failure=False)
      except Exception as err:
        ERROR("Failed to delete bucket %s, Err : %s"%(bucket, err))
        return
    if bucket in self._bucket_info["bucket_names"]:
      self._bucket_info["bucket_names"].remove(bucket)
    if bucket in self._bucket_info[
                                "delete_bucket_to_marker_map"]["empty_buckets"]:
      self._bucket_info[
                  "delete_bucket_to_marker_map"]["empty_buckets"].remove(bucket)
    if bucket in self._bucket_info["delete_bucket_to_marker_map"]:
      WARN("Removing bucket: %s, from delete_bucket_to_marker_map"%(bucket))
      self._bucket_info["delete_bucket_to_marker_map"].pop(bucket)

  def _delete_empty_buckets(self, skip_bucket_deletion):
    if skip_bucket_deletion:
      return
    for bucketname in [bucketn for bucketn in
             self._bucket_info["delete_bucket_to_marker_map"]["empty_buckets"]]:
      self._delete_bucket(bucketname)

  def _delete_objects(self, **kwargs):
    if kwargs["skip_objects_delete"]:
      WARN("Skpping Objects delete : %s"%(self._objectscluster.objects_name))
      return
    count = 0
    errs = []
    stime = time()
    manual_msp_deleted = False
    manual_vg_cleanup = False
    service_to_restart = kwargs["services_to_restart_during_delete"]
    res = None
    while True:
      try:
        res = self._objectscluster.delete(**kwargs)
        break
      except Exception as err:
        errs.append(err.message)
        errmsg = "Failed to Delete objects,Error : %s, during num_deploy : %s,"\
                     "num_nodes : %s, Count : %s, DepStatus : %s"\
                     %(err, self._total_ops_recorded["deploy"]["num_deploy"],
                      self._total_ops_recorded["deploy"]["num_nodes"],
                      count, self._total_ops_recorded["deploy"])
        LOGEXCEPTION(errmsg)
        count +=1
        if count == 10:
          self._set_exit_marker(errmsg)
        if "submitting multicluster groups request" in err.message or \
                                 "Predeploy checks failed" in err.message:
          continue
        if "storage container" in err.message:
          if self._objectscluster.get_mspcluster_uuid(False):
            WARN("Deleting MSP cluster manually due to %s"%(err))
            self._objectscluster.destroy_msp_cluster()
            manual_msp_deleted = True
            continue
          ctrname = None
          for i in err.message.split(" "):
            if i.strip().startswith("objectsm"):
              ctrname = i.strip()
              break
          self._objectscluster.clean_up_all_vgs(ctrname)
          manual_vg_cleanup = True
          continue
        self._set_exit_marker(errmsg)
    self._total_ops_recorded["deploy"]["deployments"][-1]["delete"] = {
      "time":time()-stime, "manual_vg_cleanup":manual_vg_cleanup,
      "manual_msp_deleted":manual_msp_deleted, "retry_count":count,
      "errors":errs, "services_restarted":res,
      "service_tobe_restarted" : service_to_restart}

  def _delete_objects_from_bucket(self, bucket, objects, size, dmarkers,
                                  skipped, skip_head_after_delete,
                                  skip_read_before_delete, all_versions,
                                  skip_version_deletes):
    if not skip_read_before_delete and all_versions:
      self._read_objects(bucket, all_versions, 5,
                         [Constants.READ_TYPE_FULL_READ], False, 10,
                         False, None, [4096, 8192])
    params = {"Bucket":bucket, "Delete":{"Objects":objects}, "rsize":size,
              "rcount":len(objects)}
    if choice([True, False]):
      params["Delete"] = {"Objects":objects, "Quiet":True}
    self._execute(self._s3ops.s3_delete_objects, **params)
    if self._exit_test("Delete Objects"):
      return
    if (dmarkers > 0 or skipped > 0) and ("s3_delete_objects" in
                                          self._total_ops_recorded):
      self._total_ops_recorded["s3_delete_objects"].setdefault(
                                                          "deleted_dmarker", 0)
      self._total_ops_recorded["s3_delete_objects"].setdefault(
                                                          "deleted_skipped", 0)
      self._total_ops_recorded["s3_delete_objects"].setdefault("tombstones", 0)
      with self._lock:
        self._total_ops_recorded["s3_delete_objects"][
                                                "deleted_dmarker"] += dmarkers
        self._total_ops_recorded["s3_delete_objects"][
                                                "deleted_skipped"] += skipped
        if skip_version_deletes:
          self._total_ops_recorded["s3_delete_objects"][
                                              "tombstones"] += len(all_versions)
    self._lock_unlock_bucket_for_delete(bucket, lock=False, unlock=True)
    if skip_head_after_delete:
      return
    self._validate_deleted_objects(bucket, objects)

  def _deploy_objects(self, num_nodes, **kwargs):
    if kwargs["skip_objects_deploy"]:
      WARN("Skpping Objects deploy : %s"%(self._objectscluster.objects_name))
      self._total_ops_recorded["deploy"]["deployments"].append(
                                                        {"deploy":"Skiped"})
      return
    kwargs["num_nodes"] = randint(num_nodes[0], num_nodes[1])
    num_deploy = self._total_ops_recorded["deploy"]["num_deploy"]
    self._total_ops_recorded["deploy"]["num_nodes"] = kwargs["num_nodes"]
    INFO("Num Loop Deployment : %s, num_nodes : %s"%(num_deploy,
                                                     kwargs["num_nodes"]))
    stime = time()
    res = None
    try:
      res = self._objectscluster._deploy(**kwargs)
    except Exception as err:
      print_exc()
      fatal = "Failed to deploy objects, Error : %s, during num_deploy : %s, "\
              "num_nodes : %s, DepStatus : %s."%(err, num_deploy,
              kwargs["num_nodes"], self._total_ops_recorded["deploy"])
      self._set_exit_marker(fatal, exception=''.join(format_exc()))
    alerts = self._objectscluster.get_objects_alerts()
    self._total_ops_recorded["deploy"]["deployments"].append({"deploy":{
          "num_nodes":kwargs["num_nodes"], "time":time()-stime,
          "bind":res["bind"], "services_restarted" : \
                res["restarted_services"] if res and not res["bind"] else None,
          "services_tobe_restared":kwargs["services_to_restart_during_deploy"],
          "cluster_details":self._objectscluster._get_objects_details(),
          "alerts":alerts, "num_deploy":num_deploy}})
    if kwargs.get("skip_alerts_check_post_deployment", True):
      if alerts:
        raise Exception("Alerts found on newly deployed cluster L %s"%(alerts))
    sleep(kwargs["loop_deploy_delay"])

  def _dump_data_to_file(self, filename, data):
    fh = open(filename, "w")
    fh.write(data)
    fh.close()

  def _execute(self, method, **kwargs):
    return self._opexecutor._execute(method, **kwargs)

  def _exit_on_cntr_c(self):
    if self._signal_handler_registered:
      return
    def signal_handler(sig, frame):
      if self._total_ops_recorded["vars"]["force_kill"]:
        WARN('You pressed Ctrl+C again!, FORCE KILLING')
        self._kill_test()
      self._total_ops_recorded["vars"]["force_kill"] = True
      WARN('You pressed Ctrl+C!, exiting workload')
      self._stop_workload("Ctrl+C Pressed")
      exit("Exiting from sigint.....")
    signal(SIGINT, signal_handler)
    self._signal_handler_registered = True

  def _exit_test(self, workload, force=False, verbose=True):
    """
    it will return False if _time_to_exit and _execution_completed both are
    False else it will return True and if Force is True, it will first stop the
    currentThread and return True, irrespective of the workload

    Args:
      workload(str): name of the workload
      force(bool,Optional): if True, it will stop the thread calling this method
                            Default: False

    Returns: bool
    """
    if not self._time_to_exit and not self._execution_completed:
      return False
    if verbose:
      INFO("Workload : %s, Exit is called. Exit Marker Found : %s, Test "
           "Timed out : %s, Workload Change Detected : %s"
           %(workload.upper(), self._time_to_exit,
             self._total_ops_recorded["vars"]["test_timeout_reached"],
             self._total_ops_recorded["vars"]["change_workload_detected"]))
    if force:
      thrdname = currentThread().name
      try:
        if currentThread().isAlive():
          INFO("Force stopping worker : %s"%(thrdname))
          currentThread()._Thread__stop()
      except Exception as err:
        ERROR("Failed to stop worker %s. Error : %s"%(thrdname, err))
    return True

  def _finalize_parts(self, bucket, objname, uploadid, parts, size, force=True):
    if not parts:
      return
    res = self._execute(self._s3ops.s3_complete_multipart_upload, Bucket=bucket,
               Key=objname, UploadId=uploadid, MultipartUpload={"Parts":parts},
                         force=force)
    currentobj = {"Bucket" : bucket, "Key" : objname}
    if "VersionId" in res: currentobj["VersionId"] = res["VersionId"]
    try:
      headres = self._head_object(**currentobj)
    except Exception as err:
      ERROR("Hit error while validating head-object after successful "
            "bucket/Key : %s / %s. FinalizeObjRes : %s"%(bucket, objname, res))
      raise
    if res["ETag"].strip("\"") != headres["ETag"].strip("\""):
      msg = "%s : ETag mismatch from, finalize part and head object output"\
            "Expected md5 vs found : %s vs %s. Complete_multipart_upload_res"\
            " vs Head_Object : %s vs %s"%(currentobj,
            res["ETag"].strip("\""), headres["ETag"].strip("\""), res, headres)
      ERROR(msg)
      self._set_exit_marker(msg, exception=''.join(format_exc()))
    if headres["ContentLength"] != size:
      msg = "%s : Size mismatch from, expected and head object output"\
            "Expected size vs found : %s vs %s"%(currentobj, size,
                                                 headres["ContentLength"])
      ERROR(msg)

  def _head_object(self, Bucket, Key, VersionId=None, **kwargs):
    params = {"Bucket":Bucket, "Key":Key}
    if VersionId is not None:
      params["VersionId"] = VersionId
    params.update(kwargs)
    return self._execute(self._s3ops.s3_head_object, **params)

  def _is_sparse_object(self, objname, filename):
    if objname and objname.startswith(Constants.SPARSE_FILE_DEFAULT_PREFIX):
      return True
    if filename and Constants.SPARSE_FILE_DEFAULT_PREFIX in filename:
      return True
    return False

  def _is_object_exists(self, Bucket, Key, VersionId=None):
    res = self._head_object(Bucket, Key, VersionId,
                      ignore_errors=["Not Found", 'NoSuchVersion'])
    if not res:
      return False
    return True

  def _get_object(self, Bucket, Key, objsize, VersionId=None, **kwargs):
    params = {"Bucket":Bucket, "Key":Key, "rsize":objsize}
    if VersionId:
      params["VersionId"] = VersionId
    params.update(kwargs)
    try:
      if "Range" in kwargs:
        return self._execute(self._s3ops.s3_get_object_range, retry_count=0,
                           set_exit_marker_on_failure=False, **params)
      return self._execute(self._s3ops.s3_get_object, retry_count=0,
                           set_exit_marker_on_failure=False, **params)
    except Exception as err:
      anomalis = "skipped_expired_range_reads" if "Range" in kwargs \
                                              else "skipped_expired_reads"
      is_expired = self._is_object_expired(Bucket, Key, anomalis)
      if not is_expired:
        msg = "%s, is_expired : %s"%(err.message, is_expired)
        err.message = msg
        raise
      ERROR("%s, is_expired : %s. But still choosen for read workload. "
            "Skip Read : %s, Read Error : %s"%(params, is_expired,
            self._kwargs["skip_expired_object_read"], err))
      if not self._kwargs["skip_expired_object_read"]:
        raise

  def _generate_static_data(self, objsize):
    compressible = self._kwargs["compressible"]
    zero_data = self._kwargs["zero_data"]
    objsize = 1024*1024 if objsize < 1024*1024 else objsize
    INFO("Generating static data. Size : %s, Compressible : %s, ZeroData : %s"
         ", Static : %s"%(objsize, compressible, zero_data, self._static_data))
    self._dataobj = RandomData(objsize, compressible, zero_data,
                               self._static_data)
    while True:
      data = self._dataobj.read()
      if not data:
        break
      self._data = self._data+data
    self._data = self._data
    self._md5, _ = self._dataobj.md5sum()
    return self._data, self._md5

  def _get_all_bucket_names(self, bucket_name=None, bucket_prefix=""):
    all_buckets = bucket_name.split(",")
    if not bucket_name:
      INFO("Looking all buckets with name / prefix : %s /  %s"
           %(all_buckets, bucket_prefix.split(",")))
      all_buckets = []
      bucket_list = self._execute(self._s3ops.s3_list_buckets)["Buckets"]
      for bucket_pref in bucket_prefix.split(","):
        if not bucket_pref: continue
        abuckets = [bu["Name"] for bu in bucket_list
                    if bu["Name"].startswith(bucket_pref)]
        all_buckets = all_buckets + abuckets
    for bucketname in [bucket for bucket in all_buckets]:
      if not self._is_bucket_exists(bucketname):
        all_buckets.remove(bucketname)
    if not all_buckets:
      self._set_exit_marker("No buckets found with prefix/name : %s/%s"
                            %(bucket_prefix, bucket_name))
    INFO("Total buckets found : %s"%(all_buckets))
    return all_buckets

  def _get_data(self, objsize):
    if objsize == 0:
      return "", "d41d8cd98f00b204e9800998ecf8427e"
    if self._static_data:
      if not self._data:
        self._generate_static_data(objsize)
      if objsize < len(self._data):
        remaining_data = len(self._data) - objsize
        start_index =  randrange(0,remaining_data, 1024)
        return self._data[start_index:start_index+objsize], None
      return self._data, self._md5
    return RandomData(objsize, self._kwargs["compressible"],
                      self._kwargs["zero_data"], self._static_data), None

  def _get_expiry_obj_prefix(self, bucket):
    policy = ""
    try:
      pol = self._execute(self._s3ops.s3_get_bucket_lifecycle_configuration,
                          Bucket=bucket, set_exit_marker_on_failure=False,
                          retry_count=0)
      policy = pol["Rules"]
    except Exception as err:
      self._bucket_info["prefixes"][bucket] = "NoExp"
      return
    eprefix = ""
    for rule in policy:
      if not rule["Status"]:
        continue
      prefix=""
      if rule.get("Filter"):
        if rule["Filter"].get("And"):
          if rule["Filter"]["And"].get("Tags"):
            prefix += "_T"
      if rule.get("Expiration"):
        prefix += "E%s"%(rule["Expiration"]["Days"])
      if rule.get("NoncurrentVersionExpiration"):
        prefix += "V%s"%(rule["NoncurrentVersionExpiration"]["NoncurrentDays"])
      if rule.get("AbortIncompleteMultipartUpload"):
        prefix += "A%s"%(rule["AbortIncompleteMultipartUpload"][
                                                        "DaysAfterInitiation"])
      eprefix += prefix
    self._bucket_info["prefixes"][bucket] = eprefix

  """
  def _get_filehandle(self, mntpt, key, mode=posix.O_RDONLY):
    filename = path.join(mntpt, key)
    res =  self._execute(self._posixops.nfs_op_open, filename=filename,
                         mode=mode)
    return res, filename
  """

  def _get_files_dirs_from_s3list(self, listres):
    res = []
    for i in listres.get("Contents", []):
      res.append(i["Key"].split("/")[-1])
    for i in listres.get("CommonPrefixes", []):
      res.append(i["Prefix"][:-1].split("/")[-1])
    return res

  def _get_instance_level_stats(self):
    """Get Objcts UI stats for the bucket.
    """
    data = self._objectscluster.get_instance_stats()
    if not data:
      return 0, 0
    return int(data.get("num_objects",0)), int(data.get("usage_bytes",0))

  def _get_key(self, depth):
    if not self._objnames:
      self._objnames = self._get_key_list()
    size = randint(depth[0], depth[1])
    key=choice(self._objnames)
    while size > 0:
      key=key+"/"+choice(self._objnames)
      size = size - 1
    if key.endswith("/.."):
      key=key.replace("/..","")
    elif key.endswith("/."):
      key=key.replace("/.","")
    elif key.endswith("/./"):
      key=key.replace("/./","")
    elif key.endswith("/../"):
      key=key.replace("/../","")
    return "%s_%.f"%(key, time())

  def _get_key_list(self, size=(8,12), sizeoflist=10):
    strs = digits + letters
    safe_hex_keys = "/!_.*()"
    unsafe_hex_keys = "&$@=;:+ ,?'"
    unsupported_hex_keys = '%#'
    chars_to_avoid = '\\{^}`]>[~<|'
    if self._kwargs["use_hexdigits_keynames"]:
      strs = strs + hexdigits + safe_hex_keys
    if self._kwargs["use_unsafe_hex_keynames"]:
      strs = strs + unsafe_hex_keys + chars_to_avoid
    # % & & is not supported. And rest of signs above, have some problem
    #while boto deals with them in response.
    # - we use this in keyname n have specific logic in code. So avoid
    #using this in objname.
    strs = strs.replace("%","").replace("#","").replace("/","").\
           replace("?","").replace(" ","").replace("-","").replace("+", "").\
           replace("sparse", "dkjfhc").replace("delete", "lftdj")
    res = []
    while len(res) < sizeoflist:
      data = ''.join(sample(strs,randint(size[0], size[1])))
      if "SIG" in data:
        data = data.replace("SIG","XxX")
      res.append(data)
    return res

  def _may_be_ignore_object(self, objname):
    for i in self._total_ops_recorded["workload_to_prefix_map"][
                                                          "prefixes_to_ignore"]:
      if i in objname:
        return True
    return False

  def _get_random_nfs_bucket(self, unique=True, retry_interval=1, timeout=30):
    stime = time()
    while time()-stime < timeout:
      if not self._nfsops._registered_buckets:
        ERROR("No NFS buckets registered")
        sleep(1)
        return None
      buckets = self._nfsops._registered_buckets
      if unique:
        buckets = list(set(buckets)-set(self._bucket_info["nfs_buckets"][
                                                        "in_use_nfs_buckets"]))
      bucket = choice(buckets) if buckets else ""
      if bucket:
        if bucket not in self._bucket_info["nfs_buckets"]["in_use_nfs_buckets"]:
          with self._lock:
            if bucket not in self._bucket_info["nfs_buckets"][
                                                          "in_use_nfs_buckets"]:
              self._bucket_info["nfs_buckets"][
                                            "in_use_nfs_buckets"].append(bucket)
              return bucket
      sleep(retry_interval)

  def _get_bucket_for_delete(self, unique=True, retry_interval=1, timeout=10):
    stime = time()
    while len(self._bucket_info["bucket_names"]) > 0 and time()-stime < timeout:
      buckets = list(set(self._bucket_info["bucket_names"])-set(
            self._bucket_info["delete_bucket_to_marker_map"]["empty_buckets"]))
      if not buckets:
        ERROR("No bucket to chose from, all buckets are empty")
        if self._is_delete_workload_running and len(self._workload.keys()) == 1:
          ERROR("No bucket to chose from, Test completed")
          self._total_ops_recorded["vars"]["execution_completed"] = True
        sleep(10)
        return None
      if unique:
        buckets = list(set(buckets)-set(self._bucket_info[
                            "delete_bucket_to_marker_map"]["in_use_buckets"]))
      bucket = choice(buckets) if buckets else ""
      if bucket:
        self._lock_unlock_bucket_for_delete(bucket, lock=True, unlock=False)
        return bucket
      sleep(retry_interval)

  def _get_range_to_read(self, size, range_read_size):
    start_offset = 0
    if  size > 0:
      start_offset = randrange(0, size, 1024)
      return start_offset,  start_offset+range_read_size
    #When size is 0, then start-offset has to be greater than endoffset
    #else it throws invalid range error
    end_offset = start_offset+range_read_size
    return end_offset + 1, end_offset

  def _init_s3ops(self, endpoint, access, secret, read_timeout,
                   max_pool_connections, addressing_style, debug, force=False):
    self._s3ops = S3Ops(self, endpoint, access, secret, read_timeout,
                        max_pool_connections, addressing_style, debug, force)

  def _initiate_upload(self, bucket, key, num_part):
    res = self._execute(self._s3ops.s3_create_multipart_upload, Bucket=bucket,
                        Key=key)
    self._debug_logging("Bucket/Key : %s/%s, UploadId : %s, Num Part to upload"
                        " : %s"%(bucket, key, res["UploadId"], num_part), 3)
    return res["UploadId"]

  def _is_bucket_exists(self, bucket):
    res = self._execute(self._s3ops.s3_head_bucket,Bucket=bucket,
                     ignore_errors=["Not Found"])
    if res and res["ResponseMetadata"]["HTTPStatusCode"] < 300:
      #INFO("Bucket exists : %s"%(bucket))
      return True
    INFO("Bucket does not exists : %s"%(bucket))
    return False

  def _is_nfs_enabled_on_bucket(self, bucket):
    if bucket in self._total_ops_recorded.get("nfsops", {}).get("mounts", {}):
      return True
    res = self._execute(self._s3ops.s3_head_bucket,Bucket=bucket)
    if not res["ResponseMetadata"]["HTTPHeaders"].get(
                                                  "x-ntnx-nfs-access-enabled"):
      return False
    return res["ResponseMetadata"]["HTTPHeaders"]\
           ["x-ntnx-nfs-access-enabled"].lower() == "true"

  def _is_nfs_object(self, objname, etag=None, bucket=None, versionid=None):
    if etag :
      etag = etag.strip("\"")
      if etag.endswith(Constants.NFS_WRITE_ETAG_IDENTIFIER) or etag.endswith(
                    Constants.NFS_FILE_ETAG_IDENTIFIER) or etag.endswith(
                    Constants.NFS_OVERWRITE_ETAG_IDENTIFIER):
        return True
    if bucket:
      res = self._head_object(bucket, objname, versionid)
      if res["ETag"].strip("\"").endswith(Constants.NFS_WRITE_ETAG_IDENTIFIER):
        return True
    if Constants.NFS_OBJ_IDENTIFIER in objname:
      return True
    return False

  def _kill_test(self):
    """"
    sets the self._total_ops_recorded["TestStatus"]["status"] = "Killed" and
    kills the current process
    """
    pid=getpid()
    WARN("Killing Test PID : %s"%(pid))
    ERROR("First exception Found : %s"
          %(self._total_ops_recorded["TestStatus"].get("first_fatal")))
    INFO("Test Runtime : %s, Log file : %s"
         %(str(timedelta(seconds=time()-self._start_time)),
         self.logfile))
    self._total_ops_recorded["TestStatus"]["status"] = "Killed"
    #self._total_ops_recorded["TestStatus"]["first_fatal"] = self._fatal
    kill(pid, 9)

  def _abort_multipart_objects(self, details=None, force=False,
                               ignore_errors=True, check_for_lock=False):
    if not details and not force:
      return
    if details:
      return self._abort_multiparts(details, force=force,
                                    ignore_errors=ignore_errors)
    aborted_parts = []
    for objname in self._multipart_objects_map.keys():
      details = self._multipart_objects_map[objname]
      if check_for_lock:
        if not self._objname_distributor.may_be_lock_unlock_obj(
                  details["bucket"], objname, details["size"], Constants.LOCK):
          continue
      if self._abort_multiparts(details, force=force,
                                    ignore_errors=ignore_errors):
        aborted_parts.append(objname)
      self._objname_distributor.may_be_lock_unlock_obj(details["bucket"],
                            objname, details["size"], Constants.UNLOCK)
    return aborted_parts

  def _abort_multiparts(self, details, force, ignore_errors):
    INFO("Aborting upload %s (Force : %s, Ignore Errors : %s %s)"
         %(details, force, ignore_errors, ", Auto Deletes : True"
          if self._total_ops_recorded["vars"]["auto_deletes_enabled"] else ""))
    obj_aborted = True
    try:
      self._execute(self._s3ops.s3_abort_multipart_upload,
                    Bucket=details["bucket"], Key=details["objname"],
                    UploadId=details["uuid"], force=force)
    except Exception as err:
      obj_aborted = False
      ERROR("Failed to abort all ongoing multipart objects ("
            "Details : %s). Err  :%s"%(details, err))
      if not ignore_errors:
        raise
      return
    with self._lock:
      if details["objname"] in self._multipart_objects_map:
        self._multipart_objects_map.pop(details["objname"])
      self._total_ops_recorded["multipart"]["aborted"]["count"] += 1
      self._total_ops_recorded["multipart"]["aborted"]["size"
                                                          ] += details["size"]
      self._total_ops_recorded["multipart"]["aborted"]["parts"
                                                        ] += details["parts"]
      self._total_ops_recorded["multipart"]["totalongoing"] -= 1
    return obj_aborted

  def _list_and_delete(self, bucket, prefix):
    #bucket = kwargs["bucket"]
    #prefix = kwargs["prefix"]
    if self._total_ops_recorded["vars"]["auto_deletes_enabled"]:
      prefix = ""
    bucket_info = self._bucket_info["delete_bucket_to_marker_map"][bucket]
    res = None
    if bucket_info["versioning"] and not self._kwargs["skip_version_deletes"]:
      markers = bucket_info[prefix]
      res, vmarker, kmarker, istrunc = self._list_objects(bucket=bucket,
           versions=True, vmarker=markers["version_marker"],
           kmarker=markers["key_marker"], prefix=prefix)
      #If istrunc is false means we deleted all objects & safe to reset markers
      if not istrunc: vmarker, kmarker = "", ""
      self._bucket_info["delete_bucket_to_marker_map"][bucket][prefix].update({
        "version_marker":vmarker, "key_marker":kmarker})
    else:
      res, marker, istrunc = self._list_objects(bucket=bucket, prefix=prefix,
             versions=False, marker=bucket_info[prefix]["continuation_token"])
      if not istrunc: marker = ""
      self._bucket_info["delete_bucket_to_marker_map"][bucket][prefix].update({
        "continuation_token":marker})
    self._validate_and_delete_objects(bucket, res,
                                      self._kwargs["skip_head_before_delete"],
                                      self._kwargs["skip_head_after_delete"],
                                      self._kwargs["skip_read_before_delete"],
                                      self._kwargs["skip_version_deletes"])
    self._lock_unlock_bucket_for_delete(bucket, lock=False, unlock=True)

  def _lock_unblock_all_objs(self, bucket, objs, lock=False, unlock=False):
    res = []
    for obj in objs:
      if lock:
        if self._objname_distributor.may_be_lock_unlock_obj(bucket, obj["Key"],
                                                  obj['Size'], Constants.LOCK):
          res.append(obj)
      elif unlock:
        self._objname_distributor.may_be_lock_unlock_obj(bucket, obj["Key"],
                                          obj['Size'], Constants.UNLOCK)
    return res

  def _list_and_read(self, bucket, objprefix, num_reads_from_list,
                     skip_integrity_check, skip_zero_file_nfs_read,
                     nfs_read_type, num_nfs_range_reads_per_file,
                     nfs_range_read_size):
    maxkeys = randint(self._kwargs["maxkeys_to_list"][0],
                      self._kwargs["maxkeys_to_list"][-1])

    bucket_marker = self._bucket_info["bucket_to_marker_map"][bucket].get(
                    objprefix, {})
    version, vmarker, kmarker, istrunc =  self._list_objects(bucket=bucket,
      vmarker=bucket_marker.get("version_marker"), maxkeys=maxkeys,
      kmarker=bucket_marker.get("key_marker"), prefix=objprefix, versions=True)
    if kmarker and not kmarker.startswith(objprefix):
      ERROR("Bucket : %s, Prefix : %s, Markers : %s, KMarker %s does not start "
            "with Prefix."%(bucket, objprefix, bucket_marker, kmarker))
    self._bucket_info["bucket_to_marker_map"][bucket][objprefix] = {
                                "version_marker":vmarker, "key_marker":kmarker}
    all_versions = version.get("Versions", [])
    prefixes = version.get("CommonPrefixes", [])
    dmarkers = version.get("DeleteMarkers", [])
    if not istrunc:
      self._bucket_info["bucket_to_marker_map"][bucket][objprefix] = {}
    else:
      if all_versions and (len(all_versions) + len(prefixes) + \
                           len(dmarkers)) != maxkeys:
        fatal = "(Bucket : %s, Prefix : %s, Marker : %s, DMarker : %s) : "\
                "Num keys expected in list vs found : %s vs %s"\
                %(bucket, objprefix, bucket_marker, len(dmarkers),
                  maxkeys, len(all_versions))
        self._set_exit_marker(fatal, exception=''.join(format_exc()))
    if not all_versions:
      DEBUG("No Object found for read workload. Bucket : %s, Markers : %s, "
            "Istrunc : %s, Prefix : %s, MaxKeys : %s, ReturnedNumPrefixes : %s,"
            " ReturnedNumDeleteMarkers : %s"
            %(bucket, bucket_marker, istrunc, objprefix, maxkeys,
            len(prefixes) if isinstance(prefixes, list) else prefixes,
            len(dmarkers)  if isinstance(dmarkers, list) else dmarkers))
      sleep(1)
      return
    shuffle(all_versions)

    if not self._kwargs["skip_expired_object_read"]:
      all_versions = self._skip_expired_objects(bucket, all_versions)

    if num_reads_from_list:
      num_reads_from_list = randint(num_reads_from_list[0],
                                    num_reads_from_list[1])
      self._debug_logging("Bucket : %s, Marker : %s, Prefix : %s, Found Objects"
        " : %s"%(bucket, bucket_marker, objprefix, all_versions), 3)
      self._read_objects(bucket, all_versions,
                         self._kwargs["num_range_reads_per_obj"],
                         self._kwargs["read_type"], skip_integrity_check,
                         num_reads_from_list,
                         self._kwargs["skip_reads_before_date"],
                         self._kwargs["date_to_skip_reads"],
                         self._kwargs["range_read_size"])
    elif self._kwargs["num_nfs_reads_from_list"]:
      num_reads_from_list = randint(self._kwargs["num_nfs_reads_from_list"][0],
                                    self._kwargs["num_nfs_reads_from_list"][1])
      all_versions = all_versions[:num_reads_from_list]
      self._nfs_read_objects(bucket, all_versions, num_nfs_range_reads_per_file,
                       nfs_read_type, skip_integrity_check, nfs_range_read_size)

  def _nfs_read_objects(self, bucket, objects, num_nfs_range_reads_per_file,
                      nfs_read_type, skip_integrity_check, nfs_range_read_size):
    self._nfsops.nfsread(bucket, objects, num_nfs_range_reads_per_file,
              nfs_read_type, skip_integrity_check, nfs_range_read_size)

  def _skip_expired_objects(self, bucket, objects):
    bucket_info = self._bucket_info[bucket]
    res = []
    for obj in objects:
      if not self._is_object_expired(bucket, obj["Key"],
                                     "ExpiredObjectFoundInList"):
        res.append(obj)
    return res

  def _is_object_expired(self, bucket, objname, anomalis_to_update=None):
    if not objname.endswith("GMT"):
      return False
    try:
      expiry_day = int(objname.split("-")[1][1:])
    except ValueError:
      self._debug_logging("Failed to get expiry day for object : %s"%(objname),
                          3, Constants.MSG_TYPE_ERROR)
      return False
    if expiry_day == 0:
      return False
    try:
      creation_time = objname.split("_")[-1].split("..")[0]
      estimated_expiry = datetime.strptime(creation_time,
                                    "%d.%m.%Y")+timedelta(days=expiry_day+1)
    except Exception as err:
      print objname, "Unable to get creation/Expiry time for obj .Error : ",\
                    err, creation_time
      return False
    current_time = strftime("%d.%m.%Y %H:%M:%S", gmtime())
    current_time_gmt = datetime.strptime(current_time, "%d.%m.%Y %H:%M:%S")
    if current_time_gmt > estimated_expiry:
      headres = self._head_object(bucket, objname, ignore_errors=["Not Found"])
      self._debug_logging("Bucket/Objectname : %s / %s, was supposed to be "
        "expired. But is returned in listobjects call.Expected Expiry time : %s"
        ", Current time : %s\nHead on obj : %s "%(bucket, objname,
          estimated_expiry, current_time, headres), 3)
      if anomalis_to_update:
        self._total_ops_recorded["anomalis"][anomalis_to_update]["count"] += 1
      return True
    return False

  def _estimated_obj_expiry_time(self, bucket, objname, bucket_info=None):
    if "NoExp" in objname:
      return "-D0-"
    if not bucket_info:
      bucket_info = self._bucket_info[bucket]
    for rule in bucket_info["expiry"]:
      if objname.startswith(rule["prefix"]):
        prefix = "-D%s-"%(rule["expiry"])
        if "None" in prefix:
          if self._is_nfs_enabled_on_bucket(bucket):
            return "-D0-"
          self._debug_logging("Failed assess the expiry date, Found None. Obj "
            ": (%s) %s, Rule : %s, Bucket rule : %s"%(bucket, objname, rule,
                            bucket_info["expiry"]), 3, Constants.MSG_TYPE_ERROR)
          return "-D0-"
        return prefix
    return "-D0-"

  def _list_delimeter(self, bucket, delimeter="/", prefix="", marker="",
                      return_res=False, validate_list_against_head=False):
    if return_res:
      return self._execute(self._s3ops.s3_list_objects, Bucket=bucket,
                           Delimiter=delimeter, Prefix=prefix, Marker=marker)
    if not prefix:
      if len(self._bucket_info["list_bucket_map"][bucket]["prefixes"]) > 0:
        with self._lock:
          if len(self._bucket_info["list_bucket_map"][bucket]["prefixes"]) > 0:
            try:
              prefix = self._bucket_info["list_bucket_map"][bucket]["prefixes"
                                                                        ].pop()
              self._bucket_info["list_bucket_map"][bucket]["prefix"] = prefix
            except Exception as err:
              print "=======",err , "======="
    mkeys = randint(self._kwargs["maxkeys_to_list"][0],
                    self._kwargs["maxkeys_to_list"][-1])
    marker = self._bucket_info["list_bucket_map"][bucket]["vmarker"]
    objs = self._execute(self._s3ops.s3_list_objects, Bucket=bucket,
               MaxKeys=mkeys, Delimiter=delimeter, Prefix=prefix, Marker=marker)
    self._dump_res_to_file(objs, bucket, filename="%s_%s_list_delimeter.json"%(
                    self._get_worker_filemarker().replace(".json",""), bucket))
    if not objs:
      ERROR("List objects returned None for bucket : %s"%(bucket))
      return
    prefixes = objs.get("CommonPrefixes", [])
    with self._lock:
      self._bucket_info["list_bucket_map"][bucket]["vmarker"] = ""
      if objs.get("IsTruncated"):
        self._bucket_info["list_bucket_map"][bucket][
                                                "vmarker"] = objs["NextMarker"]
      else:
        debug_print("S3List : Resetting makers. Bucket : %s, Istrucated : %s, "
          "Prefix : %s, Delimeter : %s, Marker : %s, Sleep for a sec"
          %(bucket, objs.get("IsTruncated"), prefix, delimeter, marker))
        self._bucket_info["list_bucket_map"][bucket]["vmarker"] = ""
        sleep(1)
      if not self._bucket_info["list_bucket_map"][bucket][
                                                      "prefixes"] and prefixes:
        self._bucket_info["list_bucket_map"][bucket]["prefixes"] = [
                                            i.values()[0] for i in prefixes]
      if "s3_list_objects" in self._total_ops_recorded:
        self._total_ops_recorded["s3_list_objects"].setdefault(
                                                          "num_list_objects", 0)
        self._total_ops_recorded["s3_list_objects"].setdefault("num_prefixes",0)
        self._total_ops_recorded["s3_list_objects"]["num_list_objects"
                    ] += len(objs.get("Contents", []))
        self._total_ops_recorded["s3_list_objects"]["num_prefixes"
                    ] += len(prefixes)
      if not objs.get("Contents", []):
        return
    if validate_list_against_head:
      self._list_and_head(bucket, objs)

  def _list_and_head(self, bucket, objects):
    for obj in objects["Contents"]:
      res = self._head_object(bucket, obj["Key"])
      headsize, headtime = res["ContentLength"], res["LastModified"]
      if headsize != obj["Size"]:
        raise Exception("Mismatch in size reported for bucket/obj : %s / %s. "
                        "Head Size : %s vs List Size : %s, HeadRes : %s, "
                        "ListRes : %s"%(bucket, obj["Key"], headsize,
                        obj["Size"], res, objects))
      if headtime != obj["LastModified"]:
        raise Exception("Mismatch : last modified time for bucket/obj : %s / %s"
                        ", Head time : %s vs List time : %s, HeadRes : %s, "
                        "ListRes : %s"%(bucket, obj["Key"], headtime,
                        obj["LastModified"], res, objects))

  def _list_objects(self, bucket, vmarker=None, kmarker=None, marker=None,
                    prefix=None, versions=False, maxkeys=1000):
    params = {"MaxKeys":maxkeys}
    params["Bucket"] = bucket
    if kmarker:
      params["KeyMarker"] = kmarker
    if vmarker:
      params["VersionIdMarker"] = vmarker
    if prefix:
      params["Prefix"] = prefix
    if not versions:
      #params["Marker"] = marker
      params["ContinuationToken"] = marker
      res = self._execute(self._s3ops.s3_list_objects_v2, **params)
      self._dump_res_to_file({"api_params":params, "api_response":res}, bucket,
        filename="%s_%s_objects_v2.json"%(self._get_worker_filemarker(
          ).replace(".json",""), bucket))
      return res,res.get("NextContinuationToken",""),res.get("IsTruncated")
    else:
      self._debug_logging("Listing versions. Bucket : %s, VersionIdMarker : %s,"
        "Object Marker : %s, Prefix : %s"%(bucket, vmarker, kmarker, prefix), 3)
      res = self._execute(self._s3ops.s3_list_object_versions, **params)
      self._dump_res_to_file({"api_params":params, "api_response":res}, bucket,
        filename="%s_%s_versions.json"%(self._get_worker_filemarker(
        ).replace(".json",""), bucket))
    return res,res.get("NextVersionIdMarker",""),res.get("NextKeyMarker",""
                  ), res.get("IsTruncated")

  def _dump_res_to_file(self, res, bucket, filename=None):
    if not filename:
      filename = self._get_worker_filemarker()
    res = {"res":res, "time":datetime.now(), "bucket":bucket}
    if isinstance(res, dict):
      Utils.write_to_file(dumps(res, indent=2, sort_keys=True, default=str),
                          filename, "w")
    else:
      Utils.write_to_file(str(res), filename, "w")

  def _log_stats(self, log_debug_stats=False):
    self._opexecutor._flush_records()
    try:
      self._logstats.log_stats(self._total_ops_recorded,
                               log_debug_stats=log_debug_stats,
                               workload_start_time=self._workload_start_time)
    except Exception as err:
      print print_stack()
      ERROR("Error while processing test stats : %s"%(err.message))

  def _monitor_bucket_stats(self):
    """Monitor Objects UI and local stats reported by test.
    """
    copy_objs = self._total_ops_recorded.get("s3_copy_object", {})
    put_objs = self._total_ops_recorded.get("put_object", {})
    local_total_objects = put_objs.get("count", 0)+copy_objs.get("count", 0)
    local_total_size = put_objs.get("size", 0)+copy_objs.get("size", 0)
    if local_total_objects < 1:
      return
    deleted_objects = self._total_ops_recorded.get("s3_delete_objects",
                                                   {}).get("count",0)
    deleted_size = self._total_ops_recorded.get("s3_delete_objects",
                                                {}).get("size",0)
    instance_num_objs, instance_size = self._get_instance_level_stats()
    expected_instance_num_objs = (self._instance_num_objs +local_total_objects)\
                                 - deleted_objects
    local_total_objects = (local_total_objects+self._existing_objects) \
                          - deleted_objects
    expected_instance_size = local_total_size + self._instance_size
    local_total_size = (local_total_size + self._existing_size) - \
                        deleted_size
    total_objects , total_size = self._objectscluster._get_bucket_stats(
                                            self._bucket_info["bucket_names"])
    self._total_ops_recorded["UI-Stats"] = {
      "BucketStats" : {
          "NumObjs" :{
            "Expected" : self._utils.convert_num(local_total_objects),
            "Found" : self._utils.convert_num(total_objects),
            "res" : "%s vs %s"%(self._utils.convert_num(local_total_objects),
                                self._utils.convert_num(total_objects))
          },
          "Size" :{
            "Expected" : self._utils.convert_size(local_total_size),
            "Found" : self._utils.convert_size(total_size),
            "res" : "%s vs %s"%(self._utils.convert_size(local_total_size),
                                self._utils.convert_size(total_size))
          }
        },
      "InstanceStats" : {
          "NumObjs" :{
            "Expected" : self._utils.convert_num(expected_instance_num_objs),
            "Found" : self._utils.convert_num(instance_num_objs),
            "res" : "%s vs %s"%(
                        self._utils.convert_num(expected_instance_num_objs),
                        self._utils.convert_num(instance_num_objs))
          },
          "Size" :{
            "Expected" : self._utils.convert_size(expected_instance_size),
            "Found" : self._utils.convert_size(instance_size),
            "res": "%s vs %s"%(self._utils.convert_size(expected_instance_size),
                                self._utils.convert_size(instance_size))
          }
        },
      "time" : datetime.now()
    }

  def _multicluster_check(self, **kwargs):
    if kwargs["skip_multicluster_check"]:
      WARN("Skipping multicluster check for %s"
           %(self._objectscluster.objects_name))
      return
    stime = time()
    res = runtime = "Skipped"
    try:
      res = self._objectscluster.add_multicluster()
    except Exception as err:
      fatal = "Failed to run multicluster check, Error : %s, during num_deploy"\
              " : %s, num_nodes : %s, DepStatus : %s"\
              %(err, self._total_ops_recorded["deploy"].get("num_deploy"),
              self._total_ops_recorded["deploy"].get("num_nodes"),
              self._total_ops_recorded["deploy"])
      self._set_exit_marker(fatal, exception=''.join(format_exc()))
    if "Skipped" not in res:
      runtime = self._sanity_check(sanity_num_buckets=10, update_results=False,
                                   num_objects_per_bucket=100,
                                   skip_sanity_check=False,
                                   run_only_cert_test=False)
    self._total_ops_recorded["deploy"]["deployments"][-1]["multicluster"] = {
      "time":time()-stime, "sanity_test" : runtime, "status":res
    }

  def _prepare_objects_cluster(self, pcip, pcuser, pcpass, objects_name,
                               peip, peuser, pepass, deploy=False, **kwargs):
    """Search Objects instance in PC and return the object.
    """
    self._objects_cluster_monitor = True
    if None in (pcip, pcuser, pcpass, objects_name):
      ERROR("PC details are missing, setting Objects cluster monitor to False")
      self._objects_cluster_monitor = False
      return
    if not self._objectscluster:
      self._init_infra(pcip, pcuser, pcpass, objects_name, peip, peuser, pepass)
    if "deploy" not in self._workload.keys():
      num_nodes = kwargs['num_nodes']
      kwargs['num_nodes'] = randint(num_nodes[0], num_nodes[-1])
      self._objectscluster.deploy(**kwargs)
      if not self._access or not self._secret:
        user = self._objectscluster.create_iam_user("test", "test@scalcia.com",
                                                    recreate=True)
        self._access = user["access"]
        self._secret = user["secret"]
    if not self._objectscluster.uuid:
      ERROR("Unable to find Objects instance %s in PC : %s, setting Objects"
            "cluster monitor to False"%(objects_name, pcip))
      self._objects_cluster_monitor = False
      return
    if self._objectscluster.state == "COMPLETE":
      obj_size = self._get_instance_level_stats()
      self._instance_num_objs, self._instance_size = obj_size
    INFO("Found Objects Instance : %s, UUID : %s, Total Objects : %s, Size : %s"
         %(self._objectscluster.objects_name, self._objectscluster.uuid,
           self._utils.convert_num(self._instance_num_objs),
           self._utils.convert_size(self._instance_size)))
    setup_info = self._get_setup_info()
    self._total_ops_recorded["setup_details"] = setup_info
    INFO("Setup Info : %s"%(setup_info))

  def _init_infra(self, pcip, pcuser, pcpass, objects_name, peip, peuser,
                  pepass):
    """Search Objects instance in PC and return the object.
    """
    if None in (pcip, pcuser, pcpass, objects_name):
      ERROR("PC details are missing, setting Objects cluster monitor to False")
      self._objects_cluster_monitor = False
      return
    self._objectscluster = PCObjects(pcip, pcuser, pcpass, objects_name,
                                   self._endpoints[0], self._kwargs.get("peip"))
    self._aoscluster = AOSCluster(peip, peuser, pepass)
    self._msp = MSP(self._objectscluster)
    self._oss_cluster = ObjectsCluster(self._objectscluster, self._msp)
    self._conditions = Conditions(self)
    self._objects_cluster_monitor = True

  def _get_setup_info(self):
    service_manager_build = self._objectscluster._get_service_manager_version()
    msp_cluster_info = self._objectscluster.get_msp_cluster_details()
    objects_cluster_info = self._objectscluster._get_objects_details()
    pcip = self._objectscluster.pcip
    peip = self._objectscluster.peip
    pc_build = self._objectscluster.get_build_info(pcip, True)
    pe_build = self._objectscluster.get_build_info(peip, False)
    self._archival_server_location ="load/%s/%s/%s"\
          %(objects_cluster_info["deployment_version"],
            self._kwargs["test_start_time"], self.instance_uuid)
    return {"pc":{"ip":pcip, "build":pc_build},
            "pe":{"ip":peip,"build":pe_build},
            "service_manager_build":service_manager_build,
            "msp_cluster_info":msp_cluster_info,
            "objects_cluster_info":objects_cluster_info
            }

  def _prepare_stage(self, **kwargs):
    self._error_injection = None
    self._init_s3ops(self._endpoints, access=self._access, secret=self._secret,
                      read_timeout=kwargs["read_timeout"],
                      max_pool_connections=kwargs["parallel_threads"],
                      addressing_style=kwargs["addressing_style"],
                      debug=kwargs["enable_boto_debug_logging"])
    if not self._is_write_workload and not self._is_copy_workload \
       and not self._is_nfswrite_workload and not self._is_nfscopy_workload \
       and not self._is_nfsrename_workload and \
       not self._is_nfs_overwrite_workload:
      kwargs["skip_bucket_creation"] = True
    if not kwargs["skip_notification_endpoint_config"]:
      self.configure_notification_endpoint()
    if kwargs.get("skip_bucket_creation"):
      if not self._bucket_info["bucket_names"]:
        self._update_bucket_names_in_db(
          self._get_all_bucket_names(kwargs.pop("bucket"),
                                     kwargs.pop("bucket_prefix")))
        self._update_read_map(kwargs["nfs_mount_options"])
      return
    if kwargs["enable_tiering"]:
      self.patch_boto_schema_for_endpoint()
      ted = kwargs["tiering_endpoint_details"]
      if ted and self._objectscluster:
        self._objectscluster.add_tiering_endpoint(**ted)
        kwargs["tiering_endpoint"] = ted["endpoint_name"]
    bucket = kwargs.pop("bucket")
    enable_versioning = kwargs.pop("enable_versioning")
    bucket_names = None
    if bucket:
      bucket_names = bucket.split(",")
    else:
      num_buckets = kwargs.pop("num_buckets")
      bucket_prefix = kwargs.pop("bucket_prefix").split(",")[0]
      bucket_names = [bucket_prefix+str(i) for i in range(num_buckets)]
    delay = 0
    for bucket in bucket_names:
      if  self._create_bucket(bucket, enable_versioning, **kwargs):
        delay = 10
    self._update_bucket_names_in_db(bucket_names)
    sleep(delay)
    self._update_read_map(kwargs["nfs_mount_options"])
    INFO("%s\nPrepare Stage Completes\n%s"%("*"*10, "*"*10))

  def _update_bucket_names_in_db(self, bucket_names):
    """Update bucket details in db.
    """
    for bucket in bucket_names:
      self._bucket_info["bucket_names"].append(bucket)
      self._bucket_info["bucket_map"][bucket] = {}

      #If lookup workload is planned then init db for bucket list map
      if self._is_lookup_workload:
        self._bucket_info["bucket_map"][bucket].update(
            {"lookup" : {
              "markers":{
                "version_marker":{
                  "marker":"", "vmarker":""
                  },
                "object_marker":{
                  "marker":""
                  }
                }
              }
            }
          )


  def _put_object(self, objnum, extra_headers):
    bucket, objname, num_versions, num_parts, objsize = \
              self._objname_distributor.get_objname_and_upload_type(objnum)
    if not self._objname_distributor.may_be_lock_unlock_obj(bucket, objname,
                                                      objsize, Constants.LOCK):
      return
    if self._bucket_info[bucket]["nfs"]:
      extra_headers={}
    if num_parts > 0:
      return self._multipart_put(bucket, objname, objsize, num_parts,
                                 extra_headers, num_versions)
    part, uid = None, None
    put_object = PutObject(self._s3ops.s3_get_endpoint_host(), self._access,
                           self._secret, bucket, objname, objsize, part, uid)
    for _ in range(num_versions):
      data, md5sum = self._get_data(objsize)
      res = self._execute(put_object.put_object, Bucket=bucket, Key=objname,
                          data=data, md5sum=md5sum, rsize=objsize,
                          extra_headers=extra_headers)
      skip_head = self._kwargs["skip_head_after_put"]
      try:
        self._head_object(bucket, objname, res.get("VersionId"),
          set_exit_marker_on_failure=False if skip_head else True)
      except Exception as err:
        ERROR("Failed to validate head-object after successful put. Put Res "
              ": %s, (skip_head_after_put : %s), Error : %s"%(res, skip_head,
                              err.message))
        if skip_head:
          continue
        raise
    self._objname_distributor.may_be_lock_unlock_obj(bucket, objname, objsize,
                                                     Constants.UNLOCK)

  def _multipart_put(self, bucket, objname, objsize, num_part, extra_headers,
                     num_versions):
    if not self._kwargs["skip_abort_multipart_upload"]:
      #Hack to reduce number of abort multipart objects.
      objname = "%s%s"%(choice(["abort_", "", "", "", "", "", ""]), objname)
    uid = self._initiate_upload(bucket, objname, num_part)
    with self._lock:
      self._total_ops_recorded["multipart"]["totalongoing"] += 1
    self._multipart_objects_map[objname] = {uid:[], "bucket":bucket, "uuid":uid,
                                            "parts":num_part, "objname":objname,
                                            "size":num_part*objsize}
    total_size = 0
    for partnum in range(1, num_part+1):
      if self._exit_test("Multipart Upload") or self._pause_workload(
                                                            "multipart_put", 0):
        break
      put_object = PutObject(self._s3ops.s3_get_endpoint_host(), self._access,
                           self._secret, bucket, objname, objsize, partnum, uid)
      data, md5sum = self._get_data(objsize)
      res = self._execute(put_object.put_object, Bucket=bucket, Key=objname,
                          Part=partnum, data=data, md5sum=md5sum, rsize=objsize,
                          extra_headers=extra_headers)
      self._multipart_objects_map[objname][uid].append({
                                            "ETag":res.get("etag").strip("\""),
                                            "PartNumber":partnum})
      total_size += objsize
      with self._lock:
        self._total_ops_recorded["multipart"]["totalparts"] += 1
    if objname.startswith("abort"):
      self._abort_multipart_objects(
          details=self._multipart_objects_map.pop(objname), ignore_errors=False)
    else:
      self._finalize_parts(bucket, objname, uid,
             self._multipart_objects_map[objname][uid], total_size, force=False)
      with self._lock:
        self._multipart_objects_map.pop(objname)
        self._total_ops_recorded["multipart"]["totalongoing"] -= 1
        self._total_ops_recorded["multipart"]["totalcount"] += 1
        self._total_ops_recorded["multipart"]["totalfinalizedparts"] += num_part
    self._objname_distributor.may_be_lock_unlock_obj(bucket, objname,
                                            total_size, Constants.UNLOCK)

  def _range_read_integrity_check(self, bucket, objname, data,
                offset_range=None, etag=None, validate_range_read_offset=False,
                object_offset=None):
    if "_V2_" in objname:
      return self._range_read_integrity_check_v2(bucket, objname, data)
    if "_V3_" in objname:
      return self._range_read_integrity_check_v3(bucket=bucket, objname=objname,
                  data=data, offset_range=offset_range, etag=etag,
                  validate_range_read_offset=validate_range_read_offset,
                  object_offset=object_offset)

  def _range_read_integrity_check_v2(self, bucket, objname, data):
    num_sub_strings = len(data)/1024
    start = 0
    end = 1024
    offset = 0
    for i in range(num_sub_strings):
      substr = data[start:end]
      md5sum = substr[-33:]
      if md5sum[0] != "_":
        raise Exception("Data Corruption : Bucket/Object : %s / %s, Magic "
                        "byte \"_\" not found @ obj offset : (%s, -%s)"
                        %(bucket, objname, i, len(substr[-33:])))
      sudmd5sum = md5(substr[:-33]).hexdigest()
      if sudmd5sum != md5sum[-32:]:
        raise Exception("Data Corruption : Bucket/Object : %s / %s, md5sum"
                  "mismatch at start_offset : %s. Expected vs Found : %s vs %s"
                        %(bucket, objname, offset, md5sum[-32:],sudmd5sum))
      offset += 1024
      start = end
      end += 1024

  def _range_read_integrity_check_v3(self, bucket, objname, data, offset_range,
                         etag, object_offset, validate_range_read_offset=False):
    range_start_offset = 0
    if offset_range:
      range_start_offset, _ = offset_range.split("=")[1].split("-")
      range_start_offset = int(range_start_offset.strip())
    num_sub_strings = len(data)/1024
    start = 0
    end = 1024
    offset = 0
    msg = ""
    for i in range(num_sub_strings):
      substr = data[start:end]
      md5sum = substr[-53:]
      if self._is_sparse_object(objname , objname) and "_" not in md5sum:
        if not substr.rstrip('\0'):
          offset += 1024
          start = end
          end += 1024
          continue
      msg = "Data Corruption, Bucket/Obj/Range : %s / (%s) / %s, Obj metadata"\
            " from s3 : %s, Range sub-offset : %s (offset in Object : %s),"%(
            bucket, objname, offset_range, md5sum, offset, object_offset)
      if md5sum[0] != "_":
        raise Exception("%s Magic byte \"_\" not found in range)"%(msg))
      sudmd5sum = md5(substr[:-53]).hexdigest()
      md5sum_data =  md5sum[-52:].split("_")
      if sudmd5sum != md5sum_data[0]:
        raise Exception("%s mismatch in md5sum. Expected vs Found : %s vs %s"
                        %(msg, md5sum_data[0], sudmd5sum))
      range_start_offset += 1024
      if offset_range and Constants.STATIC_DATA_IDENTIFIER not in objname and \
          not self._is_sparse_object(objname , objname):
        if Constants.MULTIPART_OBJ_IDENTIFIER not in objname and "-" not in etag:
          if int(md5sum_data[-1].strip()) != range_start_offset:
            raise Exception("%s mismatch in data range offset, Expected vs "
                            "Found : %s vs %s"%(msg, md5sum_data[-1],
                            range_start_offset))
        elif validate_range_read_offset:
          if int(md5sum_data[-1].strip()) != range_start_offset:
            raise Exception("%s mismatch in data range offset, Expected vs "
                            "Found : %s vs %s"%(msg, md5sum_data[-1],
                            range_start_offset))
      offset += 1024
      start = end
      end += 1024
    #Below code will give false negative when end offset goes beyond obj size.
    #if range_start_offset != int(range_end_offset.strip()):
    #  raise Exception("Mismatch in Last offset of range after successful range"
    #                  "read : %s Expected vs Found : %s vs %s"%(msg,
    #                  range_end_offset.strip(), range_start_offset))

  def _read_objects(self, bucket, all_versions, num_range_reads_per_obj,
                    obj_read_type, skip_integrity_check, num_reads_from_list,
                    skip_reads_before_date, date_to_skip_reads, range_read_size):
    #obj_read_type = read_type+[]
    while all_versions and not self._exit_test("Read Objects"):
      if num_reads_from_list < 1:
        #Issue atleast 2 range reads once all the objects are read
        num_range_reads_per_obj = 5
      random_key = all_versions.pop()
      read_type = choice(obj_read_type)
      if self._pause_workload("read_objects", 10):
        DEBUG("Auto deletes are enabled, discarding %s objs from bucket : %s "
              "chosen for read"%(len(all_versions), bucket))
        return
      params = {"Bucket":bucket, "skip_integrity_check":skip_integrity_check,
          "rsize":random_key["Size"], "Key":random_key["Key"],
          "VersionId":random_key.get("VersionId"), "objsize":random_key["Size"]}
      if not self._objname_distributor.may_be_lock_unlock_obj(
                        bucket, params["Key"], params["rsize"], Constants.LOCK):
        continue
      #TODO : Possible race in future, object is chosen and same time it was
      #also chosen for deletes during auto-deletes
      if skip_reads_before_date:
        skip_read, objdate  = self._execute(self.skip_old_reads, bucket=bucket,
                          objname=params["Key"], versionid=params["VersionId"],
                          date_to_skip_reads=date_to_skip_reads)
        if skip_read:
          self._total_ops_recorded["skipped_old_reads"] += 1
          WARN("Skipping read on bucket/obj/version : %s/%s/%s, ObjDate : %s"
               %(bucket, params["Key"], params["VersionId"], objdate))
          continue
      if self._kwargs["skip_expired_object_read"]:
        if self._is_object_expired(bucket, params["Key"],
                                   "skipped_expired_reads"):
          self._debug_logging("(%s) %s seems to have expired but still returned"
            " in list. Skipping read : %s"%(bucket, params["Key"],
            self._kwargs["skip_expired_object_read"]), 3,
            Constants.MSG_TYPE_ERROR)
          continue
      num_reads_from_list -=  1
      if read_type == Constants.READ_TYPE_FULL_READ:
        params["etag"] = random_key["ETag"]
        self._execute(self.read_object, **params)
      else:
        for i in range(num_range_reads_per_obj):
          start_offset, end_offset = self._get_range_to_read(random_key["Size"],
                         self._objname_distributor.get_objsize(range_read_size))
          params["Range"]="bytes=%s-%s"%(start_offset, end_offset)
          params["rsize"] = end_offset-start_offset
          params["range_read_size"] = end_offset-start_offset
          params["objsize"] = random_key["Size"]
          if random_key["Size"] < end_offset:
            if random_key["Size"] == 0:
              params["rsize"] = 0
            else:
              params["rsize"] = random_key["Size"]-start_offset
          self._execute(self.range_read_object, **params)
      self._objname_distributor.may_be_lock_unlock_obj(bucket, params["Key"],
                                       params["rsize"], Constants.UNLOCK)

  def skip_old_reads(self, bucket, objname, versionid, date_to_skip_reads):
    headres = self._head_object(bucket, objname, versionid)
    objdate = headres["ResponseMetadata"]["HTTPHeaders"]["last-modified"]
    objdate = datetime.strptime(objdate, "%a, %d %b %Y %H:%M:%S GMT")
    skip_date = datetime.strptime(date_to_skip_reads,
                                           "%a, %d %b %Y %H:%M:%S")
    return objdate < skip_date, objdate

  def _replace_certs(self, **kwargs):
    if kwargs["skip_cert_test"]:
      WARN("Skpping Cert replace test : %s"%(self._objectscluster.objects_name))
      return
    count = 0
    while kwargs["num_cert_replace_loop"] > count:
      INFO("Cert Replacement Loop Number : %s"%(count))
      count += 1
      stime = time()
      cert_type, res = None, None
      try:
        cert_type, res = self._objectscluster.replace_certs(**kwargs)
      except Exception as err:
        print_exc()
        fatal = "Failed to replace certs, Error : %s, DepStatus : %s."%(err,
                  self._total_ops_recorded["deploy"])
        self._set_exit_marker(fatal, exception=''.join(format_exc()))
      cert_replace = "cert_replacement_%s"%(count)
      runtime = self._sanity_check(sanity_num_buckets=10, update_results=False,
                                   num_objects_per_bucket=1,
                                   skip_sanity_check=False,
                                   run_only_cert_test=False)
      self._total_ops_recorded["deploy"]["deployments"][-1][cert_replace] = {
        "time":time()-stime, "type":cert_type, "sanity_test" : runtime,
        "service_tobe_restarted" : kwargs[
                                    "services_to_restart_during_replace_cert"],
        "services_restarted":res
      }
      if "Skipped" in cert_type:
        WARN("Further cert replacement steps are skipped")
        return
      sleep(kwargs["cert_replacement_delay"])

  def _sanity_check(self, **kwargs):
    if kwargs["skip_sanity_check"]:
      WARN("Skipping sanity check for %s"%(self._objectscluster.objects_name))
      return
    stime=time()
    retries = 0
    kwargs["logpath"] = self.logpath
    try:
      sanity = SanityCheck(self._objectscluster, self, **kwargs)
      retries = sanity.run_sanity(kwargs["sanity_num_buckets"],
                                  kwargs["num_objects_per_bucket"])
    except Exception as err:
      fatal = "Failed to run sanity check, Error : %s, during num_deploy : %s,"\
              " num_nodes : %s, DepStatus : %s"\
              %(err, self._total_ops_recorded["deploy"].get("num_deploy"),
              self._total_ops_recorded["deploy"].get("num_nodes"),
              self._total_ops_recorded["deploy"])
      self._set_exit_marker(fatal, exception=''.join(format_exc()))
    if kwargs.get("update_results", True):
      self._total_ops_recorded["deploy"]["deployments"][-1]["sanity_test"] = {
        "time":time()-stime, "num_failures" : retries
      }
    else:
      return time()-stime

  def _scaleout_check(self, **kwargs):
    if kwargs["skip_scaleout_check"]:
      WARN("Skipping scaleout check for %s"%(self._objectscluster.objects_name))
      return
    res  = None
    service_to_restart = kwargs.pop("services_to_restart_during_scaleout")
    stime = time()
    try:
      res = self._objectscluster.scale_out(
                        services_to_restart_during_scaleout=service_to_restart)
    except Exception as err:
      fatal = "Failed to run scaleout check, Error : %s, during num_deploy"\
              " : %s, num_nodes : %s, DepStatus : %s"\
              %(err, self._total_ops_recorded["deploy"].get("num_deploy"),
              self._total_ops_recorded["deploy"].get("num_nodes"),
              self._total_ops_recorded["deploy"])
      self._set_exit_marker(fatal, exception=''.join(format_exc()))
    self._total_ops_recorded["deploy"]["deployments"][-1]["scaleout"] = {
      "time":time()-stime, "status" : "Success",
      "service_tobe_restarted" : service_to_restart, "services_restarted":res
    }

  def _set_exit_marker(self, fatal_msg, fatal=True, exception="",
                       raise_exception=True, caller=None, reason=None):
    """
    this method sets _time_to_exit to true and also appends fatal_msg to
    self.fatals and also updates the _total_ops_recorded["TestStatus"] fields
    and writes the fatal to Constants.EXIT_MARKER_FILE
    """
    if not caller: caller=inspect.stack()[1][3]
    fatal_msg = "(%s) %s"%(datetime.now(), fatal_msg)
    if fatal_msg not in self.fatals:
      self.fatals.append(fatal_msg)
    if self._time_to_exit or not fatal:
      ERROR("Caller : %s (%s), %s, Ignoring since TimeToExit %s or Fatal is %s"
        %(caller, inspect.stack()[1][2], fatal_msg, self._time_to_exit, fatal))
      return
    with self._lock:
      if self._time_to_exit or not fatal:
        return
    self._total_ops_recorded["TestStatus"].update({
                 "status" : Constants.TEST_STOPPING,
                 "caller":"%s (%s)"%(caller, inspect.stack()[1][2]),
                 "start_time" : "%s"%(self._test_start_time),
                 "stop_reason" : reason,
                 "first_fatal" : fatal_msg,
                 "total_runtime" : "%s"%(datetime.now()-self._test_start_time)})
    INFO("Changing time_to_exit to True")
    self._total_ops_recorded["vars"]["time_to_exit"] = True
    exception = exception + format_exception()
    fatal = "Setting EXIT-MARKER=TRUE. Exception : %s,\n%s"%(fatal_msg,
                                                                   exception)
    ERROR(fatal)
    self._time_to_exit = True
    #Unset EI and Upgrade Gflags since we are going to stop test anyway.
    self._total_ops_recorded["vars"]["expect_failure_during_ei"] = False
    self._upgrade_in_progress = False

    if raise_exception:
      Utils.write_to_file(fatal, Constants.EXIT_MARKER_FILE, "w")
      raise Exception(fatal)

  def _dump_records(self, filename=None):
    """
    dumps the _total_ops_recorded and _bucket_info in files

    Args:
      filename(str,Optional): path of file to store the _total_ops_recorded
                              Default: Constants.STATIC_RECORDS_FILE
    """
    if not filename:
      filename = Constants.STATIC_RECORDS_FILE
    try:
      if not self._time_to_exit:
        self._total_ops_recorded["workload_profiles"][-1]["mem_usage"
                                      ] = self._utils.get_mem_usage(self._pid)
      with open(path.join(filename), "w") as fh:
        dump(self._total_ops_recorded, fh, indent=2, default=str)
      with open(path.join(Constants.STATIC_BUCKET_INFO_FILE), "w") as fh:
        dump(self._bucket_info, fh, indent=2, default=str)
    except Exception as err:
      print "ERROR : Failed to dump records. Error : %s"%err

  def _start_and_monitor(self, workloads, monitor_workloads, **kwargs):
    """
    Start workload threads. And monitor progress. it will run monitoring
    processes in a while loop until time_to_exit is true
    """
    self._workload_start_time = time()
    self._release_setup_after_test = kwargs.pop("release_setup_after_test")
    self._file_put_count = 0
    if self._local_run:
      self._exit_on_cntr_c()
    failures_to_tolerate = kwargs.pop("failures_to_tolerate", -1)
    INFO("ALL Buckets : %s"%(self._bucket_info["bucket_names"]))
    count = 0
    self._total_ops_recorded["vars"]["auto_deletes_enabled"] = False
    if path.isfile(Constants.EXIT_MARKER_FILE):
      remove(Constants.EXIT_MARKER_FILE)
    self._dump_records()
    self._monitor = Monitor(self)
    while True:
      if self._time_to_exit:
        WARN("Exit marker found. Exiting from _start_and_monitor")
        try:
          self._monitor.run()
        except Exception as err:
          DEBUG("Failed to run monitor tasks. Err : %s"%err)
        return
      if not self._total_ops_recorded["vars"]["workload_started"]:
        self._start_threads(workloads, monitor_workloads, **kwargs)
      try:
        self._monitor.run()
      except Exception as err:
        DEBUG("Failed to run monitor tasks. Err : %s"%err)
      sleep(30)

  def _may_be_delete_all_objects(self):
    """
    it will initialize "auto_deletes" key in _total_ops_recorded["opstats"] and
    will call the _check_space_reclaim
    """
    self._total_ops_recorded["opstats"].setdefault("auto_deletes", {"stats":[]})
    cool_time = 1800
    cool_time = 30
    self._total_ops_recorded["opstats"]["auto_deletes"]["stats"].append({
                                      "start_time":datetime.now(),
                                      "cool_time":cool_time})
    #Just wait for 30m for things to settle down before starting cleanup
    while self._exit_test("May Be Delete All Objects"):
      if cool_time % 300 == 0:
        DEBUG("Auto delete flag is ON. Waiting for CoolTime : %s sec"%cool_time)
      if cool_time <= 0:
        break
      sleep(10)
      cool_time -= 10
      continue
    self._check_space_reclaim()

  def _check_space_reclaim(self, wait_for_reclaim=True, interval=5):
    """
    checks the space usage through local map after every certain interval in a
    loop and when it comes under the desirable value it exists from the loop,
    it will also update _total_ops_recorded["opstats"]["auto_deletes"]["stats"]'s
    last entry values

    Args:
      wait_for_reclaim(bool,Optional): if True, it will call the
       wait_for_space_reclaimation method which will trigger atlas and curator
       scans to reclaim the deleted objects space
       Default: True
      interval(int,Optional): Default: 5
    """
    storage_stats = self._get_total_objects_size()
    local_storage_usage = storage_stats["local"]["total_storage_used"]
    storage_to_free = (local_storage_usage*self._kwargs[
                                          "auto_delete_objects_threshold"])/100
    expected_storage_consumption = local_storage_usage - storage_to_free
    self._total_ops_recorded["opstats"]["auto_deletes"]["stats"][-1].update({
              "before":{"storage_stats":storage_stats,
                  "local_storage_usage":local_storage_usage,
                  "storage_to_free":storage_to_free, "time":datetime.now(),
                  "expected_storage_consumption":expected_storage_consumption}})
    INFO("Auto deletes are enabled. Will free up storage : %s, Bring it to : %s"
         ", Wait for reclaim : %s"%(self._utils.convert_size(storage_to_free),
                        self._utils.convert_size(expected_storage_consumption),
                        wait_for_reclaim))
    delete_not_converging_time = interval
    total_storage_used, time_since_pcstats_not_fetched = 0, 0
    stats = None
    while not self._exit_test("Check Space Reclaim"):
      stats = self._get_total_objects_size()
      if total_storage_used != stats["local"]["total_storage_used"]:
        total_storage_used = stats["local"]["total_storage_used"]
        delete_not_converging_time = interval
      else:
        delete_not_converging_time += interval
      DEBUG("Storage to Free : %s, Expected Storage Consumption : %s, "
            "Current Storage Stats : %s"%(storage_to_free,
            expected_storage_consumption, stats))
      if stats["local"]["total_storage_used"] <= expected_storage_consumption:
        #Total deleted size can be higher than puts, since you may have old
        #objects in bucket which would be deleted by DELETE workflow
        INFO("Freed enough storage to continue test. Current Storage Usage : "
             "%s, Expected Storage Consumption : %s"%(stats,
                                                  expected_storage_consumption))
        break
      if delete_not_converging_time%600 == 0:
        #Abort all multipart uploads if we are not able to bring down overall
        #storage usage. Check every 300 seconds so if any parts which were
        #active during previous abort parts operation, may become
        #eligible now
        res = self._abort_multipart_objects(force=True, ignore_errors=False,
                                    check_for_lock=True)
        if res:
          #This means, we were able to abort something.
          delete_not_converging_time = interval
      if delete_not_converging_time % 1800 == 0:
        ERROR("Not able to converge on auto deletes.")
      sleep(interval)
    self._total_ops_recorded["opstats"]["auto_deletes"]["stats"][-1].update({
              "after":{"storage_stats":stats, "time":datetime.now(),
                  "local_storage_usage":stats["local"]["total_storage_used"]}})
    if  wait_for_reclaim:
      INFO("Waiting for physical storage usage to come down.")
      self._wait_for_space_reclaimation()
    self._total_ops_recorded["opstats"]["auto_deletes"]["stats"][-1].update({
              "end_time":datetime.now()})

  def _wait_for_space_reclaimation(self):
    """
    this method will trigger atlas full scan and curator full scan, so that
    cluster can reclaim the space used by deleted objects and updates the
    _total_ops_recorded["opstats"]["auto_deletes"]["stats"] list's last entry
    values
    """
    if not self._objectscluster:
      ERROR("PC details not found. Cannot wait for ATLAS/Curator space reclaim")
      return
    aos_storage_usage_before, aos_storage_usage_after = 0, 0
    try:
      aos_storage_usage_before = self._aoscluster.get_storage_pool_usage()
    except Exception as err:
      ERROR("Failed to get storage usage from AOS Cluster. Err : %s"%err)
    atlas_start_time = datetime.now()
    self._wait_for_atlas_scans(scan_type=Constants.ATLAS_FULL_SCAN, num_scans=3,
                             wait_for_completion=True, delay_between_scans=300)
    atlas_end_time = datetime.now()
    self._wait_for_curator_scans(scan_type=Constants.CURATOR_FULL_SCAN,
                num_scans=3, wait_for_completion=True, delay_between_scans=300)
    try:
      aos_storage_usage_after = self._aoscluster.get_storage_pool_usage()
    except Exception as err:
      ERROR("Failed to get storage usage from AOS Cluster. Err : %s"%err)
    curator_end_time = datetime.now()
    self._total_ops_recorded["opstats"]["auto_deletes"]["stats"][-1][
                       "before"]["aos_storage_usage"] = aos_storage_usage_before
    self._total_ops_recorded["opstats"]["auto_deletes"]["stats"][-1][
                        "after"]["aos_storage_usage"] = aos_storage_usage_after
    self._total_ops_recorded["opstats"]["auto_deletes"]["stats"][-1].update({
                    "scan":{"atlas":{"start_time":atlas_start_time,
                                     "end_time":atlas_end_time,
                                     "time":atlas_end_time-atlas_start_time},
                           "curator":{"start_time":atlas_end_time,
                                       "end_time":curator_end_time,
                                       "time":curator_end_time-atlas_end_time},
                           "start_time":atlas_start_time,
                           "end_time":curator_end_time},
                    "end_time":curator_end_time})
    INFO("Auto Delete Storage Stats : %s"
         %(self._total_ops_recorded["opstats"]["auto_deletes"]["stats"][-1]))

  def _wait_for_atlas_scans(self, scan_type, num_scans, wait_for_completion,
                             delay_between_scans):
    INFO("Num %s to invoke : %s @ %s interval. Wait for completion : %s"
         %(scan_type, num_scans, delay_between_scans, wait_for_completion))
    for i in range(num_scans):
      if self._exit_test("Wait For Atlas Scans"):
        return
      self._oss_cluster.atlas.initiate_full_scan(wait_for_completion,
                  timeout=self._kwargs["atlas_scan_timeout"])
      while self._exit_test("Wait For Atlas Scans"):
        if delay_between_scans < 0:
          break
        sleep(30)
        delay_between_scans -= 30

  def _wait_for_curator_scans(self, scan_type, num_scans, wait_for_completion,
                             delay_between_scans):
    INFO("Num %s to invoke : %s @ %s interval. Wait for completion : %s"
         %(scan_type, num_scans, delay_between_scans, wait_for_completion))
    for i in range(num_scans):
      if self._exit_test("Wait For Curator Scans"):
        return
      self._aoscluster.initiate_full_scan(wait_for_completion,
                  timeout=self._kwargs["curator_scan_timeout"])
      while self._exit_test("Wait For Curator Scans"):
        if delay_between_scans < 0:
          break
        sleep(30)
        delay_between_scans -= 30

  def _get_total_objects_size(self):
    """
    returns the total storage space used by automation

    Returns: dict
      local: dict, it has values calculated based on our local map,
             it has three keys,
                  1. total_size
                  2. total_deleted_size
                  3. total_storage_used
      pc: dict, it has values which are returned from pc group apis, it has key
                  1. total_size
    """
    size = self._total_ops_recorded.get("s3_copy_object", {}).get("size",0)
    size += self._total_ops_recorded.get("put_object", {}).get("size",0)
    size += self._total_ops_recorded.get("_nfs_write", {}).get("size",0)
    size += self._total_ops_recorded.get("copy_file", {}).get("size",0)
    #size += self._total_ops_recorded.get("_nfs_over_write", {}).get("size",0)
    size += self._total_ops_recorded.get("start_vdbench", {}).get("size",0)
    size += self._total_ops_recorded.get("start_fio", {}).get("size",0)
    dsize = self._total_ops_recorded.get("s3_delete_objects", {}).get("size",0)
    dsize += self._total_ops_recorded.get("_delete_nfs_file", {}).get("size",0)
    dsize += self._total_ops_recorded.get("multipart", {}).get("aborted", {}
                                          ).get("size", 0)
    pcsize = -100
    if self._objectscluster:
      try:
        _, pcsize = self._objectscluster._get_bucket_stats(
                                            self._bucket_info["bucket_names"])
      except Exception as err:
        ERROR("Failed to get total objects size from  PC. Error : %s"%(err))
        pcsize = -100
    return {"local":{"total_size":size,
                     "total_deleted_size":dsize,
                     "total_storage_used":size-dsize},
            "pc":{"total_size":pcsize}}

  def _may_be_change_workload_profile(self):
    if self._time_to_exit:
      ERROR("Exit is called. Skipping profile change")
      return False
    duration = self._kwargs["auto_workload_stability_duration"]
    if (time() - self._workload_start_time) < duration:
      #If workload runtime is lower than duration to change, then just return
      return False
    if self._kwargs["enable_static_workload_change"]:
      INFO("Changing workload profile. Static workload change is set to True")
      self._total_ops_recorded["vars"]["change_workload_detected"] = True
      return True
    count = 0
    for err, details in self._total_ops_recorded[
                      "errors"]["err_msgs"].iteritems():
      count += details["count"]
    for err, details in self._total_ops_recorded[
                                    "errors"]["err_codes"].iteritems():
      if int(err) > 300:
        count += details["count"]
    err_threashold = self._kwargs["num_failure_for_workload_change"]
    if (count - self._num_failures_to_ignore) > err_threashold:
      INFO("Test is producing %s errors which is highe than threshold %s (%s) "
           "Increasing test stability duration by 10 perc and failure count by"
           " 10 perc"%(count, self._num_failures_to_ignore, err_threashold))
      self._num_failures_to_ignore += int(self._num_failures_to_ignore/10)
      self._kwargs["auto_workload_stability_duration"
                  ] += int(self._kwargs["auto_workload_stability_duration"]/10)
      return False
    WARN("Test has hit %s (%s)  (vs threshold for workload change %s) failures"
         " in last %s secs. Trigger workload change"%(count,
         self._num_failures_to_ignore, err_threashold, duration))
    self._num_failures_to_ignore = count
    self._total_ops_recorded["vars"]["change_workload_detected"] = True
    return True

  def _get_workload_profile(self, select_random_workload=False):
    """Select workload randomly if select_random_workload is set to TRUE.
    If enable_static_workload_change=True then just pick the workload from
    workload profiles after workload_change_time is reached.
    Else just pop the workload (which means, same workload
    wont be executed twice)
    """
    if not self._workload_profiles:
      raise Exception("No available workload profiles found. Dynamic generation"
                      " of profile is not yet supported")
    counter = 20
    if select_random_workload:
      shuffle(self._workload_profiles)
    while counter > 0:
      if not self._kwargs["enable_static_workload_change"]:
        workloads = self._workload_profiles.pop()
      else:
        workloads = self._workload_profiles.pop(0)
        self._workload_profiles.append(workloads)
      #If NFS is not enabled but if we still pick workload which has NFS in it
      if not self._kwargs["enable_nfs"]:
        if (isinstance(workloads, list) and any(["nfs" in i for i in workloads])
            ) or "nfs" in workloads:
          #Then just add the workload back to profiles and decrement counter
          #and retry. Since we dont want to pick nfs workload when nfs is turned
          #off. If enable_static_workload_change is not ON then just forget
          #the workload and move on, since we dont want to run them again
          if not self._kwargs["enable_static_workload_change"]:
            self._workload_profiles.append(workloads)
          counter -= 1
          continue
      objsize = self._objsizes[0]
      if self._kwargs["select_random_objsize"]:
        objsize = choice(self._objsizes)
      return workloads.split(","), objsize
    raise Exception("Failed to find the suitable workload after 20 retries. "
                    "Workloads : %s, (NFS %s)"%(self._workload_profiles,
                                                self._kwargs["enable_nfs"]))

  def _start_threads(self, workloads, monitor_workloads, **kwargs):
    self._total_ops_recorded["vars"]["workload_started"] = True
    kwargs["quota_type"] = "cntr"
    for workload in workloads:
      INFO("Starting workload : %s"%(workload))
      self._task_manager.add(getattr(self, workload), **kwargs)
    kwargs["quota_type"] = "monitor"
    for monitor in monitor_workloads:
      INFO("Starting monitor : %s"%(monitor))
      self._task_manager.add(getattr(self, monitor), **kwargs)

  def _stop_workload(self, reason=None, close_error_fh=True):
    """"
    this method will be called by monitor methods in case of checks are failing
    and also start_workload will call in case of exception or change_in_workload
    or after the execution_completed
    sets the _total_ops_recorded["vars"]["stop_workload_initiated"] to True,
    dump the dbs to files, unmount nfs, stops all the threads, aborts all
    ongoing multiparts, acrhives stats and db to s3 bucket, closes the web
    server and sets the value of ["TestStatus"]["status"] according to
    self.fatals
    """
    with self._lock:
      if self._total_ops_recorded["vars"]["stop_workload_initiated"]:
        ERROR("Skipping stop workload flow, since its already done")
        return
      if not self._total_ops_recorded["vars"]["stop_workload_initiated"]:
        self._total_ops_recorded["vars"]["stop_workload_initiated"] = True
    caller = inspect.stack()[1][3]
    DEBUG("Stop workload is called by %s (%s)"%(caller, inspect.stack()[1][2]))
    self._set_exit_marker("Stopping workload. Reason : %s"%(reason),
                          raise_exception=False, caller=caller, reason=reason)
    self._dump_records()
    if self._kwargs["capture_tcpdump"]: self._utils.stop_tcpdump()
    if self.rdm and self._release_setup_after_test: self.rdm.release()
    self._task_manager._time_to_exit = True
    #curframe = inspect.currentframe()
    #calframe = inspect.getouterframes(curframe, 2)
    INFO("Completing run. Reason : %s, Caller is : %s (%s)"
         %(reason, caller, inspect.stack()[1][2]))
    self._task_manager.complete_run()
    self._nfsops._umount_shares()
    if path.isfile(self._exit_marker): remove(self._exit_marker)
    if not self._wait_for_all_workers(force_kill=True): self._kill_test()
    INFO("Aborting Inprogress Multipart Objects : %s"
         %(self._multipart_objects_map))
    self._abort_multipart_objects(force=True)
    if self._kwargs["archive_results"]: self.archive_results()
    if self._kwargs["start_webserver_logging"]:
      if self._webserver:
        DEBUG("Closing webserver")
        self._webserver.close()
    self._total_ops_recorded["TestStatus"]["status"
                              ] = "Completed" if not self.fatals else "Stopped"

  def _dump_final_stats(self):
    INFO("********* FINAL STATS *********")
    self._log_stats(log_debug_stats=True)
    if self._objectscluster: self._monitor_bucket_stats()
    if self._total_ops_recorded.get("deploy"):
      print dumps(self._total_ops_recorded["deploy"],indent=2, default=str)
    ERROR("First exception Found : %s"
          %(self._total_ops_recorded["TestStatus"].get("first_fatal")))
    for fatal in self.fatals:
      print "  -> %s"%(fatal)
    final_test_status = "Completed" if not self.fatals else "Stopped"
    INFO("****** Test %s ******"%(final_test_status))
    if self._total_ops_recorded["TestStatus"]["status"] != final_test_status:
      self._total_ops_recorded["TestStatus"]["status"] = final_test_status
    print " === Dumping Records === "
    #Try 120 times to dump final records.
    for i in range(120):
      try:
        self._dump_records()
        break
      except Exception as err:
        print "Error while dumping final stats : %s"%err
      sleep(1)
    INFO("Test Runtime : %s, Log file : %s"
         %(str(timedelta(seconds=time()-self._start_time)), self.logfile))

  def _may_be_stop_writes(self):
    """
    this method will be called periodically by monitor class
    it will set the flags auto_deletes_enabled and stop_writes if we exceeded
    the limit of storage that we provided and after the enough storage is freed
    from pc, it will reset the flags, it will flush the list_for_copy map and
    it will exit

    Returns : bool(actually there is no need to return anything)
      True, if stop_writes is already True or we cleared enough space
      (_may_be_delete_all_objects will exit only after checking enough space
       reclaimed)
      False, if _storage_limit is not set or if the storage_limit is not
      exceeded
    """
    if self._total_ops_recorded["vars"]["stop_writes"]:
      WARN("Writes are already stopped, retry later")
      return True
    if self._storage_limit == -1:
      return False
    storage_stats = self._get_total_objects_size()
    if storage_stats["local"]["total_storage_used"] > self._storage_limit:
      msg = "Expected local storage consumption size reached limit. "\
            "Storage limit : %s, Local Storage Usage and Reported by PC : %s"%(
            self._storage_limit, storage_stats)
      if self._kwargs["auto_delete_objects_threshold"] > 0:
        self._total_ops_recorded["vars"]["auto_deletes_enabled"] = True
        self._total_ops_recorded["vars"]["stop_writes"] = True
      INFO("%s, Auto Deletes Enabled : %s. Write Paused : %s, Delete_threshold "
           ":%s"%(msg, self._total_ops_recorded["vars"]["auto_deletes_enabled"],
                    self._total_ops_recorded["vars"]["stop_writes"],
                    self._kwargs["auto_delete_objects_threshold"]))
      if self._total_ops_recorded["vars"]["auto_deletes_enabled"
                          ] and self._total_ops_recorded["vars"]["stop_writes"]:
        try:
          self._may_be_delete_all_objects()
        except Exception as err:
          self._set_exit_marker("%s"%err, exception=''.join(format_exc()))
        self._objname_distributor._discard_cached_list()
        INFO("Turning off stop_writes and auto_deletes_enabled flags")
        self._total_ops_recorded["vars"]["stop_writes"] = False
        self._total_ops_recorded["vars"]["auto_deletes_enabled"] = False
      return True
    return False

  def _update_bucket_to_marker_map(self, bucket):
    if not self._bucket_info["bucket_to_marker_map"]:
      for bucket in self._bucket_info["bucket_names"]:
        if bucket not in self._bucket_info["bucket_to_marker_map"]:
          self._bucket_info["bucket_to_marker_map"][bucket] = {}

  def _update_ei_results(self, comp, op, ftime, recovery_time):
    rtime = "%s (%s s)"%(recovery_time, (recovery_time-ftime).total_seconds())
    if comp not in self._total_ops_recorded["ei"]:
      self._total_ops_recorded["ei"][comp] = {op:{"count":1,
              "Last.Recorded.Time":ftime,
              "Last.Recovery.Time":"%s"%(rtime)}}
      return
    if op not in self._total_ops_recorded["ei"][comp]:
      self._total_ops_recorded["ei"][comp][op] = {"count":1,
                                                  "Last.Recorded.Time":ftime,
                                                  "Last.Recovery.Time":rtime}
      return
    self._total_ops_recorded["ei"]["count"] += 1
    self._total_ops_recorded["ei"]["Last.Recorded.Time"] = ftime
    self._total_ops_recorded["ei"]["Last.Recovery.Time"] = rtime
    self._total_ops_recorded["ei"][comp][op]["count"] += 1
    self._total_ops_recorded["ei"][comp][op]["Last.Recorded.Time"] = ftime
    self._total_ops_recorded["ei"][comp][op]["Last.Recovery.Time"] = rtime

  def _is_nfswrite_workload_running(self):
    if self._is_nfswrite_workload or self._is_nfscopy_workload or \
         self._is_nfsrename_workload or self._is_nfs_overwrite_workload or \
         self._is_nfsfio_workload or self._is_nfsvdbench_workload:
      return True
    return False

  def _update_read_map(self, nfs_mount_options):
    for bucket in self._bucket_info["bucket_names"]:
      if self._is_write_workload or self._is_copy_workload or \
          self._is_nfswrite_workload_running():
        self._get_expiry_obj_prefix(bucket)
      if self._nfs_workload_running:
        INFO("Registering %s to NFSOps"%(bucket))
        self._nfsops.register(bucket, nfs_mount_options)
      if self._is_read_workload_running or self._nfs_workload_running:
        self._update_bucket_to_marker_map(bucket)
    nfs_buckets = self._total_ops_recorded.get("nfsops", {}).get("mounts", {})
    if self._nfs_workload_running and not nfs_buckets:
      raise Exception("No NFS Enabled Bucket found")
    INFO("Local NFS Map : %s"%(nfs_buckets))
    INFO("Local Read Map : %s"%(self._bucket_info["bucket_to_marker_map"]))

  def _get_worker_filemarker(self):
    return "%s.json"%(path.join(self.logpath,
                      "".join(currentThread().name.split(" "))))

  def _validate_and_delete_objects(self, bucket, res, skip_head_before_delete,
                               skip_head_after_delete, skip_read_before_delete,
                               skip_version_deletes):
    objects = []
    dmarkers = size = skipped = 0
    all_versions = res.get("Versions", []) if "Versions" in res else res.get(
                                                                "Contents", [])
    all_versions = self._lock_unblock_all_objs(bucket, all_versions, True)
    if not self._kwargs["skip_expired_object_read"]:
      all_versions = self._skip_expired_objects(bucket, all_versions)
    for i in all_versions:
      if not skip_head_before_delete:
        headres = self._head_object(bucket, i.get("Key"), i.get("VersionId"),
                                   ignore_errors=["Not Found", "NoSuchVersion"])
        if headres and headres.get("ObjectLockMode", None):
          skipped += 1
          continue
      current_key = {"Key" :i.get("Key")}
      if "VersionId" in i: current_key["VersionId"] = i["VersionId"]
      objects.append(current_key)
      size += i["Size"]
    for i in res.get("DeleteMarkers", []):
      current_key = {"Key" :i.get("Key")}
      if "VersionId" in i: current_key["VersionId"] = i["VersionId"]
      objects.append(current_key)
      dmarkers += 1
    if objects:
      self._delete_objects_from_bucket(bucket, objects, size, dmarkers, skipped,
                 skip_head_after_delete, skip_read_before_delete, all_versions,
                 skip_version_deletes)
      return self._lock_unblock_all_objs(bucket, all_versions, False, True)
    if self._delete_empty_bucket:
      if bucket not in self._bucket_info["delete_bucket_to_marker_map"
                                                         ]["empty_buckets"]:
        total_buckets = len(self._bucket_info["bucket_names"])
        empty_buckets = len(self._bucket_info["delete_bucket_to_marker_map"
                                                         ]["empty_buckets"])
        INFO("Adding bucket to remove list : %s, Total Buckets : %s "
             "Active Buckets : %s, To_Remove buckets : %s"%(bucket,
             total_buckets, total_buckets-empty_buckets, empty_buckets))
        self._bucket_info["delete_bucket_to_marker_map"]["empty_buckets"
                                                          ].append(bucket)
      sleep(1)

  def _validate_deleted_objects(self, bucket, objects):
    for obj in objects:
      if self._time_to_exit:
        print "Skipping delete validation for %s/%s"%(bucket, obj)
        return
      res = self._head_object(bucket, obj["Key"], obj.get("VersionId"),
                        ignore_errors=["Not Found", 'NoSuchVersion'])
      if res:
        self._set_exit_marker("FATAL : Bucket/Obj : %s/%s (VersionID - %s) is "
                            "deleted but head returned success, StatusCode : %s"
                            %(bucket, obj["Key"], obj["VersionId"], res),
                            exception=''.join(format_exc()))

  def _wait_for_all_workers(self, timeout=1800, force_kill=False):
    """
    waits for all workers to free, and if force_kill=True, it will stop all the
    threads forcefully

    Args:
      timeout(int,Optional): timeout Default:1800
      force_kill(bool,Optional): flag to forcefully stop all the threads
    """
    timeout_80perc = int(timeout*0.8) #80% of timeout
    start_time = 0
    while start_time < timeout:
      if force_kill and start_time > timeout_80perc:
        WARN("Trying to kill all active workers after %s secs"%(timeout_80perc))
        self._task_manager._stop_all_workers()
        force_kill = False
      active_workers = self._task_manager.get_active_workers(verbose=True)
      INFO("Waiting for all workers to finish : %s, Time elapsed : %s secs"
           %(active_workers, start_time))
      athrds = sum([active_workers[i] for i in active_workers.keys()])
      if athrds < 1:
        INFO("All workloads exited")
        return True
      if athrds == 1 and active_workers.get("monitor", 0) == 1:
        if self._kwargs["start_webserver_logging"]:
          INFO("All workloads exited except start_webserver_logging, ignoring")
          return True
      sleep(60)
      start_time += 60
    ERROR("Timeout while waiting for all workers to exit")
    return False

class Metadata():
  def __init__(self, testobj):
    self._testobj = testobj

  def list(self):
    pass

class Monitor():
  def __init__(self, testobj):
    self._testobj = testobj
    self._utils = Utils()
    self._counter = 0
    self._checks = ["check_if_time_to_exit", "check_if_execution_completed",
                    "check_exit_marker_file", "check_mem_usage",
                    "check_failures_to_tolerate", "check_if_timeout_reached",
                    "check_dump_records", "check_enable_auto_workload",
                    "check_may_be_stop_writes", "check_live_workers",
                    "check_active_compactions"]

  def run(self):
    self._counter =+ 1
    failed_or_passed_check = None
    for check in self._checks:
      if failed_or_passed_check:
        DEBUG("Skipping Check %s, since one of the previous check created exit "
              "marker"%(check, failed_or_passed_check))
        continue
      res = getattr(self, check)()
      if res in [True, False]:
        DEBUG("Check %s, return %s. Skipping all remaining checks"%(check, res))
        failed_or_passed_check = check
    if self._counter%4 == 0:
      self._counter = 0

  def check_active_compactions(self):
    if not self._testobj._oss_cluster:
      return
    cstats = self._testobj._oss_cluster.ms.get_compaction_stats()
    for ms, stats in cstats.iteritems():
      for active_compactions in stats["active_compactions"]:
        self._total_ops_recorded["compaction_stats"][active_compactions] = []
      for pending_compactions in stats["pending_compactions"]:
        self._total_ops_recorded["compaction_stats"][pending_compactions] = []

  def check_live_workers(self):
    if self._counter%4 == 0:
      verbose = False
      if self._testobj._exit_test(verbose=False):
        verbose = True
      workers = self._testobj._task_manager.get_active_workers(verbose=True)
      print "Active Workers : %s"%workers
      #if active_workers:
      #  self._testobj._total_ops_recorded["TestStatus"]["live_workloads"
      #                                                        ] = active_workers

  def check_mem_usage(self):
    rss_limit = self._testobj._kwargs["rss_limit"]
    mem_usage = Process(self._testobj._pid).memory_info().rss
    self._testobj._total_ops_recorded["TestStatus"]["memory_usage"
                                        ] = self._utils.convert_size(mem_usage)
    if mem_usage > 2*1024*1024*1024 or self._testobj._kwargs[
                                              "enable_memory_leak_debugging"]:
      self._utils.dump_memory_info(filename=path.join(self._testobj.logpath,
                                                      "mem.info"))
    if mem_usage > rss_limit:
      ERROR("Exceeded RSS limit %s (Framework is using : %s). Stopping workload"
            %(rss_limit , mem_usage))
      self._testobj._stop_workload("Hit RSS limit of %s"%(rss_limit))
      return True

  def check_if_execution_completed(self):
    if not self._testobj._execution_completed:
      return
    INFO("Execution is completed, Stopping workload")
    self._testobj._stop_workload("Execution Completed")
    return True

  def check_failures_to_tolerate(self):
    total_failures = self._testobj._total_ops_recorded["totalfailure"]
    failures_to_tolerate = self._testobj._kwargs.get("failures_to_tolerate", -1)
    if failures_to_tolerate < 0:
      return
    if total_failures > failures_to_tolerate:
      ERROR("Total failures are above acceptable range. Found vs acceptable : "
            "%s vs %s"%(total_failures, failures_to_tolerate))
      self._testobj._stop_workload("Too many failures found")
      return True
    else:
      WARN("Total failed tasks : %s"%(failures_to_tolerate))

  def check_exit_marker_file(self):
    if not path.isfile(self._testobj._exit_marker):
      return
    WARN("Exit marker %s found. Preparing to exit"%(self._testobj._exit_marker))
    self._testobj._stop_workload("Exit marker found")
    return True

  def check_if_time_to_exit(self):
    if not self._testobj._time_to_exit:
      return
    self._testobj._stop_workload("Exit marker found")
    if not self._testobj._total_ops_recorded["vars"]["change_workload_detected"]:
      ERROR("Exit marker found w/o change_workload_detected. Exiting monitor")
      return True

  def check_if_timeout_reached(self):
    if not (self._testobj._timeout > -1 and
            (time() - self._testobj._start_time) > self._testobj._timeout):
      return
    INFO("Reason - Test timeout reached. Setting Execution_completed=True")
    self._testobj._total_ops_recorded["vars"]["execution_completed"] = True
    self._testobj._total_ops_recorded["vars"]["test_timeout_reached"] = True

  def check_dump_records(self):
    if self._counter%4 == 0:
      self._testobj._dump_records()
      self._testobj._check_live_workers()

  def check_enable_auto_workload(self):
    if not self._testobj._kwargs["enable_auto_workload"]:
      return
    if not self._testobj._may_be_change_workload_profile():
      return
    if self._testobj._time_to_exit:
      ERROR("Workload change is called, but exit marker is already set."
            "Disabling workload change")
      self._testobj._total_ops_recorded["vars"]["change_workload_detected"
                                                                      ] = False
      return False
    INFO("Workload is detected to be stable. Workload change is called.")
    return True

  def check_may_be_stop_writes(self):
    self._testobj._may_be_stop_writes()

class ObjNameDistributor():
  def __init__(self, testobj):
    self._testobj = testobj
    self._lock = RLock()
    self._utils = Utils()

  def may_be_lock_unlock_objs(self, objs, action, unlock_on_failure=True):
    res = True
    objs_to_unblock_on_failure = []
    for obj in objs:
      if action == Constants.LOCK:
        if not self.may_be_lock_unlock_obj(obj["bucket"], obj["objname"],
                                            obj["size"], Constants.LOCK):
          res = False
          if unlock_on_failure:
            self.may_be_lock_unlock_objs(objs_to_unblock_on_failure,
                                          Constants.UNLOCK)
            if obj.get("objtype") == "src":
              self._testobj._total_ops_recorded["anomalis"][
                                      "SrcCopyObjCouldNotLock"]["count"] += 1
            return res
        else:
          objs_to_unblock_on_failure.append(obj)
      elif action == Constants.UNLOCK:
        if not self.may_be_lock_unlock_obj(obj["bucket"], obj["objname"],
                                            obj["size"], Constants.UNLOCK):
          res = False
    return res

  def may_be_lock_unlock_obj(self, bucket, key, size, action, debug=False):
    """
    locks or unlocks the given object_name by putting it in "active_objects" or
    deleting it

    Args:
      bucket(str): bucket name of the object
      key(str): object name to be locked
      size(int): object_size
      action(str): Constants.LOCK if you want to lock, Constannt.UNLOCK if you
                   want to unlock
      debug(bool,Optional): True, if you want to log verbose
                            Default: False
    """
    #objkey = "%s:%s:%s"%(bucket, key, size)
    objkey = "%s:%s"%(bucket, key)
    thrdname = currentThread().name
    if action == Constants.LOCK:
      if self._is_obj_locked(bucket, key, size):
        if self._testobj._kwargs["verbose_logging_for_locks"] or debug:
          ERROR("LOCK : %s failed to lock by %s (Current lock held by : %s"
                %(objkey, thrdname,
               self._testobj._total_ops_recorded["active_objects"].get(objkey)))
        return False
      with self._lock:
        if self._is_obj_locked(bucket, key, size):
          return False
        if self._testobj._kwargs["verbose_logging_for_locks"] or debug:
          INFO("LOCK : %s is being locked by %s"%(objkey, thrdname))
        self._testobj._total_ops_recorded["active_objects"][objkey] = {
                                          "size":size, "caller":thrdname}
      return True
    if action == Constants.UNLOCK:
      if self._testobj._kwargs["verbose_logging_for_locks"] or debug:
        INFO("UNLOCK : %s is unlocked by %s"%(objkey, thrdname))
      if self._is_obj_locked(bucket, key, size):
        if thrdname != self._testobj._total_ops_recorded[
                                      "active_objects"][objkey]["caller"]:
          ERROR("UNLOCK : %s was locked by %s,but being unlocked by %s"%(objkey,
                self._testobj._total_ops_recorded["active_objects"][objkey][
                                                        "caller"], thrdname))
        del(self._testobj._total_ops_recorded["active_objects"][objkey])
      elif self._testobj._kwargs["verbose_logging_for_locks"] or debug:
        ERROR("UNLOCK : %s, Failed since object is not locked by anyone"%objkey)
      return True

  def _is_obj_locked(self, bucket, key, size=None):
    """
    returns if object is already locked or not

    Args:
      bucket(str): bucket name of the object
      key(str): object name to be locked

    Returns: True if object is already locked, False otherwise
    """
    keyname = "%s:%s"%(bucket, key)
    if keyname in self._testobj._total_ops_recorded["active_objects"]:
      return True
    return False

  def get_dst_obj_for_copy(self, bucket, count, srckey, srcsize,
                           srctag, workload):
    objprefix = self.get_object_prefix(count)
    obj_version = self._get_object_version_for_copy(srckey)
    sd, mpo = "", ""
    is_zero = Constants.ZEROBYTE_OBJ_IDENTIFIER if srcsize == 0 else ""
    if Constants.STATIC_DATA_IDENTIFIER in srckey:
      sd = Constants.STATIC_DATA_IDENTIFIER
    if "-" in srctag or Constants.MULTIPART_OBJ_IDENTIFIER in srckey:
      mpo = Constants.MULTIPART_OBJ_IDENTIFIER
    current_gmt_time = strftime("%d.%m.%Y..%H.%M.%S.GMT", gmtime())
    prefix_from_bucket = self._testobj._bucket_info["prefixes"][bucket]
    workload_idn = Constants.OBJ_IDN_FOR_S3COPY
    if workload == "nfscopy":
      workload_idn = Constants.OBJ_IDN_FOR_NFSCOPY
    elif workload == "nfsrename":
      workload_idn = Constants.OBJ_IDN_FOR_NFSRENAME
    if self._testobj._is_sparse_object(srckey, srckey):
      objprefix = objprefix.replace(objprefix[0:6],
                                    Constants.SPARSE_FILE_DEFAULT_PREFIX, 1)
    exp_expiry = self._testobj._estimated_obj_expiry_time(bucket, objprefix)
    keyname = "%s%sC%s%s%s%s%s%s_%s_%s"%(objprefix, self._testobj._uniq_obj_idn,
                  count, sd, mpo, obj_version, is_zero,
                  workload_idn, exp_expiry, current_gmt_time)
    return keyname

  def get_objname_and_upload_type(self,  objcount, is_multipart=False,
                                   is_nfs=False, sizes=None, is_sparse=False,
                                   workload=None):
    objname_prefix = self.get_object_prefix(objcount, is_sparse=is_sparse,
                                             workload=workload)
    if not self._testobj._static_data:
      sd_prefix = ""
    else:
      sd_prefix = Constants.STATIC_DATA_IDENTIFIER
    objprefix = "%s%s%s"%(objname_prefix, sd_prefix, Constants.OBJ_VERSION)
    objsize = 0
    if (objcount % self._testobj._kwargs["zero_byte_obj_index"]) == 0:
      objprefix = "%s%s"%(objprefix, Constants.ZEROBYTE_OBJ_IDENTIFIER)
    else:
      objsize = self.get_objsize(sizes=sizes)
    bucket = choice(self._testobj._bucket_info["bucket_names"])
    if is_nfs:
      objprefix = "%s%s"%(objprefix, Constants.NFS_OBJ_IDENTIFIER)
      bucket=choice(self._testobj._nfsops._registered_buckets)
    num_versions = randint(self._testobj._kwargs["num_versions"][0],
                           self._testobj._kwargs["num_versions"][-1])
    if not self._testobj._bucket_info[bucket]["versioning"]:
      num_versions = 1
    put_type = choice(self._testobj._upload_types)
    exp_expiry = self._testobj._estimated_obj_expiry_time(bucket, objprefix)
    current_gmt_time = strftime("%d.%m.%Y..%H.%M.%S.GMT", gmtime())
    prefix_from_bucket = self._testobj._bucket_info["prefixes"][bucket]
    num_parts = 1
    if (len(self._testobj._upload_types) == 1 and put_type == "multipart"
        ) or (len(self._testobj._multipart_objects_map) < self._testobj._kwargs[
            "num_parallel_multiparts"] and put_type == "multipart"):
      if objsize > 5*1024*1024:
        num_parts = randint(self._testobj._kwargs["num_parts"][0],
                           self._testobj._kwargs["num_parts"][-1])
      objname = "%s%sC%s%s%s_%s_%s_%s"%(objprefix, objcount,
                    self._testobj._uniq_obj_idn,
                    Constants.MULTIPART_OBJ_IDENTIFIER, num_parts,
                    prefix_from_bucket, exp_expiry, current_gmt_time)
      #return bucketname, objname, versions_to_create, num_parts, objsize
      return bucket, objname, num_versions, num_parts, objsize
    objname = "%sC%s%s%s_%s_%s"%(objprefix, objcount,
                                self._testobj._uniq_obj_idn, prefix_from_bucket,
                                exp_expiry, current_gmt_time)
    return bucket, objname, num_versions, 0, objsize

  def get_objsize(self, sizes):
    if not sizes:
      sizes = self._testobj._total_ops_recorded["workload_profiles"][-1][
                                                                      "objsize"]
    if len(sizes) == 1:
      return sizes[0]
    if sizes[0] == sizes[1]:
      return sizes[0]
    if sizes[1] < 1024*1024:
      return randrange(sizes[0], sizes[1], 1024)
    size = randrange(sizes[0], sizes[1], 1024)
    return size

  def get_object_prefix(self, objcount, randomobj=True, objname_prefix = None,
                         is_sparse=False, workload=None):
    if not objname_prefix:
      objname_prefix =choice(self._testobj._bucket_info["prefixes"]["prefixes"])
    if is_sparse:
      objname_prefix = Constants.SPARSE_FILE_DEFAULT_PREFIX
    if workload:
      objname_prefix = getattr(Constants, "%s_WORKLOAD"%workload.upper())
    if objname_prefix not in self._testobj._total_ops_recorded[
                                                        "prefix_distribution"]:
      self._testobj._total_ops_recorded["prefix_distribution"
                                  ].setdefault(objname_prefix, {"count" : 0})
    self._testobj._total_ops_recorded["prefix_distribution"][
                                                  objname_prefix]["count"] += 1

    delimeter_obj = self._testobj._kwargs["delimeter_obj"]
    num_leaf_objects = self._testobj._kwargs["num_leaf_objects"]
    obj_depth = self._testobj._kwargs["depth"]
    if randomobj:
      if not delimeter_obj:
        delimeter_obj = choice([True, False])
    if not delimeter_obj:
      return objname_prefix
    if not self._testobj._delimeter_prefix or \
                  objcount%self._testobj._num_leaf_objects == 0:
      with self._lock:
        if not self._testobj._delimeter_prefix or \
                  objcount%self._testobj._num_leaf_objects == 0:
          self._testobj._num_leaf_objects = randint(num_leaf_objects[0],
                                                  num_leaf_objects[1])
          self._testobj._delimeter_prefix = self._testobj._get_key(obj_depth)
    if is_sparse and not self._testobj._delimeter_prefix.startswith(
                                      Constants.SPARSE_FILE_DEFAULT_PREFIX):
      return Constants.SPARSE_FILE_DEFAULT_PREFIX+self._testobj._delimeter_prefix
    if not is_sparse and self._testobj._delimeter_prefix.startswith(
                                      Constants.SPARSE_FILE_DEFAULT_PREFIX):
      return objname_prefix+self._testobj._delimeter_prefix.replace(
              Constants.SPARSE_FILE_DEFAULT_PREFIX,
              choice(self._testobj._bucket_info["prefixes"]["prefixes"]), 1)
    return objname_prefix+self._testobj._delimeter_prefix


  def get_object_from_bucket(self, prefix,
                              expected_obj_size=17*1024*1024*1024*1024,
                              debug=False, nfs_workload=False, workload=None,
                              dstbucket=None):
    workload = workload.lower() if workload else workload
    self._testobj._total_ops_recorded["misc"].setdefault(
                                                workload, {"delay" : 0})
    delay = 0
    while not self._testobj._time_to_exit:
      bucket = self._get_bucket_for_copy(prefix, nfs_workload)
      obj = self._get_object_for_copy(bucket, prefix)
      if obj:
        if dstbucket == bucket and \
            Constants.MULTIPART_OBJ_IDENTIFIER not in obj["Key"]:
          is_nfs_enabled = self._testobj._is_nfs_enabled_on_bucket(bucket)
          if is_nfs_enabled and workload == "nfsrename":
            return bucket, obj["Key"], obj["Size"], obj["ETag"], obj.get(
                                                            "VersionId")
          elif not is_nfs_enabled and workload == "copy":
            return bucket, obj["Key"], obj["Size"], obj["ETag"], obj.get(
                                                            "VersionId")
        if obj["Size"] <= expected_obj_size:
          return bucket, obj["Key"], obj["Size"], obj["ETag"], obj.get(
                                                                "VersionId")
      self._testobj._total_ops_recorded["misc"][workload]["delay"] += 1
      if self._testobj._total_ops_recorded["misc"][workload]["delay"]%30 == 0:
        delay += 30
        ERROR("No object found for %s workload for %s secs (Prefix : %s), "
                "Retrying in 1sec."%(workload, delay, prefix))
      sleep(1)
    return None, None, 0, None, None

  def _get_bucket_for_copy(self, prefix, nfs_workload):
    """
    randomly selects bucket from bucket_info map

    Args:
      prefix(str): prefix to pass to _update_list_for_copy_map
      nfs_workload(bool): True if we want nfs enbled bucket, False otherwise

    Returns: str
      Randomly selected bucket name
    """
    bucket =None
    if nfs_workload:
      bucket = choice(self._testobj._nfsops._registered_buckets)
    else:
      bucket = choice(self._testobj._bucket_info["bucket_names"])
    with self._lock:
      self._update_list_for_copy_map(bucket, prefix)
    return bucket

  def _update_list_for_copy_map(self, bucket, prefix=None, objs=None,
                                marker=None, vmarker=None):
    """
    updates map values in "list_for_copy"

    Args:
      bucket(str): bucket name
      prefix(str): prefix to copy
      objs([str]): new list of object names to update
      marker(str): key marker to generate new list of objects when current list
                   goes empty
      vmarker(str): version marker for corresponding key marker in case of versioned bucket
    """
    if bucket not in self._testobj._bucket_info["list_for_copy"]:
      self._testobj._bucket_info["list_for_copy"][bucket] = {}
    if prefix and prefix not in self._testobj._bucket_info[
                                                  "list_for_copy"][bucket]:
      self._testobj._bucket_info["list_for_copy"][bucket][prefix] = {
                                       "update_in_progress":False,
                                       "objs":[], "marker":"", "vmarker":"",
                                       "last_update_time":time()}
    if objs:
      if not self._testobj._bucket_info["list_for_copy"][
                                                        bucket][prefix]["objs"]:
        self._testobj._bucket_info["list_for_copy"][bucket][prefix]["objs"]=objs
        self._testobj._bucket_info["list_for_copy"][bucket][prefix][
                                                    "last_update_time"] = time()
      else:
        return
    if marker is not None:
      self._testobj._bucket_info["list_for_copy"][
                                            bucket][prefix]["marker"] = marker
    if vmarker is not None:
      self._testobj._bucket_info["list_for_copy"][
                                          bucket][prefix]["vmarker"] = vmarker

  def _get_object_for_copy(self, bucket, prefix):
    """
    returns the object with current prefix map object list, if the list is empty
    it will update the map and will not return anything

    Args:
      bucket(str): bucket name from which you want to copy the object
      prefix(str): prefix of the object name you want to copy

    Returns: str or None
      if object list is not empty then it will return object_name otherwise None
    """
    with self._lock:
      if self._testobj._bucket_info["list_for_copy"][bucket][prefix]["objs"]:
        return self._testobj._bucket_info[
                                  "list_for_copy"][bucket][prefix]["objs"].pop()
    res, istrunc = None, None
    vmarker = self._testobj._bucket_info["list_for_copy"][
                                                      bucket][prefix]["vmarker"]
    kmarker = self._testobj._bucket_info["list_for_copy"][
                                                       bucket][prefix]["marker"]
    if self._testobj._bucket_info[bucket]["versioning"]:
      res, vmarker, kmarker, istrunc = self._testobj._list_objects(
                bucket=bucket, versions=True, vmarker=vmarker, kmarker=kmarker,
                prefix=prefix)
    else:
      res, kmarker, istrunc = self._testobj._list_objects(bucket=bucket,
                                prefix=prefix, versions=False, marker=kmarker)
    if not istrunc:
      vmarker, kmarker = "", ""
    with self._lock:
      objs = res.get("Contents") if "Contents" in res else res.get(
                                                                  "Versions",[])
      self._update_list_for_copy_map(bucket, prefix, objs, kmarker, vmarker)

  def _get_object_version_for_copy(self, src_obj_name):
    if Constants.OBJ_VERSION in src_obj_name:
      return Constants.OBJ_VERSION
    elif Constants.LEGACY_OBJ_VERSION in src_obj_name:
      return Constants.LEGACY_OBJ_VERSION
    else:
      return "_"

  def _discard_cached_list(self, maps=None, filename=None):
    """
    this method will append the current list_for_copy map at the end of the file
    given by filename and reset _bucket_info["list_for_copy"] to empty dict

    Args:
      filename(str,Optional): file to write the data
                              Default: Constants.CACHE_DISCARD_FILENAME
    """
    if not filename:
      filename = path.join(self._testobj.logpath,
                           Constants.CACHE_DISCARD_FILENAME)
    INFO("Discarding all the list_for_copy maps. Filename : %s"%filename)
    data = "%s\n%s : list_for_copy\n%s"%("-"*100, datetime.now(), "-"*100)
    self._utils.write_to_file(data, filename, "a")
    self._utils.write_to_file(dumps(self._testobj._bucket_info["list_for_copy"],
                                    indent=2, default=str), filename, "a")
    self._utils.write_to_file("%s\n"%("*"*150), filename, "a")
    self._testobj._bucket_info["list_for_copy"] = {}

class DynamicExecution():
  """
  this class will handle thread creation, assigning workloads to thread and
  stopping the threads, it uses queues for assigning the workloads to threads,
  it maintains the self.queues dict which has quota_type as keys and Queue
  object as value and every thread will be tied to one of the quota type, and
  will execute methods only in that queue
  """
  def __init__(self, threadpoolsize=10, timeout=7200, quotas=None,
        strict_quota_workers=["nfsdelete", "nfsrename", "delete", "nfsowrite"]):
    """
    it will initialize the dynamic execution class

    Args:
      threadpoolsize(int,Optional): no of threads that workload wants to create
                                     Default: 10
      timeout(int,Optional): no of seconds to wait before throwing timeout exception while adding load to queue
                             Default: 7200
      quotas(dict,Optional): dictionary of workload_name and its percentage
                             Default: self._default_quotas
      strict_quota_workers([str]): TBD
    """
    self._poolsize = threadpoolsize
    self._timeout = timeout
    self._strict_quota_workers = strict_quota_workers
    self._default_quotas = {"generic":{"perc":100},
              "read":{"perc":0},
              "write":{"perc":0},
              "delete":{"perc":0}
              }
    if not quotas:
      quotas = self._default_quotas
    self._quotas = quotas
    self._queues = {}
    self._time_to_exit = False
    self._workers = []
    self._lock = RLock()
    self._init_workers()

  def add(self, target, **kwargs):
    """
    it will add the target method to queue of quota_type provided in kwargs

    Raises: Exception
      if queue is raising exception on queue.put even after self._timeout time
      then it will raise the exception
    """
    timeout = kwargs.pop("timeout", self._timeout)
    quota_type = kwargs.pop("quota_type")
    queue = self._queues[quota_type]
    while timeout > 0:
      if self._time_to_exit:
        INFO("Exit is called, marking %s as no-op, Type : %s"
             %(target, quota_type))
        return
      try:
        queue.put({"target":target, "kwargs":kwargs}, timeout=1)
        return
      except:
        pass
      timeout -= 1
    raise Exception("Timeout after %s, while adding task : %s, Type : %s"
                    %(self._timeout, target, quota_type))

  def complete_run(self, wait_time=1, timeout=600):
    """
    it will set self._time_to_exit to True, so execute method of every worker
    will exit the loop and will set is_active=False, after setting the
    _time_to_exit, it will pop out every worker from self._workers

    Args:
      wait_time(int,Optional): sleep time after every iteration in while loop
                               Default: 1
      timeout(int,Optional): Default: 600
    """
    INFO("Complete run is called. Setting EXIT-MARKER=TRUE in DynamicExecution"
         ", Timeout : %s"%timeout)
    self._time_to_exit = True
    stime = time()
    while (len(self._workers) > 0) or (time()-stime > 600):
      INFO("Number of active workers : %s"%(self._workers))
      for worker in self._workers:
        if not worker.is_active():
          self._workers.pop(worker)
      sleep(wait_time)

  def wait_for_all_worker_to_be_free(self, wait_time=1, timeout=300):
    """
    it will check whether all the workers are free currently

    Args:
      wait_time(int,Optional): wait_time, Default: 1
      timeout(int,Optional): timeout, Default: 300

    Returns: bool
      True, if all workers gets free before time, False otherwise
    """
    sleep(1)
    etime = time()+timeout
    while etime-time() > 0:
      free_workers = 0
      for worker in self._workers:
        if worker.is_free():
          free_workers += 1
      print ("Number of workers : %s (Tasks : %s), Free : %s"
             %(len(self._workers),
             [{"task_name":t._active_task, "is_free":t.is_free()}
              for t in self._workers if t._active_task and not t.is_free()],
               free_workers))
      if free_workers == len(self._workers):
        return True
      sleep(wait_time)
    WARN("Not all workers are free'ed. Total/Active/Free workers : %s / %s / %s"
         %(len(self._workers), len(self._workers)-free_workers, free_workers))
    return False

  def _stop_all_workers(self, quota_type=None):
    """
    stops all the threads for given quota_type

    Args:
      quota_type(str): workload type
                       Default: self._quotas.keys()
    """
    quota_types = [quota_type]
    if not quota_type:
      quota_types = self._quotas.keys()
    INFO("Stopping all the workers : %s"%(quota_types))
    for quota_type in quota_types:
      for thrd in self._quotas[quota_type]["threads"]:
        if thrd.isAlive():
          try:
            INFO("Stopping worker : %s"%(thrd.name))
            thrd._Thread__stop()
          except Exception as err:
            ERROR("Failed to stop worker : %s, Error : %s"%(thrd.name, err))

  def get_active_workers(self, quota_type=None, verbose=False):
    """
    this method will check the no of thread that are alive for given quota_type

    Args:
      quota_type(str,Optional): quota_type to check
                                Default: self._quotas.keys()
      verbos(bool,Optional): flag
                             Default: False

    Returns: dict
      key: quota_type
      value: number of live threads of quota_type
    """
    res = {}
    quota_types = [quota_type]
    if not quota_type:
      quota_types = self._quotas.keys()
    for quota_type in quota_types:
      count = 0
      for thrd in self._quotas[quota_type]["threads"]:
        if thrd.isAlive():
          if verbose:
            INFO("Worker : %s, busy with %s job"
                 %(thrd.name, thrd._Thread__target.__name__))
          count += 1
      res[quota_type] = count
    return res

  def size(self):
    """
    returns self._poolsize
    """
    return self._poolsize

  def _assign_workers(self):
    """
    this method will create the queue for every workload and will store it in
    self._queues[quota_type]
    """
    total_workers = 0
    for key in self._quotas.keys():
      num_workers = qlength = 0
      quota = self._quotas[key]
      if quota.get("perc"):
        num_workers = (quota["perc"] * self._poolsize)/100
        if num_workers < 1:
          num_workers = 1
        qlength = num_workers*2
      elif quota.get("num_workers"):
        num_workers = quota.get("num_workers")
        qlength = num_workers
      else:
        ERROR("Unknown workload")
        continue
      total_workers = total_workers + num_workers
      quota["num_workers"] = num_workers
      self._quotas[key] = quota
      INFO("Creating queue for workload : %s, with quota : %s, Queue : %s"
           %(key, quota, qlength))
      queue = Queue(qlength)
      self._queues[key] = queue
    free_threads =  self._poolsize - total_workers
    INFO("Total workers created : %s, Queue : %s"
         %(total_workers, total_workers*2))
    if free_threads > 0:
      #TODO add free threads to generic pool
      pass

  def _get_task(self, worker_type, timeout=None):
    """
    this method will be called by worker._get_task
    it will return the task from respective worker_type(quota_type)
    queue

    Args:
      worker_type(str): quota_type
      timeout(int,Optional): timeout before raising the exception

    Returns: dict
      target: the method to be executed by thread
      kwargs: kwargs to pass to method
    """
    stime = time()
    while not self._time_to_exit:
      if self._queues[worker_type].empty():
        sleep(0.1)
        continue
      try:
        with self._lock:
          task = self._queues[worker_type].get(timeout=0.1)
          return task
      except Exception as err:
        ERROR("Failed to get item from Queue : %s, Since : %s secs"
              %(worker_type, time()-stime))
        if timeout and (stime-time() > timeout):
          raise Exception("Timeout")
        continue
    while not self._queues[worker_type].empty():
      INFO("Emptying workload:%s Queue"%(worker_type))
      try:
        self._queues[worker_type].get(timeout=1)
      except Exception as err:
        ERROR("Failed to empty Queue : %s. Error : %s" %(worker_type, err))
        continue

  def _init_workers(self):
    """
    this method intialize workers and threads needed for each
    quota_type(workload) and assign the each list of threads to
    self._quotas[quota_type]["threads"]
    """
    self._assign_workers()
    for quota_type in self._quotas.keys():
      INFO("Creating workers for : %s workload, with args : %s"
           %(quota_type, self._quotas[quota_type]))
      num_workers = self._quotas[quota_type]["num_workers"]
      threads = []
      for i in range(num_workers):
        worker_name = "%sWorkerThread %s"%(quota_type.title(), i)
        wthread = self._start_worker(quota_type, worker_name, True)
        threads.append(wthread)
      self._quotas[quota_type]["threads"] = threads

  def _start_worker(self, quota_type, worker_name, daemon, retries=3):
    """
    this method will intialize the worker of given quota_type, creates a
    thread and set the target of thread as execute method of worker and
    also pushes the worker object to self._workers list

    Args:
      quota_type(str): workload to initialize the worker object with
      worker_name(str): name to assign to the thread
      daemon(bool): True, if we want to set the thread as daemon thread
      retries(int,Optional): no of retries while creating the thread
                             Default: 3
    """
    while retries > 0:
      retries -= 1
      try:
        initworker = worker(self, quota_type, worker_name)
        wthread = Thread(target=initworker.execute)
        wthread.name =  worker_name
        wthread.daemon = daemon
        wthread.start()
        self._workers.append(initworker)
        return wthread
      except KeyError as kerr:
        ERROR("Failed to start the worker %s, Retries left : %s, Error : %s"
              %(worker_name, retries, err.message))
        if retries == 0:
          raise

class worker:
  def __init__(self, pe, worker_type, worker_name):
    self._pe = pe
    self._type = worker_type
    self._is_active = True
    self._is_free = True
    self._active_task = None
    self._name = worker_name
    INFO("Initializing worker %s, for workload : %s"%(self._name, self._type))

  def execute(self):
    while not self._pe._time_to_exit:
      task = self._get_task()
      if not task:
        continue
      self._execute_task(task)
    self._is_active = False
    INFO("Exit is called. Exiting worker %s, type %s"%(self._name, self._type))

  @property
  def type(self):
    return self._type

  def is_active(self):
    return self._is_active

  def is_free(self):
    return self._is_free

  def _execute_task(self, task):
    self._is_free = False
    self._is_active = True
    target = task.pop("target")
    self._active_task = target.__name__
    kwargs = task.pop("kwargs")
    try:
      target(**kwargs)
      self._is_free = True
      self._active_task = None
    except Exception as err:
      self._active_task = None
      self._is_free = True
      ERROR("Worker %s (type : %s), hit exception while executing task : %s,"
            "Err : (%s) %s, Trace : %s"%(self._name, self._type, target,
                                          type(err), err, format_exc()))
      self._is_active = False
      if not self._pe._time_to_exit:
        raise

  def _get_task(self):
    try:
      return self._pe._get_task(self._type)
    except Exception as err:
      if "Timeout" in err.message:
        return None
      ERROR("Worker %s (type : %s), hit exception while getting task : %s"
            %(self._name, self._type, err))
      self._is_active = False
      raise

class RandomData():
  """this class defines methods to generate and read random data"""

  def __init__(self, size, compressiable=True, zero_data=False,
               static_data=False, generate_md5=True):
    """
    Initialize the object

    Args:
      size(int): size of the data
      compressible(bool,Optional): if True, it generates local_static_data
                                    which is compressible
                                    Default: True
      zero_data(bool,Optional): if True, generates data containing all null
                                characters
                                Default: False
      static_data(bool,Optional): TBD
      generate_md5(bool,Optional): if True, it updates md5 sum after every read
                                   operation
                                   Default: True
    """
    self._static_data = static_data
    self._zero_data = zero_data
    if self._zero_data:
      self._1k_zero_data = self._get_zero_data(Constants.SUB_BLOCK_SIZE)
    self._last_block_size = 0
    self.size = size
    self._total_blocks = 0
    self._local_data_map = {}
    self._current_pointer = 0
    self._datahash = md5()
    self._md5sum = self._base64 = None
    self._compressiable = compressiable
    self._local_static_data = self._generate_static_data()
    self._generate_data()
    self._generate_md5 = generate_md5
    self._current_block_being_read = None
    self._data_returned = 0

  def __len__(self):
    """
    returns the size of data

    Returns: size of data
    """
    return self.size

  def read(self, size=1024*1024):
    """
    reads the data from self._current_block_being_read upto size, if
    current_block is already read completely it will increment
    self._current_pointer and generates new data block

    Args:
      size(int,Optional): size of the data you want to read
                          default: 1024*1024

    Returns: string of given size
    """
    if self._current_pointer == self._total_blocks and \
          self._data_returned == self.size:
      if not self._md5sum:
        self._md5sum = self._datahash.hexdigest()
        self._base64 = self._md5sum.decode("hex").encode("base64").strip()
      return ""
    if not self._current_block_being_read and self._data_returned < self.size:
      self._current_block_being_read = self._generate_data_block(
                                    self._local_data_map[self._current_pointer])
      self._current_pointer += 1
    data = self._current_block_being_read[:size]
    self._data_returned += len(data)
    self._current_block_being_read = self._current_block_being_read[size:]
    data = self._update_data_with_offset(data)
    if self._generate_md5:
      if not self._md5sum:
        self._datahash.update(data)
    return data

  def md5sum(self):
    """
    returns the md5 sum of the data generated

    Returns: tuple(string,string)
      md5 sum in hex and base64 encoding
    """
    if not self._md5sum:
      self._current_pointer = 0
      while True:
        chunk = self.read()
        if not chunk:
          break
      self._current_pointer = 0
    return self._md5sum, self._base64

  def seek(self, offset, whence=0):
    """
    seek method which will change the current_pointer according to arguments

    Args:
      offset(int): number of positions to move forward
      whence(int,Optional): defines point of reference
                            0: (default)beginning of the file
                            1: current_pointer
                            2: end_of_file
    """
    if not self._md5sum:
      self._datahash = md5()
    if offset > self._total_blocks:
      ERROR("Requested offset (%s) is outside of file. Seeking to end of file"
             %(offset))
      offset = self._total_blocks
    if whence == 2:
      self._current_pointer = self._total_blocks
      return
    if whence == 1:
      return
    if offset == 0:
      self._current_block_being_read = None
      self._current_pointer = 0
      self._data_returned = 0
      return
    self._current_pointer = offset

  def tell(self):
    """
    returns the size of data returned

    Returns: integer
    """
    return self._data_returned

  def _generate_data(self):
    """
    sets total number of blocks of local static data according to size and sets
    random starting point for each of the block in self._local_data_map
    """
    num_blocks_to_generate = self.size / Constants.LOCAL_STATIC_DATA_SIZE
    self._last_block_size = self.size%Constants.LOCAL_STATIC_DATA_SIZE
    self._total_blocks = num_blocks_to_generate
    if self._last_block_size > 0:
      self._total_blocks = self._total_blocks + 1
    while num_blocks_to_generate > 0 :
      random_data_index = randrange(0, Constants.LOCAL_STATIC_DATA_SIZE, 1024)
      self._local_data_map[self._current_pointer] = (random_data_index,
                                             Constants.LOCAL_STATIC_DATA_SIZE)
      num_blocks_to_generate = num_blocks_to_generate - 1
      self._current_pointer = self._current_pointer + 1
    if self._last_block_size > 0:
      self._local_data_map[self._current_pointer] = (0, self._last_block_size)
    self._current_pointer = 0

  def _generate_data_block(self, offset):
    """
    returns the rotated copy the local_static_data according to current_pointer

    Args:
      current_pointer(tuple(int,int)): it gives the location from where to
                                       rotate and size of data


    Returns: string
    """
    random_data_index = offset[0]
    size_of_block = offset[1]
    current_data_block = self._local_static_data[random_data_index:]
    remaining_data  = self._local_static_data[:
                  (Constants.LOCAL_STATIC_DATA_SIZE - len(current_data_block))]
    data = "%s%s"%(current_data_block, remaining_data)
    return  data[:size_of_block]

  def _generate_static_data(self):
    """
    returns static_data_block using which we can generate data of self.size
    later

    Returns: string
    """
    if self._compressiable:
      return self._get_random_str(Constants.COMPRESSEABLE_DATA_SIZE) * (
            Constants.LOCAL_STATIC_DATA_SIZE/Constants.COMPRESSEABLE_DATA_SIZE)
    if self._zero_data:
      return self._get_random_str(Constants.SUB_BLOCK_SIZE, zero_data=True) * (
                    Constants.LOCAL_STATIC_DATA_SIZE/Constants.SUB_BLOCK_SIZE)
    return self._get_random_str(Constants.LOCAL_STATIC_DATA_SIZE)

  def _get_random_str(self, size, zero_data=False):
    """
    generates a random string of given size and replace last 53 bytes with
    checksum, magic_byte and zeros

    Args:
      size(int): size of the data
      zero_data(boo,Optional): if True, it generates string of null characters
                               Default: False

    Returns: string
    """
    stime=time()
    num_subblocks_to_generate = size / Constants.SUB_BLOCK_SIZE
    data = ""
    for i in range(num_subblocks_to_generate):
      subdata = urandom(Constants.SUB_BLOCK_SIZE)
      if self._static_data:
        #Convert all data from byte->string and remove all whitespaces/newlines
        subdata = "".join(
                    [i for i in subdata.encode('base-64').strip() if i.strip()])
      subdata = subdata[:(Constants.SUB_BLOCK_SIZE-53)]
      if zero_data:
        subdata = self._1k_zero_data[Constants.SUB_BLOCK_SIZE-33]
      md5sum = md5(subdata).hexdigest()
      data += "%s%s%s%s%s"%(subdata, Constants.MAGIC_BYTE, md5sum,
                            Constants.MAGIC_BYTE, "0"*19)
    return data

  def _get_zero_data(self, size):
    """
    returns the string of null characters of given size

    Args:
      size(int): size of string to be generated

    Returns: string
    """
    with open("/dev/zero") as fh:
      return fh.read(size)

  def _update_data_with_offset(self, data):
    """
    updates the last few chars of every sub block with block no so that it can
    be used for verification later

    Args:
       data(str): data to be updated

    Returns: string
      updated data
    """
    num_sub_blocks = len(data)/Constants.SUB_BLOCK_SIZE
    final_data=""
    block_offset = self.tell()-len(data)
    for block in range(num_sub_blocks):
      subdata = data[block*Constants.SUB_BLOCK_SIZE:
                     (block*Constants.SUB_BLOCK_SIZE)+Constants.SUB_BLOCK_SIZE]
      block_offset += Constants.SUB_BLOCK_SIZE
      size_of_offset = len(str(block_offset))+1
      subdata=subdata[:len(subdata)-size_of_offset]
      subdata = subdata+"_"+str(block_offset)
      final_data += subdata
    return final_data

class CreateBucket():
  def __init__(self, url, access, secret, bucket, s3c=None, pcobj=None,
               testobj=None):
    self.bucket = bucket
    self.access = access
    self.secret = secret
    self.url = "%s/%s"%(url, self.bucket)
    self.resource = "/%s" % (self.bucket)
    self._s3c = s3c
    self._pcobj = pcobj
    self._testobj = testobj

  def create_bucket(self, enable_nfs, set_expiry=False):
    content_type = None
    date = strftime("%a, %d %b %Y %X GMT", gmtime())
    sig_data = "PUT\n\n%s\n%s\n%s" % (content_type, date, self.resource)
    signature = encodestring(new(self.secret, sig_data,
                                             sha1).digest()).strip()
    auth_string = "AWS %s:%s" % (self.access, signature)
    register_openers()
    request = Request(self.url)
    request.add_header('Date', date)
    if enable_nfs:
      request.add_header('x-ntnx-nfs-access-enabled', "true")
    request.add_header('Content-Type', content_type)
    request.add_header('Content-Length', 0)
    request.add_header('Authorization', auth_string)
    request.get_method = lambda: 'PUT'
    cont = res = None
    INFO("Creating bucket %s, NFS_status : %s"%(self.bucket, enable_nfs))
    try:
      res = urlopen(request, context=_create_unverified_context())
      cont = res.read()
    except HTTPError, error:
      raise Exception("ErrorCode : %s, Error : %s"%(error.code,error.read()))
    res = self._validate(res)
    if set_expiry:
      self._set_expiry()
    return res

  def _validate(self, res):
    info =  res.headers.dict
    info["HTTPStatusCode"] = res.code
    if res.code > 300:
      raise Exception("Bucket Creation failed. Bucket : %s, ErrorCode : %s"
            %(self.bucket, res.code))
    info["Bucket"] = self.bucket
    return info

  def enable_bucket_notification(self):
    endpt = self._testobj._kwargs["notification_endpoint"]
    types = self._testobj._kwargs["bucket_notification_types"]
    skipconfig = self._testobj._kwargs["skip_notification_endpoint_config"]
    if (not endpt and not skipconfig) or not self._pcobj:
      DEBUG("Skipping notification config on %s, Endpoint : %s, PC Obj : %s, "
            " Skip Config : %s"%(self.bucket, endpt, self._pcobj, skipconfig))
      return
    INFO("Enabling bucket notification on %s"%self.bucket)
    self._testobj._execute(self._pcobj.enable_bucket_notification,
                           bucket=self.bucket, retry_count=0,
                           kafka = "kafka" in types,
                           syslog = "syslog" in types,
                           nats = "nats" in types)

  def _set_expiry(self):
    rules = []
    self._update_expiry_lifecycle_rules(rules)
    self._update_tiering_lifecycle_rules(rules)
    self._update_tagging_lifecycle_rules(rules)
    life_cycle_config = {"Rules":rules}
    print dumps(life_cycle_config, indent=2, default=str)
    INFO("Expiry : PUT %s on bucket : %s"%(life_cycle_config, self.bucket))
    self._s3c.put_bucket_lifecycle_configuration(Bucket=self.bucket,
                                      LifecycleConfiguration=life_cycle_config)
    INFO("Expiry : GET on %s is : %s"%(self.bucket,
      self._s3c.get_bucket_lifecycle_configuration(Bucket=self.bucket)))

  def _update_expiry_lifecycle_rules(self, rules):
    rule1 = {
          "Expiration": {"Days": self._testobj._kwargs["expiry_days"]},
          "NoncurrentVersionExpiration":{
                 "NoncurrentDays":self._testobj._kwargs["version_expiry_days"]},
          "AbortIncompleteMultipartUpload": {"DaysAfterInitiation":
                                self._testobj._kwargs["multipart_expiry_days"]},
          "Status": "Enabled",
          "Filter" :{"Prefix":self._testobj._kwargs["expiry_prefix"]}
          }
    rules.append(rule1)

  def _update_tagging_lifecycle_rules(self, rules):
    if self._testobj._kwargs["enable_tagging"
                              ] and self._testobj._kwargs["tag_expiry_prefix"]:
      rule2 = {
          "Expiration": {"Days": self._testobj._kwargs["tag_expiry_days"]},
          "NoncurrentVersionExpiration":{
            "NoncurrentDays":self._testobj._kwargs["version_expiry_days"]},
          "Status": "Enabled",
          "Filter" :{
                "And":{"Prefix": self._testobj._kwargs["tag_expiry_prefix"],
                       "Tags": [{
                          "Key": "tagexp",
                          "Value": "tagvalue"
                        }]
                    }
                }
            }
      rules.append(rule2)

  def _update_tiering_lifecycle_rules(self, rules):
    if not (self._testobj._kwargs["enable_tiering"] and
            self._testobj._kwargs["tiering_endpoint"]):
      return
    tdays = self._testobj._kwargs["tiering_days"]
    for tprefix in self._testobj._kwargs["tiering_prefix"].split(","):
      rule3 = {
          "Status": "Enabled",
          "Filter" :{"Prefix":tprefix},
          "Transitions": [{"Days": tdays,
                         "Endpoint": self._testobj._kwargs["tiering_endpoint"]}]
        }
      if self._testobj._kwargs["tiering_expiry_days"] > 0:
        edays = self._testobj._kwargs["tiering_expiry_days"]
        edays = edays if edays > tdays else tdays+1
        rule3["Expiration"] = {"Days": edays}
      if self._testobj._kwargs["tiering_version_expiry_days"] > 0:
        edays = self._testobj._kwargs["tiering_version_expiry_days"]
        edays = edays if edays > tdays else tdays+1
        rule3["NoncurrentVersionExpiration"] = {"NoncurrentDays":edays}
      if self._testobj._kwargs["tiering_multipart_expiry_days"] > 0:
        rule3["AbortIncompleteMultipartUpload"] = {
              "DaysAfterInitiation":self._testobj._kwargs[
                                              "tiering_multipart_expiry_days"]}
      rules.append(rule3)


class PutObject():
  def __init__(self, url, access, secret, bucket, objname, size,
               partnumber=None, uploadid=None):
    self.size = size
    self.bucket = bucket
    self.access = access
    self.secret = secret
    self.objname = """%s"""%(objname)
    self._multipart = False
    self._endpoint = url
    self.url = "%s/%s/%s"%(url, bucket, objname)
    self.resource = "/%s/%s" % (self.bucket, self.objname)
    if partnumber and uploadid:
      part_url = "partNumber=%s&uploadId=%s"%(partnumber, uploadid)
      self.url = "%s?%s"%(self.url, part_url)
      self.resource = "%s?%s"%(self.resource, part_url)
      self._multipart = True

  def put_object(self, **kwargs):
    return self._put_object(**kwargs)

  def _put_object(self, data, md5sum, extra_headers, **kwargs):
    length = self.size
    content_type = None #"application/x-www-form-urlencoded"
    date = strftime("%a, %d %b %Y %X GMT", gmtime())
    sig_data = "PUT\n\n%s\n"% (content_type)
    sig_data += "%s\n" % (date)
    if "x-amz-tagging" in  extra_headers:
      if md5sum:
        extra_headers["x-amz-tagging"] = "etag=%s"%(md5sum)
      sig_data += "x-amz-tagging:%s\n" % (extra_headers["x-amz-tagging"])
    sig_data += "%s" % (self.resource)
    signature = encodestring(new(self.secret, sig_data,
                                             sha1).digest()).strip()
    auth_string = "AWS %s:%s" % (self.access, signature)
    register_openers()
    request = Request(self.url.rstrip(), data=data)
    request.add_header('Date', date)
    request.add_header('Content-Type', content_type)
    request.add_header('Content-Length', length)
    request.add_header('Authorization', auth_string)
    if extra_headers:
      for key, value in extra_headers.iteritems():
        request.add_header(key, value)
    request.get_method = lambda: 'PUT'
    cont = res = None
    try:
      res = urlopen(request, context=_create_unverified_context())
      cont = res.read()
    except HTTPError, error:
      resp = {}
      ntnx_error = ""
      if hasattr(error, "headers"):
        if hasattr(error.headers, "dict"):
          resp = error.headers.dict
          ntnx_error = resp.get("x-ntnx-error","")
      resp.update({"Error":{"Code":error.code, "Message":str(error.read()),
                   "Bucket":self.bucket, "Key":self.objname, "url":self.url,
                   "request_headers":request.headers, "endpoint":self._endpoint,
                   "NtnxError":ntnx_error}})
      error.response = resp
      error.s3_endpoint = self._endpoint
      raise
    except Exception as err:
      resp = {}
      if hasattr(err, "headers"):
        if hasattr(err.headers, "dict"):
          resp = err.headers.dict
      resp.update({"Error":{"Message":str(err.message), "Bucket":self.bucket,
                   "Key":self.objname, "url":self.url,
                   "request_headers":request.headers,
                   "endpoint":self._endpoint}})
      err.response = resp
      err.s3_endpoint = self._endpoint
      raise
    finally:
      if hasattr(data, "seek"):
        data.seek(0)
      try:
        res.close()
      except:
        pass
    return self._validate(data, md5sum, res)

  def _validate(self, data, md5sum, res):
    info =  res.headers.dict
    info["HTTPStatusCode"] = res.code
    if res.code > 300:
      exp = Exception("PUT failed. Bucket/Object : %s/%s, ErrorCode : %s,"
            "Response : "%(self.bucket, self.objname, res.code, info))
      exp.response = info
      raise exp
    data_size = 0
    if not md5sum:
      if isinstance(data, str):
        if len(data) == 0:
          md5sum = 'd41d8cd98f00b204e9800998ecf8427e'
        else:
          md5sum = md5(data).hexdigest()
          data_size = len(data)
      else:
        md5sum, _ = data.md5sum()
        data_size = data.size
    etag = info.get("etag").strip("\"")
    if md5sum != etag:
      cerr = "Data Corruption!!. Bucket/Objectname : %s/%s. "\
             "Expected ETag vs Found : %s vs %s, Size : %s (expected) vs %s"\
              %(self.bucket, self.objname, md5sum, etag, data_size, info)
      ERROR(cerr)
      raise Exception(cerr)
    info["Key"] = self.objname
    info["Bucket"] = self.bucket
    if "x-amz-version-id" in info:
      info["VersionId"] = info["x-amz-version-id"]
    info["s3_endpoint"] = self._endpoint
    return info


class ErrorInjection():
  def __init__(self, testobj, pcobject, vmops=None, components_for_ei=""):
    self._testobj = testobj
    self._pc = pcobject
    self._msp_cluster = MSP(self._pc)
    self.msp_cluster_details = self._pc.get_msp_cluster_details(True)
    self._components_for_failure_injection = components_for_ei.split(",")
    self.vm_ops = vmops
    self._next_op = None
    self._previous_vm = None
    self.power_on_everything()
    #TODO  : Change this to MSP class. And all common functions
    self.k8handle = self._load_kubeconfig()
    self._service_map = {"object-controller":{"port":7100},
                         "ms-server":{"port":7102},
                         "poseidon-atlas":{"port":7103}}
    self._default_namespace = "default"
    self._update_ei_action_map()
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

  def _update_ei_action_map(self):
    default_map = {
                    Constants.EI_COMPONENT_NODE:[
                      Constants.EI_OPERATION_OFF,
                      Constants.EI_OPERATION_ON,
                      Constants.EI_OPERATION_RESET,
                      Constants.EI_OPERATION_REBOOT,
                      Constants.EI_OPERATION_SHUTDOWN,
                      Constants.EI_OPERATION_REBOOT_ALL_CVM,
                      Constants.EI_OPERATION_REBOOTALL,
                      Constants.EI_OPERATION_REBOOT_HOST,
                      Constants.EI_OPERATION_REBOOT_ALL_HOST,
                      Constants.EI_OPERATION_RESETALL,
                      Constants.EI_OPERATION_REBOOT_CVM
                      ],
                    Constants.EI_COMPONENT_POD:[
                      Constants.EI_OPERATION_RESTART_POD
                      ],
                    Constants.EI_COMPONENT_SERVICE:[
                      Constants.EI_OPERATION_SERVICE_H_EXIT
                      ],
                    Constants.EI_COMPONENT_INFRA_SERVICE : [
                      Constants.EI_OPERATION_STOP_STARGATE,
                      Constants.EI_OPERATION_RESTART_STARGATE,
                      Constants.EI_OPERATION_STOP_CASSANDRA,
                      Constants.EI_OPERATION_RESTART_CASSANDRA
                      ]
                    }
    self._comptype_to_action_map = {}
    for comp, actions in default_map.iteritems():
      self._comptype_to_action_map.setdefault(comp, [])
      for action in actions:
        if action in self.vm_ops:
          self._comptype_to_action_map[comp].append(action)
      if not self._comptype_to_action_map[comp]:
        self._comptype_to_action_map.pop(comp)
    for comp in self._components_for_failure_injection:
      if comp in self._comptype_to_action_map:
        continue
      self._comptype_to_action_map[comp] = default_map[comp]
    INFO("EI Comp to action map : %s"%self._comptype_to_action_map)

  def inject_errors(self, ei_interval):
    self._testobj._total_ops_recorded["ei"] = {"count":0,
                              "Last.Recorded.Time":None, "comp_under_ei":[]}
    try:
      self._wait_for_cluster_recovery(self._testobj._kwargs["namespace_for_ei"],
                                      "on", 600, initial_delay_interval=0)
    except Exception as err:
      self._testobj._set_exit_marker("Cluser not healthy. Err:%s"%(err.message))

    #Make sure AOS cluster is up and running
    self._pc.start_cvm_cluster()
    expected_unhealthy_pods = 1
    if self._pc.num_msp_workers() <= 3:
      expected_unhealthy_pods = 3
    INFO("Expected unhealthy pods : %s, num worker : %s"
         %(expected_unhealthy_pods, self._pc.num_msp_workers()))
    retry_num = 0
    while not self._testobj._exit_test("ErrorInjection"):
      if self._testobj._upgrade_in_progress:
        INFO("Upgrade in progress, can not inject any failure. Check after 30s")
        sleep(30)
        continue
      component_to_fail = self._get_component_for_failure_injection()
      self._testobj._total_ops_recorded["vars"]["expect_failure_during_ei"]=True
      itime = datetime.now()
      INFO("Current EI OP : %s, NextOp : %s, Expect_ei_failure_flag  :%s"
           %(component_to_fail, self._next_op,
             self._testobj._expect_failure_during_ei))
      try:
        rtime, operation, component = self.initiate_failure(component_to_fail)
        retry_num = 0
      except Exception as err:
        ERROR("Failed to initiate failure : %s. Error : %s, Retrying no : %s"
                %(component_to_fail, err.message, retry_num))
        if "500 Internal Server" in err.message:
          retry_num += 1
          if retry_num < 6:
            sleep(60)
            continue
        self._testobj._set_exit_marker("Error while initiating failure : %s, "
                "Error : (%s) %s, Retry Num : %s"%(component_to_fail, type(err),
                err.message, retry_num))
      recovery_time = self._get_recovery_time_for_failure(operation,
                                                    component_to_fail["type"])
      self._testobj._total_ops_recorded["ei"]["comp_under_ei"].append({
           "component":component, "operation":operation,
           "component-type":component_to_fail["type"],"ei_injection_time":rtime,
           "expected_recovery_time":recovery_time})
      try:
          rtime = self._wait_for_recovery(comp=component, op=operation,
                          recovery_time=recovery_time,
                          namespace=self._testobj._kwargs["namespace_for_ei"],
                          expected_unhealthy_pods=expected_unhealthy_pods,
                          itime=itime, comptype=component_to_fail["type"])
      except Exception as err:
        self._testobj._set_exit_marker("(%s)%s"%(type(err), err.message))
      self._testobj._update_ei_results(component, operation, itime, rtime)
      if "envoy" in component and operation == Constants.EI_OPERATION_OFF and \
          component_to_fail["type"] == Constants.EI_COMPONENT_NODE:
        #Hack to workaround case where envoy is picked for PowerOff operation.
        #If we mark ongoing ei flag to OFF then test will fail since
        #envoy will remain down and we will exhaust all retries.
        #So just Power it on after 120 seconds then turn flag off
        WARN("Envoy was chosen for %s, so sleeping for 120 and keeping EI flag"
              " on : %s"%(operation, self._testobj._expect_failure_during_ei))
        sleep(120 if ei_interval > 120 else ei_interval)
        continue
      self._testobj._total_ops_recorded["ei"]["comp_under_ei"] = []
      self._testobj._total_ops_recorded["vars"]["expect_failure_during_ei"]=False
      DEBUG("Ongoing EI task finished. Expect_ei_failure_flag  :%s"
            %(self._testobj._expect_failure_during_ei))
      sleep(ei_interval)

  def _get_recovery_time_for_failure(self, operation, comptype):
    if comptype == Constants.EI_COMPONENT_POD:
      return Constants.EI_RECOVERY_TIME_FOR_POD
    if comptype == Constants.EI_COMPONENT_SERVICE:
      return Constants.EI_RECOVERY_TIME_FOR_SERVICE
    if operation in [Constants.EI_OPERATION_REBOOTALL,
                     Constants.EI_OPERATION_RESETALL]:
      return Constants.EI_RECOVERY_TIME_FOR_MSP_POWERCYCLE
    if operation in Constants.EI_OPERATION_REBOOT_HOST:
      return Constants.EI_RECOVERY_TIME_FOR_REBOOT_HYP_HOST
    if operation in [Constants.EI_OPERATION_REBOOT_ALL_CVM,
                     Constants.EI_OPERATION_REBOOT_ALL_HOST]:
      return Constants.EI_RECOVERY_TIME_FOR_REBOOT_ALL_HOST_CVM
    return Constants.EI_RECOVERY_TIME_DEFAULT

  def wait_for_ongoing_compaction(self, timeout=7200, retry_delay=30):
    stime = time()
    all_ms_pods = self.get_all_ms_pods()
    for podname in all_ms_pods:
      if not podname.startswith("ms"):
        continue
      self._wait_for_compaction(podname, timeout, retry_delay)

  def _wait_for_compaction(self, podname, timeout, retry_delay):
    stime=time()
    sleep(1)
    while time() - stime < timeout:
      if not self.is_compaction_running(podname):
        return
      sleep(retry_delay)
    raise Exception("Hit timeout %s, while waiting for compaction in %s"
                %(timeout, podname))

  def _get_bg_task_manager_page(self, podname):
    cmd = "links -dump http:0:7102/poseidon_db/bg_task_manager"
    return self._pod_exec(podname, cmd.split(" "))

  def is_compaction_running(self, podname, bg_tasks=None):
    res = None
    if bg_tasks:
      res = bg_tasks
    else:
      res = self._get_bg_task_manager_page(podname)
    if not res:
      return False
    for line in res.split("\n"):
      if "compaction in progress" in line.lower() and "true" in line.lower():
        return True
    return False

  def compact_cf(self, cf, podname):
    cfindex = self._get_cf_index(cf)
    cmd = "links -dump http:0:7102/poseidon_db/compact?map_id=%s"%(cfindex)
    res = self._pod_exec(podname, cmd.split(" "))
    for line in res.split("\n"):
      if "triggered" in line.lower() and  "compaction for cf" in line.lower():
        return True
    return False

  def get_all_ms_pods(self):
    unhealthy_pods, healthy_pods= self.get_unhealthy_pods("default")
    return [pod for pod in healthy_pods.keys() if pod.startswith("ms")]

  def _get_cf_index(self, cf, get_all_cfs=False):
    cfmap = {"kPoseidonBucketInfoMapType" : 0,
            "kPoseidonBucketIndexMapType" : 1,
            "kPoseidonObjectInfoMapType" : 2,
            "kPoseidonBucketListMapType" : 3,
            "kPoseidonVDiskInfoMapType" : 4,
            "kPoseidonOpenVDiskInfoMapType" : 5,
            "kPoseidonRegionInfoMapType" : 6,
            "kPoseidonBucketStatsMapType" : 7 }
    if get_all_cfs:
      return cfmap.keys()
    return cfmap[cf]

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
      if "Running" not in metadata["state"].phase:
        unhealthy_pods[service] = metadata["state"].phase
      else:
        healthy_pods[service] = metadata["state"].phase
    return unhealthy_pods, healthy_pods

  def _get_component_for_failure_injection(self):
    if self._next_op:
      INFO("Current EI Op was predecided by previous action : %s"%self._next_op)
      res = self._next_op
      self._next_op = {}
      return res
    res = {"type":choice(self._components_for_failure_injection),
           "comps":choice(self.msp_cluster_details["vm_names"])}
    next_op, self._next_op = None, {}
    if res["type"] == Constants.EI_COMPONENT_NODE:
      return self._get_node_for_failure(res)
    if res["type"] == Constants.EI_COMPONENT_INFRA_SERVICE:
      return self._get_infra_service_for_failure(res)
    if res["type"] == Constants.EI_COMPONENT_SERVICE:
      return self._get_service_for_failure()
    if res["type"] == Constants.EI_COMPONENT_MSP_SERVICE:
      return self._get_msp_service_for_failure()
    return self._get_pod_for_failure()

  def _get_node_for_failure(self, compdetails):
    res = {"type":Constants.EI_COMPONENT_NODE, "comps":compdetails["comps"],
         "op":choice(self._comptype_to_action_map[Constants.EI_COMPONENT_NODE])}
    next_op = None
    if res["op"] in [Constants.EI_OPERATION_OFF,
                     Constants.EI_OPERATION_SHUTDOWN]:
      next_op = Constants.EI_OPERATION_ON
    elif res["op"] == Constants.EI_OPERATION_PAUSE:
      next_op = Constants.EI_OPERATION_RESUME
    if not next_op:
      self._next_op = {}
    else:
      self._next_op = {"comps":compdetails["comps"],
                       "type":Constants.EI_COMPONENT_NODE, "op" :  next_op}
    return res

  def _get_infra_service_for_failure(self, compdetails):
    op = choice(self._comptype_to_action_map[
                                  Constants.EI_COMPONENT_INFRA_SERVICE])
    next_op, peip = None, compdetails["comps"]
    if op == Constants.EI_OPERATION_STOP_STARGATE:
      next_op = Constants.EI_OPERATION_START_STARGATE
      peip = choice(self._pc.get_cvmips())
    elif op == Constants.EI_OPERATION_STOP_CASSANDRA:
      next_op = Constants.EI_OPERATION_START_CASSANDRA
      peip = choice(self._pc.get_cvmips())
    if not next_op:
      self._next_op = {}
    else:
      self._next_op = {"comps":peip,
                       "type":Constants.EI_COMPONENT_INFRA_SERVICE,
                       "op" :  next_op}
    return {"comps":peip, "type":Constants.EI_COMPONENT_INFRA_SERVICE, "op":op}

  def _get_msp_service_for_failure(self):
    op = choice(self._comptype_to_action_map[
                                  Constants.EI_COMPONENT_MSP_SERVICE])
    next_op, mspip = None, choice(self._msp_cluster.get_worker_ips())
    if op == Constants.EI_OPERATION_STOP_REGISTRY:
      next_op = Constants.EI_OPERATION_START_REGISTRY
    elif op == Constants.EI_OPERATION_STOP_ETCD:
      next_op = Constants.EI_OPERATION_START_ETCD
    elif op == Constants.EI_OPERATION_STOP_DOCKER:
      next_op = Constants.EI_OPERATION_START_DOCKER
    if not next_op:
      self._next_op = {}
    else:
      self._next_op = {"comps":mspip,
                       "type":Constants.EI_COMPONENT_MSP_SERVICE,
                       "op" :  next_op}
    return {"comps":peip, "type":Constants.EI_COMPONENT_MSP_SERVICE, "op":op}

  def _get_service_for_failure(self):
    shuffle(self._testobj._kwargs["services_to_restart"])
    res = {"type":Constants.EI_COMPONENT_SERVICE,
           "op":choice(self._comptype_to_action_map[
                                               Constants.EI_COMPONENT_SERVICE]),
           "comps":self._testobj._kwargs["services_to_restart"][
                            :self._testobj._kwargs["num_services_to_restart"]]}
    return res

  def _get_pod_for_failure(self):
    allpods = self._get_pods(self._testobj._kwargs["namespace_for_ei"],
                      self._testobj._kwargs["pods_to_restart"])
    pods_to_restart = []
    if len(allpods) < self._testobj._kwargs["num_pods_to_restart"]:
      pods_to_restart = allpods
    else:
      pods_to_restart = allpods[:self._testobj._kwargs["num_pods_to_restart"]]
    res = {"type":Constants.EI_COMPONENT_POD, "comps":pods_to_restart,
            "op": Constants.EI_OPERATION_RESTART_POD,
            "namespace":self._testobj._kwargs["namespace_for_ei"]}
    return res

  def initiate_failure(self, component_to_fail):
    INFO("Initiating Failure : %s, NextAction : %s"%(component_to_fail,
         self._next_op))
    operation, component = None, None
    stime = time()
    if component_to_fail["type"] == Constants.EI_COMPONENT_NODE:
      component, operation = self._initiate_node_failure(component_to_fail)
    elif component_to_fail["type"] == Constants.EI_COMPONENT_INFRA_SERVICE:
      component, operation = self._initiate_infra_service_failure(
                                    component_to_fail)
    elif component_to_fail["type"] == Constants.EI_COMPONENT_SERVICE:
      component, operation = self._initiate_service_failure(component_to_fail)
    elif component_to_fail["type"] == Constants.EI_COMPONENT_MSP_SERVICE:
      component, operation = self._initiate_msp_service_failure(
                                                              component_to_fail)
    else:
      component, operation = self._initiate_pod_failure(component_to_fail)
    return time() - stime, operation, component

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

  def _wait_for_cluster_recovery(self, vmname, operation, timeout,
                                namespace="default", expected_unhealthy_pods=-1,
                                retry_interval=30, initial_delay_interval=120):
    if "envoy" in vmname and "all" not in operation:
      DEBUG("Skipping wait_for_cluster_recovery check for %s (OP : %s)"
            %(vmname, operation))
      sleep(10)
      return datetime.now()
    if expected_unhealthy_pods == -1:
      expected_unhealthy_pods = self._num_unhealthy_pods_to_expect(vmname,
                                                                   operation)
    INFO("Wait for recovery for %s, operation : %s, timeout : %s, Initial Delay"
         " : %s, Expected Unhealthy Pods : %s"%(vmname, operation, timeout,
         initial_delay_interval, expected_unhealthy_pods))
    msg = ""
    sleep(initial_delay_interval)
    stime = time()
    while (time()-stime) < timeout:
      unhealthy_pods = healthy_pods = None
      try:
        unhealthy_pods, healthy_pods= self.get_unhealthy_pods(namespace)
      except Exception as err:
        ERROR("Failed to get pods health status. Error : %s, Retrying in %s sec"
              %(err, retry_interval))
        sleep(retry_interval)
        continue
      msg = "Expected unhealthy POD : %s, and Found : %s, %s, Operation : %s, "\
            "Entity : %s, Healthy POD : %s"%(expected_unhealthy_pods,
              len(unhealthy_pods), unhealthy_pods, operation, vmname,
              healthy_pods)
      if len(unhealthy_pods) <= expected_unhealthy_pods:
        INFO("Cluster seems to be recovered. %s"%msg)
        return datetime.now()
      DEBUG("%s, Check after %s s"%(msg, retry_interval))
      sleep(retry_interval)
    raise Exception("Hit timeout after %s seconds. Details : %s"%(timeout, msg))

  def _wait_for_recovery(self, comp, op, recovery_time, namespace,
                      expected_unhealthy_pods, itime,  comptype):
    msg = "Initiated %s failure on %s (in time : %s). Expected recovery : %s"%(
            op, comp, datetime.now()-itime, recovery_time)
    rtime = None
    if comptype == "pod":
      rtime = self._wait_for_cluster_recovery(comp, op, recovery_time,
                                  namespace, 0, initial_delay_interval=120)
    elif comptype == "service":
      rtime = self._wait_for_cluster_recovery(comp, op, recovery_time,
                                namespace, 0, initial_delay_interval=120)
    else:
      rtime = self._wait_for_cluster_recovery(comp, op, recovery_time,
                          expected_unhealthy_pods=expected_unhealthy_pods)
    if "default" in comp or comptype == "pod" or comptype == "service":
      #If failed comp is envoy then failures are expected longer
      #else at this time, we should have already recovered.
      self._testobj._total_ops_recorded["vars"][
                                            "expect_failure_during_ei"] = False
    return rtime

  def _get_pods(self, namespace, servicetype):
    allpods = self.get_all_pods()
    pod_names = allpods[namespace]["services"].keys()
    shuffle(pod_names)
    if servicetype == Constants.EI_SERVICE_ALL:
      return pod_names
    if servicetype == Constants.EI_SERVICE_CDP:
      pods = []
      for pod in pod_names:
        if pod.startswith(("zk", "objects-controller", "ms", "poseidon-atlas")):
          pods.append(pod)
      return pods
    return [pod for pod in pod_names if pod.startswith(servicetype)]

  def _initiate_h_exit(self, service):
    port = None
    for servicename in self._service_map.keys():
      if service.startswith(servicename):
        port = self._service_map[servicename]["port"]
        break
    if not port:
      raise Exception("Port not defined for %s in %s map"
                      %(service, self.service_map))
    cmd = "links -dump http:0:%s/h/exit"%(port)
    INFO("Initiating /h/exit on %s, Cmd : %s"%(service, cmd))
    res = self._pod_exec(service, cmd.split(" "))

  def _initiate_pod_failure(self, component_to_fail):
    self._restart_pod(component_to_fail["comps"],
                      component_to_fail["namespace"])
    return  "_".join(component_to_fail["comps"]
                                          ), Constants.EI_OPERATION_RESTART_POD

  def _initiate_msp_service_failure(self, component_to_fail):
    unhealthy_pods, healthy_pods = self.get_unhealthy_pods(
                                      self._testobj._kwargs["namespace_for_ei"])
    if unhealthy_pods:
      ERROR("Unhealthy PODs found before invoking service failure : %s"
            %unhealthy_pods)
    raise Exception("Unimplemented : Implement me to use me")

  def _initiate_service_failure(self, component_to_fail):
    unhealthy_pods, healthy_pods = self.get_unhealthy_pods(
                                      self._testobj._kwargs["namespace_for_ei"])
    if unhealthy_pods:
      ERROR("Unhealthy PODs found before invoking service failure : %s"
            %unhealthy_pods)
    services_to_exit = []
    for podname in healthy_pods.keys():
      if podname.startswith(tuple(component_to_fail["comps"])):
        services_to_exit.append(podname)
    if not services_to_exit:
      raise Exception("No service founds which starts with %s in %s"
                       %(services, allpods))
    shuffle(services_to_exit)
    for service in services_to_exit:
      self._initiate_h_exit(service)
    return "_".join(component_to_fail["comps"]), \
            Constants.EI_OPERATION_SERVICE_H_EXIT

  def _initiate_node_failure(self, component_to_fail):
    method = getattr(self._pc, component_to_fail["op"])
    method(component_to_fail["comps"])
    return component_to_fail["comps"], component_to_fail["op"]

  def _initiate_infra_service_failure(self, component_to_fail):
    method = getattr(self._pc, component_to_fail["op"])
    method(component_to_fail["comps"])
    return component_to_fail["comps"], component_to_fail["op"]

  def _get_vm_for_failure_injection(self, operation):
    return choice(self.msp_cluster_details["vm_names"])

  def _get_next_vm_operation(self, operation):
    if operation: return operation
    num_workers = self._pc.num_msp_workers()
    while True:
      operation = choice(self.vm_ops)
      if num_workers > 1:
        return operation
      if operation == "off":
        #On one node cluster, Off and On will not work with current automation
        #support. So just skip it.
        continue
      return operation

  def _load_kubeconfig(self):
    kfilename = "kf"+str(time())
    Utils.write_to_file(self.msp_cluster_details["kubeconfig"].strip(),
                        kfilename, "w")
    config.load_kube_config(config_file=kfilename)
    configuration = client.Configuration()
    configuration.verify_ssl=False
    configuration.debug = False
    client.Configuration.set_default(configuration)
    k8handle = client.CoreV1Api()
    remove(kfilename)
    return k8handle

  def _num_unhealthy_pods_to_expect(self, vmname, operation):
    if "default" in vmname and operation in ["off", "pause", "shutdown"]:
      return 1
    return 0

  def _pod_exec(self, service, cmd, namespace=None, retry=3, delay=30):
    if not namespace:
      namespace = self._default_namespace
    while retry >0:
      try:
        return stream(self.k8handle.connect_get_namespaced_pod_exec, service,
                       namespace, command=cmd, stderr=True, stdin=False,
                       stdout=True, tty=False)
      except Exception as err:
        retry -= 1
        ERROR("Hit error while executing %s on %s. Error  :%s"
              %(cmd, service, err.message))
        sleep(delay)

  def _restart_pod(self, podnames, namespace):
    for pod in podnames:
      INFO("Initiating POD Restart : %s / %s"%(namespace, pod))
      self.k8handle.delete_namespaced_pod(pod, namespace, async_req=False)

class Utils():
  def convert_to_datetime(self, datestr):
    if isinstance(datestr, str) or isinstance(datestr, unicode):
      datestr = "%s"%(datestr.strip())
      return dateparser.parse(datestr)
    return datestr

  def convert_num(self, num):
    try:
      return self._convert_num(num)
    except Exception as err:
      ERROR("Failed to convert num %s, Err : %s"%(num, err))
      raise

  #Hack to catch conversion bug
  def _convert_num(self, num):
    if isinstance(num, str):
      if num.isdigit():
        num = int(num)
      else:
        return num
    if num == "-1" or num == -1 or num < 1000:
      return num
    magnitude = 0
    while abs(num) >= 1000:
      magnitude += 1
      num /= 1000.0
    return '%.3f%s' % (num, ['', 'K', 'M', 'B', 'T', 'QD', 'QT', 'SX', 'ST',
                             'OC', 'N', 'D'][magnitude])

  def cleanup_all_markers(self, markers=None):
    if not markers:
      markers = ['/tmp/stats', '/tmp/stats.err', '/tmp/mem_top',
                 '/tmp/exit_marker', '/tmp/records.json', '/tmp/stats.html',
                 '/tmp/testconfig.json', '/tmp/bucket_info.json',
                 '/tmp/stats.json', '/tmp/all_stats.json',
                 '/tmp/clients_records.json', '/tmp/final_records.json']

    for marker in markers:
      if path.isfile(marker):
        try:
          remove(marker)
        except:
          pass

  def convert_to_num(self, size):
    if (isinstance(size, int) or isinstance(size, float) or  size.isdigit() or
        size == "-1" or size == -1):
      return int(size)
    size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
    number, unit = findall(r'(\d+)(\w+)', size.upper())[0]
    idx = size_name.index(unit[0].upper())
    factor = 1024 ** idx
    return int(number) * factor

  def convert_bool(self, item):
    if isinstance(item, bool):
      return item
    try:
      return loads(item.lower())
    except:
      pass
    if item.lower() in ['true', "y", 'yes', '1', 't']:
      return True
    if item.lower() in ['false', "n", 'no', '0', 'f']:
      return False
    raise Exception("Invalid var provided for conversion : %s"%(item))

  def convert_random_bool(self, item):
    if item == "random" or item is None:
      return item
    return self.convert_bool(item)

  def extract_build(self, links):
    builds = []
    for buildlink in links.split(","):
      if buildlink.endswith("tar.gz"):
        builds.append(buildlink)
        continue
      http = Http()
      status, response = http.request(buildlink)
      for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
        build = ""
        try:
          build = link.getText()
        except:
          continue
        if build.startswith("nutanix_installer_package"
                            ) and build.endswith("tar.gz"):
          builds.append("%s/%s"%(buildlink, build))
          break
    return builds

  #Convert all unicode to strings
  def convert_unicode_to_str(self, params):
    for key, value in params.items():
      if isinstance(key, unicode):
        params[str(key)] = value
      if isinstance(value, unicode):
        params[key] = str(value)
      if isinstance(value, list):
        params[key] = [str(i) if isinstance(i, unicode) else i for i in value]
      if isinstance(value, dict):
        params[key] = self.convert_unicode_to_str(value)
    return params

  def convert_time(self, datetime_format=None, seconds=None):
    if datetime_format:
      return str(datetime(2020, 3, 30, 9, 34, 34, tzinfo=tzutc()))
    if seconds:
      return datetime.utcfromtimestamp(seconds).\
             strftime("%Y-%m-%d %H:%M:%S+00:00")

  def execute_rest_api_call(self, url, request_method,  data, timeout=120,
             retry=0, retry_delay=30, username="admin", password="Nutanix.123"):
    params = {"url":url, "headers":{"Content-Type": "application/json"},
             "auth":(username, password), "verify":False,
             "timeout":timeout}
    if data is not None:
      params["data"]=str(data)
    retry += 1
    msg = ""
    while retry > 0:
      res = request_method(**params)
      if res.status_code < 300:
        return res
      msg = "Error while executing %s. Url : %s(U:%s, P:%s) , Error : %s "\
            "(Code:%s)"%(request_method.__name__, url, username, password,
                         res.content, res.status_code)
      ERROR(msg)
      retry -= 1
      if retry>0: sleep(retry_delay)
      continue
    raise Exception(msg)

  def dump_memory_info(self, limit=1000, width=1000, filename=None):
    mem_dump = "%s"%(mem_top(limit=100, width=1000, sep='\n',
                     refs_format='{num}\t{type} {obj}',
                     bytes_format='{num}\t {obj}',
                     types_format='{num}\t {obj}', verbose_types=None,
                     verbose_file_name=Constants.STATIC_MEM_TOP_FILE))
    if filename:
      Utils.write_to_file(
          "\n=======\nTIME : %s\n=======\n%s"%(datetime.now(), mem_dump),
          filename, "a")
    else:
      DEBUG("Memory Debug Info : ")
      DEBUG(mem_dump)

  def get_objsize(self, sizes):
    if len(sizes) == 1:
      return sizes[0]
    if sizes[0] == sizes[1]:
      return sizes[0]
    if sizes[1] < 1024*1024:
      return randrange(sizes[0], sizes[1], 1024)
    size = randrange(sizes[0], sizes[1], 1024)
    return size

  def get_mem_usage(self, pid=None):
    if not pid:
      pid = getpid()
    memusage = Process(pid).memory_info().rss
    return self.convert_size(memusage)

  def convert_size(self, size_bytes):
    if size_bytes <= 0:
      return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(floor(log(size_bytes, 1024)))
    p = pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

  @staticmethod
  def write_to_file(data, filename, open_flag):
    with open(filename, open_flag) as fh:
      fh.write(str(data))

  @staticmethod
  def remote_exec(hostname, cmd, username="nutanix", password="nutanix/4u",
                  port=22, retry=3, retry_delay=30, timeout=3600,
                  background=False, debug=False, ignore_errors=None):
    if ignore_errors is None:
      ignore_errors = []
    out = err = None
    retry += 1
    while retry > 0:
      retry -= 1
      try:
        out, err = Utils._remote_exec(hostname, cmd, username, password, port,
                                      timeout, background, debug=debug)
        return out, err
      except Exception as err:
        ERROR("Failed to execute %s on %s, Err : %s, Remaining Retry Count : %s"
              %(cmd, hostname, err, retry))
        for error in ignore_errors:
          if error in str(err):
            ERROR("Hostname : %s,Cmd : %s,Ignoring error : %s,Ignore_error : %s"
                  %(hostname, cmd, err, ignore_errors))
            return out, err
        if retry > 0: sleep(retry_delay)
    return out, err

  @staticmethod
  def _remote_exec(hostname, cmd, username, password, port, timeout, background,
                   debug=False):
    if debug: print "Executing :- ",hostname, port, username, password, cmd
    client = SSHClient()
    #client.load_system_host_keys()
    client.set_missing_host_key_policy(MissingHostKeyPolicy())
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.connect(hostname, port, username, password, banner_timeout=60)
    stdin, stdout, stderr = None, None, None
    if background:
      transport = client.get_transport()
      channel = transport.open_session()
      return channel.exec_command(cmd), None
    else:
      stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out, err = stdout.read(), stderr.read()
    client.close()
    if out or not err:
      return out, err
    ex = Exception("Failed to execute %s on %s. Error : %s (out : %s)"%(cmd,
                    hostname, err, out))
    ex.output = out
    ex.outerr = err
    raise ex

  def copy_remote_file_to_local(self, remoteip, remotefile, localfile,
                 user="root", password="nutanix/4u", retries=3, retry_delay=30):
    for i in range(retries+1):
      try:
        return self._copy_file_to_local(remoteip, remotefile, localfile, user,
                                        password)
      except Exception as err:
        ERROR("Failed to copy remote file %s:%s->%s, err : %s, retry_num  :%s"
              %(remoteip, remotefile, localfile, err, i))
        if i == retries:
          raise
      sleep(retry_delay)

  def copy_local_file_to_remote(self, remoteip, localfile, remotefile,
                 user="root", password="nutanix/4u", retries=3, retry_delay=30):
    for i in range(retries+1):
      try:
        return self._copy_file_to_remote(remoteip, localfile, remotefile, user,
                                        password)
      except Exception as err:
        ERROR("Failed to copy local file %s->%s:%s, err : %s, retry_num  :%s"
              %(localfile, remoteip, remotefile, err, i))
        if i == retries:
          raise
      sleep(retry_delay)

  def _copy_file_to_remote(self, remoteip, localfile, remotefile, user,
                           password):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    ssh.set_missing_host_key_policy(MissingHostKeyPolicy())
    ssh.connect(remoteip, username=user, password=password)
    sftp = ssh.open_sftp()
    INFO("Copying %s file to remote location : %s:%s"
         %(localfile, remoteip, remotefile))
    sftp.put(localfile, remotefile)
    sftp.close()
    ssh.close()

  def _copy_file_to_local(self, remoteip, remotefile, localfile, user, passwd):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    ssh.set_missing_host_key_policy(MissingHostKeyPolicy())
    ssh.connect(remoteip, username=user, password=passwd)
    sftp = ssh.open_sftp()
    INFO("Copying %s file to local location : %s:%s"
         %(remotefile, remoteip, localfile))
    sftp.get(remotefile, localfile)
    sftp.close()
    ssh.close()

  @staticmethod
  def sleep(seconds):
    for remaining in range(seconds, 0, -1):
      stdout.write("\r")
      stdout.write("{:2d} seconds remaining.".format(remaining))
      stdout.flush()
      sleep(1)

  def get_local_ip(self):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("10.40.64.15", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

  def local_exec(self, cmd, retry=0, retry_delay=30, ignore_err=None,
                 background=False):
    out, err = None, None
    count = -1
    cmd = [i for i in cmd.split(" ") if i] if isinstance(cmd, str) else cmd
    while count < retry:
      p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      if background:
        return p
      out, err = p.communicate()
      count += 1
      if err:
        if isinstance(ignore_err, bool) and ignore_err:
          return out, err
        if ignore_err and  any([e in err.strip() for e in ignore_err]):
          return out, err
        ERROR("Failed to execute cmd : %s, Error : %s, Retry Count : %s/%s"
              %(cmd, err, count, retry))
        sleep(retry_delay)
    return out, err

  def get_vip_dsip(self, hostip, use_cluster_vip=False):
    out, err = self.local_exec("nslookup %s"%(hostip))
    fqdn = out.strip().split()[-1].split(".")
    if use_cluster_vip:
      hostname = fqdn.pop(0).split("-")
    else:
      hostname = [fqdn.pop(0)]
    vip = hostname[0]+"-v1"
    dsip = hostname[0]+"-v2"
    vip_hostname = [vip] + fqdn
    dsip_hostname = [dsip] + fqdn
    #vip_hostname[0] = vip
    #dsip_hostname[0] = dsip
    vip_hostname = ".".join([i for i in vip_hostname if i])
    dsip_hostname = ".".join([i for i in dsip_hostname if i])
    INFO("Hostname %s, VIP-Hostname : %s, DSIPhostname : %s, VIP/DSIP : %s / %s"
         %(hostname, vip_hostname, dsip_hostname, vip, dsip))
    vip_ip = self.nslookup(vip_hostname)
    dsip_ip = self.nslookup(dsip_hostname)
    INFO("Host for VIP/DSIP : %s / %s"%(vip_ip, dsip_ip))
    return vip_ip, dsip_ip

  def nslookup(self, hostname):
    out, err = self.local_exec("nslookup %s"%(hostname))
    return out.strip().split(":")[-1].strip()

  def start_tcpdump(self, cmd=None, output_dir=None):
    if not cmd:
      cmd = "sudo tcpdump -U -C 100 -W 50 -w capture -i any port 2749 -w %s"
    INFO("tcmpdump output dir : %s"%output_dir)
    cmd += " -w %s"%output_dir
    INFO("Starting tcpdump with cmd : %s"%(cmd))
    self.local_exec(cmd)

  def get_pid(self, process_name):
    p = subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    pids = []
    for line in out.splitlines():
      if process_name in line:
        pid = int(line.split(None, 1)[0])
        pids.append(pid)
    return pids

  def kill_pid(self, process_name):
    pids = self.get_pid(process_name)
    if not pids:
      ERROR("No PID found for the process name : %s"%(process_name))
    for pid in pids:
        INFO("Killing Process with ID : %s (Name : %s"%(pid, process_name))
        kill(pid, SIGKILL)

  def stop_tcpdump(self):
    INFO("Stopping tcpdump")
    self.kill_pid("tcpdump")

class SanityCheck():
  def __init__(self, pcobj, objectsobj, **kwargs):
    self.pc = pcobj
    self.obj = objectsobj
    self.s3c = None
    self._iam_creds = {}
    self.s3client()
    self._fatal = ""
    self._lock = RLock()
    self._num_api_retries = {}
    self.logpath = kwargs.get("logpath", getcwd())

  def s3client(self, endpoint=None, prefix=""):
    if self.pc.state != "COMPLETE":
      return None
    if not endpoint:
      endpoint = self.pc.get_objects_endpoint()
    user = "user%s_%s@test.com"%(int(time()), prefix)
    iam_creds, ptime = self._execute(self.pc.create_iam_user,
                                     username=user.split("@")[0], email=user)
    self._iam_creds[user] = iam_creds
    endpoint = "https://%s"%(endpoint)
    INFO("Objects Endpoint : %s, IAM Creds : %s"
         %(endpoint, iam_creds))
    cert_from_pc = self.pc.download_objects_certs()
    if not endpoint:
      raise Exception("Endpoint is empty")
    s3c = connect(ip=endpoint, access=iam_creds["access"],
                       secret=iam_creds["secret"])
    self.s3c = s3c
    return s3c, iam_creds, endpoint

  def run_sanity(self, num_buckets, num_objects_per_bucket):
    INFO("Running sanity test on %s"%(self.pc.objects_name))
    self.pc._get_objects_uuid()
    if self.pc.state != "COMPLETE":
      ERROR("%s is in %s state, skipping sanity check"
            %(self.pc.objects_name, self.pc.state))
      return
    bucket = "sanitybucket"
    endpoints = self.pc.get_objects_endpoint(True)
    num_buckets = (num_buckets/len(endpoints))+1
    thrds=[]
    num_parallel_threads = 40
    for endpoint in endpoints:
      if self._fatal:
        raise Exception(self._fatal)
      INFO("Testing endpoint : %s"%(endpoint))
      for i in range(num_buckets):
        buck = "%s%s%s%s"%(bucket, str(time()), str(i),
                           "".join(endpoint.split(".")))
        thrd = Thread(target=self.objio, args=(endpoint, buck,
                                               num_objects_per_bucket,))
        thrds.append(thrd)
        if len(thrds) > num_parallel_threads:
          [thrd.start() for thrd in thrds]
          [thrd.join() for thrd in thrds]
          thrds=[]
    [thrd.start() for thrd in thrds]
    [thrd.join() for thrd in thrds]
    if self._fatal:
      raise Exception(self._fatal)
    self.remove_users()
    return self._num_api_retries

  def remove_users(self):
    for user, creds in self._iam_creds.iteritems():
      print "Removing user : %s (%s)"%(user, creds)
      #self.pc.remove_iam_user(creds["uuid"])
      self._execute(self.pc.remove_iam_user, uuid=creds["uuid"])

  def create_bucket(self, endpoint, bucket, iam_creds, enable_nfs=False):
    INFO("Creating bucket : %s, NFS-Access : %s"%(bucket, enable_nfs))
    cb = CreateBucket(url=endpoint, access=iam_creds["access"],
        secret=iam_creds["secret"], bucket=bucket, pcobj=self.pc, testobj=self)
    return cb.create_bucket(enable_nfs, False)

  def objio(self, endpoint, buck,  num_objects_per_bucket):
    s3c , iamcreds, endpoint = self.s3client(endpoint, buck)
    INFO("Endpoint is : %s, Bucket : %s, Num Objects : %s"
         %(s3c._endpoint.host, buck, num_objects_per_bucket))
    res, _ = self._execute(self.create_bucket, endpoint=endpoint, bucket=buck,
                        iam_creds=iamcreds)
    if not res:
      _fatal = "Failed to create bucket : %s"%(buck)
      self._update_fatal(_fatal)
      raise Exception(self._fatal)
    for j in range(num_objects_per_bucket):
      stime=time()
      osize=choice(range(10,12))
      data = urandom(1024*1024*osize)
      key = "%s_%s_%s"%(buck, time(), j)
      self.put_and_read(s3c, buck, key, data)
    INFO("Deleting Bucket : %s"%(buck))
    self._execute(s3c.delete_bucket, Bucket=buck)

  def put_and_read(self, s3c, bucket, key, data):
    INFO("Putting objects in %s, Size : %s kb"%(bucket, len(data)/1024))
    method = s3c.put_object
    res, ptime = self._execute(method, Bucket=bucket, Key=key, Body=data)
    read, _ = self._execute(s3c.get_object, Bucket=bucket, Key=key)
    stime=time()
    readdata = read["Body"].read()
    rtime=time()-stime
    if data != readdata:
      unqstr = time()

      #Dump expected data in file
      expected_datafile = path.join(self.logpath,
                            "%s_%s_%s_expected_data"%(bucket, key, unqstr))
      INFO("Dumping expected data to : %s"%(expected_datafile))
      self._dump_data_to_file(expected_datafile, data)

      #Dump found data in separate file
      found_datafile = path.join(self.logpath,
                          "%s_%s_%s_found_data"%(bucket, key, unqstr))
      INFO("Dumping found data to : %s"%(found_datafile))
      self._dump_data_to_file(found_datafile, readdata)
      _fatal = "Data Corruption, Expected vs found : %s vs %s"\
                    %(expected_datafile, found_datafile)
      self._update_fatal(_fatal)
      raise Exception(_fatal)
    total_objs, size = self._ui_stats(bucket)
    if total_objs != 1 and size != len(data):
      _fatal = "%s : Mismatch in TotalObjects / Size returned in UI, "\
                      "Expected vs Found : %s(%s) vs %s(%s)"\
                      %(bucket, 1, len(data), total_objs, size)
      self._update_fatal(_fatal)
      raise Exception(_fatal)
    delres, _ = self._execute(s3c.delete_object, Bucket=bucket, Key=key)
    for i in range(3):
      total_objs, size = self._ui_stats(bucket)
      if total_objs == 0 and size == 0:
        break
      if i == 2:
        _fatal = "Mismatch in TotalObjects / Size returned in UI for bucket"\
                 " %s, Expected vs Found : %s(%s) vs %s(%s) \n"\
                 %(bucket, 0, 0, total_objs, size)
        self._update_fatal(_fatal)
        raise Exception(_fatal)
      sleep(10)
    print("TESTED : Bucket : %s, Key : %s (size : %s), PutTime/ReadTime : %s /"
          " %s, PTP/RTP : %s / %s"%(bucket, key, len(data), ptime, rtime,
          _convert_num(len(data)/ptime), _convert_num(len(data)/rtime)))

  def _dump_data_to_file(self, filename, data):
    ffh = open(filename, "w")
    ffh.write(data)
    ffh.close()

  def _ui_stats(self, bucket):
    res = None
    retry = 120
    while retry > 0:
      retry -= 1
      try:
        res = self.pc.get_bucket_info([bucket])
      except Exception as err:
        pass
      if not res or int(res[bucket]["object_count"]) < 0:
        WARN("Results : %s, are not returned properly. Retrying for %s in 60s"
             %(res, bucket))
        sleep(60)
        continue
      count = int(res[bucket]["object_count"])
      usage = int(res[bucket]["storage_usage_bytes"])
      return count, usage
    return 0, 0

  def _execute(self, method, **kwargs):
    retry = kwargs.pop("num_retry", 5)
    retry_delay = kwargs.pop("retry_delay", 60)
    stime=time()
    count = 0
    errors = []
    while retry > 0:
      try:
        res = method(**kwargs)
        self._update_errors(method, errors, count)
        return res, time()-stime
      except Exception as err:
        ERROR("Hit exception while executing : %s, Err : (%s) %s, Args : %s, "
              "Retry Num : %s"%(method, type(err), err,
                [(k, str(v)[:50]) for k, v in kwargs.iteritems()], retry))
        if "Corruption" in err.message or "Mismatch" in err.message:
          self._update_fatal(err.message)
          raise
        errors.append(err.message)
        retry -= 1
        if retry == 0:
          self._update_fatal(err.message)
          raise
        count += 1
        sleep(retry_delay)

  def _update_errors(self, method, errors, count):
    if count < 1:
      return
    with self._lock:
      if method.__name__ not in self._num_api_retries:
        self._num_api_retries[method.__name__] = {"Errors" : [],
                                                  "ErrorCount" : 0}
      count += self._num_api_retries[method.__name__]["ErrorCount"]
      errors += self._num_api_retries[method.__name__]["Errors"]
      self._num_api_retries[method.__name__] = {"Errors" : errors,
                                                "ErrorCount":count}

  def _update_fatal(self, msg):
    self._fatal = self._fatal + msg +"\n"

LCM_TASK_SUCCEEDED = "SUCCEEDED"
LCM_TASK_FAILED = "FAILED"
LCM_TASK_RUNNING = "RUNNING"

LCM_TASK_FINISH = {LCM_TASK_SUCCEEDED, LCM_TASK_SUCCEEDED}

class Upgrade:
    def __init__(self, pcobj, port=9440):
        self._pcobj = pcobj
        self._endpoint = pcobj.pcip + ":" + str(port)
        self._auth = (pcobj.username, pcobj.password)

    def inventory(self, wait_for_completion=True):
        """
        Trigger LCM inventory operation on localhost
        """
        url = "https://%s/lcm/v1.r0.b1/operations/inventory" % self._endpoint
        resp = self._pcobj._execute_rest_api_call(request_method=post,
                                              url=url, data='{}')
        """parse task id and return"""
        resp_json = resp.json()
        INFO("Inventory response[ok: %s]: %s" % (resp.ok, str(resp_json)))
        if resp.ok is False:
            ERROR("Inventory request failed with %s" % str(resp_json))
            raise RequestError(resp)
        else:
            task_id = resp_json.get("data").get("task_uuid")
            INFO("Inventory triggered, uuid %s" % task_id)
            if wait_for_completion:
                self.wait_for_task(task_id, "LCM Inventory")
            return task_id
        return None

    def wait_for_task(self, task_uuid, task_name,
                      expected_status=LCM_TASK_SUCCEEDED, timeout=3600):
        res, restime = self._wait_for_task(task_uuid, task_name)
        if "subtask_uuid_list" in res and res["subtask_uuid_list"]:
            for taskid in res["subtask_uuid_list"]:
                self.wait_for_task(taskid, task_name, timeout=restime)

    def _wait_for_task(self, task_uuid, task_name,
                       expected_status=LCM_TASK_SUCCEEDED, timeout=3600):
        stime = time()+timeout
        res=None
        while stime - time() > 0:
          res = self.task(task_uuid)
          if res["status"] == expected_status:
              INFO("%s Task (uuid %s) Completed. Status : %s"
                   %(task_name, task_uuid, res))
              return res, stime-time()
          INFO("%s Task (uuid : %s) , is still not completed. Retry after 10s. "
                "Task Statys  : %s"%(task_name, task_uuid, res))
          sleep(10)
        raise Exception("Timeout while waiting for %s task - %s, tobe in status"
                        " : %s in %s seconds, Res : %s"%(task_name, task_uuid,
                        expected_status, timeout, res))

    def get_entities_list(self, filter_rule):
        if filter_rule == None or filter_rule == "":
            err_msg = "Empty filter rule"
            ERROR(err_msg)
            raise Exception(err_msg)
        url = "https://%s/lcm/v1.r0.b1/resources/entities/list" % self._endpoint
        filter_json = {"filter": filter_rule}
        payload = dumps(filter_json)
        resp = self._pcobj._execute_rest_api_call(request_method=post,
                                                  url=url, data=payload)
        """parse task id and return"""
        resp_json = resp.json()
        INFO("Entities list response[ok: %s]: %s" % (resp.ok, str(resp_json)))
        if resp.ok is False:
            ERROR("Entities list request failed with %s" % str(resp_json))
            raise RequestError(resp)
        else:
            return resp_json["data"]["entities"]
        return None

    def objects_entities(self):
        filter_rule = "entity_class==Objects;entity_model==Objects Manager,"\
                  "entity_class==PC CORE CLUSTER;entity_model==Objects Service"
        return self.get_entities_list(filter_rule)

    def update(self, target_version, objects_manager_version=True,
               objects_service_version=False, objectstore_name=None,
               wait_for_completion=True):
        if objects_manager_version:
            entity_class = "Objects"
            entity_model = "Objects Manager"
        elif objects_service_version:
            entity_class = "PC CORE CLUSTER"
            entity_model = "Objects Service"

        INFO("entity_class: %s, entity_model: %s, target_version: %s" % \
            (entity_class, entity_model, target_version))
        spec_list = self.form_update_spec_list(entity_class, entity_model,
            target_version, objectstore_name)
        payload_dict = {"entity_update_spec_list": spec_list}
        payload = dumps(payload_dict)
        INFO("Full update spec list: %s" % str(payload_dict))
        url = "https://%s/lcm/v1.r0.b1/operations/update" % self._endpoint
        resp = self._pcobj._execute_rest_api_call(request_method=post,
                                          url=url, data=payload, timeout=300)
        """parse task id and return"""
        resp_json = resp.json()
        INFO("Update response[ok: %s]: %s" % (resp.ok, str(resp_json)))
        if resp.ok is False:
            ERROR("Update request failed with %s" % str(resp_json))
            raise RequestError(resp)
        else:
            task_id = resp_json.get("data").get("task_uuid")
            INFO("Update triggered, uuid %s" % task_id)
            if wait_for_completion:
                try:
                  self.wait_for_task(task_id, "%s Upgrade"%(entity_model))
                except Exception as err:
                  ERROR("Hit exception while waiting for upgrade task to finish"
                        "Task UUID : %s, Error : %s"%(task_id, err))
            return task_id
        return None

    def form_update_spec_list(self, entity_class, entity_model, target_version,
            objectstore_name=None):
        filter_rule = "entity_class==%s;entity_model==%s" % (entity_class, \
                entity_model)
        entities = self.get_entities_list(filter_rule)
        if len(entities) == 0:
            raise LCMEntityNotFound(entity_class, entity_model)
        INFO("Potential update entities: %s" % str(entities))
        # LCM API not supporting multiple objectstores, pick the first one
        entity = entities[0]
        found_objectstore_instance = False
        if entity_model == "Objects Service":
            if objectstore_name is None:
                raise InvalidParams("Objects name cannot be None for upgrade")
            for e in entities:
                if e.get("id") == objectstore_name:
                    found_objectstore_instance = True
                    entity = e
            if not found_objectstore_instance:
                raise LCMEntityNotFound(entity_class, entity_model,
                                        objectstore_name)
        avail_versions = entity.get("available_version_list")
        if avail_versions == None or len(avail_versions) == 0:
            raise LCMEntityAvailableVersionNotFound(entity_class,
                    entity_model, target_version)
        spec_list = list()
        found = False
        for avail_version in avail_versions:
            if avail_version["version"] == target_version:
                found = True
                dependency_list = avail_version.get("dependency_list")
                if dependency_list == None or len(dependency_list) == 0:
                    INFO("%s has no dependency required to upgrade to %s" \
                            % (filter_rule, target_version))
                else:
                    for dependent in dependency_list:
                        dependent_entity_class = dependent["entity_class"]
                        dependent_entity_model = dependent["entity_model"]
                        dependent_version = dependent["version"]
                        dependent_spec_list = self.form_update_spec_list(
                                 dependent_entity_class, dependent_entity_model,
                                 dependent_version)
                        spec_list+=dependent_spec_list
                break
        if found == False:
            raise LCMEntityAvailableVersionNotFound(entity_class,
                    entity_model, target_version)
        # form update spec for this current entity
        # TODO: because LCM API doesn't provide device_id, not able to upgrade
        # a specified Objects Service, so pick the only first one for now.
        if entity_model == "Objects Service":
            spec_list.append(
                {"entity_uuid": entity["uuid"], "version": target_version})
        else:
            for each in entities:
                entity_uuid = each["uuid"]
                INFO("adding %s entity_uuid: %s" % (entity_model, entity_uuid))
                spec_list.append(
                    {"entity_uuid": entity_uuid, "version": target_version})
        return spec_list

    def task(self, uuid):
        url = "https://%s/lcm/v1.r0.b1/resources/tasks/%s" % \
            (self._endpoint, uuid)
        resp = self._pcobj._execute_rest_api_call(request_method=get,
                                                  url=url, data='{}',
                                                  retry=30)
        """parse task id and return"""
        resp_json = resp.json()
        #INFO("Task response[ok: %s]: %s" % (resp.ok, str(resp_json)))
        if resp.ok is False:
            ERROR("Task request failed with %s" % str(resp_json))
            raise RequestError(resp)
        else:
            return resp_json["data"]

class VCenter:
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
    Utils.write_to_file(self.msp_cluster_details["kubeconfig"].strip(),
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
        return stream(self.k8handle.connect_get_namespaced_pod_exec, pod_name,
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

class PCObjects():
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
    self._utils = Utils()
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
    out, err = Utils.remote_exec(self.pcip, cmd, retry=retry)
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
    output, errors = Utils.remote_exec(peip, cmd)
    return output.strip().split(" ")

  def get_hostips(self):
    cmd= "source /etc/profile; /usr/local/nutanix/cluster/bin/hostips"
    output, errors = Utils.remote_exec(self.peip, cmd)
    return output.strip().split(" ")

  def delete_egroups(self, num_egroups_to_delete, wait_for_data_recovery,
                     hostip=None):
    if not hostip:
      hostip = choice(self.get_cvmips())
    if not self._ctr_id:
      cmd = 'source /etc/profile; /home/nutanix/prism/cli/ncli ctr ls | grep '\
            'objectsdc -B2 | grep Id | cut -d":" -f4'
      output, errors = Utils.remote_exec(hostip, cmd)
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
    output, errors = Utils.remote_exec(self.pcip, cmd)
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
    output, errors = Utils.remote_exec(self.peip, cmd)

  def on(self, vmname):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli vm.on %s"%(vmname)
    output, errors = Utils.remote_exec(self.peip, cmd)

  def pause(self, vmname):
    cmd ="source /etc/profile; /usr/local/nutanix/bin/acli vm.pause %s"%(vmname)
    output, errors = Utils.remote_exec(self.peip, cmd)

  def reboot(self, vmname):
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli vm.reboot %s"\
          %(vmname)
    output, errors = Utils.remote_exec(self.peip, cmd)

  def reboot_cvm(self, *args, **kwargs):
    cvmip = choice(self.get_cvmips())
    cmd ="source /etc/profile; /usr/bin/sudo reboot > /dev/null 2>&1 &"
    INFO("Initiating CVM reboot %s"%(cvmip))
    Utils.remote_exec(cvmip, cmd)
    return cvmip

  def reboot_all_cvm(self, *args, **kwargs):
    cvmips = self.get_cvmips()
    for cvmip in cvmips:
      cmd ="source /etc/profile; /usr/bin/sudo reboot > /dev/null 2>&1 &"
      INFO("Initiating All CVM reboot %s"%(cvmip))
      Utils.remote_exec(cvmip, cmd)
    return ",".join(sorted(cvmips))

  def reboot_host(self, *args, **kwargs):
    hostip = choice(self.get_hostips())
    cmd ="source /etc/profile; reboot > /dev/null 2>&1 &"
    INFO("Initiating Host reboot %s"%(hostip))
    Utils.remote_exec(hostip, cmd, username="root")
    return hostip

  def reboot_all_host(self, *args, **kwargs):
    hosts = self.get_hostips()
    for hostip in hosts:
      cmd ="source /etc/profile; reboot > /dev/null 2>&1 &"
      INFO("Initiating Host reboot %s"%(hostip))
      Utils.remote_exec(hostip, cmd, username="root")
    return ",".join(sorted(hosts))

  def rebootall(self, vmname):
    cmd ="source /etc/profile; /usr/local/nutanix/bin/acli vm.reboot %s*"\
         %(vmname.split("-")[0])
    Utils.remote_exec(self.peip, cmd)

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
      output, errors = Utils.remote_exec(hostip, cmd)
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
    output, errors = Utils.remote_exec(self.peip, cmd)
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
      vip, dsip = self._utils.get_vip_dsip(hostip, use_cluster_vip)
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
    output, errors = Utils.remote_exec(self.peip, cmd)
    cmd = "source /etc/profile;/usr/local/nutanix/bin/acli net.get "\
          "%s"%(network_name)
    output, errors = Utils.remote_exec(self.peip, cmd)
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
    output, errors = Utils.remote_exec(self.peip, cmd)

  def resetall(self, vmname):
    cmd ="source /etc/profile; /usr/local/nutanix/bin/acli vm.reset %s*"%(
                                                          vmname.split("-")[0])
    Utils.remote_exec(self.peip, cmd)

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
        output, errors = Utils.remote_exec(hostip, cmd)
      sleep(interval)

  def reset_stargate_storage_gflag(self, interval, gflag_value):
    gcmd = "source /etc/profile; links -dump "
    gcmd += "http:0:2009/h/gflags?stargate_disk_full_pct_threshold"
    for hostip in self.get_cvmips():
      cmd = "%s=%s"%(gcmd, gflag_value)
      INFO("Configuring gflag %s on %s"%(cmd, hostip))
      output, errors = Utils.remote_exec(hostip, cmd)
    sleep(interval)
    for hostip in self.get_cvmips():
      cmd = "%s=95"%(gcmd)
      INFO("Resetting gflag to default : %s on %s"%(cmd, hostip))
      output, errors = Utils.remote_exec(hostip, cmd)

  def resume(self, vmname):
    cmd="source /etc/profile; /usr/local/nutanix/bin/acli vm.resume %s"%(vmname)
    Utils.remote_exec(self.peip, cmd)

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
    Utils.remote_exec(self.peip, cmd)

  def skip_node_check(self):
    for hostip in self.get_svmips().split(" "):
      INFO("Enabling skip node check on : %s"%(hostip))
      cmd = "source /etc/profile; docker exec  aoss_service_manager touch"
      cmd += " /tmp/skip_node_worker_match"
      output, errors = Utils.remote_exec(hostip, cmd, retry = 0)
      self._mspctl.disable_ha_drs_flags()
      try:
        cmd="docker cp ~/tmp/delete/poseidon_master "
        cmd += "aoss_service_manager:/svc/config/"
        output, errors = Utils.remote_exec(hostip, cmd, retry=0)
      except Exception as err:
        ERROR("Error while updating supervisord & yaml templates : %s"%(err))

  def start_cassandra(self, *args, **kwargs):
    return self.start_cvm_cluster(self.peip)

  def start_cvm_cluster(self, cvmip=None, *args, **kwargs):
    if not cvmip:
      cvmip = choice(self.get_cvmips())
    cmd ="source /etc/profile; /usr/local/nutanix/cluster/bin/cluster start"
    INFO("Initiating Cluster Start on : %s"%(cvmip))
    print "Cluster Start : ", Utils.remote_exec(cvmip, cmd, retry=3,
                                                retry_delay=60)
    return cvmip

  def start_stargate(self, *args, **kwargs):
    return self.start_cvm_cluster(self.peip)

  def stop_cassandra(self, *args, **kwargs):
    cvmip = choice(self.get_cvmips())
    cmd ="source /etc/profile; "
    cmd += "/usr/local/nutanix/cluster/bin/genesis stop cassandra"
    INFO("Initiating Cassandra Stop on : %s"%(cvmip))
    Utils.remote_exec(cvmip, cmd)
    return cvmip

  def stop_stargate(self, *args, **kwargs):
    cvmip = choice(self.get_cvmips())
    cmd = "source /etc/profile; "
    cmd += "/usr/local/nutanix/cluster/bin/genesis stop stargate"
    INFO("Initiating Stargate Stop on : %s"%(cvmip))
    Utils.remote_exec(cvmip, cmd)
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
      res = Utils.remote_exec(self.pcip, wget_cmd, retry=0)
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
      res = Utils.remote_exec(self.pcip, upgrade_cmd, retry=0)
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
        res = Utils.remote_exec(pcip, cmd, retry=0)
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
    src_version = Utils.remote_exec(self.pcip, docker_cmd,
                                    retry=num_retry)[0].strip()
    res = loads(src_version.strip())
    return res["Config"]["Image"].split(":")[-1].strip()

  def _restart_service(self, service_name, restart_time=300):
    pcips = self.get_cvmips(self.pcip)
    for pcip in pcips:
      cmd = "source /etc/profile; "
      cmd += "/usr/local/nutanix/cluster/bin/genesis stop %s"%(service_name)
      INFO("Stopping service %s on %s, cmd : %s"%(service_name, pcip, cmd))
      res = Utils.remote_exec(pcip, cmd, retry=0)
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
    res = Utils.remote_exec(cvmip, cmd, retry=0)
    INFO("%s status on %s : %s"%(service_name, cvmip, res))

  def get_docker_ps(self, cvmip=None):
    if not cvmip:
      cvmip = self.get_cvmips(self.pcip)[0]
    cmd = "source /etc/profile; /usr/bin/docker ps"
    try:
      res = Utils.remote_exec(cvmip, cmd, retry=0)
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
    cluster_info = Utils.remote_exec(hostip, cluster_info_cmd)
    host_type = "PC" if pc_upgrade else "PE"
    INFO("%s Host %s, Build before upgrade : %s"
         %(host_type, hostip, cluster_info))
    cmd = "source /etc/profile; /usr/bin/wget -q -T300 %s"%(upgrade_build_url)
    INFO("Upgrade : Downloading %s on %s, cmd : %s"
         %(upgrade_build_url, hostip, cmd))
    res = Utils.remote_exec(hostip, cmd, retry=0)
    INFO("%s Upgrade : Wget res : %s"%(host_type, res))
    cmd = "source /etc/profile; tar xvfz %s;"%(upgrade_build_url.split("/")[-1])
    INFO("%s Upgrade : Untarring build %s on %s, cmd : %s"
         %(host_type, upgrade_build_url, hostip, cmd))
    res = Utils.remote_exec(hostip, cmd, retry=0)
    INFO("%s Upgrade : Untar res : %s"%(host_type, res))
    before_version, before_full_version = self.get_build_info(hostip,pc_upgrade)
    cmd = "source /etc/profile;"
    cmd += "/home/nutanix/install/bin/cluster -i install upgrade"
    INFO("%s Upgrade : Upgrading %s to %s, cmd :  %s"
         %(host_type, hostip, upgrade_build_url, cmd))
    res = Utils.remote_exec(hostip, cmd, retry=0)
    INFO("%s Upgrade : Upgrade res : %s"%(host_type, res))
    self._wait_for_upgrade(hostip, upgrade_timeout, pc_upgrade)
    cluster_info = Utils.remote_exec(hostip, cluster_info_cmd)
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
    print url
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
    vms, errors = Utils.remote_exec(peip, cmd)
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

class S3Ops():
  def __init__(self, testobj, endpoints, access, secret, read_timeout,
               max_pool_connections, addressing_style, debug, force=False):
    self._testobj = testobj
    self._endpoints = endpoints
    self._s3objs = []
    self._init_s3objs(access, secret, read_timeout, max_pool_connections,
                      addressing_style, debug, force)

  @property
  def s3obj(self):
    return choice(self._s3objs)

  def _init_s3objs(self, access, secret, read_timeout, max_pool_connections,
                   addressing_style, debug, force):
    if self._s3objs and not force:
      return
    if self._testobj and self._testobj._objectscluster:
      endpoints = []
      for endpoints_ip in self._testobj._objectscluster.get_endpoint_ips():
        endpoints.append("http://%s"%(endpoints_ip))
        endpoints.append("https://%s"%(endpoints_ip))
      if endpoints:
        INFO("Replacing endpoints : %s with %s"%(self._endpoints, endpoints))
        self._endpoints = [i for i in endpoints]
    self._s3objs = []
    for endpoint in self._endpoints:
      vstyle = choice(["virtual", "path"])
      if "//10" in endpoint or addressing_style == "path":
        vstyle = "path"
      s3obj = connect(ip=endpoint, access=access, secret=secret,
                      read_timeout=read_timeout,
                      max_pool_connections=max_pool_connections,
                      addressing_style=vstyle,
                      debug=debug)
      self._s3objs.append(s3obj)

  def s3_get_bucket_versioning(self, **kwargs):
    return self._execute_s3_op("get_bucket_versioning", **kwargs)

  def s3_get_object_lock_configuration(self, **kwargs):
    return self._execute_s3_op("get_object_lock_configuration", **kwargs)

  def s3_get_object_range(self, **kwargs):
    return self._execute_s3_op("get_object", **kwargs)

  def s3_get_object(self, **kwargs):
    return self._execute_s3_op("get_object", **kwargs)

  def s3_get_bucket_replication(self, **kwargs):
    return self._execute_s3_op("get_bucket_replication", **kwargs)

  def s3_get_bucket_lifecycle_configuration(self, **kwargs):
    return self._execute_s3_op("get_bucket_lifecycle_configuration", **kwargs)

  def s3_copy_object(self, **kwargs):
    return self._execute_s3_op("copy_object", **kwargs)

  def s3_put_bucket_versioning(self, **kwargs):
    return self._execute_s3_op("put_bucket_versioning", **kwargs)

  def s3_put_bucket_replication(self, **kwargs):
    return self._execute_s3_op("put_bucket_replication", **kwargs)

  def s3_delete_bucket(self, **kwargs):
    return self._execute_s3_op("delete_bucket", **kwargs)

  def s3_delete_objects(self, **kwargs):
    return self._execute_s3_op("delete_objects", **kwargs)

  def s3_complete_multipart_upload(self, **kwargs):
    return self._execute_s3_op("complete_multipart_upload", **kwargs)

  def s3_lookup(self, **kwargs):
    return self._execute_s3_op("head_object", **kwargs)

  def s3_head_object(self, **kwargs):
    return self._execute_s3_op("head_object", **kwargs)

  def s3_list_buckets(self, **kwargs):
    return self._execute_s3_op("list_buckets", **kwargs)

  def s3_create_multipart_upload(self, **kwargs):
    return self._execute_s3_op("create_multipart_upload", **kwargs)

  def s3_head_bucket(self, **kwargs):
    return self._execute_s3_op("head_bucket", **kwargs)

  def s3_abort_multipart_upload(self, **kwargs):
    return self._execute_s3_op("abort_multipart_upload", **kwargs)

  def s3_list_objects(self, **kwargs):
    return self._execute_s3_op("list_objects", **kwargs)

  def s3_list_objects_v2(self, **kwargs):
    return self._execute_s3_op("list_objects_v2", **kwargs)

  def s3_list_object_versions(self, **kwargs):
    return self._execute_s3_op("list_object_versions", **kwargs)

  def s3_get_endpoint_host(self, s3c=None, **kwargs):
    if s3c:
      s3c._endpoint.host
    return self.s3obj._endpoint.host

  def _execute_s3_op(self, s3_method_name, **kwargs):
    res = None
    s3c = self.s3obj
    endpoint = self.s3_get_endpoint_host(s3c)
    try:
      res = getattr(s3c, s3_method_name)(**kwargs)
    except Exception as err:
      err.s3_endpoint = endpoint
      raise
    if isinstance(res, dict):
      res["s3_endpoint"] = endpoint
    return res

class NFSOps():
  def __init__(self, testobj, nfs_endpoints, s3fs=False):
    self._testobj = testobj
    self._s3fs = s3fs
    self._execute = self._testobj._execute
    self._utils = Utils()
    self._posix = PosixOps()
    self._nfs_endpoints  = ["".join(re.findall( r'[0-9]+(?:\.[0-9]+){3}', i))
                            for i in nfs_endpoints]
    self._nfs_endpoints = [i for i in self._nfs_endpoints if i]
    INFO("NFS Endpoints : %s"%self._nfs_endpoints)
    self._testobj._total_ops_recorded["nfsops"] = {"mounts":{},
                                                   "nfsstats":{"filesdirs":{
                                                        "totaldirs":0,
                                                        "totalfiles":0,
                                                        "lookupfailed":0,
                                                        "num_open_files":0,
                                                        "num_open_fd_failed":0,
                                                        "num_close_fd_failed":0
                                                            },
                                                        },
                                                    "active_listing":{}
                                                    }
    self._nfs_parent_mount_dir = "/mnt/%s"%self._testobj._start_time
    self._registered_buckets = []
    self._lock = RLock()

  def register(self, bucket, mntopts):
    if not self._testobj._is_nfs_enabled_on_bucket(bucket):
      ERROR("NFS is not enabled on %s. Not updating NFS map"%(bucket))
      return
    mntpt = self._local_mount(bucket, mntopts)
    self._testobj._total_ops_recorded["nfsops"]["mounts"][bucket]={"path":mntpt}
    self._registered_buckets.append(bucket)

  @property
  def nfs_server(self):
    return choice(self._nfs_endpoints)

  def _local_mount(self, nfs_share_name, mount_options):
    local_mount = "%s/%s"%(self._nfs_parent_mount_dir, nfs_share_name)
    if not path.isdir(local_mount):
      out, err = self._utils.local_exec("sudo mkdir -p %s"%(local_mount))
      assert not err, "Failed to create mount dir %s, Err : %s"\
                      %(local_mount, err)
    cmd = "/usr/bin/sudo mount -o %s %s:/%s %s"%(mount_options, self.nfs_server,
                                                 nfs_share_name, local_mount)
    if self._s3fs:
      cmd = "s3fs %s %s "%(nfs_share_name, local_mount)
      cmd += "passwd_file=/tmp/s3fspass -o url=http://%s "%(nfs_server)
      cmd += "-o curldbg -o use_path_request_style"
    INFO("Mounting %s:%s to %s, with options : %s, Cmd  : %s"
         %(self.nfs_server, nfs_share_name, local_mount, mount_options, cmd))
    out, err = self._utils.local_exec(cmd)
    assert not err, "Failed to mount : %s, Err : %s"%(local_mount, err)
    return local_mount

  def _umount_shares(self):
    mounts = self._testobj._total_ops_recorded["nfsops"]["mounts"]
    INFO("Local NFS Map : %s"%(mounts))
    if not mounts.keys():
      WARN("No NFS share found, Nothing to umount")
      return
    errs = ""
    for bucket, mount in mounts.iteritems():
      if not (isinstance(mount, dict) and "path" in mount):
        ERROR("Mount path not found for bucket : %s (Mount : %s)"
              %(bucket, mount))
        continue
      cmd = "sudo umount -f %s"%(mount["path"])
      INFO("Unmounting share : %s"%(cmd))
      out, err=self._utils.local_exec(cmd, retry=10, ignore_err=["not mounted"])
      DEBUG("Umounting Share %s, Out : %s, Err : %s"%(mount["path"], out, err))
      if path.isdir(mount["path"]):
        if "device is busy" in out or "device is busy" in err:
          ERROR("Failed to unmount %s, Out/Err : %s%s, Skipping removing %s"
                %(mount["path"], out, err, mount["path"]))
          continue
        INFO("Removing local dir : %s"%(mount["path"]))
        self._utils.local_exec("sudo rm -rf %s"%(mount["path"]),
                  retry=1, ignore_err=["not mounted", "Read-only file system"])

  def _get_filepath(self, bucket, objname):
    mpth = self._testobj._total_ops_recorded["nfsops"]["mounts"][bucket]["path"]
    return path.join(mpth, objname)

  def nfsfio(self, filenum, io_type, block_size, runtime, iodepth, filesize,
             buffered_io):
    bucket, objname, _, _, size = \
          self._testobj._objname_distributor.get_objname_and_upload_type(
                      filenum, is_nfs=True, workload=Constants.NFSFIO_WORKLOAD)
    if not self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket,
                                          objname, size, Constants.LOCK):
      return
    size = filesize if filesize > 0 else size
    size = size if size > block_size else block_size
    filename = self._get_filepath(bucket, objname)
    filename = "%s_fio"%filename
    self._create_dir(filename=filename)
    fio = FioIO(filename, size, io_type, block_size, runtime, buffered_io,
                iodepth)
    self._execute(fio.start_fio, output_file=path.join(self._testobj.logpath,
                                                       "fio.log"), rsize=size)
    self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket, objname,
                                          size, Constants.UNLOCK)

  def nfsvdbench(self, filenum, io_type, block_size, runtime, iodepth, filesize,
             buffered_io, read_perc, vdbench_binary_location, vdb_output_dir):
    bucket, objname, _, _, size = \
            self._testobj._objname_distributor.get_objname_and_upload_type(
                  filenum, is_nfs=True, workload=Constants.NFSVDBENCH_WORKLOAD)
    if not self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket,
                                                objname, size, Constants.LOCK):
      return
    size = filesize if filesize > 0 else size
    size = size if size > block_size else block_size
    filename = self._get_filepath(bucket, objname)
    filename = "%s_vdbench"%filename
    self._create_dir(filename=filename)
    vdb = VdbenchIO(filename, size, io_type, block_size, runtime, buffered_io,
                iodepth, read_perc, vdbench_binary_location)
    self._execute(vdb.start_vdbench, output_dir=vdb_output_dir, rsize=size)
    self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket, objname,
                                                  size, Constants.UNLOCK)

  def nfsoverwrite(self, bucket, objname, versionid, rsize, srctag,
                   num_overwrites, size_of_write,block_size,
                   read_after_nfsoverwrite):
    mpth = self._testobj._total_ops_recorded["nfsops"]["mounts"][bucket]["path"]
    filename = path.join(mpth, objname)
    if not self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket,
                                                objname, rsize, Constants.LOCK):
      return
    for _ in range(num_overwrites):
      start_index =  randrange(0, rsize, 1024) if rsize > 0 else rsize
      size_of_write = size_of_write if isinstance(size_of_write, int
                    ) else randrange(size_of_write[0], size_of_write[-1], 1024)
      self._execute(self._nfs_over_write, filename=filename,
                    start_offset=start_index, size_of_write=size_of_write,
                    block_size=block_size, rsize=size_of_write)
    if read_after_nfsoverwrite:
      self._execute(self._nfsread, filename=filename, position_in_file=0,
            size_of_read=rsize, skip_integrity_check=True, rsize=rsize)
    self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket, objname,
                                              rsize, Constants.UNLOCK)

  def _nfs_over_write(self, filename, start_offset, size_of_write, block_size):
    data, md5sum = self._testobj._get_data(size_of_write)
    file_open_flag="rb+" #Since its overwrite, open file in r+w binary format.
    self._nfs_write(filename=filename, data=data, file_open_flag=file_open_flag,
                    block_size=block_size, seek=start_offset)

  def _nfs_debug_logging(self, msg, caller=None):
    if not self._testobj._kwargs["nfs_debug_logging"]:
      return
    if not caller: caller=inspect.stack()[1][3]
    DEBUG("(%s:%s) %s"%(caller, inspect.stack()[1][2], msg))

  def nfswrite(self, objnum, block_size):
    is_sparse = choice(self._testobj._kwargs["file_layout_type"]
                                  ) == Constants.SPARSE_FILE_DEFAULT_PREFIX
    bucket, objname, num_versions, num_parts, objsize = \
                self._testobj._objname_distributor.get_objname_and_upload_type(
                                    objnum, is_nfs=True, is_sparse=is_sparse)
    if not self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket,
                                             objname, objsize, Constants.LOCK):
      return
    if num_parts > 0:
      return self._multipart_nfswrite(bucket, objname, objsize, num_parts,
                                      num_versions, block_size)
    filename = self._get_filepath(bucket, objname)
    self._create_dir(filename)
    file_open_flag="wb" #Open file in write+binary mode
    for num_ver in range(num_versions):
      data, md5sum = self._testobj._get_data(objsize)
      self._nfs_debug_logging("Creating file : %s, Size : %s, Md5sum : %s, "
        "Fileversion : %s, OpenFlag : %s"
        %(filename, objsize, md5sum, num_ver, file_open_flag))
      if not is_sparse:
        res = self._execute(self._nfs_write, filename=filename,
                          data=data, file_open_flag=file_open_flag,
                          block_size=block_size, rsize=objsize)
      else:
        self._execute(self._nfs_sparse_write, filename=filename, data=data,
            file_open_flag=file_open_flag, block_size=block_size, rsize=objsize,
            sparse_size=self._testobj._kwargs["sparse_writes_seek_distance"])
      set_exit_marker_on_failure = True
      if self._testobj._kwargs["ignore_nfs_size_errs_vs_s3"]:
        set_exit_marker_on_failure = False
      try:
        self._execute(self._validate_s3size_against_nfs, bucket=bucket,
                    objname=objname, filename=filename,
                    set_exit_marker_on_failure=set_exit_marker_on_failure)
      except:
        if not self._testobj._kwargs["ignore_nfs_size_errs_vs_s3"]:
          raise
    self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket, objname,
                                              objsize, Constants.UNLOCK)

  def _validate_s3size_against_nfs(self, bucket, objname, filename):
    s3size = filesize = 0
    try:
      s3size = self._testobj._head_object(bucket, objname,
                           set_exit_marker_on_failure=False)
    except Exception as err:
      ERROR("Failed to validate head-object after successful filecreation. "
            "Skip head after file creation : %s, Error : %s"
            %(self._testobj._kwargs["skip_head_after_put"], err.message))
      if "Not Found" in err.message and \
          self._testobj._kwargs["skip_head_after_put"]:
        return
      raise
    try:
      filesize = self._nfs_lookup_file(filename)
    except Exception as err:
      ERROR("Failed to lookup file %s after successful filecreation. "
          "Error : %s"%(filename, err))
      if self._testobj._kwargs["skip_head_after_put"]:
        return
      raise
    if s3size["ContentLength"] != filesize["size"]:
      msg = "Mismatch in size reported for bucket/obj/filename : %s / %s / %s."\
            "Head Obj : %s, Filestats : %s"%(bucket, objname, filename, s3size,
                                             filesize)
      ERROR(msg)
      if not self._testobj._kwargs["ignore_nfs_size_errs_vs_s3"]:
        raise Exception(msg)

  def _nfs_sparse_write(self, filename, data, file_open_flag, block_size,
                  buffering=0, sparse_size=[10*1024*1024, 100*1024*1024*1024]):
    write_fh = self._open_fh(filename, file_open_flag, buffering=buffering,
                             use_os_mod=True)
    current_file_position = 0
    while True:
      data_to_write = data if isinstance(data, str) else data.read(1024*1024)
      if not data_to_write:
        self._close_fh(filename, write_fh)
        break
      seek = randrange(sparse_size[0], sparse_size[-1], 1024)
      move_forward = choice([True, False ]) if self._testobj._kwargs[
                      "randomise_sparse_write"] else False
      msg = "Sparse filename : %s, Current Position : %s"%(filename,
                                                          current_file_position)
      if move_forward and current_file_position > seek:
        current_file_position -= seek
      else:
        current_file_position += seek
      self._nfs_debug_logging("%s, New write position : %s"
                              %(msg, current_file_position))
      try:
        self._execute(self._posix.nfs_op_seek, fh=write_fh,
                      position_in_file=current_file_position, retry_count=0,
                      set_exit_marker_on_failure=False)
        self._write_data_to_file(filename, write_fh, data_to_write, block_size)
      except:
        self._close_fh(filename, write_fh)
        raise
      if isinstance(data, str): data = ""


  def _nfs_write(self, filename, data, file_open_flag, block_size, seek=0,
                  buffering=0):
    #TODO :  Handle when blocksize is larger than 1M, since we are reading max
    #1M from RandomData object, and thats the max write we support for now.
    write_fh = self._open_fh(filename, file_open_flag, buffering=buffering,
                             use_os_mod=True, seek=seek)
    self._nfs_debug_logging("Write file %s @ position : %s (Expected : %s)"
                            %(filename, write_fh.tell(), seek))
    datamd5 = None
    if isinstance(data, str) or isinstance(data, unicode):
      try:
        self._write_data_to_file(filename, write_fh, data, block_size)
      except:
        self._close_fh(filename, write_fh)
        raise
      datamd5 = md5(data).hexdigest()
    else:
      try:
        while True:
          current_position = write_fh.tell()
          tmpdata = data.read(1024*1024)
          if not tmpdata:
            break
          self._write_data_to_file(filename, write_fh, tmpdata, block_size)
          #self._nfs_debug_logging("%s : Current Position : %s (After Data "
          #       "Written size : %s, Previous_position : %s), Md5 of data : %s"
          #       %(filename, write_fh.tell(), len(tmpdata), current_position,
          #       md5(tmpdata).hexdigest()))
      except:
        self._close_fh(filename, write_fh)
        raise
      datamd5, _ = data.md5sum()
    self._close_fh(filename, write_fh)
    if self._testobj._kwargs["skip_read_after_write"]:
      return datamd5
    read_md5, read_len = self._get_md5sum_of_file(filename, len(data), seek,
                                                  buffering)
    if read_md5 != datamd5 or len(data) != read_len:
      raise Exception("Read after write file : %s : Data Corruption. "
                     "Expected md5 %s vs Found %s, Expected size %s vs Found %s"
                      %(filename, datamd5, read_md5, len(data), read_len))
    self._nfs_debug_logging("Read after write %s, Md5 Expected %s vs Found "
                            "%s, Size Expected %s vs Found %s"
                            %(filename, datamd5, read_md5, len(data), read_len))
    return datamd5

  def _get_md5sum_of_file(self, filename, size_of_data_to_read, seek=0,
                          buffering=0):
    self._nfs_debug_logging("Getting MD5sum of file : %s, Size of data to read "
      " : %s, Seek : %s, buffering : %s"%(filename, size_of_data_to_read, seek,
      buffering))
    read_fh = self._open_fh(filename, "rb", buffering=buffering,
                             use_os_mod=True, seek=seek)
    filemd5 = md5()
    filelen = 0
    while True:
      if filelen == size_of_data_to_read:
        self._close_fh(filename, read_fh)
        break
      blksize = 1024*1024
      if size_of_data_to_read < blksize:
        blksize = size_of_data_to_read
      if size_of_data_to_read - filelen < blksize:
        blksize = size_of_data_to_read - filelen
      read_data = read_fh.read(blksize)
      filelen += len(read_data)
      filemd5.update(read_data)
    return filemd5.hexdigest(), filelen

  def _increase_decrease_open_fd_count(self, increase=0, decrease=0,
                                       close_failed=0, open_failed=0):
    with self._lock:
      if increase > 0:
        self._testobj._total_ops_recorded["nfsops"]["nfsstats"]["filesdirs"
                                              ]["num_open_files"] += increase
      if decrease > 0:
        self._testobj._total_ops_recorded["nfsops"]["nfsstats"]["filesdirs"
                                              ]["num_open_files"] -= decrease
      if close_failed > 0:
        self._testobj._total_ops_recorded["nfsops"]["nfsstats"]["filesdirs"
                                        ]["num_close_fd_failed"] += close_failed
      if open_failed > 0:
        self._testobj._total_ops_recorded["nfsops"]["nfsstats"]["filesdirs"
                                        ]["num_open_fd_failed"] += open_failed

  def _write_data_to_file(self, filename, write_fh, tmpdata, block_size):
    start = 0
    while True:
      end = start + block_size
      data_to_write = tmpdata[start:end]
      start = end
      if not data_to_write:
        break
      self._nfs_debug_logging("Writing file %s, @ offset start : %s, data size "
        ": %s, blocksize : %s, Sync_on_write  :%s, file.tell : %s, Md5 of data "
        ": %s"%(filename, start, len(data_to_write), block_size,
         self._testobj._kwargs["sync_data_on_every_write_to_file"],
         write_fh.tell(), md5(tmpdata).hexdigest()))
      self._execute(self._posix.nfs_op_write, fh=write_fh, data=data_to_write,
                    retry_count=0, set_exit_marker_on_failure=False)
      if self._testobj._kwargs["sync_data_on_every_write_to_file"]:
        self._execute(self._posix.nfs_op_flush_file, fh=write_fh,
                       retry_count=0, set_exit_marker_on_failure=False)
      self._nfs_debug_logging("Write completed. file %s, @ offset start : %s,"
        " data size : %s, blocksize : %s, file.tell  :%s"%(filename, start,
         len(data_to_write), block_size, write_fh.tell()))

  def _create_dir(self, filename=None, dirname=None):
    if filename:
      dirs = filename.split("/") #If filename, then just split filename with
      dirs.pop() #Remove last item, it will be always filename
      if dirs: #If still more items in list, it mean, we need to create dir
        dirname = "/".join(dirs)
    if not dirname:
      return
    dname = "/"
    for subdir in dirname.split("/"):
      dname = path.join(dname, subdir)
      if not self._execute(self._posix.nfs_op_exists, location=dname):
        self._execute(self._posix.nfs_op_mkdir, dirpath=dname)
      else:
        if not self._execute(self._posix.nfs_op_isdir, dirpath=dname):
          ERROR("%s is not a dir but file"%dname)

  def _multipart_nfswrite(self, bucket, objname, objsize, num_part,
                          num_versions, block_size):
    total_size = 0
    file_open_flag = "wb" #Open file in w+b mode. Overwrites file if exists
    file_position = 0
    etag = ""
    mpth = self._testobj._total_ops_recorded["nfsops"]["mounts"][bucket]["path"]
    filename = path.join(mpth, objname)
    self._create_dir(filename)
    for partnum in range(1, num_part+1):
      if self._testobj._exit_test("multipart_nfswrite"):
        return
      data, md5sum = self._testobj._get_data(objsize)
      self._nfs_debug_logging("Writing multipart file %s, open_flag : %s, "
        "data size : %s, MD5sum : %s, blocksize : %s, Num_part : %s, "
        "totalsize : %s"%(filename, file_open_flag, len(data), md5sum,
                          block_size, partnum, total_size))
      res = self._execute(self._nfs_write, filename=filename, data=data,
                          file_open_flag=file_open_flag, block_size=block_size,
                          seek=total_size, rsize=objsize)
      file_open_flag = "rb+" #Open file in r+w in binary format.Does not owrites
      total_size += objsize
      self._nfs_debug_logging("Writing multipart completed. file %s, Total Size"
        " : %s"%(filename, total_size))
      if not md5sum:
        if isinstance(data, str):
          md5sum = md5(data).hexdigest()
        elif hasattr(data, "md5sum"):
          md5sum, _ = data.md5sum()
      etag += md5sum
    md5sum = "%s-%s"%(md5(etag.decode('hex')).hexdigest(), num_part+1)
    try:
      headres = self._testobj._head_object(Bucket=bucket, Key=objname)
    except Exception as err:
      ERROR("Hit error while validating head-object after successful "
            "filename : %s (parts : %s)"%(filename, num_part))
      self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket, objname,
                                              objsize, Constants.UNLOCK)
      raise
    self._nfs_debug_logging("Multipart file : file %s, Total Size : %s, "
        "Head Object : %s"%(filename, total_size, headres))
    if total_size != headres["ContentLength"]:
      msg = "%s : Size mismatch aginst locally calculated vs found in head obj"\
            " Expected md5 vs found : %s vs %s. Head_Object : %s"%(filename,
            total_size, headres["ContentLength"], headres)
      ERROR(msg)
      self._testobj._set_exit_marker(msg, exception=''.join(format_exc()))
    self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket, objname,
                                               objsize, Constants.UNLOCK)
    #TODO : Write code to validate metadata correctness w.r.t user, perm, and
    #other metadata attributes

  def nfscopy(self, Bucket, Key, CopySource, srctag, rsize, read_after_copy):
    self._move_or_copy_file(self._posix.nfs_op_copy, Bucket, Key, CopySource,
                            srctag, rsize, read_after_copy)

  def nfsrename(self, Bucket, Key, CopySource, srctag, rsize, read_after_copy):
    self._move_or_copy_file(self._posix.nfs_op_rename, Bucket, Key, CopySource,
                            srctag, rsize, read_after_copy)

  def _move_or_copy_file(self, method, Bucket, Key, CopySource, srctag, rsize,
                         read_after_move):
    src_file_md5, srcsize, file_md5, dstsize = None, None, None, None
    sbucket, sfilename = CopySource["Bucket"], CopySource["Key"]
    srcpth = self._testobj._total_ops_recorded[
                                            "nfsops"]["mounts"][sbucket]["path"]
    srcfilename = path.join(srcpth, sfilename)
    dstpth = self._testobj._total_ops_recorded[
                                            "nfsops"]["mounts"][Bucket]["path"]
    dstfilename = path.join(dstpth, Key)
    self._create_dir(dstfilename)
    if method.__name__ == "nfs_op_rename":
      return self._execute(self.rename_file, nfsop=method, Bucket=Bucket,
                           Key=Key, rsize=rsize, size=rsize, sbucket=sbucket,
                           sfilename=sfilename, srcfilename=srcfilename,
                           dstfilename=dstfilename,
                           read_after_move=read_after_move)
    else:
      return self._execute(self.copy_file, nfsop=method, Bucket=Bucket, Key=Key,
                           rsize=rsize, size=rsize, sbucket=sbucket,
                           sfilename=sfilename, srcfilename=srcfilename,
                           dstfilename=dstfilename,
                           read_after_move=read_after_move)

  def rename_file(self, nfsop, Bucket, Key, size, sbucket, sfilename,
                          srcfilename, dstfilename, read_after_move):
    self._may_be_rename_or_copy_file(nfsop, Bucket, Key, size, sbucket,
                    sfilename, srcfilename, dstfilename, read_after_move, True)

  def copy_file(self, nfsop, Bucket, Key, size, sbucket, sfilename,
                          srcfilename, dstfilename, read_after_move):
    self._may_be_rename_or_copy_file(nfsop, Bucket, Key, size, sbucket,
                         sfilename, srcfilename, dstfilename, read_after_move)

  def _may_be_rename_or_copy_file(self, method, Bucket, Key, size, sbucket,
                sfilename, srcfilename, dstfilename, read_after_move,
                skip_on_not_found=False):
    active_objs = [{"bucket":Bucket, "objname":Key, "size":size},
                   {"bucket":sbucket, "objname":sfilename, "size":size,
                    "objtype":"src"}]
    copysrc = {"Bucket":sbucket, "Key":sfilename}
    srcobj_ok, _ = self._testobj._check_src_obj_for_copy(copysrc, size,
                                          active_objs, skip_size_chk=True)
    if not srcobj_ok:
      return
    src_file_md5, srcsize =  None, None
    if read_after_move:
      src_file_md5, srcsize =  self._get_md5sum_of_file(srcfilename, size)
    self._nfs_debug_logging("Performing Operation : %s, SrcFile : %s, DstFile :"
                            " %s"%(method.__name__, srcfilename, dstfilename))
    method(filepath=srcfilename, dstfile=dstfilename)
    #TODO add copy validation and move below code out
    self._rename_copy_validation(method, sbucket, sfilename, Bucket, Key,
                                 srcfilename, dstfilename)
    self._nfs_debug_logging("Completed Operation : %s, SrcFile : %s, DstFile :"
                            " %s"%(method.__name__, srcfilename, dstfilename))
    if not read_after_move:
      self._testobj._objname_distributor.may_be_lock_unlock_objs(active_objs,
                                                              Constants.UNLOCK)
      return
    file_md5, dstsize = self._get_md5sum_of_file(dstfilename, size)
    self._testobj._objname_distributor.may_be_lock_unlock_objs(active_objs,
                                                              Constants.UNLOCK)
    self._nfs_debug_logging("SrcFile : (Name : %s, Size : %s, Md5 : %s), "
        "DstFile : (Name : %s, Size : %s, Md5 : %s)"
        %(srcfilename, srcsize, src_file_md5, dstfilename, dstsize, file_md5))
    msg = ""
    if file_md5 != src_file_md5:
      msg = "%s (src file : %s) : Data Corruption (op : %s). Expected md5sum "\
            "vs Found : %s vs %s\n"%(dstfilename, srcfilename, method.__name__,
                                   file_md5, src_file_md5)
    if dstsize != srcsize:
      msg += "%s (src file : %s) : Data Corruption (op : %s), Size Mismatch. "\
            "Expected Size vs Found : %s vs %s"%(dstfilename, srcfilename,
                                              method.__name__, dstsize, srcsize)
    if msg.strip():
      ERROR(msg.strip())
      self._testobj._set_exit_marker(msg, exception=''.join(format_exc()))
    #TODO : Write code to validate metadata correctness w.r.t user, perm, and
    #other metadata attributes

  def _rename_copy_validation(self, nfsop,  sbucket, sfilename, Bucket, Key,
                         srcfilename, dstfilename):
    msg = ""
    if nfsop.__name__ == "nfs_op_rename":
      if self._testobj._is_object_exists(sbucket, sfilename):
        msg += "%s / %s : object is renamed to %s but still exists in S3\n"%(
                                               sbucket, sfilename, dstfilename)
      if self._is_file_exists(srcfilename):
        msg += "%s file is renamed to %s, but still exists in nfs share\n"%(
                          srcfilename, dstfilename)
      if not self._is_file_exists(dstfilename):
        msg += "%s file is renamed to %s, but %s not found in nfs share \n"%(
                      srcfilename, dstfilename, dstfilename)
      if not self._testobj._is_object_exists(Bucket, Key):
        msg += "%s is renamed to %s, %s / %s object is not found in bucket"%(
                  srcfilename, dstfilename, Bucket, Key)
    else:
      if not self._testobj._is_object_exists(sbucket, sfilename):
        msg += "%s / %s : object is copied --> %s but not found in S3\n"%(
                                                sbucket, sfilename, dstfilename)
      if not self._is_file_exists(srcfilename):
        msg += "%s file is copied --> %s, but not found nfs share\n"%(
                          srcfilename, dstfilename)
      if not self._is_file_exists(dstfilename):
        msg += "%s file is copied --> %s, but %s not found in nfs share \n"%(
                      srcfilename, dstfilename, dstfilename)
      if not self._testobj._is_object_exists(Bucket, Key):
        msg += "%s is copied --> %s, %s / %s object is not found in bucket"%(
                  srcfilename, dstfilename, Bucket, Key)
    if msg.strip():
      raise Exception("%s : %s"%(nfsop.__name__, msg))

  def _is_multipart_file(self, filename, etag):
    if "-" in etag or self.testobj._multipart_obj_identifier in filename:
      return True
    return False

  def _get_data_md5sum(self, data):
    if hasattr(data, "md5sum"):
      return data.md5sum()[0]
    if isinstance(data, str):
      filemd5 = md5()
      filemd5.update(data)
      return filemd5.hexdigest()
    ERROR("%s is of unknown type to compute md5sum"%(type(data)))

  def _close_fh(self, filename, fd, ignore_failure=False, retry_count=0,
                set_exit_marker_on_failure=False, caller=None):
    if not caller: caller=inspect.stack()[1][3]
    if not fd:
      self._nfs_debug_logging("Filename : %s, Filehandle is NONE, caller : %s"
        %(filename, caller))
      return
    try:
      if self._testobj._kwargs["sync_data_on_file_close"]:
        self._nfs_debug_logging("Filename : %s, Fsyncing data, caller : %s"
          %(filename, caller))
        self._execute(self._posix.nfs_op_fsync, fh=fd,
                     retry_count=retry_count,
                     set_exit_marker_on_failure=set_exit_marker_on_failure)
        self._nfs_debug_logging("Filename : %s, Flushing file, Caller : %s"
          %(filename, caller))
        self._execute(self._posix.nfs_op_flush_file, fh=fd,
                     retry_count=retry_count,
                     set_exit_marker_on_failure=set_exit_marker_on_failure)
        self._nfs_debug_logging("File : %s, Fsynced/Flushed data, Caller : %s"
          %(filename, caller))
      if isinstance(fd, file):
        self._nfs_debug_logging("File : %s, closing file, Caller : %s"
          %(filename, caller))
        self._execute(fd.close, retry_count=retry_count,
                       set_exit_marker_on_failure=set_exit_marker_on_failure)
        self._nfs_debug_logging("Filename : %s, closed file, Caller : %s"
          %(filename, caller))
      else:
        self._execute(self._posix.nfs_op_close, fd=fd, retry_count=retry_count,
                       set_exit_marker_on_failure=set_exit_marker_on_failure)

      self._increase_decrease_open_fd_count(0, 1)
    except Exception as err:
      print print_stack()
      if ignore_failure:
        self._increase_decrease_open_fd_count(close_failed=1)
        ERROR("Failed to close file with fd : %s, Error : %s, Caller : %s"
              %(fd, err, caller))
        return None
      raise

  def _open_fh(self, filename, mode, retry_count=0, use_os_mod=False, seek=None,
               set_exit_marker_on_failure=False, caller=None, **kwargs):
    if not caller: caller=inspect.stack()[1][3]
    fh = None
    buffering = kwargs.pop("buffering", 0)
    if use_os_mod:
      self._nfs_debug_logging("Filename : %s, opening file. Flag : %s, "
        "buffering : %s, Caller : %s"%(filename, mode, buffering, caller))
      fh = self._execute(self._posix.nfs_op_open_fh, filename=filename,
                         mode=mode, buffering=kwargs.pop("buffering", 0),
                         retry_count=retry_count,
                   set_exit_marker_on_failure=set_exit_marker_on_failure,
                   **kwargs)
      if seek:
        self._nfs_debug_logging("File : %s, seeking file to : %s, Caller : %s"
                                %(filename, seek, caller))
        self._execute(self._posix.nfs_op_seek, fh=fh, position_in_file=seek,
                      retry_count=retry_count,
                      set_exit_marker_on_failure=set_exit_marker_on_failure)
    else :
      fh = self._execute(self._posix.nfs_op_open, filename=filename,
                   mode=mode, retry_count=retry_count,
                   set_exit_marker_on_failure=set_exit_marker_on_failure,
                   **kwargs)
    self._increase_decrease_open_fd_count(1)
    self._nfs_debug_logging("File : %s is opened and seeked : %s. Caller : %s"
                             %(filename, seek, caller))
    return fh

  def nfsread(self, bucket, objects, num_nfs_range_reads_per_file,
              nfs_read_type, skip_integrity_check, nfs_range_read_size):
    mpth = self._testobj._total_ops_recorded["nfsops"]["mounts"][bucket]["path"]
    while objects and not self._testobj._exit_test("nfsread"):
      obj = objects.pop()
      #NFS write size will keep changing until client finishes entire write.
      #So skip those objects if found
      if self._testobj._objname_distributor._is_obj_locked(bucket, obj["Key"],
                                                                  obj["Size"]):
        continue
      size, objname, lasttime = obj["Size"], obj["Key"], obj["LastModified"]
      if not objname.endswith("GMT"):
        self._testobj._total_ops_recorded["anomalis"]["DirFoundInS3List"][
                                                                  "count"] += 1
        if self._testobj._debug_level >= 3:
          ERROR("Skipping NFS read on %s, objname not ending with GMT"%objname)
        continue
      filename = path.join(mpth, objname)
      lookupres = self._nfs_lookup_file(filename)
      filesize = lookupres["size"]
      if size != filesize:
        #Possible when list was issued, nfsclient was still writing file.
        #so list will return less size than fine nfsfile since writes were
        #still in progress.
        tname = currentThread().name
        msg = "%s : Mismatch between filesize (%s) and objsize (%s), LookUp : "\
              "%s, S3List : %s, Filesize > S3size : %s"%(filename, filesize,
                                            size, lookupres, obj, filesize>size)
        if not self._testobj._kwargs["ignore_nfs_size_errs_vs_s3"]:
          self._testobj._total_ops_recorded["failed_workers"][tname] = msg
          raise Exception("(%s) : %s"%(tname, msg))
        ERROR("(%s) %s"%(tname, msg))
      read_type = choice(nfs_read_type)
      if read_type == Constants.NFS_READ_TYPE_FULL_READ:
        if self._testobj._is_sparse_object(None , filename):
          self._execute(self._nfs_sparse_file_read, filename=filename,
              position_in_file=0, size_of_read=filesize,
              skip_integrity_check=skip_integrity_check, rsize=filesize)
        else:
          self._execute(self._nfsread, filename=filename, position_in_file=0,
              size_of_read=filesize, skip_integrity_check=skip_integrity_check,
              rsize=filesize)
        continue
      for i in range(num_nfs_range_reads_per_file):
        start_offset, end_offset = self._get_range_to_read(filesize,
                                   self._utils.get_objsize(nfs_range_read_size))
        #Increment endoffset by 1 to match S3 behaviour
        size_of_read = (end_offset+1)-start_offset
        if self._testobj._is_sparse_object(None , filename):
          self._execute(self._nfs_sparse_range_read, filename=filename,
                    rsize=size_of_read, position_in_file=start_offset,
                    size_of_read=size_of_read, read_complete_file=False,
                    skip_integrity_check=skip_integrity_check)
        else:
          self._execute(self._nfs_range_read, filename=filename,
                    rsize=size_of_read, position_in_file=start_offset,
                    size_of_read=size_of_read, read_complete_file=False,
                    skip_integrity_check=skip_integrity_check)

  def _get_range_to_read(self, size, range_read_size):
    start_offset = 0
    if  size > 0:
      start_offset = randrange(0, size, 1024)
      return start_offset,  start_offset+range_read_size
    #When size is 0, then start-offset has to be greater than endoffset
    #else it throws invalid range error
    end_offset = start_offset+range_read_size
    return end_offset + 1, end_offset

  def _nfsread(self, filename, position_in_file, skip_integrity_check,
               size_of_read, block_size=[4*1024, 1024*1024],
               read_complete_file=True, skip_if_not_found=False):
    return self._nfsfileread(filename, position_in_file, skip_integrity_check,
              size_of_read, block_size, read_complete_file, skip_if_not_found)

  def _nfs_sparse_file_read(self, filename, position_in_file,
             skip_integrity_check, size_of_read, block_size=[4*1024, 1024*1024],
             read_complete_file=True, skip_if_not_found=False):
    return self._nfsfileread(filename, position_in_file, skip_integrity_check,
              size_of_read, block_size, read_complete_file, skip_if_not_found)

  def _nfsfileread(self, filename, position_in_file, skip_integrity_check,
               size_of_read, block_size=[4*1024, 1024*1024],
               read_complete_file=True, skip_if_not_found=False):
    if not self._is_file_exists(filename):
      if skip_if_not_found:
        ERROR("%s not found, skipping nfs read")
        return
    return self._read_file(filename, position_in_file, skip_integrity_check,
                          size_of_read, block_size, read_complete_file)

  def _nfs_range_read(self, filename, position_in_file, skip_integrity_check,
               size_of_read, block_size=[4*1024, 1024*1024],
               read_complete_file=False, skip_if_not_found=False):
    return self._nfs_file_range_read(filename, position_in_file,
             skip_integrity_check, size_of_read, block_size, read_complete_file,
             skip_if_not_found)

  def _nfs_sparse_range_read(self, filename, position_in_file,
             skip_integrity_check, size_of_read, block_size=[4*1024, 1024*1024],
             read_complete_file=False, skip_if_not_found=False):
    return self._nfs_file_range_read(filename, position_in_file,
             skip_integrity_check, size_of_read, block_size, read_complete_file,
             skip_if_not_found)

  def _nfs_file_range_read(self, filename, position_in_file,
           skip_integrity_check, size_of_read, block_size=[4*1024, 1024*1024],
           read_complete_file=False, skip_if_not_found=False):
    if not self._is_file_exists(filename):
      if skip_if_not_found:
        ERROR("%s not found, skipping nfs range read")
        return
    return self._read_file(filename, position_in_file, skip_integrity_check,
                          size_of_read, block_size, read_complete_file)


  def _read_file(self, filename, position_in_file, skip_integrity_check,
               size_of_read, block_size, read_complete_file):
    STLFS = "Stale file handle"
    fh = self._open_fh(filename, "r", use_os_mod=True, seek=position_in_file,
                       set_exit_marker_on_failure=STLFS)
    self._nfs_debug_logging("Reading file : %s, Seek Position : %s"
                            %(filename, position_in_file))
    datalen = 0
    while True:
      length = block_size
      if isinstance(block_size, list):
        if len(block_size) == 1 or block_size[0] >= block_size[-1]:
          length = block_size[0]
        elif (block_size[-1]-block_size[0]) > 1:
          length = randrange(block_size[0], block_size[-1], 1024)
      data = self._execute(self._posix.nfs_op_os_read, fh=fh, length=length,
                           set_exit_marker_on_failure=[STLFS], retry_count=0)
      fh_location = fh.tell()
      if not data:
        self._close_fh(filename, fh, True)
        if not skip_integrity_check:
          if datalen > size_of_read:
            ERROR("%s : NFS read more data vs size returned in S3list"
                  ". (CompleteRead : %s) Filesize : (size_of_read) %s vs "
                  "(datalen) %s, ReadBlockSize : %s, Tell : %s, "
                  "(Ignore_on_size_diff : %s)"%
                  (filename, read_complete_file, size_of_read, datalen,
                   length, fh_location,
                   self._testobj._kwargs["ignore_nfs_size_errs_vs_s3"]))
            self._testobj._total_ops_recorded["anomalis"][
                                                "NfsReadMismatch"]["count"] += 1
          if not self._testobj._kwargs["ignore_nfs_size_errs_vs_s3"]:
            assert size_of_read == datalen, "Incomplete NFS Read, (%s) "\
                "Mismatch in read size (CompleteRead : %s) Filesize : "\
                "(size_of_read)  %s vs (datalen) %s, ReadBlockSize : %s, Tell "\
                ": %s"%(filename, read_complete_file, size_of_read, datalen,
                        length, fh_location)
        return {}
      self._nfs_debug_logging("File : %s, current_filelocation : %s, read data "
             "size : %s, length_of_read : %s"%(filename, fh_location, len(data),
                                               length))
      if skip_integrity_check: continue
      try:
        self._testobj._range_read_integrity_check(bucket="", objname=filename,
                                              data=data, object_offset=datalen)
      except Exception as err:
        self._close_fh(filename, fh, True)
        if "Corruption" not in err.message:
          raise
        utime = time()
        filename = "%s_%s_%s_%s_%s"%(utime, filename.split("/")[3],
                              filename.split("/")[-1], position_in_file, length)
        nfs_fn = "%s_%s_nfs"%(self._testobj.logpath, filename)
        self._testobj._dump_data_to_file(nfs_fn, data)
        ERROR("Corrupt data dumped to : %s"%(nfs_fn))
        raise
      datalen += len(data)
      if not read_complete_file:
        break
    self._close_fh(filename, fh, True)
    return {}

  def nfslist(self, bucket, skip_list_validation_vs_s3, prefix="",
              walk_all_dirs=True):
    if self._testobj._exit_test("list_nfs_dir"):
      return {}
    self._testobj._is_nfslist_workload = True
    mpth = self._testobj._total_ops_recorded["nfsops"]["mounts"][bucket]["path"]
    if walk_all_dirs:
      return self._execute(self._nfs_walk_dir, bucket=bucket, mountpath=mpth)
    s3_files, s3list = [] , {}
    if not skip_list_validation_vs_s3:
      s3list = self._testobj._list_delimeter(bucket, delimeter="/",
                                             prefix=prefix, return_res=True)
      s3_files =  self._testobj._get_files_dirs_from_s3list(s3listres)
    dirpath = path.join(mpth, prefix)
    nfs_list = self._execute(self._posix.nfs_op_listdir, dirpath=dirpath)
    if not skip_list_validation_vs_s3 and len(s3_files) < len(nfs_list):
        if not self._testobj._is_delete_workload_running and not \
            s3list.get("IsTruncated"):
          msg = "%s:%s : Num files found in s3list (%s) less than nfs (%s)"\
                %(bucket, prefix, len(s3_files), len(nfs_list))
          ERROR(msg)
          self._testobj._set_exit_marker(msg, exception=''.join(format_exc()))
    total_files, total_dirs, total_non_existing = 0, [], 0
    for item in nfs_list:
      fullpath = path.join(dirpath, item)
      try:
        self._nfs_lookup_file(fullpath)
      except Exception as err:
        total_non_existing += 1
        ERROR("Failed to loopup file : %s, Error : %s, Is delete running : %s"
              %(fullpath, err, self._testobj._is_delete_workload_running))
        if not self._testobj._may_be_ignore_object(fullpath):
          raise
        continue
      if path.isdir(fullpath):
        total_dirs.append(fullpath)
      else:
        total_files += 1
    with self._lock:
      if bucket in self._testobj._bucket_info["nfs_list_bucket_map"]["in_use"]:
        self._testobj._bucket_info["nfs_list_bucket_map"][
                                                        "in_use"].remove(bucket)
      self._testobj._total_ops_recorded["nfsops"]["nfsstats"][
                                    "filesdirs"]["totaldirs"] += len(total_dirs)
      self._testobj._total_ops_recorded["nfsops"]["nfsstats"][
                                    "filesdirs"]["totalfiles"] += total_files
      self._testobj._total_ops_recorded["nfsops"]["nfsstats"][
                              "filesdirs"]["lookupfailed"] += total_non_existing

  def _nfs_walk_dir(self, bucket, mountpath):
    if bucket not in self._testobj._total_ops_recorded[
                                                    "nfsops"]["active_listing"]:
      with self._lock:
        if bucket not in self._testobj._total_ops_recorded[
                                                    "nfsops"]["active_listing"]:
          bucketiter = walk(mountpath)
          self._testobj._total_ops_recorded["nfsops"][
                           "active_listing"][bucket] = {"iterator" : bucketiter}
    try:
      root, dirs, files = self._testobj._total_ops_recorded["nfsops"][
                                    "active_listing"][bucket]["iterator"].next()
    except StopIteration as err:
      with self._lock:
        bucketiter = walk(mountpath)
        self._testobj._total_ops_recorded["nfsops"][
                        "active_listing"][bucket] = {"iterator" : bucketiter }
      root, dirs, files = self._testobj._total_ops_recorded["nfsops"]\
                          ["active_listing"][bucket]["iterator"].next()
    with self._lock:
      if bucket in self._testobj._bucket_info["nfs_list_bucket_map"]["in_use"]:
        self._testobj._bucket_info["nfs_list_bucket_map"][
                                                        "in_use"].remove(bucket)
    numfiles, numlookupfailed = 0, 0
    for filename in files:
      fullpath = path.join(root, filename)
      try:
        self._nfs_lookup_file(fullpath)
        numfiles += 1
      except Exception as err:
        numlookupfailed += 1
        if self._testobj._may_be_ignore_object(fullpath):
          continue
        ERROR("Failed to loopup file : %s, Error : %s, Is delete running : %s"
              %(fullpath, err, self._testobj._is_delete_workload_running))
        raise
    with self._lock:
      self._testobj._total_ops_recorded["nfsops"]["nfsstats"][
                                                  "filesdirs"]["totaldirs"] += 1
      self._testobj._total_ops_recorded["nfsops"]["nfsstats"][
                                          "filesdirs"]["totalfiles"] += numfiles
      self._testobj._total_ops_recorded["nfsops"]["nfsstats"][
                                 "filesdirs"]["lookupfailed"] += numlookupfailed

  def _is_file_exists(self, filename):
    try:
      self._execute(self._posix.nfs_op_fstat, filename=filename,
                  mode=posix.O_RDONLY, set_exit_marker_on_failure=False)
      return True
    except Exception as err:
      if "No such file or directory" in str(err):
        return False
      raise

  def _nfs_lookup_file(self, filename, retry=-1):
    retry = retry if retry > 1 else self._nfs_op_retry(filename)
    res = self._execute(self._posix.nfs_op_fstat, filename=filename,
                  mode=posix.O_RDONLY, retry_count=retry,
                  set_exit_marker_on_failure=False)
    osres = self._execute(self._posix.nfs_op_isfile, filepath=filename,
                  retry_count=retry,
                  set_exit_marker_on_failure=False)
    result = {"res":res, "atime":self._utils.convert_time(seconds=res.st_atime),
              "dev":res.st_dev,
              "mtime":self._utils.convert_time(seconds=res.st_mtime),
              "nlink":res.st_nlink,
              "ctime":self._utils.convert_time(seconds=res.st_ctime),
              "uid":res.st_uid,
              "size":res.st_size, "filename":filename, "mode":res.st_mode,
              "guid":res.st_gid, "inode":res.st_ino, "isfile":osres,
              "isdir": True if not osres else False}
    return result

  def _nfs_op_retry(self, key):
    if self._testobj._may_be_ignore_object(key):
      return 0
    return self._testobj._retry_count

  def mountpath(self, bucket):
    return self._testobj._total_ops_recorded["nfsops"]["mounts"][bucket]["path"]

  def nfsdelete(self, bucket, objects, skip_lookup_before_delete,
                skip_lookup_after_delete, skip_read_before_delete):
    files_not_deleted = {}
    num_files_to_complete_read = 5
    for obj in objects:
      filename = path.join(self.mountpath(bucket), obj["Key"])
      filesize = obj["Size"]
      if (filesize == 0 and
          Constants.ZEROBYTE_OBJ_IDENTIFIER not in obj["Key"]) or (
          self._testobj._objname_distributor._is_obj_locked(bucket, obj["Key"],
                               obj["Size"])) or (".nfs000000000" in obj["Key"]):
        #Skip object which are active or objects which were being created when
        #S3 list was executed, which will return 0 size but _nfs_read will
        #complain or found nfs tmp files which are created by nfs-client during
        #deletes
        continue
      else:
        if not self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket,
                                      obj["Key"], obj["Size"],  Constants.LOCK):
          continue
      if not skip_read_before_delete:
        if num_files_to_complete_read > 0:
          self._execute(self._nfsread, filename=filename, position_in_file=0,
              size_of_read=filesize, skip_integrity_check=False,
              rsize=filesize, skip_if_not_found=True)
          num_files_to_complete_read -= 1
        elif filesize > 1024:
          read_size = 4096 if filesize > 4096 else 1024
          self._execute(self._nfs_range_read, filename=filename,
                        position_in_file=randrange(0, filesize, read_size),
                        size_of_read=read_size, skip_integrity_check=False,
                        rsize=read_size, skip_if_not_found=True)
      if not skip_lookup_before_delete:
        try:
          self._nfs_lookup_file(filename, retry=1)
        except Exception as err:
          ERROR("Failed to lookup file %s, Error : %s"%(filename, err))
      self._execute(self._delete_nfs_file, filename=filename, rsize=filesize)
      self._testobj._objname_distributor.may_be_lock_unlock_obj(bucket,
                              obj["Key"], obj["Size"], Constants.UNLOCK)
      if not skip_lookup_after_delete:
        #Fix below for loop. This is just temp fix to retry lookup
        #if it succeeds after successful deletion.
        for i in range(3):
          try:
            lookup_details = self._nfs_lookup_file(filename, retry=0)
            if filename not in files_not_deleted:
              files_not_deleted[filename] = lookup_details
            continue
          except Exception as err:
            if filename in files_not_deleted:
              files_not_deleted.pop(filename)
            break
          sleep(30)
    if files_not_deleted:
      ERROR("Files deleted but lookup still succeeded : %s"%(files_not_deleted))
      self._testobj._total_ops_recorded["anomalis"][
                                              "LookUpAfterDelete"]["count"] += 1
      #self._testobj._set_exit_marker(msg, exception=''.join(format_exc()))

  def _delete_nfs_file(self, filename):
    if self._is_file_exists(filename):
      return self._posix.nfs_op_remove(filename)
    ERROR("%s not found, Skipping deletion"%filename)

class IOWorkload(object):
  def __init__(self, workload_type):
    self._workload_type = workload_type

  def start_io_workload(self, filename, size, io_type, block_size, runtime,
                        buffered_io, iodepth):
    if self._workload_type == "fio":
      fiow = FioIO(filename, size, io_type, block_size, runtime, buffered_io,
                   iodepth)
      fiow.start()
      return fiow
    else:
      rdw = RandomDataIO(filename, size, io_type, block_size, runtime,
                         buffered_io)
      rdw.start()
      return rdw

class RandomDataIO():
  def __init__(self, filename, size, io_type, block_size, runtime, buffered_io):
    self._filename = filename
    self._size = size
    self._io_type = io_type
    self._block_size = block_size
    self._runtime = runtime
    self._buffered_io = buffered_io

  def start(self):
    if self.is_running():
      return True
    raise Exception("Failed to run RandomData Workload.")

  def is_running(self):
    return False

class FioIO():
  def __init__(self, filename, size, io_type, block_size, runtime, buffered_io,
               iodepth, read_perc=None):
    self._filename = filename
    self._size = size if size > 1024*1024 else 1024*1024 #Hack for fio to run
    self._io_type = io_type
    self._block_size = block_size
    self._runtime = runtime
    self._buffered_io = buffered_io
    self._iodepth = iodepth
    self._utils = Utils()

  def start_fio(self, background=False, output_file="/dev/null"):
    cmd = 'fio -name=%s --filename=%s '%(self._io_type, self._filename)
    if background:
      cmd = 'nohup fio -name=%s --filename=%s '%(self._io_type, self._filename)
    cmd += '--ioengine=libaio --time_based '
    cmd += '--runtime=%s --direct=%s '%(self._runtime, self._buffered_io)
    cmd += '--bs=%s --iodepth=%s '%(self._block_size, self._iodepth)
    cmd += '--rw=%s --size=%s'%(self._io_type, self._size)
    #cmd += ' > %s 2>&1 '%output_file
    #INFO("Starting fio workload with cmd : %s, Backgroud : %s, OutputFile : %s"
    #     %(cmd, background, output_file))
    #Utils.remote_exec(client, cmd, "root", "nutanix/4u", background=True)
    try:
      out, err = self._utils.local_exec(cmd, retry=0, ignore_err=True)
    except Exception as err:
      ERROR("Failed to execute fio workload.Filename : %s, Cmd : %s, Error : %s"
            %(self._filename, cmd, err))
      raise
    try:
      msg = "TIMESTAMP : %s\n"%datetime.now()
      msg += "Filename : %s\n"%self._filename
      msg += "Cmd : %s\n"%cmd
      msg += "Out : \n========\n%s\n"%out
      msg += "Err : \n%s\n"%err
      msg += "%s\n\n"%("*"*120)
      Utils.write_to_file(msg, output_file, "a")
    except Exception as err:
      print "FIO failed to write logs. Error : %s"%err
    if not background:
      return out, err
    if self.is_running():
      return True
    ERROR("Failed to run fio on %s. Error : %s"%(self._filename, err))
    raise Exception("Failed to run fio. Error : %s"%(err))

  def is_running(self):
    out, err = self._utils.local_exec("ps aux| grep fio |grep %s | grep -v grep"
                                      %(self._filename), retry=0)
    if self._filename in out:
      return True
    return False

class VdbenchIO():
  def __init__(self, filename, size, io_type, block_size, runtime, buffered_io,
               iodepth, read_perc=50, vdbench_binary_location="/root/vdbench"):
    self._filename = filename
    self._size = size if size > 1024*1024*10 else 10*1024*1024
    self._io_type = io_type
    self._block_size = block_size
    self._runtime = runtime
    self._buffered_io = buffered_io
    self._iodepth = iodepth
    self._utils = Utils()
    self._read_perc = read_perc
    self._vdbench_binary_location = vdbench_binary_location

  def start_vdbench(self, output_dir, background=False):
    interval = 10 if self._runtime > 30 else 1
    parm = "sd=sd1,lun=%s,size=%s,openflags=%s\n"%(self._filename, self._size,
                                                   self._buffered_io)
    parm += "wd=wd1,sd=sd1,xf=%s,rdpct=%s\n"%(self._block_size, self._read_perc)
    parm += "rd=rd1,wd=wd1,iorate=%s,elapsed=%s,interval=%s"%(self._iodepth,
                                                        self._runtime, interval)
    output_dir = path.join(output_dir, md5(self._filename).hexdigest())
    if not path.isdir(output_dir):
      self._utils.local_exec("mkdir -p %s"%(output_dir), retry=0,
                                        ignore_err=True)
    params = path.join(output_dir, "param")
    Utils.write_to_file(parm, params, "w")
    cmd = "%s -o %s -f %s"%(self._vdbench_binary_location, output_dir, params)
    try:
      return self._utils.local_exec(cmd, retry=0, ignore_err=True)
    except Exception as err:
      ERROR("Failed to execute vdbench workload. Filename : %s, Cmd : %s, "
            "Error : %s"%(self._filename, cmd, err))
      raise
    ERROR("Failed to run vdbench on %s. Error : %s"%(self._filename, err))
    raise Exception("Failed to run vdbench. Error : %s"%(err))

  def is_running(self):
    cmd = "ps aux| grep %s | grep %s | grep -v grep"%(
            self._vdbench_binary_location.split("/")[-1],
            self._filename.split("/")[-1])
    out, err = self._utils.local_exec(cmd, retry=0)
    if self._filename in out:
      return True
    return False

class PosixOps():
  import shutil
  def __init__(self):
    pass

  def nfs_op_fsync(self, fh):
    fsync(fh)

  def nfs_op_flush_file(self, fh):
    if hasattr(fh, "flush"):
      fh.flush()
      return True
    return False

  def nfs_op_os_read(self, fh, length):
    return fh.read(length)

  def nfs_op_open_fh(self, filename, mode, buffering):
    return open(filename, mode, buffering)

  def nfs_op_seek(self, fh, position_in_file):
    fh.seek(position_in_file)

  def nfs_op_exists(self, location):
    return path.exists(location)

  def nfs_op_isdir(self, dirpath):
    return path.isdir(dirpath)

  def nfs_op_isfile(self, filepath):
    return path.isfile(filepath)

  def nfs_op_fstat(self, filename, mode, **kwargs):
    fd = self.nfs_op_open(filename, mode)
    res = posix.fstat(fd)
    self.nfs_op_close(fd)
    return res

  def nfs_op_access(self, filename, mode, **kwargs):
    return posix.access(filename, mode)

  def nfs_op_openfd(self, fd, mode, bufsize, **kwargs):
    return posix.openfd(fd, mode, bufsize)

  def nfs_op_open(self, filename, mode, **kwargs):
    return posix.open(filename, mode)

  def nfs_op_close(self, fd, **kwargs):
    return posix.close(fd)

  def nfs_op_lseek(self, fd, position, mode, **kwargs):
    return posix.lseek(fd, position, mode)

  def nfs_op_read(self, fd, length, **kwargs):
    return posix.read(fd, length)

  def nfs_op_listdir(self, dirpath, **kwargs):
    return posix.listdir(dirpath)

  def nfs_op_rename(self, filepath, dstfile=None, **kwargs):
    if not dstfile:
      dstfile = filepath+str(time())
    return shutil.move(filepath, dstfile)

  def nfs_op_copy(self, filepath, dstfile=None, **kwargs):
    if not dstfile:
      dstfile = filepath+str(time())
    return shutil.copyfile(filepath, dstfile)

  def nfs_op_statvfs(self, filepath, **kwargs):
    return posix.statvfs(filepath)

  def nfs_op_getcwd(self, filepath, **kwargs):
    return posix.getcwd()

  def nfs_op_chdir(self, filepath, **kwargs):
    return posix.chdir(filepath)

  def nfs_op_chmod(self, filepath, mode=S_IWOTH, **kwargs):
    return posix.chmod(filepath, mode)

  def nfs_op_getuid(self, **kwargs):
    return posix.getuid()

  def nfs_op_getgid(self, **kwargs):
    return posix.getgid()

  def nfs_op_chown(self, filepath, uid=None, gid=None, **kwargs):
    if not uid:
      uid = posix.getuid()
    if not gid:
      gid = posix.getgid()
    return posix.chown(filepath, uid, gid)

  def nfs_op_pathconf(self, filepath, **kwargs):
    return {i:posix.pathconf(filepath, i) for i in pathconf_names.keys()}

  def nfs_op_link(self, filepath, dstfile=None, **kwargs):
    if not dstfile:
      dstfile = filepath+str(time())
    return posix.link(filepath, dstfile)

  def nfs_op_lstat(self,filepath, **kwargs):
    return posix.lstat(filepath)

  def nfs_op_mkdir(self, dirpath, bind=True, **kwargs):
    if self.nfs_op_exists(dirpath):
      if self.nfs_op_isdir(dirpath) and bind:
        return
      elif self.nfs_op_isfile(dirpath):
        ERROR("%s exists but is dir instead of file"%dirpath)
    return posix.mkdir(dirpath)

  def nfs_op_mkfifo(self, filepath, **kwargs):
    return posix.mkfifo(filepath)

  def nfs_op_mknod(self, filepath, **kwargs):
    return posix.mknod(filepath)

  def nfs_op_remove(self, filepath, **kwargs):
    return posix.remove(filepath)

  def nfs_op_rmdir(self, filepath, **kwargs):
    return posix.rmdir(filepath)

  def nfs_op_symlink(self, filepath,  dstpath=None, **kwargs):
    if not dstpath:
      dstpath = filepath+str(time())
    return posix.symlink(filepath, dstpath)

  def nfs_op_unlink(self, filepath, **kwargs):
    return posix.unlink(filepath)

  def nfs_op_statvfs(self, filepath, **kwargs):
    return posix.statvfs(filepath)

  def nfs_op_write(self, fh, data):
    return fh.write(data)

class LoadWebserver():
  def __init__(self, testobj, port=37000):
    # Define socket host and port
    self._testobj = testobj
    self._host = self.get_host_ip()
    self._port = self.get_open_port(port)
    self.server_socket = None
    self.client_connection = None
    self.inuse = True

  def get_open_port(self, port):
    if not port:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.bind(("",0))
      s.listen(1)
      port = s.getsockname()[1]
      s.close()
    return port

  def get_host_ip(self):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    host = s.getsockname()[0]
    s.close()
    return host

  def run(self):
    # Create socket
    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server_socket.settimeout(10)
    self.server_socket.bind((self._host, self._port))
    self.server_socket.listen(1)
    INFO('Starting webserver on http://%s:%s' %(self._host, self._port))
    self._url = 'http://%s:%s' %(self._host, self._port)
    retry = 5
    while self.inuse:
      try:
        # Wait for client connections
        self.client_connection, client_address = self.server_socket.accept()
      except socket.timeout:
        continue
      except Exception as err:
        ERROR("Failed to start webclient : %s"%err)
        sleep(30)
        if retry < 1:
          raise
        retry -= 1

      # Get the client request
      request = self.client_connection.recv(1024).decode()
      print(request)

      # Get the content of htdocs/index.html
      try:
        content = self._convert_ansi_to_html(request)
        response = 'HTTP/1.0 200 OK\n\n' + content
      except Exception as err:
        response = 'HTTP/1.0 404 NOT FOUND\n\n Err : %s'%err

      # Send HTTP response
      self.client_connection.sendall(response.encode())
      self.client_connection.close()

  def _convert_ansi_to_html(self, request, convert_ansi=False):
    # Parse HTTP headers
    headers = request.split('\n')
    filename = headers[0].split()[1]
    default_page = False
    content = None
    if filename == '/' or filename == "/home":
      default_page = True
      filename = Constants.STATIC_STATS_HTML_FILE
    elif filename == "/rstats":
      return dumps(self._testobj._total_ops_recorded, indent=2, sort_keys=True,
                   default=str)
    elif filename == "/config":
      filename = Constants.STATIC_TESTCONFIG_FILE
    elif filename == "/pstats":
      filename = Constants.STATIC_STATS_HTML_FILE
    elif filename == "/h":
      content = self._add_more_pages("")
    elif filename == "/memory_usage":
      return str(self._testobj._utils.get_mem_usage()).rstrip()
    elif filename == "/test_start_time":
      return str(self._testobj._start_time)
    elif filename == "/exit_test":
      self._testobj._set_exit_marker("Client invoked test exit via API",
                                     raise_exception=False)
      return '{"status":True}'
    elif filename == "/update_compaction_stats":
      print "*"*100, request
      self._testobj._total_ops_recorded["compaction_stats"] = None
      return '{"status":True}'
    if not content:
      with open(filename) as fin:
        content = fin.read()
    if convert_ansi:
      conv = Ansi2HTMLConverter()
      content = conv.convert(content)
    if filename == Constants.STATIC_STATS_HTML_FILE and default_page:
      content += "<br/>"
      content += "<p>%s</br>"%("*"*80)
      content += "<h3>Detailed Raw Stats : </h3>"
      content += "<p>%s</br>"%("*"*80)
      content += json2html.json2html.convert(
                                json=self._testobj._total_ops_recorded)
      content = self._add_more_pages(content)
    return content

  def _add_more_pages(self, content):
    content += "<br/>"
    content += "<p>%s</br>"%("*"*80)
    content += "<h3>Available Pages :</h3>"
    content += "<p>%s</br>"%("*"*80)

    rstats_url = "%s/rstats"%self._url
    config_url = "%s/config"%self._url
    pstats_url = "%s/pstats"%self._url
    memory_usage_url = "%s/memory_usage"%self._url
    sstime_url = "%s/test_start_time"%self._url
    exit_url = "%s/exit_test"%self._url
    compaction_url = "%s/update_compaction_stats"%self._url
    h_url = "%s/h"%self._url
    content += "<a href=%s>/h</a>       : Help</br>"%h_url
    content += "<a href=%s>/home</a>    : Default Landing Page</br>"%self._url
    content += "<a href=%s>/rstats</a>  : Get Raw stats in json format</br>"%(
                                                                    rstats_url)
    content += "<a href=%s>/config</a>  : Get processed test config in "\
               "json format</br>"%config_url
    content += "<a href=%s>/pstats</a>  : Get Stats in prettytable</br>"%(
                                                                    pstats_url)
    content += "<a href=%s>/test_start_time</a>  : Test start time</br>"%(
                                                                    sstime_url)
    content += "<a href=%s>/exit_test</a>  : Exit Test</br>"%exit_url
    content += "<a href=%s>/memory_usage/a>  : Get mem usage</br>"%(
                                                               memory_usage_url)
    content += "<a href=%s>/compaction_stats/a>  : Post Compaction Stats</br>"%(
                                                               compaction_url)
    return content

  def running(self):
    return self.inuse

  def close(self):
    # Close socket
    self.inuse = False
    if self.server_socket:
      INFO("Closing LoadWebserver. Sleeping for 30s")
      self.server_socket.close()
      print "Closing LoadWebserver"
      sleep(30)

y = '\033[96m' #Yellow
h = '\033[95m' #Header
be = '\033[94m' #Blue
g = '\033[92m' #Green
w = '\033[93m' #Warning
f = '\033[91m' #Fail
e = '\033[0m'  #End
b = '\033[1m' #Bold
u = '\033[4m'  #Underline

class Stats():

  def __init__(self, start_time, debug_stat, pid, iternum,
               pretty_print_stats=False):
    if not pretty_print_stats:
      global y, h, be, g, w, f, e, b, u
      y = h = be = g = w = f = e = b = u = ""
    self._utils = Utils()
    self._iter = iternum
    self._pid = pid
    self._start_time = start_time
    self.dstats = debug_stat
    self.totaltime = time()-start_time
    self._test_start_time = datetime.utcfromtimestamp(start_time)
    self._init_vars()

  def _init_vars(self):
    self.total_new_objects = self.total_size = 0
    self.total_read_size = self.total_reads = 0
    self._track_ui_stats = False
    self._track_oc_stats = False
    self._track_upgrade_stats = False
    self._track_compaction_stats = False
    self._track_test_errors_stats = False
    self._track_endpoint_stats = False
    self._track_misc_stats = False
    self._track_auto_delete_stats = False
    self._ops_to_skip_tracking = ["object-controller", "list_bucket_map",
                                  "UI-Stats", "multipart",
                                  "stargate_storage_gflag_change", "ei",
                                  "ms_compaction", "errors", "nfsstats",
                                  "endpoint_stats", "failed_workers",
                                  "TestStatus", "active_objects", "backup",
                                  "nfsops", "prefix_distribution", "misc",
                                  "anomalis", "opstats", "vars",
                                  "workload_to_prefix_map"]
    self._seq_to_execute = ["put_object", "copy_object", "get_object",
                            "range_read", "nfs_read", "nfs_range_read",
                            "list_nfs_dir", "list_objects", "delete_objects",
                            "skipped_old_reads", "upgrade", "workload_profiles",
                            "ms_compaction_stats", "test_errors", "endpoints",
                            "tiered_get", "tiered_range_get", "log_ui_stats",
                            "nfslist", "nfs_write", "nfs_copy", "nfs_rename",
                            "nfs_sparse_write", "nfs_sparse_range_read",
                            "nfs_sparse_read", "fio_workload",
                            "vdbench_workload", "auto_deletes", "lookup",
                            "miscstats", "nfs_over_write", "nfs_delete",
                            "common", "extended_stats", "print_result"]
    self._diff_map = {}

  def log_stats(self, total_ops_recorded, return_raw_format=False,
                log_debug_stats=False, workload_start_time=0):
    self._workload_start_time = workload_start_time
    self.debug_ei_stats = ""
    self.totaltime = time()-self._start_time
    self._init_tables()
    self._total_ops_recorded = total_ops_recorded
    self._log_debug_stats = log_debug_stats
    for method in self._seq_to_execute:
      method_to_execute = getattr(self, method)
      method_to_execute()
    if return_raw_format:
      return self.print_result(True)

  def _init_tables(self):
    self.stats = PrettyTable([y+"Workload", "Num.Of.Ops", "Total.Size",
                         "ThroughPut", "OPs/s"+e])
    self.debug_stats = PrettyTable([y+'OP.Wise.Distribution', 'Successful.Ops',
                                    'Failed.Ops(U/I/E)', "Last.Failure.Time",
                                    "Time Since Stable"+e])
    self.ui_stats = PrettyTable([y+"UIStatsType", "Objs - Expected vs Found",
                                 "Size - Expected vs Found",
                                 "Last Recorded Time"+e])
    self.upgrade_stats = PrettyTable([y+"UpgradeEntity", "SrcBuild", "DstBuild",
                                      "UpgradeTime", "Last Recorded Time"+e])
    self.oc_stats = PrettyTable([y+"OP.Name", "Object-Controller", "Total"+e])
    self.compaction_stats = PrettyTable([y+"MS.Name", "Stats"+e])
    self.test_errors_stats = PrettyTable([y+"Error Message/Code", "Last.OC",
                  "Last.OP", "OP.Time(sec)","Count", "Last.Failure.Time (PST)",
                                          "Time Since Stable"+e])
    self.endpoint_stats = PrettyTable([y+"Endpoint", "Total", "TotalFailed",
                              "FailuredIgnored", "FailuresExpected"+e])
    self.workload_stats = PrettyTable([y+"IP", "Count", "Workload",
          "Object Size", "Start Time", "Status", "RunTime", "Memory Usage",
          "WL Iteration"+e])
    self.misc_stats = PrettyTable([y+"StatName", "StatValue"+e])
    self.auto_deletes_stats = PrettyTable([y+"AutoDeleteStartTime", "EndTime",
                        "Storage (Usage/Freed/Time)", "AtlasScanTime",
                  "CuratorScanTime", "AOSStorageUsage (Before/After/Freed)"+e])

  def log_ui_stats(self):
    if "UI-Stats" in self._total_ops_recorded:
      self._track_ui_stats = True
      self.ui_stats.add_row([g+"BucketStats"+e,
        self._total_ops_recorded["UI-Stats"]["BucketStats"]["NumObjs"]["res"],
        self._total_ops_recorded["UI-Stats"]["BucketStats"]["Size"]["res"],
        self._total_ops_recorded["UI-Stats"]["time"]])
      self.ui_stats.add_row([g+"InstanceStats"+e,
        self._total_ops_recorded["UI-Stats"]["InstanceStats"]["NumObjs"]["res"],
        self._total_ops_recorded["UI-Stats"]["InstanceStats"]["Size"]["res"],
        self._total_ops_recorded["UI-Stats"]["time"]])

  def ms_compaction_stats(self):
    if "ms_compaction" not in self._total_ops_recorded:
      return
    self._track_compaction_stats = True
    for ms, stats in self._total_ops_recorded["ms_compaction"].iteritems():
      cf_stats = ""
      for cf, cfstats in stats.iteritems():
        if cfstats["totalcount"] > 0:
          cf_stats += "%s: %s :%s\n"%(cf, cfstats["totalcount"],
                                      cfstats["last"])
      self.compaction_stats.add_row([g+ms+e, cf_stats])

  def upgrade(self):
    if "upgrade" not in self._total_ops_recorded:
      return
    for infratype, results in  self._total_ops_recorded["upgrade"].iteritems():
      self._track_upgrade_stats = True
      itype = infratype.upper()
      for res in results:
        self.upgrade_stats.add_row([g+itype+e,  res["builds"]["before"],
                      res["builds"]["after"], res["upgrade_time"], res["time"]])

  def workload_profiles(self):
    if "workload_profiles" not in self._total_ops_recorded:
      return
    count = 1
    for item in self._total_ops_recorded["workload_profiles"]:
      runtime = item["time"]
      if runtime == "InProgress":
        runtime = datetime.now() - self._utils.convert_to_datetime(
                                                            item["start_time"])
      self.workload_stats.add_row(
                         [be+item["ip"]+e, str(count), item["workload"],
                         [self._utils.convert_size(i) for i in item["objsize"]],
                          item["start_time"], item.get("status"), runtime,
                          item["mem_usage"], item["iteration"]])

  def skipped_old_reads(self):
    skipped_reads = self._total_ops_recorded["skipped_old_reads"]
    if skipped_reads > 0:
      self.stats.add_row([g+"SkippedOldReads"+e, skipped_reads, "N/A", "N/A",
                          "N/A"])

  def test_errors(self):
    if not self._total_ops_recorded["errors"]["err_msgs"]:
      if not self._total_ops_recorded["errors"]["err_codes"]:
        return
    self._track_test_errors_stats = True
    for err_code,  errdetails in self._total_ops_recorded["errors"][
                                                      "err_codes"].iteritems():
      stable_since, last_failure = g+"Test Start Time"+e, g+"N/A"+e
      if errdetails["Last.Recorded.Time"]:
        stable_since = f+"%s"%(datetime.now() - self._utils.convert_to_datetime(
                                            errdetails["Last.Recorded.Time"]))+e
        last_failure = f+"%s"%(errdetails["pst_stime"])+e
      self.test_errors_stats.add_row([g+"%s"%(err_code)+e, errdetails["oc"],
                           errdetails["opname"], errdetails.get("etime", "-"),
                           self._utils.convert_num(errdetails["count"]),
                           last_failure, stable_since])
    for err_msg,  errdetails in self._total_ops_recorded["errors"
                                                      ]["err_msgs"].iteritems():
      if "object-controller" in err_msg: continue
      stable_since, last_failure = g+"Test Start Time"+e, g+"N/A"+e
      if errdetails["Last.Recorded.Time"]:
        stable_since = f+"%s"%(datetime.now() - self._utils.convert_to_datetime(
                                          errdetails["Last.Recorded.Time"]))+e
        last_failure = f+"%s"%(errdetails["pst_stime"])+e
      self.test_errors_stats.add_row([g+"%s"%(err_msg)+e, errdetails["oc"],
                   errdetails["opname"], errdetails.get("etime", "-"),
                   self._utils.convert_num(errdetails["count"]), last_failure,
                   stable_since])

  def endpoints(self):
    self._track_endpoint_stats = True
    for endpt, stats in self._total_ops_recorded["endpoint_stats"].iteritems():
      self.endpoint_stats.add_row([g+str(endpt)+e,
                                   self._utils.convert_num(stats["total"]),
                                   self._utils.convert_num(stats["failures"]),
                                   self._utils.convert_num(stats["ignored"]),
                                   self._utils.convert_num(stats["expected"])])

  def tiered_get(self):
    if "tiered_get" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["tiered_get"]
      res = self._get_op_stats(g+"S3TieredGET"+e, total_objs)
      self.stats.add_row(res)

  def tiered_range_get(self):
    if "tiered_range_get" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["tiered_range_get"]
      res = self._get_op_stats(g+"S3TieredRangeGET"+e, total_objs)
      self.stats.add_row(res)

  def put_object(self):
    if "put_object" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["put_object"]
      self.total_new_objects += total_objs["count"
                          ]+self._total_ops_recorded["multipart"]["totalcount"]
      self.total_new_objects -= self._total_ops_recorded[
                                            "multipart"]["totalfinalizedparts"]
      self.total_size += total_objs["size"]
      self.stats.add_row(self._get_op_stats(g+"S3PUT"+e, total_objs))
    if self._total_ops_recorded["multipart"]["totalparts"] > 0:
      parts = self._utils.convert_num(
                          self._total_ops_recorded["multipart"]["totalparts"])
      objs = self._utils.convert_num(
                          self._total_ops_recorded["multipart"]["totalcount"])
      inprogress = self._utils.convert_num(
                          self._total_ops_recorded["multipart"]["totalongoing"])
      total_ops = "MO:%s, P:%s, IP:%s, Ab:%s"%(self._utils.convert_num(objs),
                   self._utils.convert_num(parts),
                   self._utils.convert_num(inprogress),
                   self._utils.convert_num(self._total_ops_recorded[
                                              "multipart"]["aborted"]["count"]))
      aborted_size = self._utils.convert_size(
                      self._total_ops_recorded["multipart"]["aborted"]["size"])
      self.stats.add_row([g+"S3MultipartPUT"+e, total_ops,
                          "(Ab : %s)"%(aborted_size), "-", "-"])

  def auto_deletes(self):
    if "auto_deletes" not in self._total_ops_recorded["opstats"]:
      return
    self._track_auto_delete_stats = True
    for stat in self._total_ops_recorded["opstats"]["auto_deletes"]["stats"]:
      freed, time, usage = "InPrgress", "InProgress", "InProgress"
      aos_space_before, aos_space_after = "NotFound", "NotFound"
      if "before" in stat:
        usage = stat["before"]["local_storage_usage"]
        aos_space_before = stat["before"].get("aos_storage_usage", "NotFound")
      if "after" in stat:
        freed = self._utils.convert_size(
                                    usage-stat["after"]["local_storage_usage"])
        time = self._utils.convert_to_datetime(stat["after"]["time"]
                    ) - self._utils.convert_to_datetime(stat["before"]["time"])
        aos_space_after = stat["after"].get("aos_storage_usage", "NotFound")
      aos_space_freed = "NotFound"
      if isinstance(aos_space_before, int) and isinstance(aos_space_after, int):
        aos_space_freed = self._utils.convert_size(
                                          aos_space_before - aos_space_after)
        aos_space_before = self._utils.convert_size(aos_space_before)
        aos_space_after = self._utils.convert_size(aos_space_after)
      if isinstance(usage, int):
        usage = self._utils.convert_size(usage)
      atlas = stat.get("scan", {}).get("atlas", {}).get("time", "NotFound")
      curator = stat.get("scan", {}).get("curator", {}).get("time", "NotFound")
      endtime = stat.get("end_time", "InProgress")
      self.auto_deletes_stats.add_row([str(stat["start_time"]),
        str(endtime), "%s/%s/%s"%(usage, freed, time), atlas, curator,
        "%s / %s (%s)"%(aos_space_before, aos_space_after, aos_space_freed)])

  def nfslist(self):
    dirs = self._total_ops_recorded["nfsstats"]["filesdirs"]["totaldirs"]
    files = self._total_ops_recorded["nfsstats"]["filesdirs"]["totalfiles"]
    if dirs > 0 or files > 0:
      dirsfiles = "Dirs:%s/Files:%s"%(self._utils.convert_num(dirs),
                                      self._utils.convert_num(files))
      self.stats.add_row([g+"NFSList"+e, dirsfiles, "N/A", "N/A", "N/A"])

  def copy_object(self):
    if "s3_copy_object" not in self._total_ops_recorded:
      return
    total_objs = self._total_ops_recorded["s3_copy_object"]
    self.total_new_objects += total_objs["count"]
    self.total_size += total_objs["size"]
    self.stats.add_row(self._get_op_stats(g+"S3COPY"+e, total_objs))

  def get_object(self):
    if "s3_get_object" not in self._total_ops_recorded:
      return
    total_objs = self._total_ops_recorded["s3_get_object"]
    self.total_reads += total_objs["count"]
    self.total_read_size += total_objs["size"]
    ser = self._total_ops_recorded["anomalis"][
                                          "skipped_expired_reads"]["count"]
    res = self._get_op_stats(g+"S3GET"+e, total_objs,
                           "" if ser < 1 else " (SkipExpired : %s)"%ser)
    self.stats.add_row(res)

  def range_read(self):
    if "s3_get_object_range" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["s3_get_object_range"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      ser = self._total_ops_recorded["anomalis"][
                                "skipped_expired_range_reads"]["count"]
      res = self._get_op_stats(g+"S3RangeGET"+e, total_objs,
                               "" if ser < 1 else "\n(SER:%s)"%ser)
      self.stats.add_row(res)

  def nfs_sparse_read(self):
    if "_nfs_sparse_file_read" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["_nfs_sparse_file_read"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsSparseRead"+e, total_objs)
      self.stats.add_row(res)

  def vdbench_workload(self):
    if "start_vdbench" not in self._total_ops_recorded:
      return
    total_objs = self._total_ops_recorded["start_vdbench"]
    ip = self._total_ops_recorded["counts"]["vdbench"][
                                                "count"] - total_objs["count"]
    self.total_reads += total_objs["count"]
    self.total_read_size += total_objs["size"]
    res = self._get_op_stats(g+"VDBenchRW"+e, total_objs)
    res[1] = "%s (IP : %s)"%(res[1], ip)
    self.stats.add_row(res)

  def fio_workload(self):
    if "start_fio" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["start_fio"]
      ip = self._total_ops_recorded["counts"]["fio"][
                                                  "count"] - total_objs["count"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"FioRW"+e, total_objs)
      res[1] = "%s (IP : %s)"%(res[1], ip)
      self.stats.add_row(res)

  def nfs_read(self):
    if "_nfsread" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["_nfsread"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsRead"+e, total_objs)
      self.stats.add_row(res)

  def nfs_delete(self):
    if "_delete_nfs_file" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["_delete_nfs_file"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsDelete"+e, total_objs)
      self.stats.add_row(res)

  def miscstats(self):
    open_fds = self._total_ops_recorded["nfsops"]["nfsstats"]["filesdirs"
                                              ]["num_open_files"]
    open_failed_fds = self._total_ops_recorded["nfsops"]["nfsstats"][
                                              "filesdirs"]["num_open_fd_failed"]
    close_failed_fds = self._total_ops_recorded["nfsops"]["nfsstats"][
                                            "filesdirs"]["num_close_fd_failed"]
    active_objs = len(self._total_ops_recorded["active_objects"])
    self._track_misc_stats = True
    if open_fds > 0: self.misc_stats.add_row(["CurrentOpenFilesCount",
                             self._utils.convert_num(open_fds)])
    if open_failed_fds > 0: self.misc_stats.add_row(["FailedOpenFileCount",
                               self._utils.convert_num(open_failed_fds)])
    if close_failed_fds > 0: self.misc_stats.add_row(["FailedCloseFileCount",
                               self._utils.convert_num(close_failed_fds)])
    if active_objs > 0: self.misc_stats.add_row(["ActiveObjFiles",
                                          self._utils.convert_num(active_objs)])
    for k, v in self._total_ops_recorded["prefix_distribution"].iteritems():
      if v["count"] > 0: self.misc_stats.add_row(["%s_Prefix_Count"%k,
                               self._utils.convert_num(v["count"])])
    for workload, delays in self._total_ops_recorded["misc"].iteritems():
      if delays["delay"] > 0:
        self.misc_stats.add_row(["%sDelay"%workload.title(), delays["delay"]])
    for k, v in self._total_ops_recorded["anomalis"].iteritems():
      if v["count"] > 0: self.misc_stats.add_row(["Anomaly_%s"%k,
                                 self._utils.convert_num(v["count"])])

  def nfs_copy(self):
    if "copy_file" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["copy_file"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsCopy"+e, total_objs)
      self.stats.add_row(res)

  def nfs_over_write(self):
    if "_nfs_over_write" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["_nfs_over_write"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsOverWrite"+e, total_objs)
      self.stats.add_row(res)

  def nfs_rename(self):
    if "rename_file" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["rename_file"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsRename"+e, total_objs)
      self.stats.add_row(res)

  def nfs_sparse_write(self):
    if "_nfs_sparse_write" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["_nfs_sparse_write"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsSparseWrite"+e, total_objs)
      self.stats.add_row(res)

  def nfs_write(self):
    if "_nfs_write" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["_nfs_write"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsWrite"+e, total_objs)
      self.stats.add_row(res)

  def nfs_sparse_range_read(self):
    if "_nfs_sparse_range_read" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["_nfs_sparse_range_read"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsSparseRangeRead"+e, total_objs)
      self.stats.add_row(res)

  def nfs_range_read(self):
    if "_nfs_range_read" in self._total_ops_recorded:
      total_objs = self._total_ops_recorded["_nfs_range_read"]
      self.total_reads += total_objs["count"]
      self.total_read_size += total_objs["size"]
      res = self._get_op_stats(g+"NfsRangeRead"+e, total_objs)
      self.stats.add_row(res)

  def list_nfs_dir(self):
    if "_nfs_walk_dir" in self._total_ops_recorded:
      tdirs = self._total_ops_recorded["nfsops"][
                                          "nfsstats"]["filesdirs"]["totaldirs"]
      tfiles = self._total_ops_recorded["nfsops"][
                                          "nfsstats"]["filesdirs"]["totalfiles"]
      tlfailed = self._total_ops_recorded["nfsops"][
                                        "nfsstats"]["filesdirs"]["lookupfailed"]
      total_objs = self._total_ops_recorded["_nfs_walk_dir"]
      res = self._get_op_stats(g+"NfsList"+e, total_objs,
                           "(D:%s, F:%s,LF:%s)"%(self._utils.convert_num(tdirs),
                           self._utils.convert_num(tfiles),
                           self._utils.convert_num(tlfailed)))
      self.stats.add_row(res)

  def list_objects(self):
    if "s3_list_objects" not in self._total_ops_recorded:
      return
    total_objs = self._total_ops_recorded["s3_list_objects"]
    totallst = self._utils.convert_num(total_objs.get("count",0))
    list_objs = self._utils.convert_num(total_objs.get("num_list_objects",0))
    num_pref = self._utils.convert_num(total_objs.get("num_prefixes", 0))
    self.stats.add_row([g+"S3LIST"+e,
                        "%s (Objs:%s, Pref:%s)"%(totallst, list_objs, num_pref),
                        "N/A", "N/A", "N/A"])

  def lookup(self):
    if "s3_lookup" not in self._total_ops_recorded:
      return
    stats = self._total_ops_recorded["s3_lookup"]
    i_failures = stats["ignored_failure_count"]
    e_failures = stats["expected_failure_count"]
    u_failures = stats["failcount"]-(i_failures+e_failures)
    if not (stats["count"] > 0 and stats.get("negative_lookups", 0) > 0): return
    count = self._utils.convert_num(stats["count"])
    negative = self._utils.convert_num(stats.get("negative_lookups", 0))
    total_num = stats["count"] + i_failures + e_failures
    self.stats.add_row([g+"S3LookUps"+e,"%s (N/I/E/U : %s/%s/%s/%s)"%(count, negative,
      self._utils.convert_num(i_failures), self._utils.convert_num(e_failures),
      self._utils.convert_num(u_failures)), "N/A", "N/A",
      self._utils.convert_num(total_num/int(time()-self._start_time))])

  def delete_objects(self):
    if "s3_delete_objects" not in self._total_ops_recorded:
      return
    total_objs = self._total_ops_recorded["s3_delete_objects"]
    tombstones = self._utils.convert_num(
            self._total_ops_recorded["s3_delete_objects"].get("tombstones", 0))
    dsize = total_objs["size"]
    count = total_objs["count"]
    parts_size = self._total_ops_recorded.get("multipart", {}).get("aborted", {}
                                        ).get("size", 0)
    parts = self._total_ops_recorded.get("multipart", {}).get("aborted", {}
                                        ).get("parts", 0)
    total_deletes = self._utils.convert_num(count+parts)
    if parts > 0:
      total_deletes = "%s (P:%s)"%(total_deletes,self._utils.convert_num(parts))
    size = self._utils.convert_size(dsize+parts_size)
    if parts_size > 0:
      size = "%s (P:%s)"%(size, self._utils.convert_size(parts_size))
    self.stats.add_row([g+"S3DELETE"+e,
                        "%s (Tombstones %s)"%(total_deletes, tombstones), size,
                        "DMarker : %s"%(total_objs.get("deleted_dmarker", 0)),
                        "Skipped : %s"%(total_objs.get("deleted_skipped",0))])

  def common(self):
    t_failures = self._total_ops_recorded["totalfailure"]
    i_failures = self._total_ops_recorded["totalfailureignored"]
    e_failures = self._total_ops_recorded["expected_failure_count"]
    if t_failures > 0 or i_failures > 0 or e_failures > 0:
      self.stats.add_row([f+"."*16,"."*16,"."*15,"."*15,"."*15+e])
    if t_failures > 0:
      total_failures_recorded = "%s(U:%s)"%(self._utils.convert_num(t_failures),
                 self._utils.convert_num(t_failures-(i_failures + e_failures)))
      self.stats.add_row([f+"Total.Failed.OPs"+e,total_failures_recorded, "N/A",
        f+"Last.Failure.Time"+e, self._total_ops_recorded.get("Last.Recorded.Time")])
    if  i_failures> 0:
      self.stats.add_row([w+"Ignored.Failures"+e, self._utils.convert_num(i_failures),"N/A", "N/A",
                          "N/A"])
    if e_failures > 0:
      self.stats.add_row([b+"Expected.Failures"+e, self._utils.convert_num(e_failures),"N/A", "N/A",
                          "N/A"])
    #self._add_summary()

  def _add_summary(self):
    self.stats.add_row(["=======", "=======","=======","=======","======="])
    ts_summary = "W/R: %s/%s"%(self._utils.convert_size(self.total_size),
                               self._utils.convert_size(self.total_read_size))
    tp_summary = "W/R: %s/%s"%(
                  self._utils.convert_size(self.total_size/self.totaltime),
                  self._utils.convert_size(self.total_read_size/self.totaltime))
    to_summary = "T/W/R : %s/%s/%s"%(
                self._utils.convert_num(self._total_ops_recorded["totalcount"]),
                self._utils.convert_num(self.total_new_objects),
                self._utils.convert_num(self.total_reads))
    self.stats.add_row([b+"Summary", to_summary, ts_summary, tp_summary,
                 str(self._total_ops_recorded["totalcount"]/self.totaltime)+e])


  def extended_stats(self):
    if not self.dstats:
      return
    self._update_failure_extended_stats()
    self._update_stargate_storage_gflag_stats()
    self._extended_ei_stats()
    self._update_oc_extended_stats()

  def _update_failure_extended_stats(self):
    for k, v in self._total_ops_recorded.iteritems():
      if k in self._ops_to_skip_tracking:
        continue
      if isinstance(v, dict):
        last_failure_time, failed_ops = "N/A", 0
        stable_since = g+"%s"%(datetime.now()-self._test_start_time)+e
        if v.get("failcount", 0) > 0 and v["Last.Recorded.Time"]:
          last_failure_time = f+"%s"%(v["Last.Recorded.Time"])+e
          stable_since = f+"%s"%(datetime.now()-self._utils.convert_to_datetime(
                                                    v["Last.Recorded.Time"]))+e
          i_failures = v["ignored_failure_count"]
          e_failures = v["expected_failure_count"]
          u_failures = v["failcount"]-(i_failures+e_failures)
          failed_ops = "%s(%s,%s,%s)"%(f+str(v["failcount"])+e,
                       f+str(u_failures)+e, y+str(i_failures)+e,
                       y+str(e_failures)+e)
        self.debug_stats.add_row([be+k+e,
                                  self._utils.convert_num(v.get("count",0)),
                                  failed_ops, last_failure_time, stable_since])

  def _update_stargate_storage_gflag_stats(self):
    if "stargate_storage_gflag_change" in  self._total_ops_recorded:
      data = self._total_ops_recorded["stargate_storage_gflag_change"]
      count = data["count"]
      stable_since = "%s"%(data["Last.Recorded.Time"])
      self.debug_stats.add_row([be+"stargate_gflag_flip"+e,
                                self._utils.convert_num(count),
                                0, "N/A", g+stable_since+e])

  def _update_oc_extended_stats(self):
    if not self._total_ops_recorded["object-controller"]["stats"]:
      return
    overall_oc_stats={}
    overall_oc_count = 0
    for opname, stats in self._total_ops_recorded["object-controller"][
                                                         "stats"].iteritems():
      op_level_oc_stats = {}
      op_level_oc_count = 0
      for oc_instance, stat in stats.iteritems():
        op_level_oc_stats[oc_instance] = op_level_oc_stats.get(
                          oc_instance, 0) + stat["success"]
        overall_oc_stats[oc_instance] = overall_oc_stats.get(
                          oc_instance, 0) + stat["success"]
        op_level_oc_count += stat["success"]
      overall_oc_count += op_level_oc_count
      self.oc_stats.add_row([be+opname+e,
       {k:self._utils.convert_num(v) for k, v in op_level_oc_stats.iteritems()},
       self._utils.convert_num(op_level_oc_count)])
      self._track_oc_stats = True
    if self._track_oc_stats:
      self.oc_stats.add_row(["="*32, "="*60, "="*15])
      self.oc_stats.add_row(["Summary",
        {k:self._utils.convert_num(v) for k, v in overall_oc_stats.iteritems()},
        self._utils.convert_num(overall_oc_count)])

  def _extended_ei_stats(self):
    if "ei" not in self._total_ops_recorded:
      return
    self.debug_ei_stats = PrettyTable([y+'EI.Component', 'EI.Action',
                                         'Num.Error.Injected',
                                         "Last.Failure.Time",
                                         "Last.Recovery.Time"+e])
    lrecorded_time = totalei = 0
    last_ftime = self._total_ops_recorded["ei"].get("Last.Recorded.Time")
    if last_ftime:
      lrecorded_time = datetime.now()-last_ftime
    for comp, eistats in self._total_ops_recorded["ei"].iteritems():
      if  isinstance(eistats, dict):
        for eiop, eicount in eistats.iteritems():
          totalei += eicount.get("count",0)
          self.debug_ei_stats.add_row([b+comp+e, eiop, eicount.get("count",0),
                                    "%s"%(eicount.get("Last.Recorded.Time")),
                                    "%s"%(eicount.get("Last.Recovery.Time"))])
    self.debug_ei_stats.add_row([b+"=====","=====","=====","=====","====="+e])
    self.debug_ei_stats.add_row([b+"TotalEI"+e, " - ", totalei, last_ftime,
                                 lrecorded_time])

  def print_result(self, return_raw_results=False):
    wl_start_time = str(timedelta(seconds=time()-self._workload_start_time)
                        ) if self._workload_start_time != 0 else 0
    auto_deletes = "" if not self._total_ops_recorded["vars"][
                      "auto_deletes_enabled"] else ", AutoDeletesEnabled : True"
    stop_writes = "" if not self._total_ops_recorded["vars"]["stop_writes"
                    ] else ", StopWrites : True"
    msg = "PID  :%s, Memory usage : %s, Test Runtime : %s (WL : %s), Iteration"\
          " : %s%s%s\n%s"%(self._pid,
            self._utils.convert_size(Process(self._pid).memory_info().rss),
            str(timedelta(seconds=time()-self._start_time)), wl_start_time,
            self._iter, auto_deletes, stop_writes, self.stats)
    html_text = ""
    if self._track_ui_stats:
      msg += "\n%s"%(self.ui_stats)
      html_text += self.ui_stats.get_html_string(format=True)
      html_text += "<br/>"
    if self._track_upgrade_stats:
      msg += "\n%s"%(self.upgrade_stats)
      html_text += self.upgrade_stats.get_html_string(format=True)
      html_text += "<br/>"
    if not self._log_debug_stats:
      INFO(msg)
    if self.dstats:
      msg += "\n%s"%(self.debug_stats)
      html_text += self.debug_stats.get_html_string(format=True)
      html_text += "<br/>"
      if self.debug_ei_stats:
        msg += "\n%s"%(self.debug_ei_stats)
        html_text += self.debug_ei_stats.get_html_string(format=True)
        html_text += "<br/>"
      if self._track_oc_stats:
        msg += "\n%s"%(self.oc_stats)
        html_text += self.oc_stats.get_html_string(format=True)
        html_text += "<br/>"
      if self._track_compaction_stats:
        msg += "\n%s"%(self.compaction_stats)
        html_text += self.compaction_stats.get_html_string(format=True)
        html_text += "<br/>"
      if self._track_test_errors_stats:
        msg += "\n%s"%(self.test_errors_stats)
        html_text += self.test_errors_stats.get_html_string(format=True)
        html_text += "<br/>"
      if self._track_endpoint_stats:
        msg += "\n%s"%(self.endpoint_stats)
        html_text += self.endpoint_stats.get_html_string(format=True)
        html_text += "<br/>"
      if self._track_misc_stats:
        msg += "\n%s"%(self.misc_stats)
        html_text += self.misc_stats.get_html_string(format=True)
        html_text += "<br/>"
      if self._track_auto_delete_stats:
        msg += "\n%s"%(self.auto_deletes_stats)
        html_text += self.auto_deletes_stats.get_html_string(format=True)
        html_text += "<br/>"
      msg += "\n%s"%(self.workload_stats)
      html_text += self.workload_stats.get_html_string(format=True)
      html_text += "<br/>"
    if return_raw_results:
      return msg
    Utils.write_to_file("%s :\n%s\n"%(datetime.now(), self.test_errors_stats),
                        Constants.STATIC_ERROR_FILE, "w")
    Utils.write_to_file("%s : %s\n"%(datetime.now(), msg),
                        Constants.STATIC_STATS_FILE, "w")
    Utils.write_to_file(html_text, Constants.STATIC_STATS_HTML_FILE, "w")
    if self._log_debug_stats:
      INFO(msg)

  def _get_op_stats(self, opname, stats, op_details=""):
    count = self._utils.convert_num(stats["count"])
    size = self._utils.convert_size(stats["size"])
    active_objects = self._total_ops_recorded["active_objects"]
    active_size = [i["size"] for i in active_objects.values() if "write" in
                   i["caller"] or "copy" in i["caller"]]
    if opname == g+"PUT"+e: #Just a hack to capture inprogress PUTs
      in_puts = len(active_objects)
      if in_puts > 0:
        count = "C:%s, IP:%s"%(count, in_puts)
        size = "C:%s, IP:%s"%(size, self._utils.convert_size(sum(active_size)))
    return [opname, "%s%s"%(count, op_details), size,
            self._utils.convert_size(stats["size"]/self.totaltime),
            "%.3f"%(stats["count"]/self.totaltime)]

class Conditions():
  """
  this class is exepcted to check some conditions on ms before reporting
  failures and existing tests
  """

  def __init__(self, testobj):
    """
    initialize the object

    Args:
      testobj(obj): UploadObj class instance, required for db
    """
    self._testobj = testobj

  def wait_for_bucket_list_compaction(self, wait_for_completion=True,
            timeout=3600*4, interval=60):
    """"
    it will
    """
    stime = datetime.now()
    cstats = None
    while (datetime.now()-stime).total_seconds() > timeout:
      cstats = self._testobj._total_ops_recorded["compaction_stats"]
      if Constants.POSEIDON_BUCKETLIST_MAP not in cstats:
        return False
      DEBUG("Bucket list compaction is running, cstats : %s, timeout : %s"
            %(cstats[Constants.POSEIDON_BUCKETLIST_MAP][-1], timeout))
      if not wait_for_completion:
        return True
      sleep(interval)
    raise Exception("Hit timeout after %s, while waiting for bucket list "
           "compaction to finish. All compactions stats : %s"%(timeout, cstats))

class OpExecutor():
  """
  this class handles the execution of every operation and records its
  execution details
  """
  def __init__(self, uploadobj,  error_fh_tracker, **kwargs):
    """
    it will initialize the given class and also initialize the lock object,
    which every thread need to acquire before updating db

    Args:
      uploadobj(obj): object needed to update db
      error_fh_tracker(str): file path to append all the errors
      kwargs(dict): anything needed for the class
    """
    self._lock = RLock()
    self._objects = uploadobj
    self._prepare_retry_conditions_for_ops()
    self._error_fh_tracker = error_fh_tracker
    self._local_records = {}
    self._last_update_time = time()
    self._errmsp = {
              "Connection reset by peer":"Connection reset by peer",
              "Connection was closed" : "Connection was closed",
              "Broken pipe" : "Broken pipe",
              "SSL validation failed":"SSL validation failed",
              "IncompleteReadError":"IncompleteReadError",
              "Read timeout":"Read timeout",
              "violation of protocol":"SSL validation failed",
              "total bytes expected is":"IncompleteReadError",
              "EOF occurred in violation of protocol":"SSL validation failed",
              "Could not connect":"Could not connect",
              "Connect timeout on endpoint":"Connect timeout on endpoint",
              "Incomplete NFS Read":"IncompleteNFSReadError",
              "ServiceUnavailable":"ServiceUnavailable",
              "Service Unavailable":"ServiceUnavailable", "SlowDown":"SlowDown",
              "RequestTimeTooSkewed":"RequestTimeTooSkewed",
              "SignatureDoesNotMatch":"SignatureDoesNotMatch"
      }
    INFO("Op Retries : %s"%(self._op_retries))

  def _prepare_retry_conditions_for_ops(self):
    """
    prepares the self._op_retries dict based on "op_retries" passed as argument
    in script
    """
    self._per_op_retries = self._objects._kwargs.pop("op_retries")
    self._op_retries = {}
    if not self._per_op_retries:
      return
    if isinstance(self._per_op_retries, str):
      for ops in self._per_op_retries.split(","):
        op, retry = ops.split(":")
        self._op_retries[op.strip()] = {"retries" : int(retry.strip())}
      return
    for opname, opdetails in self._per_op_retries.iteritems():
      self._op_retries[opname] = opdetails
      #if opdetails.get("condition"):
      #  method = getattr(self._objects._conditions, opdetails["condition"])
      #  self._op_retries[opname]["condition"] = method

  def _execute_op_on_failure(self, conditional_method, **kwargs):
    res = False
    try:
      res = self._execute(conditional_method, set_exit_marker_on_failure=False,
                    **kwargs)
    except Exception as err:
      return False
    return res

  def _get_op_retry_details(self, opname, current_retry_count,
                            current_retry_delay, condition_for_retry):
    """
    returns the retry_count, retry_delay and op_condtion according to opname,

    Args:
      opname(str): operation name
      current_retry_count(int): retry count
      current_retry_delay(int): delay in seconds
      condition_for_retry(str): method name of Condition class to be checked

    Returns: (int,int)
    """
    retry_delay = self._objects._retry_delay
    retry_count = self._objects._retry_count
    if current_retry_delay > -1:
      retry_delay = current_retry_delay
    if current_retry_count > -1:
      retry_count = current_retry_count
    #if opname in self._op_retries:
    retry_count = self._op_retries.get(opname, {}).get("retries", retry_count)
    op_condition = self._op_retries.get(opname, {}).get("condition",
                            condition_for_retry)
    if op_condition and self._objects._conditions:
      op_condition = getattr(self._objects._conditions, op_condition)
    return retry_count, retry_delay, op_condition

  def _execute(self, method, **kwargs):
    """
    it will execute the method passed as first param, it will also takes care of
    retries in case of exceptions, it will  also records the response of api in
    self._local_records

    Args:
      method(obj): callable method object
      kwargs(dict): other params needed

    Raises: Exception
      if enough number of retries are done for method
    """
    retry_count, retry_delay, condition_for_retry= self._get_op_retry_details(
      method.__name__, kwargs.pop("retry_count", -1),
      kwargs.pop("retry_delay", -1),  kwargs.pop("condition_for_retry", None))
    ignore_errors = kwargs.pop("ignore_errors", self._objects._ignore_errors)
    force_ignore_failures = kwargs.pop("force_ignore_failures", False)
    set_exit_marker_on_failure = kwargs.pop("set_exit_marker_on_failure", [])

    #Size of obj to track data pushed in each obj. Delete-objects will have
    #addition of all obj sizes being deleted in one call vs PUT/COPY will have
    #just one obj so just one size
    rsize = kwargs.pop("rsize", 0)

    #Count of items in request. e.g- delete_objects can have 1K objects.
    #So this value can be 1000 while PUT/COPY/HEAD will work on just one obj
    rcount = kwargs.pop("rcount", 1)
    force = kwargs.pop("force", False)
    failure_recorded_time, stime,  err,  i = None, None, None, 0
    api_etime = time()
    while i < retry_count+1:
      if self._objects._time_to_exit and not force:
        return {}
      if hasattr(kwargs.get("Body"), "seek"):
        kwargs["Body"].seek(0)
      api_etime = time()
      try:
        return self._may_be_execute_op(method, i, rcount, failure_recorded_time,
                                       rsize, err, **kwargs)
      except Exception as err:
        failure_recorded_time = datetime.now()
        if (self._objects._expect_failure_during_ei or
            self._objects._upgrade_in_progress):
          retry_count += 1
        ret = self._process_op_err(method, err, set_exit_marker_on_failure,
                      rcount, failure_recorded_time, rsize, ignore_errors,
                      force_ignore_failures, retry_count, retry_delay,
                      api_etime, force, i, **kwargs)
        if ret is not None:
          if (set_exit_marker_on_failure is bool and
              not set_exit_marker_on_failure):
            ERROR("Debugging : Op : %s, RetryCount : %s, CurrentCount : %s"
                  %(method.__name__, retry_count, i))
          return ret
        i += 1
        if condition_for_retry and i >= retry_count+1:
          if self._check_condition_for_retry(condition_for_retry, method, err):
            #Give breathing time to MS post compaction completion. Hence 2 retry
            retry_count += 2
    self._record_api_execution(opname=method.__name__, total_count=rcount,
        fail_count=1, failure_time=failure_recorded_time, rsize=rsize, err=err)
    prnt_args = self._remove_large_items_from_dict(**kwargs)
    fatal_msg = "%s / %s : Hit exception after %s retries while executing %s "\
                "with args : %s. Error : (%s) %s"%(kwargs.get("Bucket"),
                kwargs.get("Key"), retry_count, method, prnt_args, type(err),
                err)
    self._objects._set_exit_marker(fatal_msg, caller=inspect.stack()[1][3])
    raise Exception(fatal_msg)

  def _check_condition_for_retry(self, condition_for_retry, method, err):
    """
    this method will check if reason for the given failure is legitimate i.e.
    compaction is running in background or number of scans are too high

    Args:
      condition_for_retry(obj): method pointer to check if failure is due to
                                some condtions
      method(obj): operation for which we are checking
      err(obj): not needed

    Returns: bool
    """
    if not self._objects._objectscluster:
      debug_print("ObjectsCluster obj is None. Skipping condition %s check for "
                  "%s"%(condition_for_retry.__name__, method.__name__))
      return False
    try:
      if condition_for_retry(wait_for_completion=False):
        DEBUG("Api : %s, Condition : %s, returned True"
              %(method.__name__, condition_for_retry.__name__))
        return True
    except Exception as err:
      ERROR("Failed to run condition %s for %s, Err : %s"
            %(condition_for_retry.__name__, method.__name__, err))
    return False

  def _process_op_err(self, method, err, set_exit_marker_on_failure, rcount,
                      failure_recorded_time, rsize, ignore_errors,
                      force_ignore_failures, retry_count, retry_delay,
                      api_etime, force, i, **kwargs):
    """
    this method does some checks i.e. whether to ignore the error, whether to
    raise exception right away, whether the error is related to data corruption
    etc... and after all the checks it will record the api execution and then
    sleep for some time(retry_delay) and returns the None if all the checks were
    done

    Args:
      method(obj): operation name
      err(obj): error object
      set_exit_marker_on_failure(bool,[str]): either bool or list of errors
      rcount(int): number of objects involved in operation
      failure_recorded_time(date): time of the failure of given retry
      rsize(int): size of all the objects involved in operation
      ignore_errors([str]): list of errors to ignore
      force_ignore_failures(bool): flag to ignore all errors
      retry_count(int): maximum number of retries allowed
      retry_delay(int): time in seconds
      api_etime(date): total execution time of the api
      force(bool): flag to pass to _detect_data_corruption method
      i(int): number of failed attempt for the operation
      kwargs(dict): any other arguments

    Returns: {} or None
      if the error needs to be ignored then return {}, else if all the checks
      doen then return None

    Raises: Exception
      in case of _may_be_set_exit_marker returns False then it will raise the
      exception and also _detect_data_corruption will also raise the exception
      if corruption detected
    """
    self._may_be_track_error_stack(method.__name__, err, **kwargs)
    expected_failure = False
    opdetails = err.opdetails if hasattr(err, "opdetails") else None
    if not opdetails: opdetails = {"ErrMsg":err.message}
    if not opdetails["ErrMsg"]:
      if hasattr(err, "reason"):
        opdetails["ErrMsg"] = err.reason
    if not self._may_be_set_exit_marker(err, set_exit_marker_on_failure,
        method.__name__, rcount, 1, failure_recorded_time, rsize, False,
        0, err):
      raise
    if self._may_be_ignore_error(err, ignore_errors, method.__name__,
          kwargs.get("Bucket"), kwargs.get("Key"), err, rcount, 1,
          failure_recorded_time, rsize, True, force_ignore_failures):
      return {}
    if self._objects._debug_level >= 3:
      print_exc()
    etime = "{:.3f}".format(time() - api_etime)
    self._detect_data_corruption(method.__name__, i, retry_count,
                retry_delay, err, etime, force=force, **kwargs)
    if (self._objects._expect_failure_during_ei or
        self._objects._upgrade_in_progress):
      expected_failure = True
    if self._objects._debug_level > 2: LOGEXCEPTION(err)
    self._record_api_execution(opname=method.__name__, total_count=rcount,
        fail_count=1, expected_failure_count=1 if expected_failure else 0,
        rsize=rsize, err=err, failure_time=failure_recorded_time,
        apires={"opdetails":opdetails})
    self._track_result(kwargs.get("Bucket"), kwargs.get("Key"),
                         method.__name__, i, failure_recorded_time, err)
    if i < retry_count: sleep(retry_delay)
    return None

  def _may_be_track_error_stack(self, method, err, **kwargs):
    """
    this method will log the error stack if error is in the
    _kwargs["errors_to_dump_stack_strace"]

    Args:
      method(str): operation name
      err(obj): error object
      kwargs(dict): all the other arguments needed
    """
    op_err_msg = ""
    if hasattr(err, "opdetails"):
      op_err_msg = str(err.opdetails.get("ErrMsg"))
    elif hasattr(err, "message"):
      op_err_msg = str(err.message)
    for errormsg in self._objects._kwargs["errors_to_dump_stack_strace"]:
      if errormsg in op_err_msg or errormsg in str(err):
        prnt_args = self._remove_large_items_from_dict(**kwargs)
        ERROR("Opname : %s\nArgs : %s\nStack : %s"
              %(method, prnt_args, format_exception()))

  def _remove_large_items_from_dict(self, **kwargs):
    """
    removes the large items like large lists, large dicts, large strings and
    other large objects from kwargs and returns new dict

    Returns: dict
    """
    prnt_args = {}
    for k,v in kwargs.iteritems():
      if k in ("Body","data", "Delete", "Deleted", "DeleteMarkers"):
        prnt_args[k] = 'Dropped_due_to_size_of_value_%s'%(len(str(v)))
        continue
      if isinstance(v, dict):
        if len(v) > 40:
          prnt_args[k] = 'Dropped_due_to_size_of_value_%s'%(len(str(v)))
          continue
      if isinstance(v, list):
        if len(v) > 40:
          prnt_args[k] = 'Dropped_due_to_size_of_value_%s'%(len(str(v)))
          continue
      if isinstance(v, str):
        if len(v) > 200:
          prnt_args[k] = 'Dropped_due_to_size_of_value_%s'%(len(str(v)))
          continue
      prnt_args[k] = v
    return prnt_args

  def _may_be_execute_op(self, method, i, rcount, failure_recorded_time,
                         rsize, err=None, **kwargs):
    """
    executes the method,if successful, collects the response, records the
    execution detail in _local_records and returns the response

    Args:
      method(obj): method to execute
      i(int): number of unsuccessful attempts executing this api request
      rcount(int): no of objects involved in given operation
      failure_recorded_time(date): it is the time of the last failed request
      rsize(int): total size of all the objects included in operation
      err(obj): error object from last failed attempt
      kwargs(dict): all the other arguments needed

    Returns: dict
    """
    stime, res = self._execute_api(method, **kwargs)
    res_data = {}
    if isinstance(res, dict):
      res_data = self._remove_large_items_from_dict(**res)
    if i > 0:
      prnt_args = self._remove_large_items_from_dict(**kwargs)
      DEBUG("%s / %s, %s executed after : %s retry, Args : %s, Res : %s, "
            "Recent Error : %s"%(kwargs.get("Bucket"), kwargs.get("Key"),
            method.__name__, i, prnt_args, res_data if res_data else  res,
            err))
    self._record_api_execution(opname=method.__name__, total_count=rcount,
      fail_count=0,failure_time=failure_recorded_time, rsize=rsize,
      ignored_failure=False, expected_failure_count=0, apires=res)
    return res

  def _may_be_set_exit_marker(self, error, errors_to_exit, opname, total_count,
                              fail_count, failure_time, rsize, ignored_failure,
                              expected_failure_count, err):
    """
    this methods decides whether we want to continue with retries(and after the
    sufficient retries if the execution still fails, the _execute method will
    set the exit_marker and raise the error) so if this method returns True,
    the execution will continue with retry, otherwise the _process_op_err will
    raise the exception and the caller(from UploadObjs class) will handle it the
    way it wants, if it is returning false, it will record the api execution
    details first and then returns

    Args:
      error(obj): this is a redundant parameter with last argument(err), so we
                  will remove it
      errors_to_exit(bool,list): this arguments decides the method output, if
                                 a. its a list and err.message is in the list
                                     then it will return false, else True
                                 b. its a bool, False, then method will return
                                    False, else True
      opname(str):  operation name
      total_count(int): no of objects involved in given operation
      fail_count(int): no of times given operation failed
      failure_time(time): time of failure if operation failed or None
      rsize(int): total size of all the objects involved
      ignored_failure(bool): True, if we want to ignore the failure
      expected_failure_count(int): 1 if currently EI or upgrade is in progress
      err(obj): error object

    Returns: bool
    """
    if isinstance(errors_to_exit, bool):
      if not errors_to_exit:
        expected_failure_count += 1
        self._record_api_execution(opname=opname, total_count=total_count,
          fail_count=fail_count, failure_time=failure_time, rsize=rsize,
          ignored_failure=ignored_failure,
          expected_failure_count=expected_failure_count, err=err)
        return False
      return True
    for error in errors_to_exit:
      if (hasattr(err, "strerror") and error in err.strerror) or (error in
          err.message):
        expected_failure_count += 1
        self._record_api_execution(opname=opname, total_count=total_count,
              fail_count=fail_count, failure_time=failure_time, rsize=rsize,
              ignored_failure=ignored_failure,
              expected_failure_count=expected_failure_count, err=err)
        return False
    return True

  def _may_be_ignore_error(self, error, ignore_errors, opname, bucket, key, err,
                 total_count, fail_count, failure_time, rsize, ignored_failure,
                 force_ignore_failures):
    """
    it will return True if given error is in the list ignore_errors or
    force_ignore_failures is True, else False

    Args:
      error(obj): redundant(of no use)
      ignore_errors([str]): list of errors to ignore
      opname(str): operation name
      bucket(str): bucket name for given operation
      key(str): object name for given operation
      err(obj): error object
      total_count(int): no of objects involved in given operation
      fail_count(int): no of times given operation failed
      failure_time(time): time of failure if operation failed or None
      rsize(int): total size of all the objects involved
      ignored_failure(bool): True, if we want to ignore the failure
      force_ignore_failures(bool): True, if we want to ignore failure for any
                                   kind of error, else False

    Returns: bool
    """
    if force_ignore_failures:
      self._record_api_execution(opname=opname, total_count=total_count,
                  fail_count=fail_count, failure_time=failure_time, rsize=rsize,
                  ignored_failure=ignored_failure, err=err)
      return True
    for error in ignore_errors:
      if error and error in err.message:
        if self._objects._debug_level > 3:
          WARN("Bucket/Object/Action : %s / %s / %s, Ignoring error : %s"
             %(opname, bucket, key, err))
        self._record_api_execution(opname=opname, total_count=total_count,
          fail_count=fail_count, failure_time=failure_time, rsize=rsize,
          ignored_failure=ignored_failure, err=err)
        return True
    return False


  def _detect_data_corruption(self, method, current_count, retry_count,
                              retry_delay, err, api_etime, force, **kwargs):
    """
    this method will log the error message with every detail about the operation
    and iff the reason of error is data corruption, it will raise the Exception

    Args:
      method(str): operation name
      current_count(int): no of times operation already failed
      retry_count(int): maximum no of retries allowed
      err(obj): error object
      api_etime: total execution time of given method
      force(bool): this is a flag to indicate that operation will be executed
                   by _execute method even if exit-marker is already set, its
                   needed here just for logging purpose
      kwargs(dict): all the other arguments needed in the method

    Raises: Exception
      only if exception is bcz of data corruption
    """
    prnt_args = self._remove_large_items_from_dict(**kwargs)
    prnt_args.update({"force":force,"time_to_exit":self._objects._time_to_exit})
    ndel = 0
    if "Delete" in kwargs and "Objects" in kwargs["Delete"]:
      ndel = len(kwargs["Delete"]["Objects"])
    space_length = "%s-"%(" "*25)
    op_err_msg = ""
    if hasattr(err, "opdetails"):
      op_err_msg = str(err.opdetails.get("ErrMsg"))
    if hasattr(err, "message"):
      op_err_msg += "(Err.msg : %s)"%str(err.message)
    if self._objects._time_to_exit or self._objects._execution_completed:
      op_err_msg += ", TimeToExit : %s, TestCompleted : %s"%(
              self._objects._time_to_exit, self._objects._execution_completed)
    msg = "(%s) %s/%s : Hit exception while executing %s%s\n"%(
           currentThread().name, kwargs.get("Bucket"), kwargs.get("Key"),
           method, "(Num objects to Delete : %s)"%(ndel) if ndel>0 else "")
    msg += "%sArgs : %s\n"%(space_length, prnt_args)
    msg += "%sRetry Count : %s/%s, Retry Delay : %s sec, Execution time : %s%s"\
           %(space_length, current_count, retry_count, retry_delay, api_etime,
             self._get_extended_msg_for_ei())
    msg += "%sERROR : (%s) %s\n"%(space_length, type(err), op_err_msg)
    msg += "%sOpdetails  : %s"%(space_length,
           err.opdetails if hasattr(err, "opdetails") else self._get_pst_time())
    ERROR(msg)
    if isinstance(err.message, str) and "Corruption" in err.message or\
        "Corruption" in str(err) or "Corruption" in msg:
      self._objects._set_exit_marker(msg, caller=inspect.stack()[1][3])
      raise err

  def _get_extended_msg_for_ei(self):
    """
    Returns the string indicating whether EI or Upgrade is in progress

    Returns: str
    """
    msg = ""
    if self._objects._expect_failure_during_ei:
      msg += ", EI In Progress : True, Component Under EI : %s"%(
              self._objects._total_ops_recorded["ei"]["comp_under_ei"])
    if self._objects._upgrade_in_progress:
      msg += ", Upgrade In Progress : True"
    msg += "\n"
    return msg

  def _get_pst_time(self):
    """
    returns the current pst_time

    Returns: time obj
    """
    tz = timezone('America/Los_Angeles')
    return datetime.fromtimestamp(int(time()), tz).isoformat()

  def _execute_api(self, method, **kwargs):
    """
    it will execute api, collects the response, add some details in
    it and returns the response

    Args:
      method(obj): function pointer to execute
      kwargs(dict): kwargs to pass to method

    Returns: dict (returning start time is of no use, will remove it in future from code)
      response from execution and it also has properties execution_time and opdetails
      opdetails has more info like operation start_time, end_time, total execution time, oc detials.. etc

    Raises: botocore.exceptions.ClientError or Exception
    it also adds property opdetails which is same as opdetails as response
    except for the case of delete objects, in which case it will raise plain
    Exception object
    """
    stime=time()
    opdetails = {"PST-STime" : self._get_pst_time(),
                 "ErrCode":None, "ErrMsg":None}
    try:
      res =  method(**kwargs)
      opdetails["PST FTime"] = self._get_pst_time()
      if isinstance(res, dict):
        if "ResponseMetadata" in res:
          opdetails["ErrCode"] = res["ResponseMetadata"]["HTTPStatusCode"]
        elif "HTTPStatusCode" in res:
          opdetails["ErrCode"] = res["HTTPStatusCode"]
    except Exception as err:
      opdetails["PST FTime"] = self._get_pst_time()
      opdetails["etime"] = time()-stime
      err_resp = {}
      if hasattr(err, "response") and isinstance(err.response, dict):
        err_resp = err.response
        opdetails["RESPONSE"] = err_resp
      ntnxerror, opdetails["ErrCode"], opdetails["ErrMsg"] = \
                                        self._get_err_msg_code(err_resp, err)
      opdetails["endpoint"] = err_resp.get("Error", {}).get("endpoint", None)
      if hasattr(err, "operation_name"):
        opdetails["OPName"] = err.operation_name
      oc = self._get_oc_details(err)
      opdetails["object-controller"] = {"pod":oc, "component-id":oc,
                      "GMT":strftime("%a, %d %b %Y %X GMT", gmtime()),
                      "PST":self._get_pst_time()}
      if not opdetails.get("endpoint"):
        opdetails["endpoint"] = self._get_endpoint(method, None, err)
      err.opdetails = opdetails
      raise
    etime=time()
    opdetails["endpoint"] = self._get_endpoint(method, res, None)
    opdetails["etime"] = etime-stime
    oc = self._get_oc_details(None, res)
    opdetails["object-controller"] = {"pod":oc, "component-id":oc,
                    "GMT":strftime("%a, %d %b %Y %X GMT", gmtime()),
                    "PST":self._get_pst_time()}
    if type(res) is dict:
      res["etime"] = etime-stime
      res["opdetails"] = opdetails
      if res.get("Errors"):
        #ERROR(res.get("Errors"))
        raise Exception("Detected failures in delete-objects call")
    return stime, res

  def _get_oc_details(self, err, res=None):
    """
    returns the object-controller details from either error or response

    Returns: str
    """
    oc = None
    if res and type(res) is dict:
      oc  = res.get("ResponseMetadata", {}).get("HTTPHeaders",
                                                {}).get("x-ntnx-id")
    if not oc and hasattr(err, "response"):
      if type(err.response) is dict:
        if "x-amz-request-id" in err.response:
          oc  = err.response["x-amz-request-id"]
        else:
          oc = err.response.get("ResponseMetadata", {}).get("HTTPHeaders",
                                {}).get("x-amz-request-id")
    try:
      #oc = oc.split("+")
      #oc = int(oc.split("+")[0].replace('10000',''))-1
      return "oc-%s"%(oc.split("+")[0])
    except:
      pass

  def _get_err_msg_code(self, err_resp, err):
    """
    returns the tuple of error messages and code

    Args:
      err_resp(obj): err.response in case of botocore exception object
      err(obj): error object

    Returns: tuple: (ntnxerr, errcode, errmsg)
    """
    httpcode, errcode, errormsg = "", "", ""
    nxerror, errreason, strerror, errno = "", "", "", ""
    if hasattr(err, "response") and isinstance(err.response, dict):
      resp = err.response
      httpcode = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
      errcode = resp.get("Error", {}).get("Code")
      errormsg = resp.get("Error", {}).get("Message")
      nxerror = resp.get("ResponseMetadata", {}).get("HTTPHeaders",
                                                     {}).get("x-ntnx-error")
    if not errcode and err_resp.get("Error") and \
                                            "Code" in err_resp.get("Error"):
      errcode = err_resp["Error"]["Code"]
    if not errormsg and err_resp.get("Error") and \
                                        "Message" in err_resp.get("Error"):
      errormsg = err_resp["Error"]["Message"]
    if not errormsg:
      if hasattr(err, "message"):
        errormsg = str(err.message)
      else:
        errormsg = str(errmsg)
    if errormsg:
       errormsg = self._get_error_msg(errormsg)
    if hasattr(err, "reason"):
      errreason = str(err.reason)
      errreason = self._get_error_msg(errreason)
    if hasattr(err, "strerror"):
      strerror = str(err.strerror)
      strerror = self._get_error_msg(strerror)
      errno = err.errno
    errmsg = "%s%s%s%s%s%s"%("%s_"%(httpcode) if httpcode else "",
                             "%s_"%(nxerror) if nxerror else "",
                             "%s_"%(errcode) if errcode else "",
                             errormsg,
                             "_%s"%(errreason) if errreason else "",
                             "_%s"%(strerror) if strerror else "")
    errcode = "%s%s%s"%("%s"%(httpcode) if httpcode else "",
                        "_%s"%(nxerror) if nxerror else "",
                        "_%s"%(errcode) if errcode else "")
    if not errmsg:
      ERROR("ErrMsg is None : %s (err_resp : %s)"%(err, err_resp))
      errmsg = str(type(err))
    return nxerror, errcode, errmsg

  def _get_endpoint(self, method, method_output, err=None):
    """
    it will return the endpoint used by given method

    Args:
      method(obj): function pointer
      method_output(dict): response from method execution
      err(dict): in case of error response from method execution

    Returns: string
    """
    if method.__name__.startswith("s3_"):
      if isinstance(method_output, dict) and "s3_endpoint" in method_output:
        return method_output["s3_endpoint"]
      if err and hasattr(err, "s3_endpoint"):
        return err.s3_endpoint
    if hasattr(method, "im_self"):
      if hasattr(method.im_self, "_endpoint"):
        if hasattr(method.im_self._endpoint, "host"):
          return method.im_self._endpoint.host
        return method.im_self._endpoint
    return "NonS3Op"

  def _track_result(self, bucket, obj, opname, retry_count, failure_time,
                    err, **kwargs):
    """
    it will append the error details in self._error_fh_tracker file

    Args:
      bucket(str): bucket name
      obj(str): object name
      opname(str): operation name
      retry_count(int): no of retries already done
      failure_time(date): failed time
      err(obj): error object
    """
    msg = "%s : %s(%s), Opname: %s, RetryCount: %s, FailureTime: %s,Err : "\
          "(%s) %s\n"%(failure_time, bucket, obj, opname, retry_count,
                       failure_time, type(err), err)
    Utils.write_to_file(msg, self._error_fh_tracker, "a")

  def _record_api_execution(self, opname, total_count, fail_count, failure_time,
               rsize, ignored_failure=False,expected_failure_count=0, err=None,
               apires=None):
    """
    after the api execution is done here we will add its detail in
    self._local_records(which will update the _total_ops_recorded periodically)
    if the execution was succesfull in first attempt(i.e. failure_time is None)
    else in case of failure it will directly updates the _total_ops_recorded
    without pushing it to _local_records

    Args:
      opname(str): api_operation executed
      total_count(int): no of objects involved in given operation
      fail_count(int): no of times given operation failed
      failure_time(time): time of failure if operation failed or None
      rsize(int): total size of all the objects involved
      ignored_failure(bool): True, if we ignored the failure
      expected_failure_count(int): 1 if currently EI or upgrade is in progress
      err(obj): error object in case of failure
      apires(dict): response of given api
    """
    if opname == "s3_head_object" and apires and not err:
      objsize = apires["ContentLength"]
      if apires["ResponseMetadata"]["HTTPHeaders"].get(
                                                    "x-ntnx-location") == "kS3":
        if rsize == 0:
          rsize = apires["ContentLength"]
          opname = "tiered_get"
        elif rsize < objsize:
          opname = "tiered_range_get"
    opdetails = apires["opdetails"] if isinstance(apires, dict) and  \
                  "opdetails" in apires else None
    if failure_time:
      with self._lock:
        self._update_results(opname, total_count, fail_count, failure_time,
                             rsize, ignored_failure, expected_failure_count,
                             err, opdetails)
      return
    self._local_records[uuid4().hex] = [opname, total_count, fail_count,
                                        failure_time, rsize, ignored_failure,
                                        expected_failure_count, err, opdetails]
    #if len(self._local_records) > 25000 or (time()-self._last_update_time
    #                                   ) > Constants.OP_EXECUTOR_FLUSH_INTERVAL:
    #  self._flush_records()

  def _flush_records(self, force=True):
    """
    this method will be called by _log_stats of UploadObjs method periodically
    to flush the self._local_records and append it to _total_ops_recorded

    Args:
      force(bool): if True, it will directly flush the records without checking
                   last_update_time, else it checks the last_update_time and if
                    Constants.OP_EXECUTOR_FLUSH_INTERVAL time is passed then
                    only it will start flushing
    """
    if not force:
      is_flush_time = (time()-self._last_update_time
                                      ) > Constants.OP_EXECUTOR_FLUSH_INTERVAL
      if not is_flush_time:
        return
    num_records = len(self._local_records)
    sstime = datetime.now()
    is_flush_time = None
    with self._lock:
      if not force:
        is_flush_time = (time()-self._last_update_time
                                      ) > Constants.OP_EXECUTOR_FLUSH_INTERVAL
        if not is_flush_time:
          return
      sstime = datetime.now()
      for record  in self._local_records.keys():
        margs = self._local_records.pop(record)
        self._update_results(*margs)
      self._last_update_time = time()
      print "%s : Flush Stats : Num Records, Size : %s, FTime : %s, Force : %s"\
            ", Is Flush : %s, Records Unflushed : %s"%(
            datetime.now(), num_records, datetime.now()-sstime, force,
            is_flush_time, len(self._local_records))

  def _update_results(self, opname, total_count, fail_count, failure_time,
                rsize, ignored_failure, expected_failure_count, err, opdetails):
    """
    updating the _total_ops_recorded[opname] fields: failcount,
    Last.Recorded.Time, ignored_failure_count, expected_failure_count
    and also some of the direct fields of _total_ops_recorded: count, size,
    totalcount, totalfailure, totalfailureignored, expected_failure_count
    and also errors, endpoint_stats, object-controller of _total_ops_recorded
    """
    if opname not in self._objects._total_ops_recorded:
      self._objects._total_ops_recorded[opname] = {"failcount" : fail_count,
                      "Last.Recorded.Time":failure_time,
                      "count":total_count, "size":rsize,
                      "ignored_failure_count": 1 if ignored_failure else 0,
                      "expected_failure_count" : expected_failure_count
                      }
    else:
      self._objects._total_ops_recorded[opname]["failcount"] += fail_count
      if failure_time:
        self._objects._total_ops_recorded[opname]["Last.Recorded.Time"
                                                              ] = failure_time
        self._objects._total_ops_recorded["Last.Recorded.Time"
                                                          ] = failure_time
      self._objects._total_ops_recorded[opname]["ignored_failure_count"
                                              ] += 1 if ignored_failure else 0
      self._objects._total_ops_recorded[opname]["expected_failure_count"
                                                   ] += expected_failure_count
      self._objects._total_ops_recorded[opname]["count"] += total_count
      self._objects._total_ops_recorded[opname]["size"] += rsize
    self._objects._total_ops_recorded["totalcount"] += total_count
    self._objects._total_ops_recorded["totalfailure"] += fail_count
    self._objects._total_ops_recorded["totalfailureignored"
                                          ] += 1 if ignored_failure else 0
    self._objects._total_ops_recorded["expected_failure_count"
                                                  ] += expected_failure_count
    self._update_error_details_in_db(opdetails, opname, failure_time, err)
    self._update_oc_details(opname, opdetails)
    self._update_endpoint_details(opname, opdetails, total_count, fail_count,
                            1 if ignored_failure else 0, expected_failure_count)

  def _update_error_details_in_db(self, opdetails, opname, failure_time, err):
    """
    it will update the total_ops_recorded['errors'] in db accroding to
    error_code

    Args:
      opdetails(dict): operation details
      opname(str): operation name
      failure_time(date): time
      err(obj): not needed
    """
    if self._objects._time_to_exit:
      #ERROR("Skipping Error updation for %s due to exit marker found"%opname)
      return
    if not opdetails:
      return
    psttime = opdetails.get("PST-STime")
    oc = opdetails.get("object-controller", {}).get("pod", "NF")
    etime = opdetails.get("etime")
    if isinstance(etime, float) or isinstance(etime, int):
      etime = "{:.4f}".format(opdetails.get("etime"))
    stat_type, error = "err_msgs", None
    if opdetails.get("ErrMsg"):
      error = opdetails["ErrMsg"]
    elif opdetails.get("ErrCode"):
      error, stat_type = opdetails["ErrCode"], "err_codes"
    if not error:
      #ERROR("Error Code is empty or none. Opdetails : %s, Err (Type) : %s (%s)"
      #      %(opdetails, err, type(err)))
      error = opdetails.get("endpoint", "UnknownOpAndUnknownError")
    error = str(error)
    if error not in self._objects._total_ops_recorded["errors"][stat_type]:
      self._objects._total_ops_recorded["errors"][stat_type][error]= {"count":0}
    self._objects._total_ops_recorded["errors"][stat_type][error]["count"] += 1
    self._objects._total_ops_recorded["errors"][stat_type][error].update({
                    "opname":opname, "Last.Recorded.Time":failure_time, "oc":oc,
                    "pst_stime":psttime, "etime":etime})

  def _update_endpoint_details(self, opname, opdetails, total_count, fail_count,
                               totalfailureignored, expected_failure_count):
    """
    it will update the _total_ops_recorded["endpoint_stats"] with result of
    current api execution

    Args:
      opname(str): api_operation executed, not needed
      opdetails(dict): operation details
      total_count(int): no of objects involved in given operation
      fail_count(int): no of times given operation failed
      totalfailureignored(int): 1, if we ignored the failure else 0
      expected_failure_count(int): 1 if currently EI or upgrade is in progress
    """
    if not opdetails:
      return
    endpoint = opdetails.get("endpoint")
    if endpoint not in self._objects._total_ops_recorded["endpoint_stats"]:
      self._objects._total_ops_recorded["endpoint_stats"][endpoint] = {
            "total":0, "failures":0, "ignored":0, "expected":0}
    self._objects._total_ops_recorded["endpoint_stats"][endpoint][
                                                        "total"] += total_count
    self._objects._total_ops_recorded["endpoint_stats"][endpoint][
                                                      "failures"] += fail_count
    self._objects._total_ops_recorded["endpoint_stats"][endpoint][
                                              "ignored"] += totalfailureignored
    self._objects._total_ops_recorded["endpoint_stats"][endpoint][
                                          "expected"] += expected_failure_count

  def _update_oc_details(self, opname, opdetails):
    """
    it will update the object-controller stats which served this api request

    Args:
      opname(str): operation name
      opdetails(dict): operation details
    """
    if not opdetails or "object-controller" not in opdetails:
      return
    pod = opdetails["object-controller"]["pod"]
    if opname not in self._objects._total_ops_recorded["object-controller"][
                                                                      "stats"]:
      self._objects._total_ops_recorded["object-controller"]["stats"][opname]={}
    if pod not in self._objects._total_ops_recorded["object-controller"][
                                                              "stats"][opname]:
      self._objects._total_ops_recorded["object-controller"]["stats"][
                                    opname][pod] = {"success":0, "failures":0}
    self._objects._total_ops_recorded["object-controller"]["stats"][
                                    opname][pod]["success"] += 1

  def _get_error_msg(self, error):
    """
    it will look for key of self._errmsp in error(if the key is sbustring
    in error), if its present then returns the respective value as message
    if it doesn't find any key, it will returns the error message upto 100
    characters

    Args:
      error(str): error message

    Returns: str
    """
    for err, msg in self._errmsp.iteritems():
      if err in error:
        return msg
    return str(error).strip()[0:100]

class RDMDeploy():
  def __init__(self, rdm_user, rdm_password):
    self._user = rdm_user
    self._pass = rdm_password
    self._rdm_url = "https://rdm_plugin_host_1.eng.nutanix.com/api/v1/"
    self._rdm_url += "scheduled_deployments"
    self._deployment_id = None

  def get_spec(self, rdm_clustername, nos_version, build_type, hypervisor_type,
             hypervisor_version, node_pool_name, cluster_num_nodes,
             rdm_setup_duration, pc_build_url, pc_num_nodes, pc_vm_size,
             **kwargs):
    return {
        "name": rdm_clustername,
        "retry": 0,
        "resource_specs": [
          {
            "dependencies": [
              "CLUSTER-1",
              "PC-2"
            ],
            "name": "registration_link_CLUSTER-1_PC-2",
            "type": "$REGISTER_PE_PC"
          },
          {
            "type": "$NOS_CLUSTER",
            "name": "CLUSTER-1",
            "is_new": True,
            "auto_generate_cluster_name": True,
            "is_nested_base_cluster": False,
            "software": {
              "nos": {
                "version": nos_version,
                "build_type": build_type,
                "redundancy_factor": "default"
              },
              "hypervisor": {
                "version": hypervisor_version,
                "type": hypervisor_type,
                "installation_type": "gui",
                "link_speed": "default"
              }
            },
            "hardware": {
              "svm_gb_ram": 16,
              "svm_num_vcpus": 8,
              "cluster_min_nodes": cluster_num_nodes
            },
            "network": {
              "subnet_local": True
            },
            "image_resource": True,
            "resources": {
              "type": "node_pool",
              "entries": [
                node_pool_name
              ]
            },
            "secondary_datacenters": {
              "vsphere": {
              "vcenters": [
                  kwargs["secondary_vcenter"]
                ]
              }
            }
          },
          {
            "type": "$PRISM_CENTRAL",
            "name": "PC-2",
            "is_new": True,
            "software": {
              "prism_central": {
                "build_url": pc_build_url
              }
            },
            "scaleout": {
              "num_instances": pc_num_nodes,
              "pcvm_size": pc_vm_size
            },
            "provider": {
              "host": "CLUSTER-1"
            },
            "dependencies": [
              "CLUSTER-1"
            ]
          }
        ],
        "expand_links": False,
        "tags": [
          "Anirudha"
        ],
        "demo_mode": False,
        "lock_scheduled_deployment": False,
        "duration": rdm_setup_duration,
        "client_timezone_offset": 480
      }

  def deploy(self, deployment_spec, wait_for_deployment=True, deployments=None,
             deployment_id=None):
    self._deployments = deployments
    self._deployment_id = deployment_id
    if not self._deployments and not self._deployment_id:
      INFO("Initiating RDM deployment with spec : %s"%(deployment_spec))
      res = post(self._rdm_url, auth=(self._user, self._pass),
                          headers={"Content-Type": "application/json"},
                          data=dumps(deployment_spec), verify=False)
      if not res.ok:
        raise Exception("Failed to deploy RDM resources : %s, Error : %s"
                        %(deployment_spec, res.content))
      if not wait_for_deployment:
        return res.content
      self._deployment_id = res.json()["id"]
      deployments = self._get_status(self._deployment_id,
                                     True)["data"]["deployments"]
      self._deployments = []
      for i in deployments:
        self._deployments.append(str(i["$oid"]))
    INFO("Deployments : --deployment_id %s --deployments %s"
         %(self._deployment_id, " ".join(self._deployments)))
    _, ttime = self._wait_for_task(self._deployment_id,
                                   expected_state="SUCCESS", timeout=3600*4,
                                   unexpected_states=["ABORTING", "RELEASED",
                                                      "FAILED", "ABORTED"])
    INFO("Deployment completed. Deployment ID : %s, Deployments : %s, Cluster "
         "details : %s, DeployTime : %s"%(self._deployment_id,
         self._deployments, self.get_cluster_details(), ttime))
    return self._deployment_id, self._deployments

  def get_cluster_details(self):
    for deployment in self._deployments:
      state =  self._get_cluster_details(deployment)
      if state["data"]["type"] != "$NOS_CLUSTER":
        continue
      INFO("Cluster state from RDM : %s"%(state))
      INFO("Resource allocated from RDM : %s"
           %(state["data"]["allocated_resource"]))
      pe = state["data"]["allocated_resource"]["svm_ip"]
      pc = state["data"]["allocated_resource"]["static_ips"][-1]["ip"]
      cluster_name = state["data"]["allocated_resource"]["host"]
      return pe, pc, cluster_name

  def release(self, deployment_id=None):
    if not deployment_id:
      deployment_id = self._deployment_id
    INFO("Releasing deployment with ID : %s"%(deployment_id))
    url = "https://rdm_plugin_host_1.eng.nutanix.com/api/v1/"\
          "scheduled_deployments/%s/release_resources"%(deployment_id)
    res = post(url, headers={"Content-Type": "application/json"},
                        data='{}',  auth=(self._user ,self._pass), verify=False)
    if not res.ok:
      raise Exception("Failed to release the deployment : %s"%(deployment_id))
    self._wait_for_task(deployment_id, expected_state = "RELEASED", timeout=600)

  def _wait_for_task(self, rdm_id, expected_state, timeout,
                     unexpected_states=[]):
    stime = time()
    while time()-stime < timeout:
      state =  self._get_status(rdm_id)
      INFO("Deployment status for id - %s : %s, Expected : %s, Retry in %s secs"
           ", Time Elapsed : %s secs"%(rdm_id, state, expected_state, 30,
           int(time() - stime)))
      if state == expected_state:
        return True, int(time() - stime)
      if unexpected_states and state in unexpected_states:
        raise Exception("Deployment in unexpected state. ID : %s, Expected vs "
                        "Found : %s vs %s, Unexpected States : %s"%(rdm_id,
                        expected_state, state, unexpected_states))
      sleep(30)
    raise Exception("Timeout while waiting for state. ID : %s, Expected vs "
                    "Found for ID : %s vs %s"%(rdm_id, expected_state, state))

  def _get_status(self, rdm_id, get_all_details=False):
    url = self._rdm_url+"/%s"%(rdm_id)
    res =get(url, headers={"Content-Type": "application/json"},
                      data='{}',  auth=(self._user, self._pass), verify=False)
    if res.json()["data"]["status"] == "FAILED":
      print res.json()
    if get_all_details:
      return res.json()
    return res.json()["data"]["status"]

  def _get_cluster_details(self, rdm_id, get_all_details=False):
    url = "https://rdm_plugin_host_1.eng.nutanix.com/api/v1/deployments/"
    url += rdm_id
    res =get(url, headers={"Content-Type": "application/json"}, data='{}',
             auth=(self._user, self._pass), verify=False)
    return res.json()

class InvalidDeployStart(Exception):
    pass

class InvalidParams(Exception):
    pass

class ReqExcept(Exception):
    pass

class ObjectstoreNotFound(Exception):
    pass

class RequestError(Exception):
    def __init__(self, response):
        self.value = response
    def __str__(self):
        return "Failed %s --> %s --> %s" % \
            (self.value.request.method, self.value.url, self.value.content)

class FetchingObjectstoreError(RequestError):
    def __init__(self, response, identity):
        self.identity = identity
        super().__init__(response)
    def __str__(self):
        return "Fetching objectstore %s failed. %s" % \
            (self.identity, super().__str__())

class LCMEntityNotFound(Exception):
    def __init__(self, entity_class, entity_model, device_id=None):
        self.entity_class = entity_class
        self.entity_model = entity_model
        self.device_id = device_id
    def __str__(self):
        return "entity_class: %s, entity_model: %s, device_id: %s not found "\
               "in LCM"%(self.entity_class, self.entity_model, self.device_id)

class LCMEntityAvailableVersionNotFound(Exception):
    def __init__(self, entity_class, entity_model, target_version):
        self.entity_class = entity_class
        self.entity_model = entity_model
        self.target_version = target_version
    def __str__(self):
        return "entity_class: %s, entity_model: %s doesn't have available "\
               "version %s"%(self.entity_class, self.entity_model,
                             self.target_version)

class Constants:

  MSG_TYPE_DEBUG = "debug"
  MSG_TYPE_ERROR = "error"
  MSG_TYPE_WARN = "warn"

  REMOTE_POPS_QUOTA = "remote_execution"
  TEST_NOT_STARTED = "NotStarted"
  TEST_STOPPING = "Stopping"
  TEST_NOT_RUNNING = "NotRunning"
  TEST_STARTING = "Starting"
  TEST_RUNNING = "Running"
  TEST_FAILED = "Failed"
  TEST_COMPLETED = "Completed"
  CACHE_DISCARD_FILENAME = "dicarded_cache.json"

  MSP_DEFAULT_NAMESPACE = "default"
  MSP_POD_RUNNING_STATE = "Running"

  OBJECTS_ATLAS_SERVICE = "poseidon-atlas"
  OBJECTS_MS_SERVICE = "ms"

  ATLAS_SCAN_RUNNING_STATUS = "Running"
  ATLAS_FULL_SCAN = "full"
  ATLAS_PARTIAL_SCAN = "partial"
  ATLAS_SELECTIVE_SCAN = "selective"
  CURATOR_FULL_SCAN = "curator_full_scan"
  CURATOR_PARTIAL_SCAN = "curator_partial_scan"
  ATLAS_MASTER_POD = "atlas_master"
  ATLAS_WORKER_POD = "atlas_worker"
  ATLAS_DEFAULT_SCAN_TIMEOUT = 24*3600

  ATLAS_WEB_PAGE = 'http://0:7103'
  ATLAS_MASTER_WEBPAGE = '%s/master/api/client/StartCuratorTasks'%ATLAS_WEB_PAGE
  ATLAS_PARTIAL_SCAN_TRIGGER_PAGE = '%s?task_type=21'%ATLAS_MASTER_WEBPAGE
  ATLAS_FULL_SCAN_TRIGGER_PAGE = '%s?task_type=22'%ATLAS_MASTER_WEBPAGE
  ATLAS_SELECTIVE_SCAN_TRIGGER_PAGE = '%s?task_type=23'%ATLAS_MASTER_WEBPAGE

  READ_TYPE_FULL_READ = "full_read"
  READ_TYPE_RANGE_READ = "range_read"
  NFS_READ_TYPE_FULL_READ = "full_read"
  NFS_READ_TYPE_RANGE_READ = "range_read"

  EI_SERVICE_UI="ui"
  EI_SERVICE_ZK="zk"
  EI_SERVICE_MS="ms"
  EI_SERVICE_CDP="cdp"
  EI_SERVICE_ALL="all"
  EI_COMPONENT_POD="pod"
  EI_COMPONENT_NODE="node"
  EI_SERVICE_ATLAS="atlas"
  EI_SERVICE_OBJECT="object"
  EI_COMPONENT_SERVICE="service"
  EI_COMPONENT_MSP_SERVICE="msp_service"
  EI_COMPONENT_INFRA_SERVICE = "infra_service"

  EI_OPERATION_ON="on"
  EI_OPERATION_OFF="off"
  EI_OPERATION_RESET="reset"
  EI_OPERATION_PAUSE="pause"
  EI_OPERATION_REBOOT="reboot"
  EI_OPERATION_RESETALL="resetall"
  EI_OPERATION_SHUTDOWN="shutdown"
  EI_OPERATION_REBOOTALL="rebootall"
  EI_OPERATION_RESTART_POD="restart"
  EI_OPERATION_SERVICE_H_EXIT="h_exit"
  EI_OPERATION_REBOOT_CVM="reboot_cvm"
  EI_OPERATION_REBOOT_HOST="reboot_host"
  EI_OPERATION_STOP_STARGATE="stop_stargate"
  EI_OPERATION_REBOOT_ALL_CVM="reboot_all_cvm"
  EI_OPERATION_STOP_CASSANDRA="stop_cassandra"
  EI_OPERATION_REBOOT_ALL_HOST="reboot_all_host"
  EI_OPERATION_START_STARGATE="restart_stargate"
  EI_OPERATION_START_CASSANDRA="restart_cassandra"
  EI_OPERATION_RESTART_STARGATE="restart_stargate"
  EI_OPERATION_RESTART_CASSANDRA="restart_cassandra"

  EI_RECOVERY_TIME_FOR_POD = 120
  EI_RECOVERY_TIME_DEFAULT = 121 #Just have unique timeout for default
  EI_RECOVERY_TIME_FOR_SERVICE = 60
  EI_RECOVERY_TIME_FOR_MSP_POWERCYCLE = 720
  EI_RECOVERY_TIME_FOR_REBOOT_HYP_HOST = 900
  EI_RECOVERY_TIME_FOR_REBOOT_ALL_HOST_CVM = 1800

  LOCK="lock"
  UNLOCK="unblock"

  STATIC_STATS_FILE="/tmp/stats"
  STATIC_ERROR_FILE="/tmp/stats.err"
  STATIC_MEM_TOP_FILE='/tmp/mem_top'
  EXIT_MARKER_FILE="/tmp/exit_marker"
  STATIC_RECORDS_FILE="/tmp/records.json"
  STATIC_STATS_HTML_FILE="/tmp/stats.html"
  STATIC_TESTCONFIG_FILE="/tmp/testconfig.json"
  STATIC_BUCKET_INFO_FILE="/tmp/bucket_info.json"
  WEBSERVER_JSON_FILE_LOCATION="/tmp/stats.json"
  STATIC_ALL_STATS_RECONRDS_FILE="/tmp/all_stats.json"
  STATIC_ALL_CLIENTS_RECORDS_FILE="/tmp/clients_records.log"
  STATIC_FINAL_RECONRDS_FILE="/tmp/final_records.json"

  FEXST = "File exists"
  IAD = "Is a directory"
  NAD = "Not a directory"
  ROFS = "Read-only file system"
  DORB = "Device or resource busy"
  ICDL = "Invalid cross-device link"

  NFSFIO_WORKLOAD="nfsfio"
  NFSFIO_WL_DEFAULT_PREFIX="fio"
  NFS_WRITE_ETAG_IDENTIFIER="nfs"
  NFS_FILE_ETAG_IDENTIFIER="-nfs"
  NFSVDBENCH_WORKLOAD="nfsvdbench"
  DELETE_WL_DEFAULT_PREFIX="delete"
  NFSWRITE_WL_DEFAULT_PREFIX="keynfs"
  NFSCOPY_WL_DEFAULT_PREFIX="nfscopy"
  SPARSE_FILE_DEFAULT_PREFIX="sparse"
  NFS_OVERWRITE_ETAG_IDENTIFIER="-s3nfs"
  NFSVDBENCH_WL_DEFAULT_PREFIX="vdbench"
  NFSDELETE_WL_DEFAULT_PREFIX="nfsdelete"
  NFSOWRITE_WL_DEFAULT_PREFIX="nfsowrite"
  NFSRENAME_WL_DEFAULT_PREFIX="nfsrename"

  MAGIC_BYTE="_"
  SUB_BLOCK_SIZE=1024
  WEBSERVER_PORT=37000
  OP_EXECUTOR_FLUSH_INTERVAL = 35
  COMPRESSEABLE_DATA_SIZE = 8*1024
  LOCAL_STATIC_DATA_SIZE = 1024*1024
  ZERO_DATA_ETAG='d41d8cd98f00b204e9800998ecf8427e'

  OBJ_VERSION="_V3_"
  LEGACY_OBJ_VERSION="_V2_"
  OBJ_IDN_FOR_S3COPY="_S3CP_"
  OBJ_IDN_FOR_NFSCOPY="_NCP_"
  NFS_OBJ_IDENTIFIER = "_NFS_"
  OBJ_IDN_FOR_NFSRENAME="_NRN_"
  STATIC_DATA_IDENTIFIER = "_SD_"
  ZEROBYTE_OBJ_IDENTIFIER="_ZRO_"
  MULTIPART_OBJ_IDENTIFIER="_MLT_"
  DEFAULT_TAG_FOR_EXPIRY = "tagexp=tagvalue"

  POSEIDON_BUCKETINFO_MAP = "PoseidonBucketInfoMap"
  KPOSEIDON_BUCKETINDEX_MAP = "kPoseidonBucketIndexMap"
  POSEIDON_BUCKETLIST_MAP = "PoseidonBucketListMap"
  POSEIDON_VDISKINFO_MAP = "PoseidonVdiskInfoMap"
  POSEIDON_OPENVDISKINFO_MAP = "PoseidonOpenVdiskInfoMap"
  POSEIDON_REGIONINFO_MAP = "PoseidonRegionInfoMap"
  POSEIDON_BUCKETSTATS_MAP = "PoseidonBucketStatsMap"
  POSEIDON_BUCKET_REPLICATIONINFO_MAP = "PoseidonBucketReplicationInfoMap"
  POSEIDON_OBJECT_REPLICATIONINFO_MAP = "PoseidonObjectReplicationInfoMap"
  POSEIDON_OBJECT_TAGINFO_MAP = "PoseidonObjectTagInfoMap"
  POSEIDON_VNODESTATS_MAP = "PoseidonVnodeStatsMap"
  POSEIDON_DIRECTORYINFO_MAP = "PoseidonDirectoryInfoMap"
  POSEIDON_LIFECYCLE_POLICYINFO_MAP = "PoseidonLifecyclePolicyInfoMap"
  POSEIDON_GLOBAL_REGIONINFO_MAP = "PoseidonGlobalRegionInfoMap"
  POSEIDON_BUCKET_TAGINFO_MAP = "PoseidonBucketTagInfoMap"

  COMPACTION_STATUS_RUNNING = "running"
  COMPACTION_STATUS_COMPLETED = "completed"
  COMPACTION_STATUS_PENDING = "pending"

class Procs:
  def __init__(self):
    pass

class ObjectsCluster():
  def __init__(self, pcobj, mspcluster):
    self._msp = mspcluster
    self._pc = pcobj
    self._init_components()

  def _init_components(self):
    self.atlas = ATLAS(self._pc, self._msp)
    self.ms = MS(self._pc, self._msp)

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

class ATLAS():
  def __init__(self, pcobj, mspcluster,
               namespace=Constants.MSP_DEFAULT_NAMESPACE):
    self._pcobj = pcobj
    self._msp = mspcluster
    self._namespace = namespace
    self._master_pod = None
    self._atlas_master = None
    self._init_atlas_pods()

  @property
  def namespace(self):
    return self._namespace

  @property
  def service_name(self):
    return Constants.OBJECTS_ATLAS_SERVICE

  def is_master_changed(self):
    if not self._atlas_master:
      self.get_atlas_master()
      return False
    current_master = self.get_atlas_master()
    for podname, details in self._init_details.iteritems():
      if details["is_master"]:
        if (current_master["name"] != podname or
            current_master["incarnation_id"] != details["incarnation_id"]):
          ERROR("ATLAS master is changed. Old : %s, Current : %s"
                %(details, current_master))
          return True
    return False

  def get_atlas_master(self, refresh=True):
    master_changed = False
    pod = None
    if self._atlas_master:
      pod = self._allpods[self._atlas_master["name"]]
      details = self._get_pod_details(pod)
      if details["is_master"]:
        return self._atlas_master
      else:
        master_changed = True
    if master_changed:
      ERROR("ATLAS master changed. Old : %s"%(pod.name))
      if not refresh:
        return self._atlas_master
    self._atlas_master = self._get_atlas_master()
    return self._atlas_master

  def initiate_scan(self, scan_type, wait_for_completion=True,
                        timeout=Constants.ATLAS_DEFAULT_SCAN_TIMEOUT,
                        wait_for_ongoing_scan_to_finish=True):
    self.get_atlas_master()
    if wait_for_ongoing_scan_to_finish:
      self._wait_for_scan_to_finish(timeout=timeout)
    cmd = 'links -dump %s'%Constants.ATLAS_FULL_SCAN_TRIGGER_PAGE
    if scan_type == Constants.ATLAS_PARTIAL_SCAN:
      cmd = 'links -dump %s'%Constants.ATLAS_PARTIAL_SCAN_TRIGGER_PAGE
    elif scan_type == Constants.ATLAS_SELECTIVE_SCAN:
      cmd = 'links -dump %s'%Constants.ATLAS_SELECTIVE_SCAN_TRIGGER_PAGE
    pod = self._allpods[self._atlas_master["name"]]
    INFO("Initiating %s scan, cmd : %s on %s"%(scan_type, cmd, pod.name))
    res = pod.exec_cmd(cmd)
    sleep(10)
    is_scan_running = self._is_atlas_scan_running()
    if not is_scan_running:
      raise Exception("%s scan is invoked successfully but no scan is found on"
                      " atlas page"%cmd)
    if is_scan_running.lower() != "%s scan"%scan_type:
      ERROR("%s scan is invoked successfully but %s running scan "
                      "found on ATLAS page"%(scan_type, is_scan_running))
    if not wait_for_completion:
      return "%s scan"%scan_type
    self._wait_for_scan_to_finish(timeout=timeout, scan_type=scan_type)
    if self.is_master_changed():
      raise Exception("ATLAS Master changed during or after %s scan"%scan_type)

  def initiate_partial_scan(self, wait_for_completion=True,
                         timeout=Constants.ATLAS_DEFAULT_SCAN_TIMEOUT,
                         wait_for_ongoing_scan_to_finish=True):
    return self.initiate_scan(Constants.ATLAS_PARTIAL_SCAN, wait_for_completion,
                              timeout, wait_for_ongoing_scan_to_finish)

  def initiate_full_scan(self, wait_for_completion=True,
                         timeout=Constants.ATLAS_DEFAULT_SCAN_TIMEOUT,
                         wait_for_ongoing_scan_to_finish=True):
    return self.initiate_scan(Constants.ATLAS_FULL_SCAN, wait_for_completion,
                              timeout, wait_for_ongoing_scan_to_finish)

  def _get_atlas_master(self):
    timeout = 300
    etime = time()+timeout
    while etime-time() > 0:
      for pod, podinst in self._allpods.iteritems():
        details = self._get_pod_details(podinst)
        if details["is_master"]:
          self._atlas_master = details
          return self._atlas_master
      ERROR("ATLAS Master not found. Retrying in 30s")
      sleep(30)
    raise Exception("Failed to find ATLAS master POD")

  def _get_pod_details(self, pod):
    res = pod.exec_cmd("links -dump http:0:7103".split(" "))
    if not res:
      ERROR("Failed to execute command in %s"%pod.name)
    res = [i.rstrip() for i in res.split("\n")[0:15]]
    timeformat='%Y%m%d-%H:%M:%S-GMT'
    pod_details = {"is_master":False, "name":pod.name}
    for line in res:
      if "Start time" in line:
        pod_start_time = [i.rstrip("|").rstrip() for i in line.split()][-1]
        pod_start_time = "".join(pod_start_time.partition('GMT')[0:2])
        pod_details["pod_start_time"] = datetime.strptime(pod_start_time,
                                                        timeformat)
        continue
      if "Uptime" in line and "Master" not in line:
        pod_details["pod_uptime"]= [i.rstrip("|").strip()
                                    for i in line.split("  ")][-1]
        continue
      if "Atlas master" in line:
        pod_details["is_master"] = True
        continue
      if "Master Start" in line:
        master_start_time = [i.rstrip("|").rstrip() for i in line.split()][-1]
        master_start_time = "".join(master_start_time.partition('GMT')[0:2])
        pod_details["master_start_time"] = datetime.strptime(master_start_time,
                                                           timeformat)
        continue
      if "Master uptime" in line:
        pod_details["master_uptime"] = [i.rstrip("|").strip()
                                      for i in line.split("  ")][-1]
        continue
      if "Incarnation id" in line:
        pod_details["incarnation_id"] = [i.rstrip("|").rstrip()
                                      for i in line.split()][-1]
        continue
    return pod_details

  def _init_atlas_pods(self):
    healthy_pods, unhealthy_pods = self._msp.get_all_pods_for_service(
                            self.service_name, self.namespace)
    if unhealthy_pods:
      ERROR("Found Unhealthy ATLAS PODs : %s"%unhealthy_pods)
    if not healthy_pods:
      ERROR("No healthy ATLAS PODs found")
    self._allpods = {}
    for podname in healthy_pods:
      self._allpods[podname] = POD(self._msp, podname, self.namespace)
    self._init_details = {podname:self._get_pod_details(podinst)
                             for podname, podinst in self._allpods.iteritems()}
    self.get_atlas_master()
    INFO("Current ATLAS Master : %s"%(self._atlas_master))

  def _is_atlas_scan_running(self):
    master = self._allpods[self._atlas_master["name"]]
    res = master.exec_cmd("links -dump http:0:7103")
    start_tracking = False
    for line in res.split("\n"):
      if "Atlas Jobs" in line:
        start_tracking = True
        continue
      if start_tracking:
        if Constants.ATLAS_SCAN_RUNNING_STATUS in line:
          res = [i for i in line.split("|") if "scan" in i.lower()
                 or "selective" in i.lower() or "partial" in i.lower()
                 or "full" in i.lower()]
          if not res:
            ERROR("Could not find scan details in : %s"%line)
            continue
          return res[0].rstrip()
    if not start_tracking:
      ERROR("Failed to get the atlas scan details from %s. Is Master Changed : "
            "%s"%(master.name, self.is_master_changed()))
    return False

  def _wait_for_scan_to_finish(self, timeout, scan_type=None):
    current_scan = self._is_atlas_scan_running()
    if not current_scan:
      DEBUG("No ATLAS scan is running")
      return
    if scan_type and scan_type not in current_scan.lower():
      ERROR("Expecting ATLAS scan %s to be running but found %s"
                      %(scan_type, current_scan))
    INFO("ATLAS is running %s scan. Scan timeout : %s"%(current_scan, timeout))
    stime = datetime.now()
    etime =  time()+timeout
    timespent = 0
    expected_scan = "" if not scan_type else "(Expected : %s)"%scan_type
    while etime-time() > 0:
      current_scan = self._is_atlas_scan_running()
      scan_time = datetime.now()-stime
      if not current_scan:
        INFO("ATLAS scan finished in %s"%(scan_time))
        return
      if timespent%300 == 0:
        DEBUG("ATLAS Scan %s%s is running since %s"
              %(current_scan, expected_scan, scan_time))
      else:
        print("ATLAS Scan %s%s is running since %s"
              %(current_scan, expected_scan, scan_time))
      sleep(30)
      timespent += 30
    raise Exception("Timeout after %s secs, while waiting for ATLAS scan %s%s "
              "to finish."%(datetime.now()-stime, current_scan, expected_scan))

class AOSCluster():
  def __init__(self, peip, adminuser, adminpass, username="nutanix",
               password="nutanix/4u"):
    self._peip = peip
    self._adminuser = adminuser
    self._adminpass = adminpass
    self._user = username
    self._pass = password
    self._utils = Utils()
    self._pe_url = "https://%s:9440"%self._peip

  def get_storage_pool_usage(self, name=None):
    url = "%s/PrismGateway/services/rest/v1/storage_pools/"%self._pe_url
    res = self._utils.execute_rest_api_call(url=url, request_method=get,
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
        print "Failed to get curator master. Err : %s"%err
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
      ip = re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", out)
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
        print "Failed to get VM details : %s, Err : %s"%(vmname, err)
      if "ipAddresses" not in details or not details['ipAddresses'
                                          ] or len(details['ipAddresses']) < 1:
        print "Waiting for %s to get ip. Current Details : %s"%(
                                            vmname, details.get("ipAddresses"))
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
      print "%s : %s"%(vm, res[vm]['ipAddresses'])
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
        print "VM %s alredy exists, binding "%new_vmname
        continue
      vms_to_clone.append(new_vmname)
    if vms_to_clone:
      print "VM to clone : %s"%vms_to_clone
    return vms_to_clone

  def nic_add(self, vmname, network):
    cmd = "vm.nic_create %s network=%s"%(vmname, network)
    return self._acli(cmd)

  def disk_add(self, vmname, vmimage):
    cmd = "vm.disk_create %s clone_from_image=%s"%(vmname, vmimage)
    return self._acli(cmd)

  def get_vm(self, vmname=None):
    url = "https://%s:9440/PrismGateway/services/rest/v1/vms"%self._peip
    res = self._utils.execute_rest_api_call(url=url, request_method=get,
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
    output, errors = Utils.remote_exec(peip, cmd)
    if output is None and errors is None:
      raise Exception("Output and Errors are returned as None. Cmd : %s, "
                      "PE : %s"%(cmd, peip))
    if output: output.rstrip()
    if errors: errors.rstrip()
    return output, errors

  def _acli(self, cmd):
    # TO much hack here. Go REST route
    cmd = "source /etc/profile; /usr/local/nutanix/bin/acli -o json %s"%(cmd)
    out, errors = Utils.remote_exec(self._peip, cmd)
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

class RemoteExecution():
  def __init__(self, clients, logpath, **kwargs):
    self._clients = clients
    self._logpath = logpath
    self._task_manager = DynamicExecution(10, quotas={
                               Constants.REMOTE_POPS_QUOTA:{"num_workers":10}})
    self._lock = RLock()
    self._utils = Utils()
    self._aoscluster = None
    self._time_to_exit = False
    self._is_db_initialized = False
    self._init_db()
    self._kwargs = kwargs

  def _init_db(self):
    if self._is_db_initialized:
      DEBUG("DB is already initialized. Nothing to do here")
      return
    if not self._clients:
      ERROR("Self._clients is empty, nothing to initialise here")
      return
    self._db = {"test_args":{}, "clients":{}}
    default_test_status = Constants.TEST_NOT_STARTED
    if self._kwargs["remote_execution_monitor_only"]:
      WARN("Remote only execution is enabled, test wont be restarted as its "
           "hard to detect if test failed or not-started")
      default_test_status = Constants.TEST_NOT_RUNNING
    for client in self._clients:
      self._db['clients'][client] = {"status":default_test_status,
                                     "starttime":None,
                                     "num_retries_to_start_test":0,
                                     "start_attempt_time":datetime.now(),
                                     "active_stats":None}
    self._is_db_initialized = True

  def _set_exit_marker(self, fatal_msg, raise_exception=True, exception=None):
    if self._time_to_exit:
      return
    self._time_to_exit = True
    ERROR("Setting Exit marker = True, Reason : %s"%fatal_msg)
    if raise_exception:
      if exception:
        raise exception
      raise Exception(fatal_msg)

  def _exit_test(self, workload):
    if not self._time_to_exit:
      return False
    WARN("Workload : %s, Exit is called. Exit Marker Found : %s"
         %(workload.upper(), self._time_to_exit))
    return True

  def _download(self, url, json_format=True):
    response = urlopen(url, timeout=10)
    html = response.read()
    if not json_format:
      return html.strip()
    return loads(html)

  def _download_client_stats(self, clientip, stat_type, json_format):
    url = "http://%s:37000/%s"%(clientip, stat_type)
    memurl = "http://%s:37000/%s"%(clientip, "memory_usage")
    mem_usage = "NotAbleToFetch"
    try:
      mem_usage = self._download(memurl, False)
    except:
      pass
    #print "Getting raw stats from : %s (Mem Usage : %s)"%(url, mem_usage)
    return self._download(url, json_format)

  def _get_test_start_time(self, clientip=None, timeout=1):
    counter = 0
    while counter < timeout:
      try:
        return self._get_test_start_time_from_client(clientip)
      except Exception as err:
        ERROR("Failed to get test start time from %s. Err : %s"%(clientip, err))
      counter += 1
      if timeout > 1: sleep(10)
    raise Exception("Failed to fetch test start time from any client after "
                    "timeout %s sec"%timeout)

  def _get_test_start_time_from_client(self, clientip=None):
    if clientip:
      return float(self._download("http://%s:37000/test_start_time"%(clientip),
                                  False))
    for clientip in self._clients:
      url = "http://%s:37000/test_start_time"%(clientip)
      try:
        return float(self._download(url, False))
      except Exception as err:
        print "Client : %s, Err : %s"%(clientip, err)
    raise Exception("Failed to fetch test start time from any client.")

  def _merge_records(self, d1, d2):
    #d1 = dict(record1)
    #d2 = dict(record2)
    for k, v in d2.iteritems():
      if k not in d1:
        d1[k]=v
        continue
      if isinstance(v, int):
        d1[k] += d2[k]
        continue
      if isinstance(v, str) or isinstance(v, unicode) or v is None:
        continue
      if isinstance(v, dict):
        if isinstance(d1[k], dict):
          self._merge_records(d1[k], d2[k])
          continue
      if isinstance(v, list):
        d1[k].extend(v)
        continue
    return d1

  def log_all_stats(self, interval, debug_stat_interval, ignore_errors):
    test_time = self._get_test_start_time(timeout=600)
    self._ls = Stats(start_time=test_time, debug_stat=True, pid=getpid(),
                        iternum=1, pretty_print_stats=True)
    debug_interval = 1
    while not self._exit_test("log_all_stats"):
      log_debug_stats = False
      if debug_interval > debug_stat_interval:
        log_debug_stats = True
        debug_interval = 1
      self._process_stats(log_debug_stats, ignore_errors)
      sleep(interval)
      debug_interval += interval

  def _get_all_clients(self):
    res = {"active_clients":[], "dead_clients":[], "yet_to_start_clients":[],
           "unknown":[], "corrupt_clients":[]}
    active_clients, dead_clients, yet_to_start_clients, unknown = [], [], [], []
    for client in self._db['clients'].keys():
      if self._db['clients'][client].get("result", {}).get("corruption_found"):
        res["corrupt_clients"].append(client)
      if self._db['clients'][client]["status"] == Constants.TEST_NOT_STARTED:
        res["yet_to_start_clients"].append(client)
        continue
      if self._db['clients'][client]["status"] in [Constants.TEST_STOPPING,
                             Constants.TEST_FAILED, Constants.TEST_NOT_RUNNING]:
        res["dead_clients"].append(client)
        continue
      if self._db['clients'][client]["status"] == Constants.TEST_STARTING:
        res["yet_to_start_clients"].append(client)
        continue
      if self._db['clients'][client]["status"] == Constants.TEST_RUNNING:
        res["active_clients"].append(client)
        continue
      print "Client %s is in \"%s\" state, declaring it as untreacable"%(client,
              self._db['clients'][client])
      res["unknown"].append(client)
    return res

  def _process_stats(self, log_debug_stats, ignore_errors, timeout=300):
    stime = datetime.now()
    for client in self._clients:
      if self._db['clients'][client]["status"] == Constants.TEST_FAILED and \
          self._db['clients'][client].get("result", {}).get("fatal"):
        continue
      self._task_manager.add(self._get_client_stat,
                           quota_type=Constants.REMOTE_POPS_QUOTA,
                           client=client, ignore_errors=ignore_errors)
    print "Will wait for all workers to process client stats"
    self._task_manager.wait_for_all_worker_to_be_free()
    res = self._get_all_clients()
    if len(res["active_clients"]) == 0:
      if len(res["yet_to_start_clients"]) == 0:
        try:
          Utils.write_to_file(dumps(self._db, indent=2, default=str),
                          path.join(self._logpath, "final_db.json"), "w")
        except Exception as err:
          print "ERROR : Failed to dump self._db in final_db.json. Err : %s"%err
        raise Exception("None of the clients are alive. Existing : %s"
                        %self._logpath)
      else:
        DEBUG("No active client found, test is yet to start on %s, retrying"
              %len(res["yet_to_start_clients"]))
        return
    print "*"*200
    INFO("Processing logs (Time Took : %s): \nAlive clients : %s (Total : %s)\n"
         "Dead Clients : %s (Total : %s)\nClients not running test: "
          "%s (Total : %s)\nUntraceable Clients : %s (Total : %s)\n"
          "Corrupt Clients : %s"%(datetime.now()-stime, res["active_clients"],
          len(res["active_clients"]), res["dead_clients"],
          len(res["dead_clients"]), res["yet_to_start_clients"],
          len(res["yet_to_start_clients"]), res["unknown"], len(res["unknown"]),
          res["corrupt_clients"]))
    print "*"*200
    all_stats = self._merge_all_client_stats(res)
    Utils.write_to_file(dumps(all_stats, indent=2, default=str),
                        Constants.STATIC_ALL_STATS_RECONRDS_FILE, "w")
    try:
      self._ls.log_stats(all_stats, False, log_debug_stats)
    except Exception as err:
      ERROR("Failed to process stats for all clients. Err : %s, Stack : %s"
            %(err, format_exception()))
    stime = datetime.now()
    self._get_reason_for_dead_clients()
    if not self._kwargs["remote_execution_monitor_only"]:
      self._restart_tests_on_uninitialized_clients(res["yet_to_start_clients"])
    try:
      Utils.write_to_file(dumps(self._db, indent=2, default=str),
                        path.join(self._logpath, "db.json"), "w")
    except Exception as err:
      print "ERROR : Failed to dump self._db in db.json. Err : %s"%err
    print "="*5, datetime.now()-stime

  def _restart_tests_on_uninitialized_clients(self, clients):
    script_location = self._db["test_args"]["remote_script_location"]
    for client in clients:
      self._task_manager.add(self._reboot_client_and_restart_test,
                           quota_type=Constants.REMOTE_POPS_QUOTA,
                           client=client)

  def _reboot_client_and_restart_test(self, client):
    force_restart_wait = self._kwargs["restart_test_wait_time"]
    script_location = self._db["test_args"]["remote_script_location"]
    is_test_running, out, err = self._is_wl_running(client, script_location)
    prev_time = self._db['clients'][client]["start_attempt_time"]
    #if is_test_running:
    #  if (datetime.now() - self._db['clients'][client]["start_attempt_time"]
    #                                   ).total_seconds() < force_restart_wait:
    #    return
    if (datetime.now()-prev_time).total_seconds() < force_restart_wait:
      return
    if self._db['clients'][client]["num_retries_to_start_test"
                                ] > self._kwargs["num_retries_to_start_test"]:
      debug_print("We tried restarting test in %s for more than 20 times, "
                  "giving up now"%client)
      return
    self._db['clients'][client]["num_retries_to_start_test"] += 1
    self._db['clients'][client]["start_attempt_time"] = datetime.now()
    DEBUG("Restarting Test on Client : %s (Num Restart  :%s).Resetting time "
          "from %s->%s after %s. Is Script running : %s (%s, %s). Restart Test "
          "Wait Time : %s"%(client,
          self._db['clients'][client]["num_retries_to_start_test"],
          prev_time, self._db['clients'][client]["start_attempt_time"],
          datetime.now()-prev_time, is_test_running, out, err,
          force_restart_wait))
    if self._aoscluster:
      vmname = self._db['clients'][client]["vmname"]
      INFO("Resetting client : %s, VM : %s"%(client, vmname))
      self._aoscluster.power_reset(vmname=vmname)
    else:
      rcmd = 'nohup reboot -f > /dev/null 2>&1 &'
      INFO("Rebooting client : %s, cmd : %s"%(client, rcmd))
      Utils.remote_exec(client, rcmd, "root", "nutanix/4u", background=True)
    sleep(60)
    self._is_wl_running(client, script_location)
    nfsmountcmd = self._db["test_args"].get("nfs_mount_for_logs")
    if nfsmountcmd:
      self._mount_and_list_bucket(client, nfsmountcmd)
    testcmd = self._db["test_args"]["test_cmd"][client]
    INFO("Starting workload on %s with cmd : %s"%(client, testcmd))
    self._db['clients'][client]["start_attempt_time"] = datetime.now()
    try:
      Utils.remote_exec(hostname=client, cmd=testcmd, username="root",
                      password="nutanix/4u")
    except Exception as err:
      ERROR("Failed to start test on %s. TestCMD : %s, Err : %s"
            %(client, testcmd, err))
      raise

  def _get_reason_for_dead_clients(self):
    for client in self._db['clients'].keys():
      if self._db['clients'][client]["status"] != Constants.TEST_NOT_RUNNING:
        continue
      fname = path.join(self._logpath, "records_%s.json"%client)
      logfname = path.join(self._logpath, "objects_load_testlog_%s.log"%client)
      self._task_manager.add(self._get_remote_client_records, client=client,
                               fname=fname, logfname=logfname,
                               quota_type=Constants.REMOTE_POPS_QUOTA)
    print "Will wait for all workers to process dead client logs"
    self._task_manager.wait_for_all_worker_to_be_free()


  def _get_remote_client_records(self, client, fname, logfname):
      if "fatal" in self._db['clients'][client].get("result", {}):
        return
      try:
        self._get_records_from_remote_client(client, fname, logfname)
      except Exception as er:
        print "Failed to get records file from dead client (%s). Err : %s"%(
                                                                     client, er)

  def _get_records_from_remote_client(self, client, filename, logfname):
    rcmd = "cat %s"%Constants.STATIC_RECORDS_FILE
    out, err = Utils.remote_exec(client, rcmd, "root", "nutanix/4u", retry=0)
    if out:
      out = out.strip().rstrip()
    if not out:
      print "ERROR : Failed to read file from %s. Out : %s, Err : %s"%(client,
                                                                      out, err)
      return
    records = {}
    try:
      records = loads(out.strip().rstrip())
    except Exception as err:
      print "Error while loading records. Client : %s, Err : %s"%(client, err)
      debug_print("Trying to use last fetched stats from DB for client : %s"
                  %client)
      records = self._db['clients'][client]["active_stats"]
    if not records or  "first_fatal" not in records["TestStatus"]:
      return
    self._copy_clients_records_logs(client, records, filename, logfname)

  def _copy_clients_records_logs(self, client, records, filename, logfname):
    Utils.write_to_file(dumps(records, indent=2, default=str), filename, "w")
    data = "%s\nTime : %s, Client : %s\n%s\n%s\n\n\n"%("*"*10, datetime.now(),
                          client, "*"*10, records["TestStatus"]["first_fatal"])
    Utils.write_to_file(data, Constants.STATIC_ALL_CLIENTS_RECORDS_FILE, "a")
    Utils.write_to_file(data, path.join(self._logpath, "clients.err"), "a")
    err, remote_log_file = None, None
    try:
      remote_log_file = records["TestStatus"]["logs"]["logfile"]
      self._utils.copy_remote_file_to_local(client, remote_log_file, logfname)
    except Exception as err:
      debug_print("ERROR : Failed to copy file from %s. File : %s. Err : %s"
                  %(client, remote_log_file, err))
    ERROR("Client : %s appears to be Dead. Stats dumped in :%s, Logfile Copied "
         "to : %s Reason : %s" %(client, filename,
       logfname if not err else str(err), records["TestStatus"]["first_fatal"]))
    cf = "corruption" in str(records["TestStatus"]["first_fatal"]).lower()
    self._db['clients'][client]["result"] = {"records":filename,
                                  "fatal":records["TestStatus"]["first_fatal"],
                                  "endtime":datetime.now(), "logfile":logfname,
                                  "corruption_found": cf}
    self._db['clients'][client]["status"] = Constants.TEST_FAILED

  def _merge_all_client_stats(self, client_stats):
    alive_clients = client_stats["active_clients"]
    if not self._kwargs["skip_dead_clients_stats"]:
      debug_print("Including dead clients for stats processing")
      alive_clients += client_stats["dead_clients"
                       ] + client_stats["corrupt_clients"
                       ] + client_stats["unknown"]
    final_records = {}
    for client in alive_clients:
      stats = self._db['clients'][client].get("active_stats")
      if not stats: continue
      final_records = self._merge_records(final_records, stats)
    return final_records

  def _get_client_stat(self, client, ignore_errors, get_stats_only=False):
    res = None
    try:
      res = self._download_client_stats(client, "rstats", True)
    except Exception as err:
      debug_print("Not able to fetch stats from client : %s. Err : %s"
                  %(client, err))
      if not ignore_errors:
        raise
    if get_stats_only:
      return res
    if not res:
      if self._db['clients'][client]["status"] in [Constants.TEST_NOT_STARTED,
                                                   Constants.TEST_STARTING]:
        debug_print("%s : Test doesnt seems to have started"%client)
        return
      if self._db['clients'][client]["status"] != Constants.TEST_NOT_RUNNING:
        WARN("%s : Failed to download stats. Declaring it dead. Changing status"
             "from %s -> %s"%(client, self._db['clients'][client]["status"],
                              Constants.TEST_NOT_RUNNING))
        with self._lock:
          self._db['clients'][client]["status"] = Constants.TEST_NOT_RUNNING
      return
    self._db['clients'][client]["active_stats"] = res
    with open(path.join(self._logpath, "stats_%s.json"%client), "w") as fh:
      fh.write(dumps(res, indent=2, sort_keys=True, default=str))
    if self._db['clients'][client]["status"] != Constants.TEST_RUNNING:
      DEBUG("Client %s, seems alive but current status is : %s. Updating DB."
            %(client, self._db['clients'][client]["status"]))
      self._db['clients'][client]["status"] = Constants.TEST_RUNNING
      if not self._db['clients'][client].get("starttime"):
        self._db['clients'][client]["starttime"] = datetime.now()

  def _is_test_running(self, clients=None, check_workloads=False):
    if not clients:
      clients = self._clients
    for client in clients:
      WARN("Checking Test Status : %s"%(client))
      try:
        res = self._get_test_start_time(client)
        WARN("Test is running in : %s, since  (%s)"%(client, res))
        if not check_workloads:
          return True
        if self._check_workload_on_remote_client(client):
          return True
      except Exception as err:
        WARN("Test is not running on : %s (%s)"%(client, err))
    return False

  def _check_workload_on_remote_client(self, client, workloads_to_check=None):
    if workloads_to_check is None:
      workloads_to_check = ["nfsrename", "nfscopy", "nfsoverwrite", "nfswrite"]
    DEBUG("Checking if %s is running any of workload from %s"
          %(client, workloads_to_check))
    try:
      res = self._get_client_stat(client, True, True)
    except Exception as err:
      ERROR("Failed to check active workload from %s. Err : %s"%(client, err))
      return True
    try:
      live_workloads = res["TestStatus"]["live_workloads"]
    except Exception as err:
      ERROR("Failed to check active workload from %s. Err : %s"%(client, err))
      return True
    if not live_workloads:
      DEBUG("%s is not running any workload. Test Status : %s"%(client, res["TestStatus"]))
      return False
    for workload, num_workers in live_workloads.iteritems():
      if workload in workloads_to_check:
        DEBUG("%s is still running %s workload"%(client, workload))
        return True
    INFO("Client : %s, is alive but not running any NFS write workload : %s"
          %(client, live_workloads))
    return False

  def _stop_test(self, wait_for_cleanup=True, timeout=2400, ignore_errors=True):
    reboot_all_clients = self._kwargs["reboot_all_clients_before_test"]
    if not self._kwargs["skip_graceful_shutdown_of_test"]:
      for client in self._clients:
          self._task_manager.add(self._stop_test_on_client, client=client,
              ignore_errors=ignore_errors, quota_type=Constants.REMOTE_POPS_QUOTA)
      self._task_manager.wait_for_all_worker_to_be_free()
      if not wait_for_cleanup:
        DEBUG("Test exit is initiated, but wait_for_cleanup is False")
        return
      etime = timeout+time()
      is_workload_stopped = False
      while (etime - time()) > 0 :
        if not self._is_test_running(
                        check_workloads=True if reboot_all_clients else False):
          DEBUG("Workload is stopped on all clients")
          is_workload_stopped = True
          break
        INFO("Waiting for all clients to stop test. Time elapsed : %s secs"
             %((time()+timeout)-etime))
        sleep(30)
      if not is_workload_stopped:
        raise Exception("Hit timeout after %s secs, while waiting for workload"
                        " to stop"%timeout)
    else:
        WARN("Graceful shutdown is disabled. We will reset/reboot clients now")
    if reboot_all_clients:
      self.reboot_all_clients()
    self.is_workload_running(
      self._kwargs["local_script_location"].split("/")[-1], debug=True)

  def _stop_test_on_client(self, client, ignore_errors):
    if not self._is_test_running([client]):
      return
    url = "http://%s:37000/exit_test"%(client)
    INFO("Stopping test on %s, URL : %s"%(client, url))
    try:
      self._download(url, False)
    except Exception as err:
      ERROR("Failed to stop test on %s. Err : %s"%(client, err))
      if ignore_errors:
        return
      raise

  def _update_worker_packages(self, files_to_copy, remote_test_location):
    INFO("Copying %s to all clients"%(files_to_copy))
    for client in self._clients:
      for filename in files_to_copy:
        self._task_manager.add(self._utils.copy_local_file_to_remote,
                remoteip=client, localfile=filename,
                remotefile=remote_test_location+"/"+filename.split("/")[-1],
                user="root", password="nutanix/4u",
                quota_type=Constants.REMOTE_POPS_QUOTA)
    self._task_manager.wait_for_all_worker_to_be_free()
    INFO("All workers are updated with latest framework package")

  def _remote_test_execution(self, remote_script_location,
                           cfgfile_for_remote_execution, remote_cmdline_args,
                           use_unique_bucket_prefix, ntp_server):
    cmd = 'ntpdate -s %s; nohup python %s --cfgfile %s'%(ntp_server,
                remote_script_location, cfgfile_for_remote_execution)
    self._db["test_args"]["test_cmd"] = {}
    counter = 0
    for client in self._clients:
      bucket_prefix = "" if not use_unique_bucket_prefix else \
                "--bucket_prefix %s%s"%(use_unique_bucket_prefix, counter)
      counter += 1
      Utils.remote_exec(client, "sudo service firewalld stop", "root",
                        "nutanix/4u", background=True)
      rcmd = '%s %s %s  > /dev/null 2>&1 &'%(cmd, bucket_prefix,
                             remote_cmdline_args if remote_cmdline_args else "")
      self._db["test_args"]["test_cmd"][client] = rcmd
      INFO("Starting test on %s with cmd : %s"%(client, rcmd))
      self._task_manager.add(Utils.remote_exec, hostname=client,
                         cmd=rcmd, username="root", password="nutanix/4u",
                         quota_type=Constants.REMOTE_POPS_QUOTA)
      #Utils.remote_exec(client, rcmd, "root", "nutanix/4u", background=False)
      self._db['clients'][client].update({"status" : Constants.TEST_STARTING,
                                          "start_attempt_time":datetime.now()})
      self._db['clients'][client]["num_retries_to_start_test"] += 1
    debug_print("Will wait for all workers to start test on clients")
    self._task_manager.wait_for_all_worker_to_be_free()

  def reboot_all_clients(self, vmprefix=None):
    if not vmprefix: vmprefix = self._kwargs["remote_clients_vmname"]
    if self._aoscluster:
      INFO("Resetting all clients from prism : %s"%self._aoscluster._peip)
      self._aoscluster.power_reset(vmprefix)
    else:
      for client in self._clients:
        rcmd = 'nohup reboot -f > /dev/null 2>&1 &'
        INFO("Rebooting client : %s"%(client))
        Utils.remote_exec(client, rcmd, "root", "nutanix/4u", background=True)
    DEBUG("Wait for 60s post reboot")
    sleep(60)

  def is_workload_running(self, remote_script_location, debug=False):
    for client in self._clients:
      self._task_manager.add(self._is_wl_running, client=client,
                  script_location=remote_script_location, debug=debug,
                  quota_type=Constants.REMOTE_POPS_QUOTA)
    debug_print("Will wait for all workers get workload status from clients")
    self._task_manager.wait_for_all_worker_to_be_free()

  def _is_wl_running(self, client, script_location, debug=False):
    rcmd = "ps aux| grep %s | grep -v grep"%(script_location.split("/")[-1])
    out, err = Utils.remote_exec(client, rcmd, "root", "nutanix/4u")
    if out or err:
      if debug:
        ERROR("Workload is still running in %s, Out : %s"%((out, err)))
      return True, out, err
    #INFO("Is workload running  %s : %s"%(client, (out, err)))
    return False, out, err

  def mount_all_buckets(self, nfs_mount_for_logs):
    if not nfs_mount_for_logs:
      self._db["test_args"]["nfs_mount_for_logs"] = None
      return
    cmd = "mount -o rw,vers=3,proto=tcp,sec=sys,nfsvers=3 "
    cmd += "%s /root/auto/logs"%nfs_mount_for_logs
    self._db["test_args"]["nfs_mount_for_logs"] = cmd
    for client in self._clients:
      self._task_manager.add(self._mount_and_list_bucket, client=client,
                             cmd=cmd, quota_type=Constants.REMOTE_POPS_QUOTA)
    debug_print("Will wait for all workers to mount logs share on all clients")
    self._task_manager.wait_for_all_worker_to_be_free()

  def _mount_and_list_bucket(self, client, cmd):
    INFO("Mounting logs bucket on : %s, Cmd : %s"%(client, cmd))
    try:
      res = Utils.remote_exec(client, cmd, "root", "nutanix/4u",
                              ignore_errors=["is busy or already mounted"])
    except Exception as err:
      self._set_exit_marker("Failed to mount logs bucket on %s. Cmd : %s, Err "
                ": %s"%(client, cmd, err), raise_exception=True, exception=err)
    INFO("Logs Bucket Mount Res : %s on Client : %s"%(res, client))
    print client, " = ", Utils.remote_exec(client, "df -h /root/auto/logs",
                                           "root", "nutanix/4u")

  def deploy_remote_clients(self):
    if self._clients:
      self._init_db()
      return self._clients
    INFO("Deploying %s clients for test"%self._kwargs["num_remote_clients"])
    if not self._aoscluster:
      self._aoscluster = AOSCluster(self._kwargs["remote_clients_peip"],
                              self._kwargs["remote_clients_pe_admin_user"],
                              self._kwargs["remote_clients_pe_admin_password"])
    clients = self._aoscluster.create_vm_from_image(
                self._kwargs["remote_clients_vmname"],
                self._kwargs["remote_clients_vmimage"],
                self._kwargs["remote_clients_vmimage_url"],
                self._kwargs["remote_clients_storage_ctr"],
                self._kwargs["remote_clients_network_name"],
                self._kwargs["num_remote_clients"],
                self._kwargs["remote_clients_memory"],
                self._kwargs["remote_clients_vcpu"],
                self._kwargs["remote_clients_cores_per_vcpu"],
                self._kwargs["remote_clients_network_vlanid"])
    self._clients = [details['ip'] for vm, details in clients.iteritems()]
    self._init_db()
    for vm, details in clients.iteritems():
      self._db['clients'][details['ip']]["vmname"] = vm

  def start_test_on_remote_client(self):
    local_script_location = self._kwargs["local_script_location"]
    cfgfile_for_remote_execution = self._kwargs["cfgfile_for_remote_execution"]
    remote_test_location = self._kwargs["remote_test_location"]
    remote_script_location = path.join(remote_test_location,
                                      local_script_location.split("/")[-1])
    self._db["test_args"]["remote_script_location"] = remote_script_location
    self._stop_test()
    if self._kwargs["use_nfs_mount_for_logs"]:
      self.mount_all_buckets(self._kwargs["nfs_mount_for_logs"])
    if self._is_test_running():
      if not self._kwargs["restart_test_if_required"]:
        raise Exception("Test is already runing on one or more clients. "
                        "Restart is disabled")
    self._update_worker_packages([local_script_location,
                            cfgfile_for_remote_execution], remote_test_location)
    remote_cfgfile_location = path.join(remote_test_location,
                                    cfgfile_for_remote_execution.split("/")[-1])
    self._remote_test_execution(remote_script_location, remote_cfgfile_location,
        self._kwargs["remote_cmdline_args"],
        use_unique_bucket_prefix=self._kwargs["use_unique_bucket_prefix"],
        ntp_server=self._kwargs["ntp_server"])
    debug_print("Sleeping for 60 seconds")
    sleep(60)

  def run(self):
    self._exit_on_cntr_c()
    ignore_errors = True
    self.deploy_remote_clients()
    if self._kwargs["cfgfile_for_remote_execution"] and not self._kwargs[
                                              'remote_execution_monitor_only']:
      self.start_test_on_remote_client()
    return self.log_all_stats(self._kwargs["remote_stats_monitor_interval"],
                              120, ignore_errors)

  def _exit_on_cntr_c(self):
    def signal_handler(sig, frame):
      if self._time_to_exit:
        if not self._is_nfswrite_workload_running():
          pid=getpid()
          WARN('You pressed Ctrl+C again!, FORCE KILLING PID : %s'%pid)
          kill(pid, 9)
        else:
          WARN("Can not force kill since NFS write workload is running")
      WARN('You pressed Ctrl+C!, exiting workload')
      self._set_exit_marker('You pressed Ctrl+C!, exiting workload', False)
      if self._task_manager:
        self._task_manager._time_to_exit = True
      exit("Exiting from sigint.....")
    signal(SIGINT, signal_handler)

class WorkloadRunner():
  def __init__(self):
    self.rdm = None
    self._utils = Utils()

  def arg_parser(self):
    parser = ArgumentParser(description="Generate various S3 workload")

    #Boto related args
    parser.add_argument('--endpoint_url', required=False, help="S3Endpoint URL")
    parser.add_argument('--access_key', required=False, help="S3 Access key")
    parser.add_argument('--secret_key', required=False, help="S3 Secret key")
    parser.add_argument('--read_timeout', required=False, type=int,
                        help="Boto3 read_timeout. Default=60", default=60)
    parser.add_argument('--enable_boto_debug_logging', required=False, help="To"
                        " enable boto debug logging. Default=False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--addressing_style", required=False, type=str,
                        help="Bucket style access. Default:virtual",
                        default="virtual")

    #API Execution related args
    parser.add_argument("--retry_count", required=False, type=int,
                        help="Num of retries for object put", default=3)
    parser.add_argument("--retry_delay", required=False, type=int, help="Delay "
                        "between each retry. Default:10s", default=60)
    op_retry_format = {"opname":{"retries":10,
                                 "condition":"wait_for_bucket_list_compaction"}}
    parser.add_argument("--op_retries", required=False, type=str, nargs="*",
                        help="Delay  between each retry for specific op. "
                        "Default:None. Expected format : %s"%op_retry_format,
                        default=None)

    #Test specific args
    parser.add_argument("--cfgfile", required=False, help="Config file to load "
                        "params from. Default:None", default=None)
    parser.add_argument("--parallel_threads", required=False, type=int,
                        help="Parallel operations. Default:10", default=10)
    parser.add_argument("--runtime", required=False, type=int, help="How long"
                        " to run the workload. Default:Nolimit", default=-1)
    parser.add_argument("--log_path", required=False, help="Logpath."
                        "Default:<cwd>/logs", default="logs")
    parser.add_argument("--log_path_symlink", required=False, help="Logpath "
                        "symlink. Default:test.log", default="test.log")
    parser.add_argument("--ignore_errors", required=False, help="Errors to "
                        "ignore seperated by comma. Default:None", default="")
    parser.add_argument("--force_kill", required=False, help="Force kill test "
                        "without waiting on stop_workload call.Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--force_ignore_errors", required=False, help="Force "
                        "ignore all the errors. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--debug_stats", required=False,
                        help="Log more test stats. Default:true",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--debug_stats_interval", required=False, type=int,
                      help="Interval for debug stats. Default:120", default=300)
    parser.add_argument("--errors_to_dump_stack_strace", required=False,
                        type=str, nargs="+", help="When these errors are seen "
                        "dump entire stack trace in logs. Useful for debugging"
                        "Default:[]", default=[])
    parser.add_argument("--v", required=False, type=int, help="Verbosity level. "
                        "Default:2, 0/1 for INFO, 2 for DEBUG, 3 VERBOSE, 4 "
                        "EXTRA-VERBOSE", default=2)
    parser.add_argument("--rotate_logs", required=False,
                        type=self._utils.convert_bool,
                        help="Rotate logs. Default:100MB, BackupFiles : 10 "
                        "Default : False", default=False)
    parser.add_argument("--stats_interval", required=False, type=int, help=""
                        "Interval to print test stats. Default:10", default=30)
    parser.add_argument("--enable_memory_leak_debugging", required=False,
                        type=self._utils.convert_bool,
                        help="Print memory usage, useful in"
                        " debugging memory leaks. Default:False", default=False)
    parser.add_argument("--track_errors", required=False,
                        help="Track Errors. Default:True",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--storage_limit", required=False, type=int,
                        help="Limit storage for IOs.Default:-1", default=-1)
    parser.add_argument("--enable_auto_delete_on_storage_full", required=False,
                        help="TODO : Delete all data when storage limit is "
                        "reached", nargs="?", type=self._utils.convert_bool,
                        default=False)
    parser.add_argument("--restart_zk", required=False, type=int, help="Restart"
                        " ZK during deployment.Default:0",default="0")
    parser.add_argument("--do_not_start_workload", required=False, help="Do not"
                        " start workload. Just return upload obj.Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--local_run", required=False, help="Notify if its "
                        "local or remote. Default:True", nargs="?",
                        type=self._utils.convert_bool, default=True)
    parser.add_argument("--thread_monitoring_interval", required=False,
                        type=int, help="Monitor each thread. Default:Disabled",
                        default=-1)
    parser.add_argument("--skip_cert_validation", required=False, help="Skip "
                        "cert validation during i/o test.Default:false",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--rss_limit", required=False, type=int,
                        help="RSS limit. Default:5G", default=5*1024*1024*1024)
    parser.add_argument("--num_iterations", required=False, type=int,
                        help="num_iterations. Default:1", default=1)
    parser.add_argument("--ntp_server", required=False, type=str, help="NTP "
                        "Server. Default:10.40.64.15", default="10.40.64.15")
    parser.add_argument("--reboot_all_clients_before_test", required=False,
                        type=self._utils.convert_bool,
                        help="Reboot clients before test", default=True)
    parser.add_argument("--use_nfs_mount_for_logs", required=False,
                        type=self._utils.convert_bool,
                        help="Use nfs bucket for logs.True", default=True)
    logs_share = "goldfinger.buckets.nutanix.com:/test-logs"
    parser.add_argument("--nfs_mount_for_logs", required=False, type=str,
                        help="nfs share for logs. Default:%s"%logs_share,
                        default=logs_share)
    parser.add_argument("--preserve_db_across_iterations", required=False,
                        type=self._utils.convert_bool,
                        help="Preserve DB across test iters.", default=True)

    #Data related args
    parser.add_argument("--static_data", required=False,
                        help="Whether to use static data or not",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--compressible", required=False, help="Whether to "
                        "generate compressible data or not",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--zero_data", required=False, help="Whether to "
                        "generate zero data or not",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_integrity_check", required=False, help="Skip "
                        "integrity during GET. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--zero_byte_obj_index", required=False, type=int,
                        help="At every 250 index, zero byte obj. Default:250",
                        default=250)

    #Actual workload related args
    parser.add_argument("--workload", required=False,  type=str, nargs="+",
                        help="Workload to run. Supported Values : list|"
                        "write|read|delete|write,read|write,read,delete|read,"
                        "delete|write,delete|write:80,read:20|write:40,read:40,"
                        "delete:20. Default:write", default=["write,read"])
    parser.add_argument("--is_delete_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_read_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_copy_workload_running", required=False,
                        help="is_copy_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_write_workload_running", required=False,
                        help="is_write_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfsread_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfswrite_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfsrename_workload_running", required=False,
                        help="is_nfsrename_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfscopy_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfsdelete_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfslist_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfs_overwrite_workload_running", required=False,
                        help="is_nfs_overwrite_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfsvdbench_workload_running", required=False,
                        help="is_nfsvdbench_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_nfsfio_workload_running", required=False,
                        help="is_nfsfio_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--is_lookup_workload_running", required=False,
                        help="is_lookup_workload_running. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)

    #Auto workload profiler related args
    parser.add_argument("--enable_auto_workload", required=False,
                        help="Enable workload monitoring and change it on "
                        "runtime. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--enable_static_workload_change", required=False,
                        help="Skip failure check and change workload profile"
                        " after stability duration. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--select_random_workload", required=False,
                        help="Select workload randomly else sequentially. "
                        "Default:True", nargs="?",type=self._utils.convert_bool,
                        default=True)
    parser.add_argument("--auto_workload_stability_duration", required=False,
                        help="Change workload if its stable for specified "
                        "duration. Default:48hr in seconds", type=int,
                        default=48*3600)
    parser.add_argument("--num_failure_for_workload_change", required=False,
                        help="Change workload if hit specified failures "
                        "in auto_workload_stability_duration "
                        "Default:600", type=int, default=600)
    parser.add_argument("--failure_types_workload_change", required=False,
                        help="Change workload if hit specified failures "
                        "in auto_workload_stability_duration "
                        "Default:None", type=str, default=None)
    parser.add_argument("--auto_delete_objects_threshold", required=False,
                        help="Pause all other workloads and delete all objects"
                        " worth 75% of used storage by test. Default: 75",
                        type=int, default=75)
    parser.add_argument("--atlas_scan_timeout", required=False,
                        help="Atlas scan timeout. Default:24*3600", type=int,
                        default=Constants.ATLAS_DEFAULT_SCAN_TIMEOUT)
    parser.add_argument("--curator_scan_timeout", required=False,
                        help="Curator scan timeout. Default:24*3600", type=int,
                        default=Constants.ATLAS_DEFAULT_SCAN_TIMEOUT)

    #Bucket related args
    parser.add_argument("--bucket", required=False, help="Bucketname",
                        default="")
    parser.add_argument("--enable_versioning", required=False,
                        help="Enable Versioning. Default : random", nargs="?",
                        type=self._utils.convert_random_bool, default="random")
    parser.add_argument("--num_buckets", required=False, type=int,
                        help="Num buckets to work on. Default:1", default=1)
    parser.add_argument("--bucket_prefix", required=False, help="Bucket prefix."
                        " Default:bucketsworkload", default="bucketworkload")
    parser.add_argument("--skip_bucket_deletion", required=False, help="Skip "
                        "bucket deletion. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--skip_bucket_creation", required=False, help="Skip "
                        "bucket creation. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--unique_bucket_for_delete", required=False,
                        help="Choose unique bucket for delete for every delete "
                        "objects call. Default:True",
                        nargs="?", type=self._utils.convert_bool, default=True)

    #prefix, expiry, version_expiry, abort_multipart, notification
    notification_format = {"syslog_endpoint":"endpoint1,endpoint2,endpoint3",
                           "nats_endpoint":"endpoint1,endpoint2,endpoint3",
                           "kafka_endpoint":"endpoint1,endpoint2,endpoint3"}
    parser.add_argument("--notification_endpoint", required=False,
                        help="Configure notification on bucket. Expected format"
                        " : %s. Default : None"%notification_format,
                        default=None)
    parser.add_argument("--bucket_notification_types", required=False,
                        help="Configure notification on bucket. Expected format"
                        " : [kafka syslog nats]. Default : None",
                        nargs="+", default=None)
    parser.add_argument("--skip_notification_endpoint_config", required=False,
                        help="Skip configuring notification endpoint.t",
                        type=self._utils.convert_bool, default=False)
    parser.add_argument("--set_expiry", required=False,
                        help="Set expiry on bucket.Default : false",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--expiry_days", required=False, type=int,
                        help="Expiry days. Default : 1", default=1)
    parser.add_argument("--version_expiry_days", required=False, type=int,
                        help="Non-versioned expiry days. Default:1", default=1)
    parser.add_argument("--multipart_expiry_days", required=False, type=int,
                        help="Abort multiparts days. Default : 1", default=1)
    parser.add_argument("--expiry_prefix", required=False,
                        help="Expiry prefix. Default : all", default="expiry")

    #Tagging related args
    parser.add_argument("--enable_tagging", required=False,
                        help="Enable tagging. Default : True",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--tag_expiry_prefix", required=False, help="Tag expiry"
                        " prefix. Default : tag_expiry",default="tag_expiry")
    parser.add_argument("--tag_expiry_days", required=False, type=int,
                        help="Tag Expiry days. Default : 1", default=1)

    #Tiering related args
    parser.add_argument("--enable_tiering", required=False,
                        help="Enable Tiering. Default : False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--tiering_prefix", required=False,
                        help="tiering_prefix. Default : tier", default="tier")
    parser.add_argument("--tiering_days", required=False, type=int,
                        help="tiering_days. Default : 1", default=1)
    parser.add_argument("--tiering_expiry_days", required=False, type=int,
                        help="Expire objs which are tired. Default : 2",
                        default=2)
    parser.add_argument("--tiering_version_expiry_days", required=False,
                        type=int, help="Expire versions which are tiered. "
                        "Default : 2", default=2)
    parser.add_argument("--tiering_multipart_expiry_days", required=False,
                        type=int, help="Abort multipart unfinalized uploads "
                        "which could have been tiered. Default : 1", default=1)
    parser.add_argument("--tiering_endpoint", required=False,
                        help="tiering_endpoint. Default : None", default="")
    tef = {"endpoint_name":"xxx", "endpoint_ip":"xxx", "bucket_name":"xxx",
           "access_key":"xxx", "secret_key":"xxx"}
    parser.add_argument("--tiering_endpoint_details", required=False,
                        help="Endpoint details. eg : %s. Default : None"%(tef),
                        default=None)


    #Replication related args
    replc = {"bucket":"xxx", "endpoint_list":"xxx", "oss_fqdn":"xxx",
             "oss_uuid":"xxx"}
    parser.add_argument("--repl_cluster", required=False, help="Destination "
                  "replication cluster details. eg : %s. Default:None"%(replc),
                        default=None)
    parser.add_argument("--create_new_bucket_for_replication", required=False,
                       help="Create new bucket on destination cluster for every"
                        "replication relationship. Default:True",
                        nargs="?", type=self._utils.convert_bool, default=True)

    #Object related args
    parser.add_argument("--objsize", required=False, type=str, nargs="+",
                      help="Object size. Default:1m 2m", default=["1mb", "2mb"])
    parser.add_argument("--select_random_objsize", required=False, nargs="?",
                        help="Randomize objsize. Default:True",
                        type=self._utils.convert_bool, default=True)
    parser.add_argument("--objprefix", required=False,
                        help="Objname_prefix. Default:key", default="keyreg")
    parser.add_argument("--num_versions", required=False, nargs="+",
                        help="num_versions. Default:1", default=[1,1])
    parser.add_argument("--use_hexdigits_keynames", required=False,
                        help="use_hexdigits_keynames. Default:true",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--use_unsafe_hex_keynames", required=False,
                        help="use_unsafe_hex_keynames. Default:false",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--delimeter_obj", required=False, help="Upload "
                        "delimeter object.Default:False",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--dir_depth", required=False, type=int, nargs="+",
                        help="Create dir depth structure.Default:15 20",
                        default=[1, 10])
    parser.add_argument("--num_objects", required=False, type=int,
                        help="Number of objects to create in given bucket. "
                        "Default:unlimited", default=-1)
    parser.add_argument("--upload_type", required=False, help="Use multipart/"
                        "standard PUT. Default:standard,multipart"
                        "Accepted Values : standard or multipart or both",
                        default="standard,multipart")
    parser.add_argument("--num_parts", required=False, type=int, nargs="+",
                        help="Num parts to upload for multipart PUT. "
                        "Default:[10, 15]",default=[10, 15])
    parser.add_argument("--num_parallel_multiparts", required=False, type=int,
                         help="num_parallel_multiparts. Default:2", default=2)
    parser.add_argument("--skip_abort_multipart_upload", required=False,
                         help="skip_abort_multipart_upload. Default:False",
                         nargs="?", type=self._utils.convert_bool,default=False)
    parser.add_argument("--skip_parts_subblock_integrity_check", required=False,
                         help="Skip parts range integrity check at subblock "
                         "level. Default:False", nargs="?",
                         type=self._utils.convert_bool, default=False)
    parser.add_argument("--num_leaf_objects", required=False, type=int,
                        nargs="+", help="Num of leaf  objects to create in "
                        "given dir in bucket. Default:100", default=[10, 150])
    parser.add_argument("--read_after_copy", required=False, help="Read after"
                        " copy. Default : False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--copy_srcobj_size_limit", type=int,
                        help="Src obj size limit for selecting it for copy."
                        "Default : 300mb", default=300*1024*1024)
    parser.add_argument("--copy_srcobj_refresh_frequency", type=int,
                        help="Copy src object x number of times before picking "
                        "new object for copy. Default : 1000", default=1000)
    parser.add_argument("--read_type", required=False, help="Initiate"
                        "range reads or full read or random."
                        "Default=[range_read, full_type]", nargs="+",
                        type=str, default=[Constants.READ_TYPE_RANGE_READ,
                                           Constants.READ_TYPE_FULL_READ])
    parser.add_argument("--num_reads_from_list", required=False, type=int,
                        nargs="+", help="Num of reads from bucket list."
                        "Default:100 300", default=[100, 300])
    parser.add_argument("--range_read_size", required=False, type=int,
                        nargs="+", help="Size of each range read. Default:"
                        "[1kb, 100kb]", default=["10kb", "100kb"])
    parser.add_argument("--num_range_reads_per_obj", required=False, type=int,
                        help="Num range read per obj. Default:100", default=10)
    parser.add_argument("--skip_version_deletes", required=False, help="Issue "
                        "regular deletes on versioned buckets. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_head_before_delete", required=False,
                        help="Dont do head before delete. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_head_after_delete", required=False,
                        help="Dont do head after delete. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_read_before_delete", required=False,
                        help="Dont read object before delete. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_head_after_put", required=False, help="Dont "
                        "head object after put. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--maxkeys_to_list", required=False, type=int,
                        nargs="+", help="Max keys to list in one call."
                        "Default:[1, 1000]", default=[1, 2])
    parser.add_argument("--validate_list_against_head", required=False,
                        help="validate_list_against_head.Default:false",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_reads_before_date", required=False, help="Skip"
                        " reads on object before specific date. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--date_to_skip_reads", required=False, help="Skip"
                        " reads on object before this date. "\
                        "Default:Sun, 28 Feb 2020 23:59:59",
                        default="Sun, 28 Feb 2020 23:59:59")
    parser.add_argument("--skip_expired_object_read", required=False,
                        help="Skip reads on expired objects. Default:True",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--ignore_all_errors_forcefully", required=False,
                        help="Skip skip all errors. Default:True",
                        nargs="?", type=self._utils.convert_bool, default=True)

    #PC & PE related args
    parser.add_argument("--pcip", required=False, help="PC IP", default=None)
    parser.add_argument("--pcuser", required=False, help="PC User",
                        default="admin")
    parser.add_argument("--pcpass", required=False, help="PC Pass",
                        default="Nutanix.123")
    parser.add_argument("--objects_name", required=False, help="Objects cluster"
                        "  name in PC", default="Objects%s"%(int(time())))
    parser.add_argument("--peip", required=False,
                        help="PE VIP. Default:None", default=None)
    parser.add_argument("--client_network", required=False,
                        help="Client network name.Default:None", default=None)
    parser.add_argument("--internal_network", required=False,
                        help="Internal network name.Default:None", default=None)
    parser.add_argument("--client_ips", required=False,
                        help="Client IPs.Default:None", default=None)
    parser.add_argument("--internal_ips", required=False,
                        help="InternalIPs.Default:None", default=None)

    #Deployment related args
    parser.add_argument("--deploy", required=False,
                        help="Deploy Objects. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--num_loop_deploments", required=False, type=int,
                        help="Deploy Objects in loop. Default:1", default="1")
    parser.add_argument("--loop_deploy_delay", required=False, type=int,
                        help="Sleep in Objects loop deploy. Default:60",
                        default="60")
    parser.add_argument("--domain_name", required=False,
                        help="Domain Name. Default:scalcia.com",
                        default="scalcia.com")
    parser.add_argument("--num_nodes", required=False, type=int, nargs="+",
                        help="Num nodes to deploy.Default:1 3", default=[1, 3])
    parser.add_argument("--storage_capacity", required=False, type=int,
                        help="Storage capacity.Default:1G",default=1)
    parser.add_argument("--sanity_num_buckets", required=False, type=int,
                        help="Num buckets to create in sanity chk", default=40)
    parser.add_argument("--num_objects_per_bucket", required=False, type=int,
                        help="Num objs per bucket in sanity check", default=40)
    parser.add_argument("--cert_type", required=False, help="Cert type to "
                        "replace .Default:self_signed", default="self_signed")
    parser.add_argument("--cert_replacement_delay", required=False, type=int,
                        help="Cert replacement loop delay.Default:30",
                        default=30)
    parser.add_argument("--skip_cert_test", required=False, help="Skip cert "
                        "replacement during deploy loop test",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_sanity_check", required=False, help="Skip "
                        "sanity test during deploy/cert loop test",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_multicluster_check", required=False,
                        help="Skip multicluster test during loop test",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_scaleout_check", required=False, help="Skip "
                        "scaleout test during deploy loop test",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_objects_delete", required=False, help="Skip "
                        "objects delete during deploy/cert loop test",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_objects_deploy", required=False, help="Skip "
                        "objects deploy during loop test",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--num_cert_replace_loop", required=False, type=int,
                        help="Num of cert replace test loop.Default:2",
                        default=3)
    expected_values = ["aoss_service_manager:50", "msp_controller:50"]
    parser.add_argument("--services_to_restart_during_scaleout", required=False,
                        nargs="+", help="Services to restart during scaleout "
                        "operation. Expected_values : %s.  Default:[]"%(
                        expected_values), default=[])
    parser.add_argument("--services_to_restart_during_deploy", required=False,
                        nargs="+", help="Services to restart during deploy "
                        "operation.Expected_values : %s. Default:[]"%(
                        expected_values), default=[])
    parser.add_argument("--services_to_restart_during_replace_cert",
                        required=False, nargs="+", help="Services to restart "
                        "during replace cert operation. Expected_values : %s. "
                        "Default:[]"%(expected_values), default=[])
    parser.add_argument("--services_to_restart_during_delete", required=False,
                        nargs="+", help="Services to restart during objects "
                        "delete operation. Expected_values : %s. Default:[]"%(
                        expected_values), default=[])

    #Error injection related args
    parser.add_argument("--inject_errors", required=False,
                        help="Inject errors.Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--ei_interval", required=False, type=int,
                        help="EI Interval in secs.Default:600", default=10*60)
    parser.add_argument("--pods_to_restart", required=False, help="Services "
                        "or service type to restart. Expected Values : "
                        "cdp/all/object/ms/atlas/ui/zk. Default:cdp",
                        default="cdp")
    parser.add_argument("--eiops", required=False, type=str, nargs="+",
                        help="Operations to perform on  infra."
                        "Default:[off, reset, reboot, pause, shutdown]",
                        default=["off", "reset", "reboot", "shutdown",
                                 "rebootall", "resetall", "stop_stargate",
                                 "restart_stargate", "reboot_cvm",
                                 "stop_cassandra", "restart_cassandra",
                                 "reboot_host", "reboot_all_host",
                                 "reboot_all_cvm"])
    parser.add_argument("--namespace_for_ei", required=False,
                        help="Namespace to restart pod.Default:default",
                        default="default")
    parser.add_argument("--num_pods_to_restart", required=False, type=int,
                        help="Num PODS to restart at the same time. Default:2",
                        default=2)
    parser.add_argument("--pod_ei_timeout", required=False, type=int,
                        help="Wait until POD comes back online. Default:120",
                        default=120)
    parser.add_argument("--component_for_ei", required=False,
                        help="Component type to inject failure.Expected Values "
                        ": node/pod/service/infra_service. Default:pod",
                        default="node,pod,service,infra_service")
    parser.add_argument("--num_services_to_restart", required=False, type=int,
                        help="Num services to exit around same time. Default:2",
                        default=2)
    default_services = ["object-controller", "ms-server", "poseidon-atlas"]
    parser.add_argument("--services_to_restart", required=False, type=str,
                        nargs="+", help="Services to h_exit.Default : %s"%(
                        default_services), default=default_services)
    parser.add_argument("--stargate_storage_gflag", required=False, type=int,
                        help="Reset to stargate storage gflag to given perc."
                        "Default:95", default=95)
    parser.add_argument("--stargate_gflag_switch_interval", required=False,
                      type=int, help="Sleep time between stargate storage gflag"
                        "switch. Default:95", default=30)
    parser.add_argument("--storage_test_interval", required=False, type=int,
                        help="Run stargate storage gflag switch interval."
                        "Default:1800", default=900)
    parser.add_argument("--stargate_test_initial_delay", required=False,
                        type=int, help="Delay before stargate gflag test."
                        "Default:900", default=900)
    parser.add_argument("--simulate_egroup_data_corruption", required=False,
                        help="Delete egroups from Objects data container."
                        "Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)

    #NFS related args
    parser.add_argument("--enable_nfs", required=False, help="Enable NFS access"
                        " in bucket. Default:random", nargs="?",
                        type=self._utils.convert_random_bool, default="random")
    parser.add_argument("--skip_nfs_read_only_integrity_check", required=False,
                        help="Skip integrity check.Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_list_validation_vs_s3", required=False,
                        help="Skip validating against s3 list.Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--skip_functional_check", required=False,
                        help="Skip functional_check.Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--ignore_nfs_metadata_check_errors", required=False,
                        help="ignore_nfs_metadata_check_errors. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--ignore_nfs_size_errs_vs_s3", required=False,
                        help="--ignore_nfs_size_errs_vs_s3. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    nfs_checks = ["rename", "chmod", "chown", "link", "remove", "rmdir",
                  "symlink",  "unlink", "chdir"]
    parser.add_argument("--nfs_functional_checks", required=False, type=str,
                        nargs="+", help="NFS functional_checks. Default:%s"%(
                        nfs_checks), default=nfs_checks)
    parser.add_argument("--nfs_read_type", required=False, help="Issue NFS "
                        "range read or complete reads or choose randomly. "
                        "Default=[nfs_range_read, nfs_full_type]", nargs="+",
                        type=str, default=[Constants.NFS_READ_TYPE_RANGE_READ,
                                           Constants.NFS_READ_TYPE_FULL_READ])
    parser.add_argument("--skip_zero_file_nfs_read", required=False, help="Skip"
                        " zero size file read. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--nfs_range_read_size", required=False, type=int,
                        nargs="+", help="Issue NFS range read",
                        default=["1kb", "100kb"])
    mnt_opts = "ro,relatime,vers=3,proto=tcp,sec=sys,mountport=2749,port=2749,"\
                        "nfsvers=3,hard,rsize=1048576,wsize=1048576,actimeo=0,"\
                        "local_lock=none,retrans=2"
    parser.add_argument("--nfs_mount_options", required=False, help="Mount opts"
                        "Default:%s"%(mnt_opts), default=mnt_opts)
    parser.add_argument("--num_nfs_reads_from_list", required=False, type=int,
                        nargs="+", help="Num of nfs reads from bucket list."
                        "Default:100 300", default=[100, 300])
    parser.add_argument("--num_nfs_range_reads_per_file", required=False,
                        type=int, help="Num NFS range reads. Default:10",
                        default=100)
    parser.add_argument("--nfs_debug_logging", required=False,
                        type=self._utils.convert_bool,
                        help="NFS debug logging for debugging. Default : False",
                        default=False)
    parser.add_argument("--nfs_write_block_size", required=False,
                        type=int, help="nfs_write_block_size. Default:1M",
                        default=1024*1024)
    parser.add_argument("--num_over_writes_per_file", required=False,
                        type=int, help="num_over_writes. Default:5",
                        default=5)
    parser.add_argument("--randomise_sparse_write", required=False,
                        type=self._utils.convert_bool, help="num_over_writes. "
                        "Default:True", default=True)
    parser.add_argument("--sparse_writes_seek_distance", required=False,
                        type=int, nargs="+", help="How apart sparse writes "
                        "should be. Seek to this location and write the data. "
                        "Default:[10*1024*1024, 1024*1024*1024*1024]",
                        default=[10*1024*1024, 10*1024*1024*1024])
    parser.add_argument("--file_layout_type", required=False, nargs="+",
                        type=str, help="num_over_writes. Default:[random]",
                        default=["sparse", "regular"])
    parser.add_argument("--size_of_over_write", required=False,
                        type=int, help="size_of_write. Default:2M",
                        default=2*1024*1024)
    parser.add_argument("--over_write_block_size", required=False,
                        type=int, help="over_write_block_size. Default:1M",
                        default=1024*1024)
    parser.add_argument("--read_after_nfsoverwrite", required=False,
                        type=self._utils.convert_bool,
                        help="read_after_nfsoverwrite.Default:true",
                        default=True)
    parser.add_argument("--sync_data_on_file_close", required=False,
                        type=self._utils.convert_bool,
                        help="Sync data before closing file. Default:true",
                        default=True)
    #Keep it to False to avoid smaller writes to NFS server
    parser.add_argument("--sync_data_on_every_write_to_file", required=False,
                        type=self._utils.convert_bool, help="Sync data "
                        "immdiately after writing to file. Default:False",
                        default=False)
    parser.add_argument("--verbose_logging_for_locks", required=False,
                        type=self._utils.convert_bool, help="Enable versbose "
                        "logging for locking/unblocking objs. Default:False",
                        default=False)
    parser.add_argument("--skip_read_after_write", required=False, help="Dont "
                        "read nfsfile after write. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)

    parser.add_argument("--capture_tcpdump", required=False,
                        help="Capture tcpdump. Default:false",
                        nargs="?", type=self._utils.convert_bool, default=False)
    cmd = "sudo tcpdump -U -C 100 -W 50 -w capture -i any port 2749"
    parser.add_argument("--tcpdump_cmd", required=False,
                        help="Tcpdump cmd. Default:%s"%(cmd), default=cmd)

    #FIO related args
    parser.add_argument("--fio_io_type", required=False, type=str,
                        help="Fio IOType refer --rw option in fio manpage."
                             "Default:randrw", default="randrw")
    parser.add_argument("--fio_write_block_size", required=False, type=int,
                        help="Fio block size refer --bs option in fio manpage"
                             "Default:1M", default=1024*1024)
    parser.add_argument("--fio_runtime", required=False, type=int,
                        help="Fio runtime refer --runtime option in fio manpage"
                             "Default:1800", default=1800)
    parser.add_argument("--fio_iodepth", required=False, type=int,
                        help="Fio iodepth refer --iodepth option in fio manpage"
                             "Default:10", default=10)
    parser.add_argument("--fio_filesize", required=False, type=int,
                        help="Fio file size refer --size option in fio manpage"
                             "Default:-1", default=-1)
    parser.add_argument("--fio_buffered_io", required=False, type=int,
                        help="Fio buffered_io refer --direct opt in fio manpage"
                             "Default:1", default=1)

    #VDBench related args
    parser.add_argument("--vdb_io_type", required=False, type=str,
                        help="Vdb IOType refer --rw option in fio manpage."
                             "Default:randrw", default="randrw")
    parser.add_argument("--vdb_write_block_size", required=False, type=int,
                        help="vdb block size refer --bs option in fio manpage"
                             "Default:1M", default=1024*1024)
    parser.add_argument("--vdb_runtime", required=False, type=int,
                        help="Vdb runtime refer --runtime option in fio manpage"
                             "Default:1800", default=1800)
    parser.add_argument("--vdb_iodepth", required=False, type=int,
                        help="Vdb iodepth refer --iodepth option in fio manpage"
                             "Default:10", default=10)
    parser.add_argument("--vdb_filesize", required=False, type=int,
                        help="Vdb file size refer --size option in fio manpage"
                             "Default:-1", default=-1)
    parser.add_argument("--vdb_buffered_io_flag", required=False, type=str,
                        help="Vdb buffered_io refer openflags in vdb manpage"
                             "Default:o_direct", default="o_direct")
    parser.add_argument("--vdbench_read_perc", required=False, type=int,
                        help="Vdb vdbench_read_perc"
                             "Default:50", default=50)
    parser.add_argument("--vdbench_logs_location", required=False, type=str,
                        help="Vdb vdbench_logs_location"
                             "Default:logpath/vdb", default=None)
    parser.add_argument("--vdbench_binary_location", required=False, type=str,
                        help="Vdb vdbench_binary_location"
                             "Default:/root/vdbench", default="/root/vdbench")

    #S3FS related args
    parser.add_argument("--enable_s3fs_mount", required=False,
                        help="Mount using s3fs. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)

    #RDM Related args
    parser.add_argument("--rdm_deploy", required=False, help="Deploy resources "
                        "via rdm. Default:false",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--rdm_clustername", required=False, help="RDM Cluster "
                        "name. Default:ObjectsCluster",
                        default="ObjectsCluster%s"%(int(time())))
    parser.add_argument("--nos_version", required=False, help="NOS version to "
                        "deploy. Default:", default="euphrates-5.15.4-stable")
    parser.add_argument("--build_type", required=False, help="NOS Build type"
                        "Default:release", default="release")
    parser.add_argument("--hypervisor_type", required=False, help="Hypervisor "
                        " type. Default:ahv", default="ahv")
    parser.add_argument("--hypervisor_version", required=False,
                        help="Hypervisor version. Default:branch_symlink",
                        default="branch_symlink")
    parser.add_argument("--secondary_vcenter", required=False, help="Secondary "
                        "vcenter. Default:10.46.1.215", default="10.46.1.215")
    parser.add_argument("--node_pool_name", required=False, help="Nodepool name"
                        "to deploy. Default:object-store-qa-ronaldo",
                        default="object-store-qa-ronaldo")
    parser.add_argument("--rdm_setup_duration", required=False, type=int,
                        help="Deployment duration in hrs. Default:12",
                        default=48)
    parser.add_argument("--cluster_num_nodes", required=False, type=int,
                        help="Num nodes to deploy Default:1", default=1)
    pc_build_url = "http://endor.dyn.nutanix.com/builds/pc-builds/"
    pc_build_url += "euphrates-5.19-stable-pc-0/latest/x86_64/release/"
    parser.add_argument("--pc_build_url", required=False, help="PC Build URL"
                        "Default:5.19 URL", default=pc_build_url)
    parser.add_argument("--pc_num_nodes", required=False, type=int,
                        help="Num PC Nodes. Default:1", default=1)
    parser.add_argument("--pc_vm_size", required=False, help="PC deployment "
                        "size. Default:small", default="small")
    parser.add_argument("--vlan_id", required=False, type=int, help="vlan ID to"
                        " create network. Default:871", default=871)
    parser.add_argument("--rdm_user", required=False, help="RDM username."
                        "Default:anirudh.sonar", default="anirudh.sonar")
    parser.add_argument("--rdm_pass", required=False, help="RDM password."
                        "Default:None", default=None)
    parser.add_argument("--network_prefix", required=False, help="Network "
                        "prefix. Gateway/CIDN. Default:10.44.192.1/19",
                        default="10.44.192.1/19")
    parser.add_argument("--start_ip", required=False, help="Start IP from "
                        "network. Default:None", default=None)
    parser.add_argument("--end_ip", required=False, help="Eng IP from network"
                        "Default:None", default=None)
    parser.add_argument("--pe_vip", required=False, help="PE VIP to configure "
                        " during deployment. Default:None", default=None)
    parser.add_argument("--pe_dsip", required=False, help="PE VIP to configure "
                        "during deployment. Default:None", default=None)
    parser.add_argument("--release_setup_after_test", required=False,
                        help= "Release cluster deployed via RDM after test."
                        "Default:True", nargs="?", type=self._utils.convert_bool,
                        default=True)
    parser.add_argument("--skip_aos_setup_post_rdm_deployment", required=False,
                        help= "skip_aos_setup_post_rdm_deployment Default:Fals",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--deployments", required=False, type=str, nargs="+",
                        help= "Deployment ids which ongoing. Continue using it. "
                        "Default:None", default=None)
    parser.add_argument("--deployment_id", required=False, help= "Main "
                        "deployment  id which ongoing. Continue using it. "
                        "Default:None", default=None)

    #Upgrade related args
    parser.add_argument("--pc_upgrade_build_url", required=False, help="PC "
                        "upgrade build tarfile url. Default:None", default=None)
    parser.add_argument("--pe_upgrade_build_url", required=False, help="PE "
                        "upgrade build url. Default:None", default=None)
    parser.add_argument("--upgrade_timeout", required=False, type=int,
                        help="Upgrade timeout. Default:3600", default=3600)
    parser.add_argument("--objects_upgrade_version", required=False,
                        help="objects_upgrade_version. Default:None",
                        default=None)
    parser.add_argument("--objects_upgrade_rc_version", required=False,
                        help="objects_upgrade_rc_version. Default:None",
                        default=None)
    parser.add_argument("--upgrade_delay", required=False, type=int,
                        help="upgrade_delay before infra upgrade. Default:3600",
                        default=3600)
    parser.add_argument("--delay_between_upgrade", required=False, type=int,
                        help="delay_between_upgrade. Default:900",
                        default=900)
    parser.add_argument("--read_only_workload_after_upgrade", required=False,
                        type=int, help="read_only_workload_after_upgrade. "
                        "Default:1800", default=1800)
    script_url = "http://mars.corp.nutanix.com/home/kaustubh.gondhalekar/"\
                 "rc_upgrade_utility/rc_upgrade.py"
    parser.add_argument("--objects_upgrade_script_url", required=False,
                      help="objects_upgrade_script_url.Default:%s"%(script_url),
                        default=script_url)
    parser.add_argument("--wait_before_objects_upgrade", required=False,
                        type=int, help="wait_before objects_upgrade. "
                        "Default:3600", default=3600)
    parser.add_argument("--objects_unreleased_version", required=False,
                        help="objects_unreleased_version. Default:None",
                        default=None)
    parser.add_argument("--objects_unreleased_rc_version", required=False,
                        help="objects_unreleased_rc_version. Default:None",
                        default=None)
    parser.add_argument("--manual_upgrade_build_url", required=False,
                        help="manual_upgrade_build_url. Default:None",
                        default=None)
    parser.add_argument("--upgrade_seq", required=False, type=str, nargs="+",
                        help="Upgrade sequence to carry out upgrade. Default: "
                        "[pc, pe, objects_released, objects_unreleased]",
                        default=["pc", "pe", "objects_released",
                                 "objects_unreleased"])

    #MS compaction args
    parser.add_argument("--initiate_periodic_compaction", required=False,
                        help="Initiate periodic compactions. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--compaction_interval", required=False, type=int,
                        help="Interval . Default:900", default=900)


    #vCenter Details
    parser.add_argument("--vcenter_ip", required=False, help="vcenter ip."
                        " Default:None", default=None)
    parser.add_argument("--vcenter_user", required=False, help="vcenter user"
                        " Default:administrator@vsphere.local",
                        default="administrator@vsphere.local")
    parser.add_argument("--vcenter_pwd", required=False, help="vcenter pwd"
                        " Default:Nutanix/4u", default="Nutanix/4u")
    parser.add_argument("--vcenter_port", required=False, help="vcenter port."
                        " Default:443", default=443)
    parser.add_argument("--vcenter_dc", required=False, help="vcenter dc"
                        " Default:None", default=None)
    parser.add_argument("--vcenter_cluster", required=False, help="vcenter "
                        "cluster Default:None", default=None)

    #Webserver Details
    parser.add_argument("--start_webserver_logging",required=False, help="Start"
                        " webserver to monitor runtime progress Default:True",
                        nargs="?", type=self._utils.convert_bool, default=True)
    parser.add_argument("--webserver_logging_port", required=False, help="Web"
                        " Server Port. Default:37000", type=int, default=37000)

    #Remote execution
    parser.add_argument("--remote_execution", required=False, help="Start"
                        " remote execution. Default:False",
                        nargs="?", type=self._utils.convert_bool, default=False)
    parser.add_argument("--restart_test_wait_time", required=False, help=""
                        "Restart test on remote client if its not running for "
                        "secs Default:1800", type=int, default=1800)
    parser.add_argument("--num_retries_to_start_test", required=False, help=""
                        "Number of times to start test if not running "
                        "Default:20", type=int, default=20)
    parser.add_argument("--remote_stats_monitor_interval", required=False,
                        help="Interval (in secs) to log stats. Default:30",
                        type=int, default=30)
    parser.add_argument("--cfgfile_for_remote_execution", required=False,
                        help="Cfgfile for remote remote execution.Default:None",
                        type=str, default=None)
    parser.add_argument("--remote_execution_monitor_only", required=False,
                        help="Cfgfile for remote remote execution.Default:None",
                        type=self._utils.convert_bool, default=False)
    parser.add_argument("--remote_cmdline_args", required=False, help="Cfgfile "
                        "for remote remote execution. Default:None", type=str,
                        default=None)
    parser.add_argument("--use_unique_bucket_prefix", required=False, help=""
                        "User unique bucket per client. Default:None", type=str,
                        default=None)
    parser.add_argument("--remote_test_location", required=False, help="Remote "
                        "location to copy package.Default:/root/auto", type=str,
                        default="/root/auto")
    parser.add_argument("--local_script_location", required=False, help="Local "
                        "filepath for package. Default:Self-script-location",
                        type=str, default=path.realpath(__file__))
    parser.add_argument("--skip_graceful_shutdown_of_test", required=False,
                        help="skip_graceful_shutdown_of_test. "
                        " Default:False", type=self._utils.convert_bool,
                        nargs="?", default=False)
    parser.add_argument("--restart_test_if_required", required=False,
                        help="Restart the test if its already running. "
                        " Default:False", type=self._utils.convert_bool,
                        nargs="?", default=True)
    parser.add_argument("--distributed_clients", required=False, help="Is test "
                        "running across multiple clients.Default:False",
                        type=self._utils.convert_bool, default=False)
    parser.add_argument("--remote_clients", required=False, help="List of "
                        "remote client IPs to starts/monitor remove execution."
                        " Default:[]", type=str, nargs="+", default=[])
    parser.add_argument("--remote_clients_peip", required=False, help="PEIP for"
                        " deploying remote clients. Default:None",
                        type=str, default=None)
    parser.add_argument("--remote_clients_pe_admin_user", required=False,
                        help="Remote PE admin user. Default : admin",
                        type=str, default="admin")
    parser.add_argument("--remote_clients_pe_admin_password", required=False,
                        help="Remote PE admin user pass. Default : Nutanix.123",
                        type=str, default="Nutanix.123")
    parser.add_argument("--remote_clients_storage_ctr", required=False, help=""
                        "Storage ctr for deploying remote clients. Default : "
                        "NutanixManagementShare", type=str,
                        default="NutanixManagementShare")
    parser.add_argument("--remote_clients_network_name", required=False, help=""
                        "Network for deploying remote clients. Default : "
                        "vlan.0", type=str, default="vlan.0")
    parser.add_argument("--remote_clients_network_vlanid", required=False,
                        help="Network vlan id for deploying remote clients."
                        "Default : 0", type=int, default=0)
    parser.add_argument("--num_remote_clients", required=False, help="Num remot"
                        "e clients to deploy. Default : 4", type=int, default=4)
    parser.add_argument("--remote_clients_memory", required=False, help="Memory"
                        " in mb to assign while deploying remote clients. "
                        "Default : 8192", type=int, default=4096)
    parser.add_argument("--remote_clients_vcpu", required=False, help="Num "
                        " vcpus to assign while deploying remote clients. "
                        "Default : 2", type=int, default=2)
    parser.add_argument("--remote_clients_cores_per_vcpu", required=False,
                        help="Num cores per vcpu to assign while deploying "
                        "remote clients. Default : 2", type=int, default=2)
    vmimage = "http://uranus.corp.nutanix.com/~anirudh.sonar/data/data/objects/"
    vmimage += "vm/objects_test_client.qcow2"
    parser.add_argument("--remote_clients_vmname", required=False, help="VM  "
                        "name for deploying clients.Default : objects-test-"
                        "client", type=str, default="objects-test-client")
    parser.add_argument("--remote_clients_vmimage", required=False, help="VM  "
                        "image for deploying clients.Default : "
                        "ObjectsTestClientImage", type=str,
                        default="objects-test-client-vm-image")
    parser.add_argument("--remote_clients_vmimage_url", required=False, help=""
                        "VM image url for deploying clients.Default:%s"%vmimage,
                        type=str, default=vmimage)
    parser.add_argument("--skip_dead_clients_stats", required=False,
                        help="Include dead client stats clients.Default:False",
                        type=self._utils.convert_bool, default=False)

    #Test result archival server
    current_time = strftime("%H-%M-%S-%d%h%Y", gmtime())
    parser.add_argument("--test_start_time", required=False, help="Test UUID "
                        " which can be managed remotely. Default:CDate",
                        type=str, default=current_time)
    parser.add_argument("--archive_results", required=False, help="Archive "
                        " test results. Default:True when local execution",
                        type=str, default=True)
    parser.add_argument('--archival_endpoint_url', required=False, help="S3 "
                        "Endpoint URL for archival server.Default:Goldfinger",
                      type=str, default="http://goldfinger.buckets.nutanix.com")
    parser.add_argument('--archival_access_key', required=False, help="S3 "
                        "Access key to connect to archival server", type=str,
                        default="Ty1H802tIvWlXrvKb_gAK8UrasmK69Nw")
    parser.add_argument('--archival_secret_key', required=False, help="S3 "
                        "Secret key to connect to archival server", type=str,
                        default="Cvg3tyTMiYj-1Z8h0LfkYQDnw8zMuCLX")
    parser.add_argument('--archival_bucket_name', required=False, help="S3 "
                        "bucket name for archiving results", type=str,
                        default="results")
    autocomplete(parser)
    return parser

  def process_args(self):
    parser = self.arg_parser()
    args = vars(parser.parse_args())

    cfg_file = args.pop("cfgfile")
    if cfg_file:
      with open(cfg_file) as fh:
        args.update( literal_eval(fh.read()))

    password = args.pop("rdm_pass")
    if path.exists(".pass"):
      with open(".pass") as fh:
        password = fh.read().strip()

    if not (args["access_key"] and args["secret_key"] and args["endpoint_url"]):
      if not args["remote_execution"]:
        if not args["pcip"] or not args["peip"] or not args["objects_name"]:
          raise Exception("Provide access_key/secret_key/endpoint details or "
                      "PC/PE/ObjectsName details for local execution")

    self._utils.cleanup_all_markers()
    logpath, logfile = self._prepare_log_paths(args.get("log_path"),
                                               args.pop("log_path_symlink"))
    original_args = dumps(args, indent=2, sort_keys=True, default=str)
    args = self._process_nargs(**args)
    logconfig(logfile, args["v"], args["rotate_logs"])
    #_extract_build
    pe_url, pc_url = self._process_build_urls(args["pe_upgrade_build_url"],
                                              args["pc_upgrade_build_url"])
    args.update({"pe_upgrade_build_url":pe_url, "pc_upgrade_build_url":pc_url,
                  "log_path":logpath, "logfile":logfile})

    args = self._chose_workload(**args)

    self._write_args_to_file(logpath, original_args, args)
    args["password"] = password
    return args

  def _process_build_urls(self, pe_upgrade_build_url, pc_upgrade_build_url):
    if pe_upgrade_build_url:
      pe_upgrade_build_url = self._utils.extract_build(pe_upgrade_build_url)
      if not pe_upgrade_build_url:
        raise Exception("Failed to find valid PE Upgrade Build URL @ %s"
                        %(pe_upgrade_build_url))
      INFO("PE Upgrade URL : %s"%(pe_upgrade_build_url))

    if pc_upgrade_build_url:
      pc_upgrade_build_url = self._utils.extract_build(pc_upgrade_build_url)
      if not pc_upgrade_build_url:
        raise Exception("Failed to find valid PC Upgrade Build URL @ %s"
                        %(pc_upgrade_build_url))
      INFO("PC Upgrade URL : %s"%(pc_upgrade_build_url))
    return pe_upgrade_build_url, pc_upgrade_build_url

  def _process_nargs(self, **args):
    objsize = args["objsize"]
    if isinstance(objsize, str) or isinstance(objsize, int):
      args["objsize"] = [[objsize]]
    elif isinstance(objsize, list) and not  isinstance(objsize[0], list):
      args["objsize"] = [objsize]
    objsize = args["objsize"]
    args["objsize"] = [[self._utils.convert_to_num(i[0]),
                        self._utils.convert_to_num(i[-1])]
                        for i in objsize]
    shuffle(args["objsize"])
    args["num_parts"] = [self._utils.convert_to_num(args["num_parts"][0]),
                         self._utils.convert_to_num(args["num_parts"][-1])]
    args["range_read_size"] = [
                        self._utils.convert_to_num(args["range_read_size"][0]),
                        self._utils.convert_to_num(args["range_read_size"][-1])]
    args["nfs_range_read_size"] = [
                    self._utils.convert_to_num(args["nfs_range_read_size"][0]),
                    self._utils.convert_to_num(args["nfs_range_read_size"][-1])]

    args["ignore_errors"] = args.pop("ignore_errors").split(",")
    args["depth"] = [args["dir_depth"][0], args["dir_depth"][-1]]
    args["num_leaf_objects"] = [args["num_leaf_objects"][0],
                                args["num_leaf_objects"][-1]]
    args["num_reads_from_list"] = [args["num_reads_from_list"][0],
                                   args["num_reads_from_list"][-1]]
    args["storage_limit"] =self._utils.convert_to_num(args.pop("storage_limit"))
    args["num_nodes"] = [args["num_nodes"][0], args["num_nodes"][-1]]

    args["endpoint"] = args.pop("endpoint_url")
    if args["remote_execution"]:
      args.update({"storage_limit":-1, "archive_results":False,
                   "auto_delete_objects_threshold":-1})
    return args

  def _prepare_log_paths(self, log_path, symlinkfilename):
    current_date = datetime.today().strftime('%Y-%m-%d.%H.%M.%S')
    parent_log_path = path.join(getcwd(), log_path)
    ippath = path.join(parent_log_path,self._utils.get_local_ip())
    logpath = path.join(ippath, current_date)
    dir_symlink = path.join(parent_log_path, "latest")
    makedirs(logpath)
    try:
      if path.isdir(dir_symlink):
        remove(dir_symlink)
      symlink(logpath, dir_symlink)
    except Exception as err:
      debug_print("Failed to create symlink from %s to %s. Err : %s"
                  %(logpath, dir_symlink, err))
    file_symlink = path.join(ippath, symlinkfilename)
    logfile="{0}/objects_workload.log".format(logpath)
    if path.isfile(file_symlink):
      remove(file_symlink)
    open(logfile, "w").close()
    symlink(logfile, file_symlink)
    return logpath, logfile

  def _write_args_to_file(self, logpath, original_args, args):
    with open(path.join(logpath, "args.log"), "w") as fh:
      fh.write("Original Arguments passed to framework : %s"%original_args)
      fh.write("\n"+"*"*100)
      fh.write("\nFinal Arguments passed to program : ")
      fh.write(dumps(args, indent=2, sort_keys=True, default=str))
      fh.write("\n"+"*"*100)
    INFO("Final Args passed to framework : %s"%(path.join(logpath, "args.log")))
    Utils.write_to_file(dumps(args, indent=2, sort_keys=True, default=str),
                        Constants.STATIC_TESTCONFIG_FILE, "w")

  def _chose_workload(self, **kwargs):
    if isinstance(kwargs["workload"], list):
      return kwargs
    workload_profile = kwargs["workload"]
    if not workload_profile.startswith("wl_"):
      return kwargs
    if not kwargs.get(workload_profile):
      raise Exception("Workload profile %s not found in args"%workload_profile)
    INFO("Choosing %s from given args for workload"%workload_profile)
    kwargs["workload"] = kwargs[workload_profile]
    return kwargs

  def _get_creds(self, args):
    if args["remote_execution"]:
      return args
    args["access"] = args.pop("access_key")
    args["secret"] = args.pop("secret_key")
    if (args["access"] and args["secret"] and args["endpoint"]):
      return args
    ERROR("Objects access creds not provided.Generating one. PC : %s, PE : %s, "
          "ObjectsName : %s"%(args["pcip"], args["peip"], args["objects_name"]))
    pcobj = PCObjects(args["pcip"], args["pcuser"], args["pcpass"],
                  args["objects_name"], "", args["peip"])
    creds = pcobj.create_iam_user("Anirudha", "anirudha@scalcia.com", True)
    args["access"], args["secret"] = creds["access"], creds["secret"]
    args["endpoint"] = ",".join(["http://%s,https://%s"%(i, i)
                                 for i in pcobj.get_objects_endpoint(True)])
    INFO("Updating Objects access with : %s, %s"%(creds, args["endpoint"]))
    return args


  def setup_infra(self):
    args = self.process_args()
    if not args["rdm_deploy"]:
      args = self._get_creds(args)
      return args
    password = args.pop("password")
    if not password:
      password = getpass()
    self.rdm = RDMDeploy(args["rdm_user"], password)
    spec = self.rdm.get_spec(**args)
    deployment_id, deployments = self.rdm.deploy(spec,
          deployment_id=args["deployment_id"], deployments=args["deployments"])
    pe, pc, cluster_name = self.rdm.get_cluster_details()
    args["pcip"] = pc
    args["peip"] = pe
    pcobj = PCObjects(pc, args["pcuser"], args["pcpass"],
                      args["objects_name"], args["endpoint"], args["peip"])
    vip, dsip = args["pe_vip"], args["pe_dsip"]
    if not args["skip_aos_setup_post_rdm_deployment"]:
      vip, dsip = pcobj.configure_vip_dsip(peip=pe, vip=args["pe_vip"],
                                         dsip=args["pe_dsip"])
      INFO("VIP/DSIP/DeploymentsIDs : --pe_vip %s --pe_dsip %s --deployment_id "
           "%s --deployments %s --skip_aos_setup_post_rdm_deployment true"
           %(vip,dsip, deployment_id, " ".join(deployments)))
      pcobj.configure_dns_ntp(pe)
      pcobj.configure_dns_ntp(pc)
      if args["hypervisor_type"] == "ahv":
        pcobj.create_network(args["internal_network"], args["vlan_id"],
                             args["network_prefix"], args["start_ip"],
                             args["end_ip"])
      else:
        vcenter = VCenter(args["vcenter_ip"], args["vcenter_user"],
                          args["vcenter_pwd"], args["vcenter_dc"],
                          args["vcenter_cluster"], args["vcenter_port"])
        vcenter.add_host_portgroup("vSwitch0", args["internal_network"],
                                   args["vlan_id"])
        pcobj.register_to_vcenter(vcenter)
        pcobj.create_esxi_network(vcenter, args["internal_network"],
                                  args["netmask"], args["gateway"],
                                  args["start_ip"], args["end_ip"])
    args["peip"] = vip
    num_nodes = args["num_nodes"]
    args["num_nodes"] = choice(num_nodes)
    pcobj.deploy(**args)
    args["num_nodes"] = num_nodes
    user = pcobj.create_iam_user("test", "test@scalcia.com", recreate=True)
    args["access"] = user["access"]
    args["secret"] = user["secret"]
    if args.get("secondary_objects_name"):
      INFO("Replacing deployment details with secondary objects cluster names")
      args["objects_name"] = args["secondary_objects_name"]
      args["client_ips"] = args["secondary_client_ips"]
      args["internal_ips"] = args["secondary_internal_ips"]
      args["endpoint_url"] = args["secondary_endpoint_url"]
    return args

  def _init_test_obj_execute_test(self, preserve_recorded_ops,
                                  total_ops_recorded, **args):
      msg = "\nStarting workload %s with args : \n%s\n%s"%(args["workload"],
              dumps(args, indent=2, sort_keys=True), "*"*100)
      Utils.write_to_file(msg, path.join(args["log_path"], "args.log"), "a")
      self._remove_pass(path.join(args["log_path"], "args.log"))
      upload = UploadObjs(**args)
      if preserve_recorded_ops and total_ops_recorded:
        upload._total_ops_recorded = total_ops_recorded
      upload.rdm = self.rdm
      if args["do_not_start_workload"]:
        return upload
      try:
        upload.start_workload()
      except Exception as err:
        try:
          upload._dump_records(filename=Constants.STATIC_FINAL_RECONRDS_FILE)
        except:
          pass
        if not upload._execution_completed:
          raise
        if ("corruption" in str(err).lower() or
              "corruption" in str(err.message).lower()):
          raise
      if preserve_recorded_ops:
        total_ops_recorded = upload._total_ops_recorded

  def _remove_pass(self, filename):
    with open(filename) as fh:
      data = fh.read()
    with open(filename, "w") as fh:
      for line in data.split("\n"):
        if "password" in line:
          continue
        fh.write(line+"\n")

  def main(self):
    args = self.setup_infra()
    iterations = 1
    if args["remote_execution"]:
      clients = args.pop("remote_clients")
      re = RemoteExecution(clients, args["log_path"], **args)
      return re.run()
    else:
      #If its local run then only bother about num-iterations.
      #Else let remote client run test in iterations
      iterations = args.pop("num_iterations")
    if not args.get("endpoint"):
      raise Exception("Endpoint details are missing")
    preserve_recorded_ops = args.pop("preserve_db_across_iterations")
    total_ops_recorded = {}
    for iternum in range(1, iterations+1):
      args["iteration_num"] = iternum
      self._init_test_obj_execute_test(preserve_recorded_ops,
                                       total_ops_recorded, **args)

if __name__ == '__main__':
  wr = WorkloadRunner()
  try:
    wr.main()
  except Exception as err:
    DEBUG("Final Exception : %s, Stack Trace : %s"%(err, format_exception()))
    raise
