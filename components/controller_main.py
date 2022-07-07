import configargparse
import os
import json
import random
import time
from datetime import datetime
from argcomplete import autocomplete

import lib.generic.utils as utils
from lib.constants import Constants
from lib.generic.logger import setup_logging, INFO, DEBUG
from lib.generic.utils import debug_print # TODO: Revisit how debug_print usecase can be addressed better
from lib.product_workflows.resource_manager.resource_manager import ResourceManager

class ControllerMain(object):

  def __new__(cls):
    parser = configargparse.ArgParser(description="Objects load script",
                                      args_for_setting_config_path=["-c", "--config-file"],
                                      config_arg_is_required=True)
    args = cls.parse_arguments(parser)
    if args["remote_execution"]:
      obj = RemoteControllerMain(parser=parser, args=args)
    else:
      obj = LocalControllerMain(parser=parser, args=args)
    return obj

  def parse_arguments(cls, parser):
    """
    Parses command line arguments

    Returns: Parsed args
    """
    #parser.add_argument('-c', '--config_file', required=True, is_config_file=True, help='config file path')
    parser.add_argument("-l", "--log_level",
                        help='Log messages whose severity >= given level would be logged',
                        type=str, required=False, default="DEBUG")
    cls._define_all_args(parser)
    args = vars(parser.parse_known_args()[0])

  def _define_all_args(cls, parser):
    #Boto related args
    parser.add_argument('--endpoint_url', required=False, help="S3Endpoint URL")
    parser.add_argument('--access_key', required=False, help="S3 Access key")
    parser.add_argument('--secret_key', required=False, help="S3 Secret key")
    parser.add_argument('--read_timeout', required=False, type=int,
                        help="Boto3 read_timeout. Default=60", default=60)
    parser.add_argument('--enable_boto_debug_logging', required=False, help="To"
                        " enable boto debug logging. Default=False",
                        nargs="?", type=utils.convert_bool, default=False)
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
    # parser.add_argument("--cfgfile", required=False, help="Config file to load "
    #                     "params from. Default:None", default=None)
    parser.add_argument("--parallel_threads", required=False, type=int,
                        help="Parallel operations. Default:10", default=10)
    parser.add_argument("--runtime", required=False, type=int, help="How long"
                        " to run the workload. Default:Nolimit", default=-1)
    parser.add_argument("--logdir", required=False, help="Local Relative or Absolute path for log directory"
                        "Default:'logs'. Resolves to:<workspace>/logs", default="logs")
    parser.add_argument("--remote_logdir", required=False, help="Remote logdir path to be used in load VMs"
                        "Default:'logs'. Resolves to:<workspace>/logs", default="logs")
    # parser.add_argument("--symlink_logfile_name", required=False, help="Symlink "
    #                     "filename of logfile belonging to latest run. Default: test.log", default="test.log")
    parser.add_argument("--ignore_errors", required=False, help="Errors to "
                        "ignore seperated by comma. Default:None", default="")
    parser.add_argument("--force_kill", required=False, help="Force kill test "
                        "without waiting on stop_workload call.Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--force_ignore_errors", required=False, help="Force "
                        "ignore all the errors. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--debug_stats", required=False,
                        help="Log more test stats. Default:true",
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--debug_stats_interval", required=False, type=int,
                      help="Interval for debug stats. Default:120", default=300)
    parser.add_argument("--errors_to_dump_stack_strace", required=False,
                        type=str, nargs="+", help="When these errors are seen "
                        "dump entire stack trace in logs. Useful for debugging"
                        "Default:[]", default=[])
    # parser.add_argument("--v", required=False, type=int, help="Verbosity level. "
    #                     "Default:2, 0/1 for INFO, 2 for DEBUG, 3 VERBOSE, 4 "
    #                     "EXTRA-VERBOSE", default=2)
    parser.add_argument("--enable_log_rotation", required=False,
                        type=utils.convert_bool,
                        help="Enable log rotation or not"
                        "Default : True", default=True)
    parser.add_argument("--rotate_logs_size_in_MB", required=False,
                        type=int,
                        help="Rotate logs after logfile reaches specified size. Default:100",
                        default=100)
    parser.add_argument("--stats_interval", required=False, type=int, help=""
                        "Interval to print test stats. Default:10", default=30)
    parser.add_argument("--enable_memory_leak_debugging", required=False,
                        type=utils.convert_bool,
                        help="Print memory usage, useful in"
                        " debugging memory leaks. Default:False", default=False)
    parser.add_argument("--track_errors", required=False,
                        help="Track Errors. Default:True",
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--storage_limit", required=False, type=int,
                        help="Limit storage for IOs.Default:-1", default=-1)
    parser.add_argument("--enable_auto_delete_on_storage_full", required=False,
                        help="TODO : Delete all data when storage limit is "
                        "reached", nargs="?", type=utils.convert_bool,
                        default=False)
    parser.add_argument("--restart_zk", required=False, type=int, help="Restart"
                        " ZK during deployment.Default:0",default="0")
    parser.add_argument("--do_not_start_workload", required=False, help="Do not"
                        " start workload. Just return upload obj.Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--local_run", required=False, help="Notify if its "
                        "local or remote. Default:True", nargs="?",
                        type=utils.convert_bool, default=True)
    parser.add_argument("--thread_monitoring_interval", required=False,
                        type=int, help="Monitor each thread. Default:Disabled",
                        default=-1)
    parser.add_argument("--skip_cert_validation", required=False, help="Skip "
                        "cert validation during i/o test.Default:false",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--rss_limit", required=False, type=int,
                        help="RSS limit. Default:5G", default=5*1024*1024*1024)
    parser.add_argument("--num_iterations", required=False, type=int,
                        help="num_iterations. Default:1", default=1)
    parser.add_argument("--ntp_server", required=False, type=str, help="NTP "
                        "Server. Default:10.40.64.15", default="10.40.64.15")
    parser.add_argument("--reboot_all_clients_before_test", required=False,
                        type=utils.convert_bool,
                        help="Reboot clients before test", default=True)
    parser.add_argument("--use_nfs_mount_for_logs", required=False,
                        type=utils.convert_bool,
                        help="Use nfs bucket for logs.True", default=True)
    logs_share = "goldfinger.buckets.nutanix.com:/test-logs"
    parser.add_argument("--nfs_mount_for_logs", required=False, type=str,
                        help="nfs share for logs. Default:%s"%logs_share,
                        default=logs_share)
    parser.add_argument("--preserve_db_across_iterations", required=False,
                        type=utils.convert_bool,
                        help="Preserve DB across test iters.", default=True)

    #Data related args
    parser.add_argument("--static_data", required=False,
                        help="Whether to use static data or not",
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--compressible", required=False, help="Whether to "
                        "generate compressible data or not",
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--zero_data", required=False, help="Whether to "
                        "generate zero data or not",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_integrity_check", required=False, help="Skip "
                        "integrity during GET. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_read_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_copy_workload_running", required=False,
                        help="is_copy_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_write_workload_running", required=False,
                        help="is_write_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfsread_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfswrite_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfsrename_workload_running", required=False,
                        help="is_nfsrename_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfscopy_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfsdelete_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfslist_workload_running", required=False,
                        help="is_delete_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfs_overwrite_workload_running", required=False,
                        help="is_nfs_overwrite_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfsvdbench_workload_running", required=False,
                        help="is_nfsvdbench_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_nfsfio_workload_running", required=False,
                        help="is_nfsfio_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--is_lookup_workload_running", required=False,
                        help="is_lookup_workload_running. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)

    #Auto workload profiler related args
    parser.add_argument("--enable_auto_workload", required=False,
                        help="Enable workload monitoring and change it on "
                        "runtime. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--enable_static_workload_change", required=False,
                        help="Skip failure check and change workload profile"
                        " after stability duration. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--select_random_workload", required=False,
                        help="Select workload randomly else sequentially. "
                        "Default:True", nargs="?",type=utils.convert_bool,
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
                        type=utils.convert_random_bool, default="random")
    parser.add_argument("--num_buckets", required=False, type=int,
                        help="Num buckets to work on. Default:1", default=1)
    parser.add_argument("--bucket_prefix", required=False, help="Bucket prefix."
                        " Default:bucketsworkload", default="bucketworkload")
    parser.add_argument("--skip_bucket_deletion", required=False, help="Skip "
                        "bucket deletion. Default:False",
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--skip_bucket_creation", required=False, help="Skip "
                        "bucket creation. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--unique_bucket_for_delete", required=False,
                        help="Choose unique bucket for delete for every delete "
                        "objects call. Default:True",
                        nargs="?", type=utils.convert_bool, default=True)

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
                        type=utils.convert_bool, default=False)
    parser.add_argument("--set_expiry", required=False,
                        help="Set expiry on bucket.Default : false",
                        nargs="?", type=utils.convert_bool, default=True)
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
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--tag_expiry_prefix", required=False, help="Tag expiry"
                        " prefix. Default : tag_expiry",default="tag_expiry")
    parser.add_argument("--tag_expiry_days", required=False, type=int,
                        help="Tag Expiry days. Default : 1", default=1)

    #Tiering related args
    parser.add_argument("--enable_tiering", required=False,
                        help="Enable Tiering. Default : False",
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=True)

    #Object related args
    parser.add_argument("--objsize", required=False, type=str, nargs="+",
                      help="Object size. Default:1m 2m", default=["1mb", "2mb"])
    parser.add_argument("--select_random_objsize", required=False, nargs="?",
                        help="Randomize objsize. Default:True",
                        type=utils.convert_bool, default=True)
    parser.add_argument("--objprefix", required=False,
                        help="Objname_prefix. Default:key", default="keyreg")
    parser.add_argument("--num_versions", required=False, nargs="+",
                        help="num_versions. Default:1", default=[1,1])
    parser.add_argument("--use_hexdigits_keynames", required=False,
                        help="use_hexdigits_keynames. Default:true",
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--use_unsafe_hex_keynames", required=False,
                        help="use_unsafe_hex_keynames. Default:false",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--delimeter_obj", required=False, help="Upload "
                        "delimeter object.Default:False",
                        nargs="?", type=utils.convert_bool, default=True)
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
                          nargs="?", type=utils.convert_bool,default=False)
    parser.add_argument("--skip_parts_subblock_integrity_check", required=False,
                          help="Skip parts range integrity check at subblock "
                          "level. Default:False", nargs="?",
                          type=utils.convert_bool, default=False)
    parser.add_argument("--num_leaf_objects", required=False, type=int,
                        nargs="+", help="Num of leaf  objects to create in "
                        "given dir in bucket. Default:100", default=[10, 150])
    parser.add_argument("--read_after_copy", required=False, help="Read after"
                        " copy. Default : False",
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_head_before_delete", required=False,
                        help="Dont do head before delete. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_head_after_delete", required=False,
                        help="Dont do head after delete. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_read_before_delete", required=False,
                        help="Dont read object before delete. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_head_after_put", required=False, help="Dont "
                        "head object after put. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--maxkeys_to_list", required=False, type=int,
                        nargs="+", help="Max keys to list in one call."
                        "Default:[1, 1000]", default=[1, 2])
    parser.add_argument("--validate_list_against_head", required=False,
                        help="validate_list_against_head.Default:false",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_reads_before_date", required=False, help="Skip"
                        " reads on object before specific date. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--date_to_skip_reads", required=False, help="Skip"
                        " reads on object before this date. "\
                        "Default:Sun, 28 Feb 2020 23:59:59",
                        default="Sun, 28 Feb 2020 23:59:59")
    parser.add_argument("--skip_expired_object_read", required=False,
                        help="Skip reads on expired objects. Default:True",
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--ignore_all_errors_forcefully", required=False,
                        help="Skip skip all errors. Default:True",
                        nargs="?", type=utils.convert_bool, default=True)

    #PC & PE related args
    parser.add_argument("--pcip", required=False, help="PC IP", default=None)
    parser.add_argument("--pcuser", required=False, help="PC User",
                        default="admin")
    parser.add_argument("--pcpass", required=False, help="PC Pass",
                        default="Nutanix.123")
    parser.add_argument("--objects_name", required=False, help="Objects cluster"
                        "  name in PC", default="Objects%s"%(int(time.time())))
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
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_sanity_check", required=False, help="Skip "
                        "sanity test during deploy/cert loop test",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_multicluster_check", required=False,
                        help="Skip multicluster test during loop test",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_scaleout_check", required=False, help="Skip "
                        "scaleout test during deploy loop test",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_objects_delete", required=False, help="Skip "
                        "objects delete during deploy/cert loop test",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_objects_deploy", required=False, help="Skip "
                        "objects deploy during loop test",
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=False)

    #NFS related args
    parser.add_argument("--enable_nfs", required=False, help="Enable NFS access"
                        " in bucket. Default:random", nargs="?",
                        type=utils.convert_random_bool, default="random")
    parser.add_argument("--skip_nfs_read_only_integrity_check", required=False,
                        help="Skip integrity check.Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_list_validation_vs_s3", required=False,
                        help="Skip validating against s3 list.Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--skip_functional_check", required=False,
                        help="Skip functional_check.Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--ignore_nfs_metadata_check_errors", required=False,
                        help="ignore_nfs_metadata_check_errors. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--ignore_nfs_size_errs_vs_s3", required=False,
                        help="--ignore_nfs_size_errs_vs_s3. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=False)
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
                        type=utils.convert_bool,
                        help="NFS debug logging for debugging. Default : False",
                        default=False)
    parser.add_argument("--nfs_write_block_size", required=False,
                        type=int, help="nfs_write_block_size. Default:1M",
                        default=1024*1024)
    parser.add_argument("--num_over_writes_per_file", required=False,
                        type=int, help="num_over_writes. Default:5",
                        default=5)
    parser.add_argument("--randomise_sparse_write", required=False,
                        type=utils.convert_bool, help="num_over_writes. "
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
                        type=utils.convert_bool,
                        help="read_after_nfsoverwrite.Default:true",
                        default=True)
    parser.add_argument("--sync_data_on_file_close", required=False,
                        type=utils.convert_bool,
                        help="Sync data before closing file. Default:true",
                        default=True)
    #Keep it to False to avoid smaller writes to NFS server
    parser.add_argument("--sync_data_on_every_write_to_file", required=False,
                        type=utils.convert_bool, help="Sync data "
                        "immdiately after writing to file. Default:False",
                        default=False)
    parser.add_argument("--verbose_logging_for_locks", required=False,
                        type=utils.convert_bool, help="Enable versbose "
                        "logging for locking/unblocking objs. Default:False",
                        default=False)
    parser.add_argument("--skip_read_after_write", required=False, help="Dont "
                        "read nfsfile after write. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)

    parser.add_argument("--capture_tcpdump", required=False,
                        help="Capture tcpdump. Default:false",
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=False)

    #RDM Related args
    parser.add_argument("--rdm_deploy", required=False, help="Deploy resources "
                        "via rdm. Default:false",
                        nargs="?", type=utils.convert_bool, default=False)
    parser.add_argument("--rdm_clustername", required=False, help="RDM Cluster "
                        "name. Default:ObjectsCluster",
                        default="ObjectsCluster%s"%(int(time.time())))
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
                        "Default:True", nargs="?", type=utils.convert_bool,
                        default=True)
    parser.add_argument("--skip_aos_setup_post_rdm_deployment", required=False,
                        help= "skip_aos_setup_post_rdm_deployment Default:Fals",
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=False)
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
                        nargs="?", type=utils.convert_bool, default=True)
    parser.add_argument("--webserver_logging_port", required=False, help="Web"
                        " Server Port. Default:37000", type=int, default=37000)

    #Remote execution
    parser.add_argument("--remote_execution", required=False, help="Start"
                        " remote execution. Default:False",
                        nargs="?", type=utils.convert_bool, default=False)
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
                        type=utils.convert_bool, default=False)
    parser.add_argument("--remote_cmdline_args", required=False, help="Cfgfile "
                        "for remote remote execution. Default:None", type=str,
                        default=None)
    parser.add_argument("--use_unique_bucket_prefix", required=False, help=""
                        "User unique bucket per client. Default:None", type=str,
                        default=None)
    parser.add_argument("--remote_test_dir", required=False, help="Remote "
                        "location to clone objects scale workspace. Default:/root/auto", type=str,
                        default="/root/auto")
    parser.add_argument("--remote_workspace_commit", required=False, 
                        help="Workspace commit to use for remote test execution. Default:HEAD", type=str,
                        default="HEAD")
    parser.add_argument("--stop_test_gracefully", required=False,
                        help="stop_test_gracefully "
                        " Default:True", type=utils.convert_bool,
                        nargs="?", default=True)
    parser.add_argument("--restart_test_if_required", required=False,
                        help="Restart the test if its already running. "
                        " Default:False", type=utils.convert_bool,
                        nargs="?", default=True)
    parser.add_argument("--distributed_clients", required=False, help="Is test "
                        "running across multiple clients.Default:False",
                        type=utils.convert_bool, default=False)
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
                        type=utils.convert_bool, default=False)

    #Test result archival server
    current_time = time.strftime("%H-%M-%S-%d%h%Y", time.gmtime())
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
    
class BaseControllerMain(object):
  
  def __init__(self, parser, args):
    self.args = args
    self.parser = parser
    self._is_db_initialized = None
    self.resource_manager = None 
    self._task_manager = None
    self.remote_execution = self.args["remote_execution"]
    self.handle_logging()
    self.cleanup_all_markers()
    self.process_arguments()
    self.load_clients = []

  def prepare_clients(self):
    # download files, yum and pip installs on all remote clients
    # for remote execution,  or prepare local client
    pass

  @property
  def task_manager(self):
    if not self._task_manager:
      self._task_manager = DynamicExecution(10, quotas={
                               Constants.REMOTE_POPS_QUOTA:{"num_workers":10}})
    return self._task_manager

   @staticmethod
  def _update_in_config_file(field, value):
    filepath = Constants.STATIC_TESTCONFIG_FILE
    with open(filepath, 'r') as fh:
      data = json.loads(fh.read())
      data[field] = value
    with open(filepath, 'w') as fh:
      fh.write(json.dumps(data, indent=2, sort_keys=True, default=str))

  def setup(self):
    self.resource_manager = ResourceManager(args=self.args)
    DEBUG("deploy or discover oss/endpoint details")
    res = self.resource_manager.create_or_bind_oss()
    # res contains endpoint_url
    # TODO: I didnt like this.. send only required args and update only
    # specific fields.
    self.args.update(res)

    if not args.get("access_key"):
      DEBUG("create access key, secret key")
      user = self._pcobj.create_iam_user("test", "test@scalcia.com", recreate=True)
      args["access_key"] = user["access"]
      args["secret_key"] = user["secret"]

    DEBUG("setup log collection cron")
    self.setup_log_collection_cron()

  def _init_db(self):
    if self._is_db_initialized:
      DEBUG("DB is already initialized. Nothing to do here")
      return
    # TODO: Handle localhost execution case here
    if not self.load_clients:
      ERROR("No clients found, nothing to initialise here")
      return
    self._db = {"test_args":{}, "clients":{}}
    default_test_status = Constants.TEST_NOT_STARTED
    if self.args["remote_execution_monitor_only"]:
      WARN("Remote only execution is enabled, test wont be restarted as its "
           "hard to detect if test failed or not-started")
      default_test_status = Constants.TEST_NOT_RUNNING
    for client in self.load_clients:
      self._db['clients'][client] = {"status":default_test_status,
                                     "starttime":None,
                                     "num_retries_to_start_test":0,
                                     "start_attempt_time":datetime.now(),
                                     "active_stats":None}
    self._is_db_initialized = True

  def setup_log_collection_cron(self):
    # TODO: Implement this method to setup cron that pushes logs from
    # PC/PE to goldfinger
    pass
    
  def start_workload(self):
    # start and monitor with background thread
    if self.args["remote_execution"]:
      pass
    else:
    #   vms = [localhost]
      pass

  def pause_workload(self):
    pass

  def resume_workload(self):
    pass

  def stop_workload(self):
    pass

  def update_configs(self):
    # update error injection details, compaction  details, configs like thread pool size
    pass

  def add_vm(self, name):
    pass

  def remove_vm(self, name):
    pass

  def monitor_load_in_foreground(self):
    # wait on monitor thread
    pass

  def change_workload(self):
    # pass - change criteria
    pass

  def handle_logging(self):
    logdir = self.args["logdir"]
    if not os.path.isabs(logdir):
      logdir = os.path.join(os.getcwd(), logdir)
    self._initialize_log_paths(all_runs_log_folder=logdir)
    rotate_info = {}
    if self.args["enable_log_rotation"]:
      rotate_info = {"sized_log_rotate": self.args["rotate_logs_size_in_MB"]}
    setup_logging(abs_log_file_path=self.logfile, log_level=self.args["log_level"],
                  rotate_info=rotate_info)

  def process_arguments(self):
    self._validate_args()
    self._process_nargs()
    self._chose_workload()
    self._write_args_to_file()
    
  def _initialize_log_paths(self, all_runs_log_folder):
    # <workspace>/logs/<datetime>/controller_<ip>/load.log
    #                 /latest -> dir_symlink to latest run's folder
    timestamp = datetime.today().strftime('%Y-%m-%d.%H.%M.%S')
    top_logdir_path = os.path.join(all_runs_log_folder, timestamp)
    dir_symlink = os.path.join(all_runs_log_folder, "latest")
    ippath = os.path.join(top_logdir_path, "controller_{0}".format(utils.get_local_ip()))
    os.makedirs(ippath)
    try:
      if os.path.isdir(dir_symlink):
        os.remove(dir_symlink)
      os.symlink(top_logdir_path, dir_symlink)
    except Exception as err:
      DEBUG("Failed to create symlink from %s to %s. Err : %s"
                  %(top_logdir_path, dir_symlink, err))
    # TODO: Identify the caller script name to use the same name for logfile
    logfile="{0}/load.log".format(ippath)
    open(logfile, "w").close()
    # This VM's logs will be placed under subfolders named after its ip
    self.logdir = ippath
    self.logfile = logfile

  def _validate_args(self):
    # TODO: Add more validation as needed here.
    
    # Ensure that either both access_key and secret_key are specified or
    # both should not be specified
    if bool(args.get("access_key")) ^ bool(args.get("secret_key")): #  XOR
      raise configargparse.ArgumentError(
        argument=None,
        message="Either provide both access_key and secret_key in args or "
                "both should not be provided")

    # This check is confusing. It doesnt do anything for remote execution
    if not (self.args["access_key"] and self.args["secret_key"] and self.args["endpoint_url"]):
      if not self.args["remote_execution"]:
        if not self.args["pcip"] or not self.args["peip"] or not self.args["objects_name"]:
          raise configargparse.ArgumentError(
            argument=None,
            message="Provide access_key & secret_key & endpoint details or "
                    "PC/PE/ObjectsName details for local execution")

  def _process_nargs(self):
    args = self.args
    objsize = args["objsize"]
    if isinstance(objsize, str) or isinstance(objsize, int):
      args["objsize"] = [[objsize]]
    elif isinstance(objsize, list) and not  isinstance(objsize[0], list):
      args["objsize"] = [objsize]
    objsize = args["objsize"]
    args["objsize"] = [[utils.convert_to_num(i[0]),
                        utils.convert_to_num(i[-1])]
                        for i in objsize]
    random.shuffle(args["objsize"])
    args["num_parts"] = [utils.convert_to_num(args["num_parts"][0]),
                         utils.convert_to_num(args["num_parts"][-1])]
    args["range_read_size"] = [
                        utils.convert_to_num(args["range_read_size"][0]),
                        utils.convert_to_num(args["range_read_size"][-1])]
    args["nfs_range_read_size"] = [
                    utils.convert_to_num(args["nfs_range_read_size"][0]),
                    utils.convert_to_num(args["nfs_range_read_size"][-1])]

    args["ignore_errors"] = args.pop("ignore_errors").split(",")
    args["depth"] = [args["dir_depth"][0], args["dir_depth"][-1]]
    args["num_leaf_objects"] = [args["num_leaf_objects"][0],
                                args["num_leaf_objects"][-1]]
    args["num_reads_from_list"] = [args["num_reads_from_list"][0],
                                   args["num_reads_from_list"][-1]]
    args["storage_limit"] =utils.convert_to_num(args.pop("storage_limit"))
    args["num_nodes"] = [args["num_nodes"][0], args["num_nodes"][-1]]

    args["endpoint"] = args.pop("endpoint_url")
    if args["remote_execution"]:
      args.update({"storage_limit":-1, "archive_results":False,
                   "auto_delete_objects_threshold":-1})

  def _chose_workload(self):
    if isinstance(self.args["workload"], list):
      return
    workload_profile = self.args["workload"]
    if not workload_profile.startswith("wl_"):
      return
    if not self.args.get(workload_profile):
      raise Exception("Workload profile %s not found in args" % workload_profile)
    INFO("Choosing %s from given args for workload" % workload_profile)
    self.args["workload"] = self.args[workload_profile]

  def _write_args_to_file(self):
    args_str = self.parser.format_values()
    args_file = os.path.join(self.logdir, "args.log")
    with open(args_file, "w") as fh:
      fh.write("Various args sources to the framework : %s" % args_str)
    INFO("Input args specified for this run: %s" % args_file)
    with open(Constants.STATIC_TESTCONFIG_FILE, "w") as fh:
      fh.write(json.dumps(self.args, indent=2, sort_keys=True, default=str))

  def cleanup_all_markers(self, markers=None):
    if not markers:
      markers = ['/tmp/stats', '/tmp/stats.err', '/tmp/mem_top',
                 '/tmp/exit_marker', '/tmp/records.json', '/tmp/stats.html',
                 '/tmp/testconfig.json', '/tmp/bucket_info.json',
                 '/tmp/stats.json', '/tmp/all_stats.json',
                 '/tmp/clients_records.json', '/tmp/final_records.json']

    for marker in markers:
      if os.path.isfile(marker):
        try:
          os.remove(marker)
        except Exception as ex:
          DEBUG("couldn't remove %s: %s" % (marker, str(ex)))

class RemoteControllerMain(BaseControllerMain):

  def __init__(self, *args, **kwargs):
    super(RemoteControllerMain, self).__init__(*args, **kwargs)
    self._aoscluster = None

  @property
  def aoscluster(self):
    if not self._aoscluster:
      self._aoscluster = AOSCluster(self.args["remote_clients_peip"],
                         self.args["remote_clients_pe_admin_user"],
                         self.args["remote_clients_pe_admin_password"])
    return self._aoscluster

  def setup(self):
    super(RemoteControllerMain, self).setup()
    # 4. create vms if remote execution and vm ips are not given
    if not self.args["remote_clients"]:
      clients = self.resource_manager.create_vms()
      self.load_clients = [details['ip'] for details in clients.values()]
      self._init_db()
      for vm, details in clients.iteritems():
        self._db['clients'][details['ip']]["vmname"] = vm
    else:
      self.load_clients = self.args["remote_clients"]
      self._init_db()
    
  def prepare(self):
    DEBUG("Stop test")
    self._stop_test()
    
    if self._is_test_running():
      DEBUG("Tets is still running")
      if not self.args["restart_test_if_required"]:
        raise Exception("Test is already running on one or more clients. "
                        "'restart_test_if_required' param is also disabled")

    DEBUG("Create the workspace")
    self._create_workspace_in_remote_clients()

    if self.args["use_nfs_mount_for_logs"]:
      DEBUG("Mount log folder")
      self._mount_all_buckets()

    DEBUG("copy args file")
    # TODO: Send an args file with a trimmed version of client only parameters
    for client in self.load_clients:
      self._task_manager.add(self._utils.copy_local_file_to_remote,
              remoteip=client, localfile=Constants.STATIC_TESTCONFIG_FILE,
              remotefile=Constants.STATIC_TESTCONFIG_FILE,
              quota_type=Constants.REMOTE_POPS_QUOTA)
    self._task_manager.wait_for_all_worker_to_be_free()

  def start_workload(self):
  
    self._remote_test_execution(remote_script_location, remote_cfgfile_location,
        self._kwargs["remote_cmdline_args"],
        use_unique_bucket_prefix=self._kwargs["use_unique_bucket_prefix"],
        ntp_server=self._kwargs["ntp_server"])
    debug_print("Sleeping for 60 seconds")
    sleep(60)

  def _create_workspace_in_remote_clients(self):
    remote_test_dir = self.args["remote_test_dir"]
    cmd = ("rm -Rf {remote_test_dir}/*; cd {remote_test_dir}; "
          "git clone {objects_scale_repo}; git reset {commit} --hard").format(
            remote_test_dir=remote_test_dir,
            objects_scale_repo=Constants.OBJECTS_SCALE_REPO,
            # TODO: Default commit should be changed to local workspace commit
            # TODO: Add support for patch url
            commit=self.args["remote_workspace_commit"]
          )
    for client in self.load_clients:
      self.task_manager.add(Utils.remote_exec, hostname=client,
                            cmd=cmd, 
                            quota_type=Constants.REMOTE_POPS_QUOTA)
    debug_print("Waiting for command execution in all clients")
    self.task_manager.wait_for_all_worker_to_be_free()

  def _remote_test_execution(self, remote_script_location,
                           cfgfile_for_remote_execution, remote_cmdline_args,
                           use_unique_bucket_prefix, ntp_server):
    cmd = 'ntpdate -s %s; nohup python %s --cfgfile %s'%(ntp_server,
                remote_script_location, cfgfile_for_remote_execution)
    self._db["test_args"]["test_cmd"] = {}
    counter = 0
    for client in self.load_clients:
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
      INFO("Resetting all clients from prism : %s" % self._aoscluster._peip)
      self._aoscluster.power_reset(vmprefix)
    else:
      for client in self.load_clients:
        # TODO: Handle command output redirection and background execution in remote_exec itself
        rcmd = 'nohup reboot -f > /dev/null 2>&1 &'
        INFO("Rebooting client: %s" % client)
        Utils.remote_exec(client, rcmd, "root", "nutanix/4u", background=True)
    DEBUG("Wait for 60s post reboot")
    time.sleep(60)

  def _mount_all_buckets(self):
    remote_logdir = self.args["remote_logdir"]
    if not os.path.isabs(remote_logdir):
      remote_logdir = os.path.join(self.args["remote_test_dir"], remote_logdir)
    cmd = "mount -o rw,vers=3,proto=tcp,sec=sys,nfsvers=3 %s %s" % \
      (self.args["nfs_mount_for_logs"], remote_logdir)
    self._update_in_config_file(field="remote_logdir", value=remote_logdir)
    
    self._db["test_args"]["nfs_mount_for_logs"] = cmd
    for client in self.load_clients:
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

  def _stop_test_before_initiating_workload(self):
    if not self.args["reboot_all_clients_before_test"]:
      DEBUG("reboot_all_clients_before_test=False, "
            "so no attempts will be made to stop the test or reboot clients")
      return
    self.stop_workload(stop_test_gracefully=self.args["stop_test_gracefully"])

  def stop_workload(self, wait_for_graceful_stop=True,
                    wait_for_graceful_stop_timeout=2400, 
                    stop_test_gracefully=True):
    if stop_test_gracefully:
      # Graceful shutdown
      for client in self.load_clients:
          self._task_manager.add(self._stop_test_on_client, client=client,
              check_response=not(ignore_errors), quota_type=Constants.REMOTE_POPS_QUOTA)
      self._task_manager.wait_for_all_worker_to_be_free()
      if not wait_for_graceful_stop:
        DEBUG("Stop workload is initiated, but wait_for_graceful_stop is False")
        return
      etime = wait_for_graceful_stop_timeout + time()
      is_workload_stopped = False
      while (etime - time()) > 0 :
        if not self._is_test_running(check_workloads=True):
          DEBUG("Workload is stopped on all clients")
          is_workload_stopped = True
          break
        INFO("Waiting for all clients to stop test. Time elapsed : %s secs"
            %((time()+timeout)-etime))
        sleep(30)
      if not is_workload_stopped:
        INFO("Hit timeout after %s secs, while waiting for workload"
             " to stop gracefully. Clients will be rebooted now" % timeout)
        self.reboot_all_clients()
    else:
        WARN("stop_test_gracefully is disabled, clients will be rebooted")
        self.reboot_all_clients()
    if self.is_workload_running(debug=True):
      raise Exception("Workload is running in remote clients inspite "
                      "of all efforts to stop")

  def _stop_test_on_client(self, client, check_response=True):
    if not self._is_test_running([client]):
      return
    url = "http://%s:37000/exit_test" % client
    INFO("Stopping test on %s, URL: %s" % (client, url))
    Utils.execute_http_request(url, method="GET", check_response=check_response)

  def is_workload_running(self, debug=False):
    for client in self.load_clients:
      self._task_manager.add(self._is_wl_running, client=client,
                  script_location="scripts/*.py", debug=debug,
                  quota_type=Constants.REMOTE_POPS_QUOTA,
                  custom={"client": client})
    debug_print("Will wait for all workers get workload status from clients")
    self._task_manager.wait_for_all_worker_to_be_free()
    all_results = self._task_manager.consume_results()
    running_clients = []
    for result in all_results.values():
      # No failures reported, so every task has got ret value
      if result["ret_value"]:
        running_clients.append(result["custom"]["client"])
    if running_clients:
      INFO("Workload is still running in: %s" % running_clients)
    return bool(len(running_clients))

  def _is_wl_running(self, client, script_path, debug=False):
    rcmd = "ps aux| grep %s | grep -v grep" % script_path
    out, err = Utils.remote_exec(client, rcmd)
    if out or err:
      if debug:
        ERROR("Workload is still running in %s, out : %s, err: %s" \
              % (client, out, err))
      return True
    return False

  def _is_test_running(self, clients=None, check_workloads=False):
    if not clients:
      clients = self.load_clients
    for client in clients:
      WARN("Checking Test Status: %s" % client)
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

class LocalControllerMain(BaseControllerMain):
  pass
  





   


"""
Identify the caller script name to use the same name for logfile

remote test execution
  handle start_workload() imple send, remote=false in commandline

overall check imple flow for local execution



is_test_running



arranging methods in Remotecontrollermain - multi client level overall vs per client methods
also remove code repetition with looping on all load_clients, passing custom info and checking results
"""
  