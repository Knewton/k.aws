"""
Methods for interacting with EMR via boto.

EMR jobs typically contain multiple map and/or reduce steps. Our planned use of
Cascading means we'll define map/reduce steps in a single jar, so instead
k.aws.emr only creates single-step jobs via simple_job() and simple_step().

k.aws.emr also assumes fixed locations for certain items:
	* input data, output data, logs, and jars are all stored in the knewton-emr
		bucket in Knewton's aws-utility env
	* output files are in s3://knewton-emr/$OUTPUT_DIR_PREFIX with a suffix
		created by _output_dir_suffix()
	* job jars are in s3://knewton-utility-build/ or if you mention the
		complete uri the system will try to find it there.
"""
from collections import namedtuple
import datetime
from distutils.version import StrictVersion
import re
import boto.emr
import k.aws.config
import k.aws.ec2
import k.aws.util
from boto.emr.step import JarStep
from boto.emr.bootstrap_action import BootstrapAction
from boto.emr.instance_group import InstanceGroup
from k.aws.config import AwsCreds, connection_hash, get_keys
from k.aws.config import RegionAwsCreds, region_connection_hash
import k.aws.s3 as k_aws_s3

TARGET_BUCKET = 'knewton-emr/'
OUTPUT_DIR_PREFIX = "s3n://" + TARGET_BUCKET + "processed-data/"
LOG_DIR = "logs/"
NOW = datetime.datetime.now()
VALID_STATES = set(
		['COMPLETED', 'FAILED', 'TERMINATED', 'RUNNING', 'SHUTTING_DOWN',
			'STARTING', 'WAITING'])
JARINFO = namedtuple('JarInfo', 'bucket name version name_prefix')

# There are two pairs of patterns here one pair for a URI and one for a jar file
# name. In any pair there is one pattern to search a name with just a major and
# a minor number and the other pattern additionally searches for the revision
# number as well. This is because our projects have ambiguous naming
# conventions.
JAR_URI_PATTERN = re.compile(
	's3n://(.*)-([0-9]+\.[0-9]+)(.jar)')
JAR_URI_PATTERN_MAJ_MIN_REV = re.compile(
	's3n://(.*)-([0-9]+\.[0-9]{1,3}\.[0-9]+)(.jar)')
JAR_NAME_PATTERN = re.compile(
	'(.+)-([0-9]+\.[0-9]+)(.jar)')
JAR_NAME_PATTERN_MAJ_MIN_REV = re.compile(
	'(.+)-([0-9]+\.[0-9]+\.[0-9]+)(.jar)')


def connect(creds):
	"""
	Connect to autoscale, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.emr.EmrConnection
	"""
	if isinstance(creds, AwsCreds):
		return boto.connect_emr(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.emr.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)


def simple_job(conn, emr_jar_info, inputdir, jarargs, ec2_key_pair, options):
	"""
	Create a simple, one-step job.

	namedtuple emr_jar_info:  (bucket name version)  of jarfile to submit
	str inputdir:   S3 key for directory containing data
	[strs] jarargs: list of strings containing args to the main class
	options obj from bin/emr-new-job with:
		int ninstances:    number of instances to use in cluster
		str isize:         instance size for cluster hosts
		str version:       Hadoop version to use for job
		bool debug:        bool indicating whether job will be debugged
		str classname:     name of main class (default: None)
		str job_flow_role: intance role to set during launch

	Returns the id of the new jobflow.
	"""
	ninstances = options.ninstances
	isize = options.isize
	version = options.version
	debug = options.debug
	classname = options.classname
	bootstrap_action = _get_bootstrap_action(options)
	instance_groups = _get_cluster_config(options, isize, ninstances)
	outputdir = options.custom_output
	job_flow_role = options.job_flow_role
	visible_to_all_users = options.visible_to_all_users

	# Build job parameters.
	projname = _get_project_name(emr_jar_info, options)
	step = simple_step(projname, emr_jar_info, inputdir, jarargs,
						classname, outputdir)
	loguri = build_job_log_prefix(emr_jar_info, options)

	return conn.run_jobflow(
			name=projname,
			log_uri=loguri,
			steps=step,
			num_instances=ninstances,
			master_instance_type=isize,
			slave_instance_type=isize,
			enable_debugging=debug,
			hadoop_version=version,
			bootstrap_actions=bootstrap_action,
			instance_groups=instance_groups,
			ami_version=options.ami_version,
			ec2_keyname=ec2_key_pair,
			keep_alive=options.keep_alive,
			job_flow_role=job_flow_role,
			visible_to_all_users=visible_to_all_users)


def _get_project_name(emr_jar_info, options):
	"""
	Get the project name based on either the jar name of
	what is in the options.
	"""
	if options.job_name is None:
		return emr_jar_info.name
	else:
		return options.job_name


def _get_cluster_config(options, isize, ninstances):
	"""
	Defines the number and the types of instances to run.
	"""
	if options.slave_spot_instances is not None:
		# Fetch current price for the instances we're requesting
		region_name, creds = k.aws.config.get_region_keys(options)
		curr_price = str(k.aws.util.get_ec2_price(region_name, isize))

		# Set up a normal master instance and spot instances for slaves.
		# NOTE: boto is set up so that if instance_groups is passed in as an
		# argument, the num_instances, master_instance_type, and
		# slave_instance_type arguments are ignored.
		return [
				InstanceGroup(1, 'MASTER', isize, 'ON_DEMAND', 'master-on_demand', ''),
				InstanceGroup(ninstances - 1, 'CORE', isize, 'ON_DEMAND',
							'core-on_demand', ''),
				InstanceGroup(options.slave_spot_instances, 'TASK', isize, 'SPOT',
							'task-spot@' + curr_price,
							curr_price)
		]
	else:
		return None


def _get_bootstrap_action(options):
	"""
	Extracts bootstrap actions and builds a BootstrapAction object if
	necessary.
	"""
	kerberize = options.kerberize
	actions = []
	if kerberize:
		actions = [BootstrapAction(
			'basic chef install',
			's3://knewton-configurations/elasticmapreduce/bootstrap-actions/install-chef-basic',
			None
		)]

	if options.bootstrap:
		bootstrap_fields = options.bootstrap.split(" ", 1)
		bootstrapArgs = None
		if (len(bootstrap_fields) > 1):
			bootstrapArgs = bootstrap_fields[1]
		actions.append(BootstrapAction(
					"bootstrap-action", bootstrap_fields[0],
					bootstrapArgs))

	return actions

def build_job_log_prefix(emr_jar_info, options):
	"""
	Builds URI where job log will be stored.
	"""
	if options.job_log_dir is None:
		return "s3n://{bucket}{logdir}{jar}/{date}".format(
				bucket=TARGET_BUCKET, logdir=LOG_DIR,
				jar=emr_jar_info.name, date=build_date_directory())
	else:
		custom_log_dir = options.job_log_dir
		if custom_log_dir.find("s3n://") == -1:
			custom_log_dir = "s3n://" + custom_log_dir
		if not custom_log_dir.endswith("/"):
			custom_log_dir += "/"
		return custom_log_dir


def build_date_directory(dtime=NOW):
	""" Builds a directory name based on the current datetime. """
	return "{y:4d}{m:02d}{d:02d}{h:02d}{min:02d}/".format(y=dtime.year,
			m=dtime.month, d=dtime.day, h=dtime.hour, min=dtime.minute)


def simple_step(projname, emr_jar_info, inputdir, jarargs,
				classname=None, outputdir=None):
	"""
	Create the step used in simple job (i.e. one step) creation. Returns a
	one-element list with a step in it.
	"""
	add_data_dir_args(inputdir, emr_jar_info, jarargs, outputdir)
	return [JarStep(name=projname + " Step 1", jar=_build_jar_loc(emr_jar_info),
			step_args=jarargs, main_class=classname)]


def _build_jar_loc(emr_jar_info):
	"""
	Builds URI for jar to be submitted using the named tuple
	argument as the input. If the emr_jar_info's name_prefix
	property is None then it is not used in the function.
	"""

	if emr_jar_info.name_prefix is None:
		return "s3n://{bucket}/{name}-{version}.jar".format(
				bucket=emr_jar_info.bucket,
				name=emr_jar_info.name,
				version=emr_jar_info.version)
	else:
		return "s3n://{bucket}/{jar_name_prefix}/{name}-{version}.jar".format(
				bucket=emr_jar_info.bucket,
				jar_name_prefix=emr_jar_info.name_prefix,
				name=emr_jar_info.name,
				version=emr_jar_info.version)


def add_data_dir_args(inputdir, emr_jar_info, jarargs, outputdir=None):
	"""
	Appending input and output directories to arglist passed to the jar's main
	class.

	str inputdir:   directory where input data lives
	namedtuple emr_jar_info: contains the name of the jar the bucket and the
	version number used to add to the jarargs
	[strs] jarargs: list of class arguments to be updated

	Modifies jar arglist in place, returns nothing.
	"""
	if inputdir.find("s3n://") == -1:
		inputdir = "s3n://" + inputdir
	if outputdir is None:
		outputdir = _build_output_dir(emr_jar_info)
	if outputdir.find("s3n://") == -1:
		outputdir = "s3n://" + outputdir
	jarargs.append(inputdir)
	jarargs.append(outputdir)


def _build_output_dir(emr_jar_info):
	""" Builds the output directory path where job output will be stored. """
	return "{pre}{job}/{ver}/{date}".format(
			pre=OUTPUT_DIR_PREFIX, job=emr_jar_info.name, ver=emr_jar_info.version,
			date=build_date_directory())


def get_job(conn, job_id):
	""" Retrieve specific jobflow from AWS.

	EMRConnection conn:		connection to AWS EMR
	str job_id:				job id to retrieve

	Returns boto.emr.emrobject.JobFlow with job info.
	"""
	return conn.describe_jobflow(job_id)


def get_jobs(conn, options):
	""" Retrieve list of jobs from AWS. Filters based on user options.

	EMRConnection conn:		connection to AWS EMR
	options:				options obj generated by optparse.OptionParser

	Returns list of boto.emr.emrobject.JobFlows.
	"""
	emropts = _create_list_filter(options)
	return conn.describe_jobflows(**emropts)


def terminate_job(conn, job_id):
	""" Terminates EMR jobflow.

	EMRConnection conn:		connection to AWS EMR
	str job_id:				string with job id to terminate
	"""
	conn.terminate_jobflow(job_id)


def _create_list_filter(options):
	"""
	Options for filtering jobflow list.

	options:				options obj generated by optparse.OptionParser

	Returns dict/hashmap with filters to supply to get_jobs().
	"""
	emropts = dict()
	if options.state:
		emropts['states'] = [options.state.upper()]
	if options.beforetime:
		emropts['created_before'] = convert_to_datetime(
				options.beforetime)
	if options.aftertime:
		emropts['created_after'] = convert_to_datetime(options.aftertime)
	return emropts


def convert_to_datetime(datestr):
	"""
	Convert user option of format YYYYMMDDhhmm to Python datetime. Also
	handles datetime string returned by boto for various jobs.

	str datestr:			date string in YYYYMMDDhhmm or YYYYMMDD format

	Returns datetime.datetime obj with date chosen by user.
	"""
	if len(datestr) == 8:
		dtime = datetime.datetime.strptime(datestr, '%Y%m%d')
	elif len(datestr) == 12:
		dtime = datetime.datetime.strptime(datestr, '%Y%m%d%H%M')
	else:		# handle YYYY-MM-DDThh:mm:ssZ stamps from boto
		dtime = datetime.datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%SZ")
	return dtime


def get_latest_jar_version(jarname_prefix, bucket_name, options):
	"""
	This method takes a jarname_prefix a bucket name and an options argument
	gotten from a OptionParser.parse_args and returns a sorted array of the
	versions of the jar present in the bucket.  It returns None if nothing is
	found in the bucket or if there were no names confirming to the
	JAR_NAME_PATTERN or the JAR_NAME_PATTERN_MAJ_MIN_REV.
	Additionally if the keyname minus the version number matches more than one
	string, so for example, if your prefix was "foo" it will match foo-0.1.1.jar
	and foobarbam-0.1.2.jar, it will return you the file name prefix for which
	the highest version was detected.  So in the above example it will return
	you foobarbam as the prefix cause it has the highest version number.
	This maynot be desirable but it is acceptable and atleast its documented here
	so that you can revisist this strategy.
	Note the usage of StrictVersion from distutils for version comparison.
	"""
	jar_versions_names = get_all_jar_versions_names(
			jarname_prefix, bucket_name, options)
	if len(jar_versions_names) > 0:
		return sorted(
			jar_versions_names,
			cmp = get_jar_version_cmp(),
			key = lambda x: x[0])[-1]
	else:
		return None


def get_jar_version_cmp():
	return lambda x, y: StrictVersion(x).__cmp__(StrictVersion(y))

def get_all_jar_versions_names(jarname_prefix, bucket_name, options):
	"""
	Returns a list of tuples, each containing the jar version and the
	key name that match the prefix jarname_prefix, present in the bucket_name.
	bucket
	"""
	keys = _get_keys(bucket_name, jarname_prefix, options)
	jar_versions = []
	for key in keys:
		key_name = key.name
		# key_name = key_name.split('/')[-1]
		matcher = JAR_NAME_PATTERN.match(key_name) or \
					JAR_NAME_PATTERN_MAJ_MIN_REV.match(key_name)
		# Look at documentation of emr-new job for these specific
		# checks, and on what is a valid jar name.
		if matcher:
			groups = matcher.groups()
			if len(groups) == 3 and \
				all([i is not None for i in groups]):
					jar_versions.append((groups[1], groups[0]))
	return jar_versions


def get_jar_info_from_name_version(
			jarname_prefix, bucket_name,
			jar_version, options):
	"""
	We do an initial check if the jar you requested is actually present
	in the right jar_version and if its not None is returned.
	"""
	jar_version_names = get_all_jar_versions_names(jarname_prefix,
												bucket_name, options)
	if len(jar_version_names) != 0 and \
			jar_version in set([v for v, _ in jar_version_names]):
		required_idx = [idx for idx, (_version, name) in enumerate(jar_version_names)
			if _version == jar_version][0]

		jar_key_name = jar_version_names[required_idx][1]
		if jar_key_name.find('/') == -1:
			# Return emr jar info with name_prefix = None
			return JARINFO(bucket=bucket_name, version=jar_version,
					name=jar_key_name, name_prefix=None)
		else:
			splits = jar_key_name.split('/')
			name_prefix = '/'.join(splits[0:-1])
			jar_name = splits[-1]
			return JARINFO(bucket=bucket_name, version=jar_version,
					name=jar_name, name_prefix=name_prefix)
	# No jar found
	return None


def get_jar_info_from_name(jar_name_prefix, bucket_name, options):
	"""
	This is used to construct jar info named tuple from the jar_name_prefix and
	the bucket_name. The options are used to obtain the credentials and thus the
	connection.  The latest version of the jar is picked from the bucket.
	returns JAR_INFO. If atleast one jar version is not found for the given
	prefix an error is raised.
	"""

	jar_version, jar_key_name = get_latest_jar_version(jar_name_prefix,
										bucket_name, options) or (None, None)
	if jar_version is None or jar_key_name is None:
		raise ValueError('Could not find any jars in the build bucket with'
						' provided jar name: %s ' % options.jar_name_prefix)
	if jar_key_name.find('/') == -1:
		# Return emr jar info with name_prefix = None
		return JARINFO(bucket=bucket_name, version=jar_version,
				name=jar_key_name, name_prefix=None)
	else:
		# Return emr jar info with name_prefix derived
		# from the name itself.
		splits = jar_key_name.split('/')
		name_prefix = '/'.join(splits[0:-1])
		jar_name = splits[-1]
		return JARINFO(bucket=bucket_name, version=jar_version,
				name=jar_name, name_prefix=name_prefix)


def get_jar_info_from_url(jar_url, options):
	"""
	Makes sure that the jar_url you mentioned is on S3 and
	then returns you the jar info.
	"""
	matcher = JAR_URI_PATTERN.match(jar_url) or \
				JAR_URI_PATTERN_MAJ_MIN_REV.match(jar_url)
	groups = matcher.groups()
	prefix = groups[0]
	splits = prefix.split('/')
	if len(splits) < 2:
		raise ValueError('URL %s is malformed' % jar_url, jar_url)
	bucket_name = splits[0]
	jar_name_prefix = '/'.join(splits[1:-1])
	jar_name = splits[-1]
	jar_version = groups[1]
	extension = groups[2]
	prefix = '{name_prefix}-{jar_version}{extension}'.format(
		name_prefix = '/'.join(splits[1:]), jar_version = jar_version,
		extension = extension)
	keys = _get_keys(bucket_name, prefix, options)
	if keys is None:
		return None
	if len([k for k in keys]) == 0:
		return None
	return JARINFO(bucket = bucket_name, version = jar_version, name = jar_name,
							name_prefix = None if jar_name_prefix == '' else jar_name_prefix)


def validate_jar_params(options, parser):
	"""
	This method is used by the emr-new-job script and it
	specifically validates if the --jar-url, --jar-name-prefix, --jar-version
	are properly set and valid.
	"""
	# The correct combination of the jar options must be present
	if options.jar_name_prefix and options.jar_url:
		parser.error("Only one of jar_name_prefix/jar_url can be sepecified")
		return False
	if not options.jar_name_prefix and not options.jar_url:
		parser.error("At least one of jar_name_prefix/jar_url must be specified")
		return False
	if options.jar_url and options.jar_version:
		parser.error("You need to specify the version as a part of the"
					" [jar_url]. Like s3n://my_bucket/foo-0.1.1.jar"
					" This option is only specified with the --jar_name_prefix"
					" argument")
		return False

	#Checks for the jar_name_url
	if options.jar_url is not None:
		matcher = JAR_URI_PATTERN.match(options.jar_url) or \
				JAR_URI_PATTERN_MAJ_MIN_REV.match(options.jar_url)
		if matcher is None:
			parser.error("The jar url does not meet the requisite pattern. Check Docs")
			return False
		else:
			# If it matches the pattern then it must be a specific pattern
			# as documented in emr-new-job
			groups = matcher.groups()
			prefix = groups[0]
			splits = prefix.split('/')
			if len(splits) < 2:
				return False
			if len(groups) < 3 or groups[-1] != '.jar' \
					or any([i is None for i in groups]):
				parser.error('Jar name does not follow requisite pattern. '
							'Check docs for right pattern of the jar')
				return False
			else:
				return True


def _get_keys(bucket_name, prefix, options):
	"""
	This is a common method we use to get keys from a s3 bucket given a bucket a
	prefix and options containing the information to validate the user's
	credentials
	"""
	creds = get_keys(options)
	conn = k_aws_s3.connect(creds, bucket_name=bucket_name)
	bucket = conn.lookup(bucket_name, validate=False)
	if bucket is None:
		raise ValueError('No bucket named %s found' % bucket_name, bucket_name)
	return bucket.list(prefix=prefix)
