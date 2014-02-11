"""The k.aws.config module provides a couple of distinct convenience
functions.  The first is that for tools to be run on the command line,
it provides a standardized set of options which can be added to other
option parsing, via the k.aws.config.get_aws_options() function.

This adds the following flags to an OptionParser object, and the
corresponding attributes to the options object that is returned when
obj.parse_args() is invoked.

    Option                           Attribute
    ======                           =========
    -A, --access-key                 obj.access_key (str)
    -S, --secret-key                 obj.secret_key (str)
    -e, --env                        obj.knewton_env (str)
    --ro                             obj.readwrite (bool)
    --rw                             obj.readwrite (bool)
    --file                           obj.forcefile (bool)
    --iam                            obj.forceiam (bool)

After the above options have been parsed, the OptionParser object
should be passed to the second distinct part, which gets
authentication credentials, the k.aws.config.get_keys() call.

The get_keys() call will attempt to get credentials for use from the
following sources:

 * The environment passed in via -e, and stored in obj.knewton_env
 * The -A/-S pair of obj.access_key and obj.secret_key.
 * The file that --file points to/obj.forcefile

When -e is used, it will by default look for keys in the environment
that is specified.  When there is no matching environment then this
takes on a special meaning.  So, given a name, e.g. 'stack_iam' that
doesn't match an environment, an attempt to open a file called
/etc/knewton/configuration/aws/<that_name>.yml will happen.  If that
file can be opened and read then the aws credentials will be read from
there.  'stack_iam' should be present on all Knewton platform hosts,
though there could be others in the future.

If obj.forcefile isn't set to True:

 - get_keys() will first attempt to load credentials from files in the
   locations that are standard at knewton, e.g. ~/.aws/*-conf.

If obj.forcefile is set to True:

 - get_keys() will try to load credentials from files in the locations
   that are standard at knewton, e.g. ~/.aws/*-conf.

In both cases, the collections.namedtuple 'AwsCreds' will be returned,
which consists of the following fields:
('access', 'secret', 'token', 'mode', 'env', 'privatekey', 'cert')

The environment variables that are required will be created as a
convenience, in addition to being available via the namedtuple.

Example:

    from optparse import OptionParse
    from boto import exception
    [...]
    usage = "usage: %prog [options] [key]\\n\\n"
    usage += "Gets the given key from an s3 bucket"
    parser = OptionParser(usage=usage)
    k.aws.config.get_aws_options(parser)
    k.aws.s3.get_s3_options(parser)
    [...]
    opts, _ = parser.parse_args()
    try:
        creds = k.aws.config.get_keys(opts)
        conn = k.aws.s3.connect(creds)
        bucket = k.aws.s3.get_bucket(conn, opts)
        get(bucket, opts.prefix, args[0])
    except boto.exception.BotoServerError, e:
        sys.stderr.write(e.message + "\\n")

After that's done, you should have a standard boto.s3 object in this
case.  Most other aws services should be usable, though the STS
interface has been presenting some challengs with CloudFormation at
the time this is being written (Aug 6, 2012).
"""


import ConfigParser
import logging
import os
import os.path
import sys
import glob
import json
import datetime
import re
import types
import yaml
import requests
from collections import namedtuple
from k.stdlib.collections import defaultnamedtuple


AWS_PATH = '/etc/knewton/configuration/aws'
K_AWS_PATH = os.path.expanduser('~/.k.aws')
DEFAULT_REGION_NAME = 'us-east-1'
OUTPUT_FORMATS = ['json', 'tsv', 'text']

AwsCreds = defaultnamedtuple(
	'AwsCreds',
	['access', 'secret', 'token', 'mode', 'env', 'privatekey', 'cert'],
	access=None, secret=None, token=None, mode=None, env=None,
	privatekey=None, cert=None)

RegionAwsCreds = namedtuple('RegionAwsCreds', ['region_name', 'creds'])

ManualOptions = defaultnamedtuple(
	'ManualOptions',
	["access_key", "secret_key", "knewton_env", "readwrite", "forcefile", "forceiam"],
	access_key=None, secret_key=None, knewton_env=None, readwrite=None,
	forcefile=None, forceiam=None)

def connection_hash(creds):
	return {
		'aws_access_key_id': creds.access,
		'aws_secret_access_key': creds.secret,
		'security_token': creds.token
	}

def region_connection_hash(region_creds):
	return {
		'region_name': region_creds.region_name,
		'aws_access_key_id': region_creds.creds.access,
		'aws_secret_access_key': region_creds.creds.secret,
		'security_token': region_creds.creds.token
	}

def get_box_iam_keys(account, timeout=0.1):
	"""Try to load keys from the instance IAM role"""
	url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/{0}".format(account)
	try:
		keys = json.loads(requests.get(url, timeout=timeout).content)
		# make IAM keys look like our login.knewton.net provided keys
		keys['accessKeyId']     = keys['AccessKeyId']
		keys['secretAccessKey'] = keys['SecretAccessKeyId']
		keys['expiration']      = keys['Expiration']
		keys['sessionToken']    = keys['Token']
		# the access is actually provided by the IAM role, so
		# the access mode isn't relevent
		return _generate_cache(account, "rw", keys)
	except ValueError:
		return None
	except requests.ConnectionError:
		return None
	except requests.Timeout:
		return None

def _generate_cache(account, access, keys):
	"""Generates an on-disk cache of the time-limited keys that've
	been obtained, so it can be re-used without extra expensive calls
	"""
	if not os.path.exists(K_AWS_PATH):
		os.mkdir(K_AWS_PATH, 0700)
	env_dir = os.path.join(K_AWS_PATH, "%s-%s" % (account, access))
	if not os.path.exists(env_dir):
		os.mkdir(env_dir, 0700)
	cache = {
		'access': keys['accessKeyId'],
		'secret': keys['secretAccessKey'],
		'token': keys['sessionToken'],
		'expiration': keys['expiration'],
		'account': account,
		'readmode': access
	}
	filename = os.path.join(env_dir, "keys.yml")
	with open(filename, "w") as yaml_file:
		yaml_file.write(yaml.dump(cache))
	return cache

def _load_cache(account, access):
	"""Load an existing cache from the filesystem and use it if it's
	there.
	"""
	env_dir = os.path.join(K_AWS_PATH, "%s-%s" % (account, access))
	filename = os.path.join(env_dir, "keys.yml")
	if not os.path.exists(filename):
		return
	with open(filename) as yaml_file:
		cache = yaml.load(yaml_file)
	logging.info('_load_cache: loading STS cache from {0}'.format(filename))
	return cache

def _is_cache_valid(cache):
	"""Checks to see whether the cache has endured beyond its useful
	lifespan
	"""
	try:
		cache_time = datetime.datetime.strptime(
			cache['expiration'], '%Y-%m-%dT%H:%M:%S.%fZ')
	except ValueError:
		# The values that amazon returns may have changed?
		cache_time = datetime.datetime.strptime(
			cache['expiration'], '%Y-%m-%dT%H:%M:%SZ')
	if datetime.datetime.utcnow() < cache_time:
		return True
	logging.info('_is_cache_valid: STS cache has expired')
	return False

def get_region_keys(options):
	"""
	Extract the region name and credentials, from the options.

	:param options: options returned from OptionParser().parse_args()
	:type options: optparse.Values

	:rtype: k.aws.config.RegionAwsCreds
	"""
	region_name = options.region
	if not region_name:
		region_name = DEFAULT_REGION_NAME

	creds = get_keys(options)

	return RegionAwsCreds(region_name, creds)

def _get_creds_from_options(options):
	"""Get credentials from command-line options.
	"""
	if not (options.access_key and options.secret_key):
		logging.info('_get_creds_from_options: either access or secret was not specified')
		return None

	logging.info('_get_creds_from_options: retrieved credentials')
	return AwsCreds(options.access_key, options.secret_key)

def _get_creds_from_environment_yaml(env):
	"""Get credentials from the relevant ~/.k.aws/<ENV>.yml or
	/etc/knewton/configuration/aws/<ENV>.yml file.
	"""
	if not env:
		logging.info('_get_creds_from_environment_yaml: env was not specified')
		return None

	aws_conf = _parse_aws_confs()
	if env not in aws_conf:
		logging.info('_get_creds_from_environment_yaml: {0} is not amongst the yaml'
				' config files'.format(env))
		return None

	token = None
	readmode = None
	access = aws_conf[env]['access_key']
	secret = aws_conf[env]['secret_key']
	if not (access and secret):
		logging.info('_get_creds_from_environment_yaml: either access or secret was not specified')
		return None

	privatekey = aws_conf[env].get('private_key')
	if privatekey:
		if not os.path.exists(privatekey):
			privatekey = None

	cert = aws_conf[env].get('cert')
	if cert:
		if not os.path.exists(cert):
			cert = None

	logging.info('_get_creds_from_environment_yaml: retrieved credentials')
	return AwsCreds(access, secret, token, readmode, env, privatekey, cert)

def _get_creds_from_legacy_aws_conf(env):
	filename = os.path.expanduser("~/.aws/aws-" + env + ".conf")
	if not os.path.exists(filename):
		filename = os.path.expanduser("~/aws/aws-" + env + ".conf")
	if not os.path.exists(filename):
		return
	conf = {}
	keylist = ['AMAZON_ACCESS_KEY_ID', 'AMAZON_SECRET_ACCESS_KEY']
	fh = open(os.path.expanduser(filename))
	lines = fh.readlines()
	fh.close()
	for line in lines:
		line = re.sub("#.*", "", line)
		line = line.replace("export ", "").strip()
		line = line.replace("'", "")
		line = line.replace("\"", "")
		chunks = line.split("=", 1)
		if len(chunks) == 2:
			if chunks[0] in keylist:
				conf[chunks[0]] = chunks[1]
	if conf.has_key('AMAZON_ACCESS_KEY_ID') and conf.has_key('AMAZON_SECRET_ACCESS_KEY'):
		return AwsCreds(conf['AMAZON_ACCESS_KEY_ID'], conf['AMAZON_SECRET_ACCESS_KEY'], None, None, env, None, None)


def _get_creds_from_aws_config_file(profile):
	"""Get access and secret keys from the file defined in the
	AWS_CONFIG_FILE environment variable, using the environment name
	as the profile name (section header).
	"""
	access, secret, token, region = None, None, None, None

	if 'AWS_CONFIG_FILE' not in os.environ:
		logging.info('_get_creds_from_aws_config_file: environemnt variable'
				' AWS_CONFIG_FILE is not specified')
		return None

	if not os.access(os.environ['AWS_CONFIG_FILE'], os.R_OK):
		logging.info('_get_creds_from_aws_config_file: {0} is not'
				' readable'.format(os.environ['AWS_CONFIG_FILE']))
		return None

	config = ConfigParser.RawConfigParser()
	config.read(os.environ['AWS_CONFIG_FILE'])

	if config.has_option(profile, 'aws_access_key_id'):
		access = config.get(profile, 'aws_access_key_id')

	if config.has_option(profile, 'aws_secret_access_key'):
		secret = config.get(profile, 'aws_secret_access_key')

	if config.has_option(profile, 'sts_token'):
		token  = config.get(profile, 'sts_token')

	# NOTE: Not using region, from the config file, right now.
	if config.has_option(profile, 'region'):
		region = config.get(profile, 'region')

	if not (access and secret):
		logging.info('_get_creds_from_aws_config_file: either access or'
				' secret was not specified')
		return None

	logging.info('_get_creds_from_aws_config_file: retrieved credentials')
	return AwsCreds(access, secret, token=token)

def _get_creds_from_aws_credential_file():
	"""Get access and secret keys from the file defined in
	the AWS_CREDENTIAL_FILE environment variable.
	"""
	access, secret = None, None

	if 'AWS_CREDENTIAL_FILE' not in os.environ:
		logging.info('_get_creds_from_aws_credential_file: environment'
			' variable AWS_CREDENTIAL_FILE was not specified')
		return None

	if not os.access(os.environ['AWS_CREDENTIAL_FILE'], os.R_OK):
		logging.info('_get_creds_from_aws_credential_file: {0} is not'
				' readable'.format(os.environ['AWS_CREDENTIAL_FILE']))
		return None

	with open(os.environ['AWS_CREDENTIAL_FILE'], 'r') as _file:
		for line in _file:
			(key, value) = line.partition('=')[::2]
			(key, value) = (key.strip(), value.strip())
			if key == 'AWSAccessKeyId':
				access = value
			elif key == 'AWSSecretKey':
				secret = value

	if not (access, secret):
		logging.info('_get_creds_from_aws_credential_file: either access'
				' or secret was not specified')
		return None

	logging.info('_get_creds_from_aws_credential_file: retrieved credentials')
	return AwsCreds(access, secret)

def _get_creds_from_environ():
	"""Get access and secret keys from the AWS_ACCESS_KEY
	and AWS_SECRET_KEY environment variables, respectively,
	or the legacy variable names.
	"""
	access, secret = None, None

	# latest access key name
	access = access or os.environ.get('AWS_ACCESS_KEY')
	# legacy access key names
	access = access or os.environ.get('AMAZON_ACCESS_KEY_ID')
	access = access or os.environ.get('AWS_ACCESS_KEY_ID')

	# latest secret key name
	secret = secret or os.environ.get('AWS_SECRET_KEY')
	# legacy secret key names
	secret = secret or os.environ.get('AMAZON_SECRET_ACCESS_KEY')
	secret = secret or os.environ.get('AWS_SECRET_ACCESS_KEY')

	privatekey = os.environ.get('EC2_PRIVATE_KEY')
	if privatekey and not os.path.exists(privatekey):
		privatekey = None

	cert = os.environ.get('EC2_CERT')
	if cert and not os.path.exists(cert):
		cert = None

	if not (access and secret):
		logging.info('_get_creds_from_environ: either access or secret'
				' was not specified')
		return None

	logging.info('_get_creds_from_environ: retrieved credentials')
	return AwsCreds(access, secret, privatekey=privatekey, cert=cert)

def _set_environment(creds):
	"""Set environment variables, based on the credentials
	tuple.
	"""
	os.putenv("AWS_ACCESS_KEY_ID", creds.access)
	os.putenv("AWS_SECRET_ACCESS_KEY", creds.secret)

	if creds.token:
		os.putenv("AWS_SESSION_TOKEN", creds.token)
	elif "AWS_SESSION_TOKEN" in os.environ:
		del os.environ['AWS_SESSION_TOKEN']

def get_keys(options):
	"""
	Returns the access and secret key based on (in order of precidence):

	* -e from options passed in generated via get_aws_options
	* -A/-S from options passed in generated via get_aws_options
	* Environment variables: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
	"""
	# Refrence: http://www.dowdandassociates.com/content/howto-install-aws-cli-security-credentials

	if not hasattr(options, "access_key"):
		raise Exception("access_key needs to exist in your options object")
	if not hasattr(options, "secret_key"):
		raise Exception("secret_key needs to exist in your options object")

	env = None
	if hasattr(options, "knewton_env"):
		env = options.knewton_env
	if not env:
		if 'AWS_ACCOUNT' in os.environ:
			env = os.environ['AWS_ACCOUNT']
		else:
			if options.forceiam:
				#if there is no env and we are using force iam, we default
				#to the instance profile autoconfigure.
				return AwsCreds()

	readmode = 'readonly'
	trm = os.environ.get('AWS_READMODE')
	if trm and trm != '':
		readmode = os.environ['AWS_READMODE']
	if options.readwrite:
		readmode = 'readwrite'

	creds = None

	# Keys passed in as options take priority.
	creds = creds or _get_creds_from_options(options)

	if env:
		# Grab credentials from the yaml config files.
		creds = creds or _get_creds_from_environment_yaml(env)

		# Grab credentials from legacy ~/.aws/aws-*.conf
		creds = creds or _get_creds_from_legacy_aws_conf(env)

		# Grab credentials from the aws config file.
		creds = creds or _get_creds_from_aws_config_file(env)

	# Grab credentials from the aws credential file.
	creds = creds or _get_creds_from_aws_credential_file()

	# Grab credentials from the environment.
	creds = creds or _get_creds_from_environ()

	if not creds:
		raise Exception("Unable to determine credentials.")

	_set_environment(creds)

	return creds

def _parse_aws_confs():
	"""Read aws config from yaml files located in the AWS_PATH.  This
	provides file-based credentials (e.g. IAMs generated by
	CloudFormation that are persisted on the filesystem).
	"""
	configs = {}

	if os.path.exists(AWS_PATH):
		for config_file in glob.glob(os.path.join(AWS_PATH, "*.yml")):
			path, fn = os.path.split(config_file)
			env, ext = fn.split('.')
			with open(config_file) as yaml_file:
				configs[env] = yaml.load(yaml_file)

	# config files under ~/.k.aws should override those under
	# /etc/knewton/configuration/aws
	if os.path.exists(K_AWS_PATH):
		for config_file in glob.glob(os.path.join(K_AWS_PATH, "*.yml")):
			path, fn = os.path.split(config_file)
			env, ext = fn.split('.')
			with open(config_file) as yaml_file:
				configs[env] = yaml.load(yaml_file)

	return configs

def get_keys_for_environment(env):
	"""Given an environment, get those keys from the appropriate
	aws configuration source."""
	aws_conf = _parse_aws_confs()
	access = aws_conf[env]['access_key']
	secret = aws_conf[env]['secret_key']
	return access, secret

def get_aws_options(parser, rw=False):
	"""Provide options that are needed to authenticate, etc. as a top-level
	feature of k.aws as opposed to being useful for a specific module.

	:type parser: optparse.OptionParser
	:param parser: This OptionParser object will be used to add options to.

	:type rw: bool
	:param rw: If this is set to True, the "--ro" option will be added, and if
	           it is False, then the "--rw" option will be added.
	"""
	parser.add_option(
		"-A", "--access-key", dest="access_key",
		help=' '.join(["Amazon Access Key (Defaults to AWS_ACCESS_KEY_ID",
			"environment variable if not set)"]))
	parser.add_option(
		"-S", "--secret-key", dest="secret_key",
		help=' '.join(["Amazon Secret Key (uses AWS_SECRET_ACCESS_KEY",
			"environment variable if not set)"]))
	parser.add_option(
		"-e", "--env", dest="knewton_env",
		help=' '.join(["Use passed in Knewton environment to connect",
			"(production, staging, utility, stack_iam, etc)"]))
	if rw:
		parser.add_option(
			"--ro", dest="readwrite",
			help="Force use of a read only key (Default: readwrite)",
			default=True,
			action="store_false")
	else:
		parser.add_option(
			"--rw", dest="readwrite",
			help="Force use of a read write key (Default: readonly)",
			default=False,
			action="store_true")
	parser.add_option(
		"--iam", "--forceiam", dest="forceiam",
		help="Force use of box IAM (Default: False)",
		default=False,
		action="store_true")
	parser.add_option(
		"--file", "--forcefile", dest="forcefile",
		help="Force use of files in knewton config (Default: False)",
		default=False,
		action="store_true")
	parser.add_option(
		"--duration", dest="duration", default=3600,
		help=' '.join(["Duration for Kerberos ticket. (Default: 3600, ",
			"range=3600-129600)"]))
	return parser

def get_file_option(parser, help_text=None):
	if help_text is None:
		help_text = "local filename"

	parser.add_option("-f", "--filename", dest="filename", default=None,
			help=help_text)

	return parser

def get_directory_option(parser, help_text=None):
	if help_text is None:
		help_text = "local directory"

	parser.add_option("-d", "--directory", dest="directory", default=None,
			help=help_text)

	return parser

def get_prefix_option(parser, help_text=None):
	if help_text is None:
		help_text = "s3 key prefix"

	parser.add_option("-p", "--prefix", dest="prefix", default=None,
			help=help_text)

	return parser

def get_verbose_option(parser, help_text=None):
	"""
	Add the verbose option to the option parser.

	:param parser: option parser
	:type parser: optparse.OptionParser

	:param help_text: help text for the option
	:type help_text: str or None

	:rtype: None
	"""
	if help_text is None:
		help_text = "verbose output"

	parser.add_option("--verbose", dest="verbose", action="store_true",
			default=False, help=help_text)

def get_region_option(parser, help_text=None):
	"""
	Add the region option to the option parser.

	:param parser: option parser
	:type parser: optparse.OptionParser

	:param help_text: help text for the option
	:type help_text: str or None

	:rtype: None
	"""
	if help_text is None:
		help_text = "aws region"

	parser.add_option("-r", "--region", dest="region", default=None,
			help=help_text)

	return parser

def get_format_option(parser, formats=None, default=None):
	"""
	Add the format option to the option parser.

	:param parser: option parser
	:type parser: optparse.OptionParser
	:param formats: output format choices
	:type formats: list(str)
	:param default: default output format
	:type default: str

	:rtype: None
	"""
	if formats is None or type(formats) not in (types.ListType, types.TupleType) or len(formats) == 0:
		raise Exception("You must pass in a list of output format choices, to get_format_option.")

	for _format in formats:
		if _format not in OUTPUT_FORMATS:
			raise Exception("{0} is not a recognized format: {1}.".format(_format, OUTPUT_FORMATS))

	if default not in formats:
		raise Exception("The proposed default, {0}, is not in the list of provided options: {1}".format(default, formats))

	help_text = "Output format ({0}; DEFAULT: {1})".format(', '.join(formats), default)

	parser.add_option("--format", dest="format", type="choice", default=default,
			choices=tuple(formats), help=help_text)

	return parser

# Local Variables:
# tab-width: 4
# indent-tabs-mode: t
# End:
