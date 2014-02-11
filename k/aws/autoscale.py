import os
import time
import boto
import boto.exception
import boto.ec2.autoscale
import k.aws.util
import functools
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

def connect(creds):
	"""
	Connect to autoscale, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.ec2.autoscale.AutoScaleConnection
	"""
	if isinstance(creds, AwsCreds):
		return boto.connect_autoscale(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.ec2.autoscale.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)

def get_autoscale_options(parser):
	"""Here so options can be added later"""
	return parser

def get_all_groups(conn, backoff_count=10):
	"""Iterator that yields all stacks, to simplify getting all of them.
	The amazon interface presents a "next_token" token that has to
	be guarded.  This makes that easier.

	:type conn: boto.ec2.autoscale.connection object
	:param conn: object as returned by connect()
	"""
	backoff_seq = range(3)
	if backoff_count > 3:
		backoff_seq = backoff_seq + [ (bo + 1) * 5 for bo in range(backoff_count - 3) ]
	def get_with_backoff(**kwargs):
		for count in range(backoff_count):
			try:
				return conn.get_all_groups(**kwargs)
			except boto.exception.BotoServerError as bse:
				time.sleep(backoff_seq[count])
				continue
	return k.aws.util.yield_aws_data(
		get_with_backoff,
		'next_token',
		k.aws.util.next_token_marker_lookup,
		k.aws.util.default_data_lookup)

def get_all_launch_configs(conn, backoff_count=10):
	"""Iterator that yields all launch configs, to simplify getting all of them.
	The amazon interface presents a "next_token" token that has to
	be guarded.  This makes that easier.

	:type conn: boto.ec2.autoscale.connection object
	:param conn: object as returned by connect()
	"""
	backoff_seq = range(3)
	if backoff_count > 3:
		backoff_seq = backoff_seq + [ (bo + 1) * 5 for bo in range(backoff_count - 3) ]
	def get_with_backoff(**kwargs):
		for count in range(backoff_count):
			try:
				return conn.get_all_launch_configurations(**kwargs)
			except boto.exception.BotoServerError as bse:
				time.sleep(backoff_seq[count])
				continue
	return k.aws.util.yield_aws_data(
		get_with_backoff,
		'next_token',
		k.aws.util.next_token_marker_lookup,
		k.aws.util.default_data_lookup)

# Local Variables:
# tab-width: 4
# indent-tabs-mode: t
# End:
