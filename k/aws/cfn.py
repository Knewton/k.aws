import os
import time
import boto
import boto.exception
import boto.cloudformation
import k.aws.util
import functools
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

def connect(creds):
	"""
	Connect to cloudformation, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.cloudformation.connection.CloudFormationConnection

	Note: IAM cannot authenticate a user using STS credentials."""
	if isinstance(creds, AwsCreds):
		return boto.connect_cloudformation(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.cloudformation.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)

def get_cfn_options(parser):
	"""Here so options can be added later"""
	return parser

def list_all_stacks(conn, stack_status_filters=['CREATE_COMPLETE', 'CREATE_FAILED', 'UPDATE_COMPLETE'], backoff_count=10):
	"""Iterator that yields all stacks, to simplify getting all of them.
	The amazon interface presents a "next_token" token that has to
	be guarded.  This makes that easier.

	:type conn: boto.iam.connection object
	:param conn: object as returned by connect()
	"""
	backoff_seq = range(3)
	if backoff_count > 3:
		backoff_seq = backoff_seq + [ (bo + 1) * 5 for bo in range(backoff_count - 3) ]
	def data_lookup(response):
		return response
	def marker_lookup(response):
		if response.next_token:
			return response.next_token
		else:
			raise KeyError, "no next_token"
	def lookup_with_backoff(**kwargs):
		for count in range(backoff_count):
			kwargs['stack_status_filters'] = stack_status_filters
			try:
				return conn.list_stacks(**kwargs)
			except boto.exception.BotoServerError as bse:
				time.sleep(backoff_seq[count])
				continue
	return k.aws.util.yield_aws_data(lookup_with_backoff, 'next_token', marker_lookup, data_lookup)

# Local Variables:
# tab-width: 4
# indent-tabs-mode: t
# End:
