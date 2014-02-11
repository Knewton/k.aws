import os
import boto
import boto.ec2.elb
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

def connect(creds):
	"""
	Connect to autoscale, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.ec2.elb.ELBConnection
	"""
	if isinstance(creds, AwsCreds):
		return boto.connect_elb(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.ec2.elb.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)


def get_elb_options(parser):
	"""Here so options can be added later"""
	return parser

