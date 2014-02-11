import os
import sys
import boto
import boto.route53
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

def connect(creds):
	"""
	Connect to autoscale, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.route53.connection.Route53Connection
	"""
	if isinstance(creds, AwsCreds):
		return boto.connect_route53(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.route53.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)
