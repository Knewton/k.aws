import os
import boto
import boto.iam
import k.aws.util
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash


def connect(creds):
	"""
	Connect to autoscale, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.iam.connection.IAMConnection

	Note IAM cannot authenticate a user using STS credentials.
	At Knewton we use this for our kerberos bridge, so for now
	this code may fail, so some checking nees to be added"""
	if isinstance(creds, AwsCreds):
		return boto.connect_iam(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.iam.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)


def get_iam_options(parser):
	"""Here so options can be added later"""
	return parser

def get_all_users(conn):
	"""Iterator that yields all IAM users, to simplify getting all
	users.  The amazon interface presents a "next" token that has to
	be guarded.  This makes that easier.  However it doesn't present
	the returned object, only the list of user dicts
	
	Enables this sort of search:
	In [210]: r = get_all_users(iconn)
	
	In [211]: for user in r:
	if user['user_id'] == iam_user:
	    print user
	
	:type conn: boto.iam.connection object
	:param conn: object as returned by connect()
	"""
	def data_lookup(response):
		return response['list_users_response']['list_users_result']['users']
	def marker_lookup(response):
		return response['list_users_response']['list_users_result']['marker']
	return k.aws.util.yield_aws_data(conn.get_all_users, 'marker', marker_lookup, data_lookup)


# Local Variables:
# tab-width: 4
# indent-tabs-mode: t
# End:
