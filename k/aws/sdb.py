import os
import sys
import boto
import boto.sdb
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

def connect(creds):
	"""
	Connect to simple db, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.sdb.connection.SDBConnection.
	"""
	if isinstance(creds, AwsCreds):
		return boto.connect_sdb(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.sdb.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)

def get_sdb_options(parser):
	parser.add_option("-d", "--domain", dest="domain", help="SDB Domain Name (uses SDB_DOMAIN environment variable if not set")

def get_domain(conn, options):
	domain_name = None
	if os.environ.has_key("SDB_DOMAIN"):
		domain_name = os.environ["SDB_DOMAIN"]
	if options.domain:
		domain_name = options.domain
	if not domain_name:
		sys.stderr.write("No domain name passed in \n")
		sys.exit(1)
	try:
		return conn.get_domain(domain_name)
	except boto.exception.SDBResponseError:
		return conn.create_domain(domain_name)

def write_item(domain, name, data):
	rmlist = []
	for key in data:
		if data[key] == None:
			rmlist.append(key)
	for key in rmlist:
		del data[key]
	if len(rmlist) > 0:
		domain.delete_attributes(name, rmlist)
	if len(data) > 0:
		item = domain.put_attributes(name, data)
