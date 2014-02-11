import os
import boto
import boto.sqs
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

def connect(creds):
	"""
	Connect to simple db, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.sqs.connection.SDBConnection.
	"""
	if isinstance(creds, AwsCreds):
		return boto.connect_sqs(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.sqs.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)

def get_sqs_options(parser):
	parser.add_option("-q", "--queue", dest="queue", help="SQS Queue Name (uses SQS_QUEUE environment variable if not set)")
	parser.add_option("-t", "--timeout", dest="timeout", help="SQS Queue timeout (default 30)")
	parser.set_defaults(timeout = "30")

def get_queue(conn, options):
	queue_name = None
	if os.environ.has_key("SQS_QUEUE"):
		queue_name = os.environ["SQS_QUEUE"]
	if options.queue:
		queue_name = options.queue
	if not queue_name:
		sys.stderr.write("No queue name passed in \n")
		sys.exit(1)
	queue = conn.get_queue(queue_name)
	if not queue:
		queue = conn.create_queue(queue_name, int(options.timeout))
	return queue

