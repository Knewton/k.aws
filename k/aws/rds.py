import boto.rds
import logging
from datetime import datetime
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

def connect(creds):
	"""
	Connect to RDS, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.rds.RDSConnection
	"""
	if isinstance(creds, AwsCreds):
		return boto.connect_rds(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.rds.connect_to_region(**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)

def prune_snapshots(conn, instance_id, keep, prefix=None, dryrun=False):
	"""
	Delete old RDS snapshots, given some criteria.

	:param conn: The connection object you get from the connect \
	method, in this file.
	:type conn: boto.rds.RDSConnection

	:param instance_id: The identifier of the database
	:type instance_id: str

	:param keep: The number of prior snapshots to keep.
	:type keep: bool

	:param prefix: Only consider snapshots beginning with \
	prefix, if one is provided.
	:type prefix: str or None

	:param dryrun: If set to True, no changes will be made \
	to the AWS account.
	:type dryrun: bool

	:rtype: None
	"""
	# Find all snapshots, for the given instance.
	snapshots = conn.get_all_dbsnapshots(instance_id=instance_id)

	# Filter by the prefix, if one is provided.
	if prefix:
		filtered_snapshots = [s for s in snapshots if s.id.startswith(prefix)]
	else:
		filtered_snapshots = snapshots[:]

	# Select all but the N (keep) most recent snapshots.
	old_snapshots = sorted(filtered_snapshots, key=lambda s: s.snapshot_create_time)[:-keep]
	for snapshot in old_snapshots:
		if snapshot.status != 'available':
			logging.info("Skipping deletion of snapshot {0}, as it is not in the"
					" available state: {1}".format(snapshot.id, snapshot.status))
			continue

		if dryrun:
			logging.info("[DRYRUN] Deleting snapshot {0}".format(snapshot.id))
		else:
			logging.info("Deleting snapshot {0}".format(snapshot.id))
			conn.delete_dbsnapshot(snapshot.id)

def create_snapshot(conn, instance_id, prefix=None, dryrun=False):
	"""
	Create a snapshot of a given RDS instance.

	:param conn: The connection object you get from the connect \
	method, in this file.
	:type conn: boto.rds.RDSConnection

	:param instance_id: The identifier of the database
	:type instance_id: str

	:param prefix: Prepend this prefix to the name of the snapshot, \
	if provided.
	:type prefix: str or None

	:param dryrun: If set to True, no changes will be made \
	to the AWS account.
	:type dryrun: bool

	:rtype: None
	"""
	now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-utc')
	if prefix:
		snapshot_id = '-'.join([prefix, now])
	else:
		snapshot_id = now

	if dryrun:
		logging.info("[DRYRUN] Creating snapshot {0} for instance {1}".format(snapshot_id, instance_id))
	else:
		logging.info("Creating snapshot {0} for instance {1}".format(snapshot_id, instance_id))
		conn.create_dbsnapshot(snapshot_id, instance_id)

# Local Variables:
# tab-width: 4
# indent-tabs-mode: t
# End:

