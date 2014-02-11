import copy
import itertools
import boto
import boto.ec2
import k.aws.util
import warnings
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

def connect(creds):
	"""
	Connect to ec2, with user-provided options.

	:param region_creds: The region name and AWS credentials.
	:type region_creds: k.aws.config.AwsCreds or k.aws.config.RegionAwsCreds

	:rtype: boto.ec2.connection.EC2Connection
	"""
	if isinstance(creds, AwsCreds):
		return boto.connect_ec2(**connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		return boto.ec2.connect_to_region(
			**region_connection_hash(creds))
	raise Exception("Unrecognized credential type: %s" % creds)

def parse_ec2_launch(lines, base):
	instances = []
	reservation = copy.copy(base)
	for line in lines:
		if line.startswith("RESERVATION"):
			reservation = copy.copy(base)
			parse_reservation_line(line, reservation)
		elif line.startswith("INSTANCE"):
			instance = parse_instance_line(line, copy.copy(reservation))
			instances.append(instance)
	return instances

def parse_reservation_line(line, reservation):
	chunks = line.strip().split("\t")
	set_field(reservation, chunks[1], 'reservation_id')
	set_field(reservation, chunks[2], 'user_id')
	set_field(reservation, chunks[3], 'groups')

def parse_instance_line(line, instance):
	chunks = line.strip().split("\t")
	if len(chunks) > 1:
		if(len(chunks) == 13): #defect in ec2-api-tools, sometimes there is one less field from ec2-run-instances
			chunks.insert(8, '')
		set_field(instance, chunks[1], 'instance_id')
		set_field(instance, chunks[2], 'ami_id')
		set_field(instance, chunks[3], 'external_dns')
		set_field(instance, chunks[4], 'internal_dns')
		set_field(instance, chunks[5], 'state')
		set_field(instance, chunks[6], 'key')
		instance['index'] = int(chunks[7])
		set_field(instance, chunks[8], 'type')
		set_field(instance, chunks[10], 'timestamp')
		set_field(instance, chunks[11], 'availability_zone')
		set_field(instance, chunks[12], 'aki_id')
		set_field(instance, chunks[13], 'ari_id')
	return instance

def set_field(instance, value, name):
	if(value == ""):
		instance[name] = None
	else:
		instance[name] = value

def get_account_id(conn):
	"""
	There's no direct API call for getting the account id
	so get it through a security group object.
	"""
	rs = conn.get_all_security_groups()
	if len(rs) > 0:
		return rs[0].owner_id
	else:
		warnings.warn("Coult not get account id.")
		return 0


def get_instance_from_name_or_ip(conn, name_or_ip):
	mapping = dict()
	for i in all_instances(conn):
		if name_or_ip in (i.private_dns_name, i.public_dns_name, i.ip_address, i.ip_address):
			return i.id
	return

def check_for_tag(instance, tag, desired_value):
	"""Check for a particular tag, and confirm that its value is what
	is equal to desired_value
	:type instance: boto.ec2.instance.Instance
	:param instance: The instance whose tags will be checked.
	
	:type tag: str
	:param tag: The name of the tag to be searched for
	
	:type desired_value: str
	:param desired_value: The string that indicates that kerberos is enabled.
	
	Returns: boolean
"""

	instance.update()
	if tag in instance.tags.keys():
		if instance.tags[tag] == desired_value:
			return True
	return False

def all_instances(conn):
	"""returns a list of instances, no reservations required"""
	all_instances = [ [inst for inst in res.instances] for res in conn.get_all_instances() ]
	return list(itertools.chain(*all_instances)) # flat-pack

def get_key_pair_name(creds):
	"""
	Returns the default key pair name to use when launching instances with in the account
	associated with the creds argument.
	"""
	ec2_conn = connect(creds)
	account_id = get_account_id(ec2_conn)
	return k.aws.util.get_key_pair_name(account_id)

class Instances(object):
	"""Gathers data from a particular account, and makes it easier to
	search for desired instance data
	"""
	def __init__(self, boto, account_info):
		"""Initialize with a reference to the boto module (so adding this class
doesn't break existing code that already calls boto) and the account
info that will be connected to.
		"""
		ec2                      = boto.connect_ec2(account_info[0], account_info[1])
		self.reservations        = ec2.get_all_instances()
		self.groups              = ec2.get_all_security_groups()
		self.instances           = all_instances(ec2)
		self.by_instance_id      = dict()
		self.by_private_ip       = dict()
		self.by_private_dns_name = dict()
		self.by_public_ip        = dict()
		self.by_public_dns_name  = dict()
		self._build_instance_id_map()
		self._build_private_ip_map()
		self._build_private_dns_name_map()
		self._build_public_ip_map()
		self._build_public_dns_name_map()

	def flatten_instances(self):
		instances = list()
		for r in self.reservations:
			instances.extend(r.instances)
		return instances

	def _build_instance_id_map(self):
		for instance in self.instances:
			self.by_instance_id[instance.id] = instance

	def _build_private_ip_map(self):
		for instance in self.instances:
			self.by_private_ip[instance.private_ip_address] = instance

	def _build_private_dns_name_map(self):
		for instance in self.instances:
			self.by_private_dns_name[instance.private_dns_name] = instance

	def _build_public_ip_map(self):
		for instance in self.instances:
			self.by_public_ip[instance.ip_address] = instance

	def _build_public_dns_name_map(self):
		for instance in self.instances:
			self.by_public_dns_name[instance.public_dns_name] = instance

	def lookup(self, key):
		"""Look in the various dicts we have for the name being
		provided.	 If that fails, I can add a fallback lookup if we
		need to be more general.
		"""
		for d in (self.by_instance_id, self.by_private_ip, self.by_public_ip, self.by_public_dns_name, self.by_private_dns_name):
			if key in d.keys():
				return d[key]

class KInstances(Instances):
	"""Gathers data from a particular account, and makes it easier to
	search for desired instance data.

    Uses k.aws-specific lingo since its superclass was written without
    knowledge of k.aws.
	"""
	def __init__(self, ec2_conn):
		self.reservations        = ec2_conn.get_all_instances()
		self.groups              = ec2_conn.get_all_security_groups()
		self.instances           = all_instances(ec2_conn)
		self.by_instance_id      = dict()
		self.by_private_ip       = dict()
		self.by_private_dns_name = dict()
		self.by_public_ip        = dict()
		self.by_public_dns_name  = dict()
		self._build_instance_id_map()
		self._build_private_ip_map()
		self._build_private_dns_name_map()
		self._build_public_ip_map()
		self._build_public_dns_name_map()

# Local Variables:
# tab-width: 4
# indent-tabs-mode: t
# End:
