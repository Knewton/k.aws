# Utilities that are useful in other k.aws modules

import json
import re
import urllib2
import warnings

def default_data_lookup(response):
	return response

def next_token_marker_lookup(response):
	if response.next_token:
		return response.next_token
	else:
		raise KeyError, "no next_token"

def yield_aws_data(fetcher, fetcher_next_flag, marker_lookup, data_lookup, yield_response=False):
	"""Function that turns a call that paginates results from AWS into
	a generator.  This is passed functions to lookup attributes or
	keys, so it should work when paginated results and a "next" item
	are stored in either an object's property or in a hash item (or
	sub-item, etc.)

	:type fetcher: function
	:param fetcher: A function that will perform a lookup. It will be
	               run with a single keyword argument and no
	               positional arguments.  The optional argument is the
	               fetcher_next_flag (boto's iam names the argument
	               "marker", its cloudformation uses "next_token".
	               Blech).

	:type fetcher_next_flag: string
	:param fetcher_next_flag: is the name of the flag that will be
	                          passed in to the fetcher.

	:type marker_lookup: function
	:param marker_lookup: This function is passed the object returned
	                      by the fetcher function, and gets the
	                      appropriate data out of the object on each
	                      iteration.

	:type data_lookup:function
	:param data_lookup: This function is passed the object returned by
	                    the fetcher function iterates over and returns
	                    the elements which will be yielded one at a
	                    time to the caller.

	:type yield_response: boolean
	:defaut yield_response: False
	:param yield_response: When yield_response is True the first item
	                       yielded will be the actual response of the
	                       first call to the fetcher function.  All
	                       subsequent yields will be data returned by
	                       data_lookup.

	If a function has a known set of flags that it should be run with,
	and the fetcher_next_flag is the only thing that will vary, then
	the fetcher should be passed in as a functools.partial object
	with those arguments already bound.

	The fetcher should raise KeyError or AttributError when there are no
	more results to be returned.
	"""
	response = fetcher()

	if yield_response:
		yield response

	while True:
		for item in (u for u in data_lookup(response)):
			yield item
		try:
			kwargs = dict()
			response = fetcher(**{ fetcher_next_flag : marker_lookup(response)})
		except (KeyError, AttributeError):
			break


##### EC2 pricing information
#: pricing URL
PRICE_URL = "http://aws.amazon.com/ec2/pricing/pricing-on-demand-instances.json"

#: the names in the JSON are not the same as EC2 in most cases
region_map = {
	'us-west-1' : 'us-west',
	'us-east-1' : 'us-east',
	'eu-west-1' : 'eu-ireland',
	'ap-northeast-1' : 'apac-tokyo',
	'ap-southeast-1' : 'apac-sin'
}

#: Conversion from the instance type prefix to the json results
type_map = {
	'm1' : 'generalPreviousGen',
	'm2' : 'hiMemCurrentGen',
	'm3' : 'generalCurrentGen',
	'c1' : 'computePreviousGen',
	'c3' : 'computeCurrentGen',
	'cc1' : 'clusterComputeI',
	'cg1' : 'clusterGPUI',
	'g2' : 'gpuCurrentGen',
	'cg1' : 'gpuPreviousGen',
	'hi1' : 'storageCurrentGen',
	't1' : 'uI'
}

#: Conversion from the account id to the default key-pair name
account_id_to_keypair_map = {
	'933536168873' : 'ANALYTICS-001-2013-08-29',
	'591455459439' : 'PRODUCTION-001-2013-08-22',
	'957700444419' : 'STAGING-001-2013-08-22',
	'512319209877' : 'UTILITY-001-2013-08-22',
	'191918158150' : 'UAT-001-2013-08-29'
}

def get_region_key(my_region):
	"""
	Returns the region key that should be used for the json data.

	Returns:
	  (str) Region key into json data
	"""
	return region_map.get(my_region, my_region)

def get_json_pricing_fetch():
	"""
	Returns the json data provided by the AWS url.

	Returns:
	  (dict) Json data
	"""
	data = urllib2.urlopen(PRICE_URL, None, 5)
	return json.load(data)

def get_instance_type_key(instance_type):
	"""
	Uses the prefix portion of the instance type which indicates
	the instance type to locate in the mapping what is the instance
	type in the JSON data.

	Returns:
	  (str) Instance type to use in the json data
	"""
	key = instance_type.split('.')[0]
	return type_map.get(key, key)

def get_instance_types_for_region(my_region, json_data):
	"""
	Returns a dict of instance type information for the region
	of our current runtime.

	Returns:
	  (dict) Instance types for our region from the json data
	"""
	my_region = get_region_key(my_region)
	for region in json_data['config']['regions']:
		this_region = region['region']
		if this_region == my_region:
			return region['instanceTypes']
	warnings.warn("No pricing information was found for the region %s"%my_region)
	return {}

def get_sizes_for_instance_type(instance_types, instance_type):
	"""
	Returns a dict of size information for our specific instance type.

	Returns:
	  (dict) Sizes for our instance type from the json data.
	"""
	for item in instance_types:
		if item['type'] == get_instance_type_key(instance_type):
			return item['sizes']
	if instance_types:
		warnings.warn("No pricing information was found for the instance type %s"%instance_type)
	return {}

def get_ec2_price_for_size(sizes, instance_type):
	"""
	Returns the price for our instance type and size.

	Returns:
	  (str) Price, or 0.0 if the price was not found
	"""
	for item in sizes:
		if item['size'] == instance_type:
			for os in item['valueColumns']:
				if os['name'] == 'linux':
					return os['prices']['USD']
	if sizes:
		warnings.warn("No pricing information was found for the instance type %s"%instance_type)
	return '0.000'


def get_ec2_price(my_region, instance_type):
	"""
	Returns the price for the requested instance type.

	Returns:
	  (float) price, or 0.0 if the price is not found
	"""
	j = get_json_pricing_fetch()
	instance_types = get_instance_types_for_region(my_region, j)
	sizes = get_sizes_for_instance_type(instance_types, instance_type)
	return float(get_ec2_price_for_size(sizes, instance_type))


def get_key_pair_name(account_id):
	"""
	Returns the key pair name to be used for an account id or
	the empty string if there is no matching key.
	"""
	return account_id_to_keypair_map.get(account_id, "")
