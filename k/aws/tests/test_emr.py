from collections import namedtuple
import random
import copy

from mock import Mock, patch

import k.aws.emr as emr_under_test
import unittest


mock_step = Mock()
mock_bootstrap_action = Mock()


@patch.object(emr_under_test, 'simple_step', mock_step)
@patch.object(emr_under_test, '_get_bootstrap_action', mock_bootstrap_action)
def test_simple_step():
	"""
	Tests simple_job()
	"""
	conn = Mock()
	emr_jar_info = namedtuple('TestInfo', 'name bucket version')
	mock_jar_info = emr_jar_info(
		name='FooName', bucket='FooBucket', version='FooVersion')
	input_dir = 's3n://foo_bucket/'
	jarargs = []
	options = Mock()
	options.job_name = None
	options.job_log_dir = None
	options.slave_spot_instances = None
	options.bootstrap = None
	options.ami_version = 'FooAMIVersion'
	options.classname = 'FooClassName'
	options.custom_output = None
	options.ninstances = 100
	options.isize = 'm1.xlarge'
	options.debug = None
	options.version = '1.0.3'
	options.job_flow_role = 'EMRJobflowDefault'
	options.slave_spot_instances = None
	options.keep_alive = False
	options.visible_to_all_users = False
	dummy_key_pair = 'dummy_key_pair'

	bootstrap_action_return_value = [Mock()]
	mock_bootstrap_action.return_value = bootstrap_action_return_value

	job_log_dir = emr_under_test.build_job_log_prefix(
			emr_jar_info = mock_jar_info, options = options)

	mock_proj_name = emr_under_test._get_project_name(mock_jar_info, options)

	step_return_value = Mock()
	mock_step.return_value = step_return_value

	# Call the actual method under test.
	emr_under_test.simple_job(conn, mock_jar_info, input_dir,
				jarargs, dummy_key_pair, options)
	mock_step.assert_called_with(
			mock_proj_name, mock_jar_info,
			input_dir, jarargs,
			options.classname,
			options.custom_output)

	conn.run_jobflow.assert_called_with(
			name = mock_jar_info.name,
			log_uri = job_log_dir,
			steps = step_return_value,
			num_instances = options.ninstances,
			master_instance_type = options.isize,
			slave_instance_type = options.isize,
			enable_debugging=None,
			hadoop_version=options.version,
			bootstrap_actions = bootstrap_action_return_value,
			instance_groups = None,
			ami_version = options.ami_version,
			ec2_keyname = dummy_key_pair,
			keep_alive = False,
			job_flow_role = options.job_flow_role,
			visible_to_all_users = False)


class TestEMRJarInfo(unittest.TestCase):

	@patch.object(emr_under_test, '_get_keys')
	def test_jar_info_from_url(self, mock_get_keys):
		"""
		Tests the k.aws.emr.get_jar_info_from_url()
		method
		"""
		mock_get_keys.return_value = ['foo']
		options = Mock()
		jar_name = 's3n://knewton-emr/foo-1.1.jar'
		info = emr_under_test.get_jar_info_from_url(jar_name, options)
		assert info.name == 'foo'
		assert info.version == '1.1'
		assert info.name_prefix is None
		assert info.bucket == 'knewton-emr'
		mock_get_keys.assert_called_with('knewton-emr', 'foo-1.1.jar', options)
		# Assert with different URI Patterns
		jar_name = 's3n://knewton-emr/bar/foo-1.1.jar'
		info = emr_under_test.get_jar_info_from_url(jar_name, options)
		assert info.name == 'foo'
		assert info.version == '1.1'
		assert info.name_prefix == 'bar'
		assert info.bucket == 'knewton-emr'
		mock_get_keys.assert_called_with('knewton-emr', 'bar/foo-1.1.jar', options)

		invalid_jar_name = 's3n://invalid-1.1.jar'
		try:
			info = emr_under_test.get_jar_info_from_url(invalid_jar_name, options)
			assert False
		except ValueError as err:
			assert err.args[1] == invalid_jar_name

		jar_name = 's3n://knewton-emr/bar/foo-1.1.1.jar'
		info = emr_under_test.get_jar_info_from_url(jar_name, options)
		assert info.name == 'foo'
		assert info.version == '1.1.1'
		assert info.name_prefix == 'bar'
		assert info.bucket == 'knewton-emr'
		mock_get_keys.assert_called_with('knewton-emr', 'bar/foo-1.1.1.jar', options)

		jar_name = 's3n://knewton-emr/bar/foo-1.1.1.1.jar'
		try:
			info = emr_under_test.get_jar_info_from_url(jar_name, options)
			assert False
		except AttributeError as err:
			assert True


	@patch.object(emr_under_test, 'get_latest_jar_version')
	def test_jar_info_from_name(self, mock_jar_version):
		"""
		Given a list of jar versions returns a
		mock for the k.aws.s3.connect which is needed by the test
		"""
		mock_jar_version.side_effect = Mock(return_value = ('0.1', 'plainemrjar'))
		mock_options = Mock()
		jar_info = emr_under_test.get_jar_info_from_name(
				'plainemrjar', 'foo_bucket', mock_options)
		self.assertEqual('plainemrjar', jar_info.name)
		self.assertEqual(None, jar_info.name_prefix)
		self.assertEqual('0.1', jar_info.version)
		self.assertEqual('foo_bucket', jar_info.bucket)
		mock_jar_version.side_effect = Mock(return_value =
											('0.1', 'foo_sub_bucket/plainemrjar'))
		jar_info = emr_under_test.get_jar_info_from_name(
				'foo_sub_bucket/plainemrjar', 'foo_bucket', mock_options)
		self.assertEqual('plainemrjar', jar_info.name)
		self.assertEqual('foo_sub_bucket', jar_info.name_prefix)
		self.assertEqual('0.1', jar_info.version)
		self.assertEqual('foo_bucket', jar_info.bucket)
		mock_jar_version.side_effect = Mock(return_value =
											('0.1', 'bar/foo_sub_bucket/plainemrjar'))
		jar_info = emr_under_test.get_jar_info_from_name(
				'bar/foo_sub_bucket/plainemrjar', 'foo_bucket', mock_options)
		self.assertEqual('plainemrjar', jar_info.name)
		self.assertEqual('bar/foo_sub_bucket', jar_info.name_prefix)
		self.assertEqual('0.1', jar_info.version)
		self.assertEqual('foo_bucket', jar_info.bucket)
		# Now return a different jar name prefix in the mocked call as compared
		# to the call to get_jar_info_from_name. the former should be present in the
		# jar info object. This is necessary to find the right jar.
		mock_jar_version.side_effect = Mock(return_value =
											('0.1', 'bar/foo_sub_bucket/plainemrjar'))

		jar_info = emr_under_test.get_jar_info_from_name(
				'bar/foo_sub_bucket/plain', 'foo_bucket', mock_options)
		self.assertEqual('plainemrjar', jar_info.name)
		self.assertEqual('bar/foo_sub_bucket', jar_info.name_prefix)
		self.assertEqual('0.1', jar_info.version)
		self.assertEqual('foo_bucket', jar_info.bucket)

	@patch.object(emr_under_test, '_get_keys')
	def test_get_latest_jar_version(self, mock_get_keys):
		"""
		Tests the get_latest_jar_version() method from emr.py
		"""
		# Set up the mocks for the keys of the buckets
		mock_jar_name = 'foo_emr-%s.jar'
		mock_jar_prefix = 'foo-emr'

		def construct_mock_get_keys(version_strings = [], jar_name = mock_jar_name):
			"""
			Given a set of version_strings, shuffle them and add values to the
			_get_keys mock
			"""
			shuffled_strings = copy.deepcopy(version_strings)
			random.shuffle(shuffled_strings)
			mock_key = namedtuple('Key', 'name')
			keys = [mock_key(name = jar_name % i)
				for i in shuffled_strings]
			mock_get_keys.return_value = keys

		sorted_version_strings = get_version_stubs()
		construct_mock_get_keys(sorted_version_strings)
		# Call the method under test
		latest_version, name = emr_under_test.get_latest_jar_version(
			mock_jar_prefix, 'foo_bucket_name', 'bar_options')

		# now verify that the appropriate method calls were made
		mock_get_keys.assert_called_with('foo_bucket_name',
										mock_jar_prefix, 'bar_options')
		# Verify that the version is indeed the latest
		assert latest_version == sorted_version_strings[-1]
		# What if the list of versions retuened is empty?
		# We should get None
		construct_mock_get_keys([])
		latest_version, name = emr_under_test.get_latest_jar_version(
			mock_jar_prefix, 'foo_bucket_name', 'bar_options') or (None, None)
		assert latest_version is None
		# What if only one string was passed.
		lonely_project_version = ['0.1']
		construct_mock_get_keys(lonely_project_version)
		latest_version, name = emr_under_test.get_latest_jar_version(
			mock_jar_prefix, 'foo_bucket_name', 'bar_options')
		assert latest_version == lonely_project_version[0]
		# Versions with no minor and revision numbers should not
		# be processed. Versions with more than a major, minor and
		# and a revision number will not be processed. So...
		bad_versions = ['0', '0.1.1.1', '0.1.23.2']
		construct_mock_get_keys(bad_versions)
		latest_version, name = emr_under_test.get_latest_jar_version(
			mock_jar_prefix, 'foo_bucket_name', 'bar_options') or (None, None)
		assert latest_version is None
		# Mix bad with the good versions
		mixed_versions = ['0.1', '1.0.1.1', '2.0', '2.1.1.1']
		construct_mock_get_keys(mixed_versions)
		latest_version, name = emr_under_test.get_latest_jar_version(
			mock_jar_prefix, 'foo_bucket_name', 'bar_options')
		assert latest_version == '2.0'

		# Now assert things about the name prefix matching more than
		# one file name as documented in the second half of docstring of the method

		construct_mock_get_keys(sorted_version_strings,
								jar_name = 'bamboozle/foo-bar-bam-%s.jar')
		# Call the method under test
		latest_version, name = emr_under_test.get_latest_jar_version(
			'bamboozle/foo-bar', 'foo_bucket_name', 'bar_options')

		# now verify that the appropriate method calls were made
		mock_get_keys.assert_called_with(
				'foo_bucket_name', 'bamboozle/foo-bar', 'bar_options')
		self.assertEquals('bamboozle/foo-bar-bam', name)


def test_validate_jar_params():
	"""
	test the validate_jar_params()
	"""
	options = Mock()
	parser = Mock()
	options.jar_name_prefix = None
	options.jar_version = None
	options.jar_url = 's3n://foo/bar-1.1.jar'
	result = emr_under_test.validate_jar_params(options, parser)
	assert result is True
	invalid_urls = [
		# foo is not a valid scheme
		'foo://foo/bar-2.2.2.jar',
		# heirarchical missing
		's3n:foo/bar-2.2.jar',
		# no bucket name
		's3n://foo-1.1.jar',
		# not a valid url location on s3
		'foourl',
		# bad version number
		's3n://foo/bar-0.1.1.1.jar'
	]

	for invalid_url in invalid_urls:
		options.jar_url = invalid_url
		result = emr_under_test.validate_jar_params(options, parser)
		assert result is False
	valid_urls = [
		's3n://foo/bar-0.1.1.jar',
		's3n://foo/bar/bam-0.1.1.jar',
		's3n://foo/bar-0.1.jar'
	]
	for valid_url in valid_urls:
		options.jar_url = valid_url
		result = emr_under_test.validate_jar_params(options, parser)
		assert result is True


def test_build_jar_loc():
	"""
	Test the _build_jar_loc()
	"""
	emr_jar_info = emr_under_test.JARINFO(
			bucket='foo_bucket', name='myemrjar', version='0.1.1',
			name_prefix='foo_prefix')
	jar_location = emr_under_test._build_jar_loc(emr_jar_info)
	assert jar_location == 's3n://foo_bucket/foo_prefix/myemrjar-0.1.1.jar'
	emr_jar_info = emr_under_test.JARINFO(
			bucket='foo_bucket', name='myemrjar', version='0.1.1',
			name_prefix=None)
	jar_location = emr_under_test._build_jar_loc(emr_jar_info)
	assert jar_location == 's3n://foo_bucket/myemrjar-0.1.1.jar'


def test_version_comparator():
	"""
	Assert some suspect corner cases
	"""
	cmp_under_test = emr_under_test.get_jar_version_cmp()
	assert cmp_under_test('1.1.10', '1.1.1') == 1
	assert cmp_under_test('1.1', '1.2.0') == -1
	assert cmp_under_test('1.1', '1.1') == 0
	assert cmp_under_test('1.0.0', '1.0') == 0
	# Now assert against a user defined sorted set of
	# version.
	stubbed_versions = get_version_stubs()
	test_versions = copy.deepcopy(stubbed_versions)
	random.shuffle(test_versions)
	# Instead of just comparing the lists the right thing to do is compare the
	# actual comparator function output.  The reason is subtle; comparison
	# between certain version numbers like for example 3.0 and 3.0.0 are equal
	# according to the emr_under_test.get_jar_version_cmp() which is correct,
	# but when sorting a shuffled list may switch the position of those element.
	for target, actual in zip(stubbed_versions,
				sorted(test_versions, cmp = cmp_under_test)):
		assert cmp_under_test(target, actual) == 0

def get_version_stubs():
	""" These is a stub used for testing get_latest_jar_version.
	Visually verified by a human to be in increasing order to test.
	"""
	return ['0.0',
		'0.1', '0.1', '0.1', '0.3', '0.4', '0.5.7',
		'0.6', '0.7', '0.7', '0.7.3', '0.7.3', '0.9.1',
		'0.9.1', '1.0', '1.0.2', '1.2', '1.2.5', '1.3',
		'1.3', '1.4', '1.4', '1.4.5', '1.4.7', '1.5',
		'1.6', '1.7', '1.8.1', '1.8.5', '1.10', '1.10',
		'1.10.9', '2.0.4', '2.0.9', '2.0.10', '2.1', '2.2.3',
		'2.2.8', '2.3', '2.4', '2.4.4', '2.5', '2.6',
		'2.6.10', '2.7.6', '2.9.9', '2.10', '2.10.9', '3.0',
		'3.0', '3.0.0', '3.1', '3.2', '3.3', '3.4',
		'3.4', '3.4.4', '3.4.6', '3.6.6', '3.6.8', '3.7.3',
		'3.8', '3.8', '3.9', '3.9.6', '3.9.7', '3.10',
		'3.10.3', '3.10.10', '4.0.1', '4.1.5', '4.1.9', '4.2', '4.3',
		'4.5', '4.5.9', '4.6', '4.6', '4.6', '4.6.7', '4.7.10',
		'4.9', '4.9', '4.10', '4.10.10', '5.0', '5.1', '5.1.5',
		'5.3', '5.3.10', '5.4', '5.4', '5.4.3', '5.5.7', '5.6',
		'5.6.6', '5.7', '5.7.9', '5.8.0', '5.8.8', '5.9', '6.1',
		'6.1', '6.2.10', '6.3.7', '6.4.2', '6.4.10', '6.5', '6.5.8',
		'6.5.8', '6.6', '6.6.8', '6.7', '6.8', '6.8.8', '6.9',
		'6.9.3', '6.10', '6.10', '7.0', '7.0', '7.0', '7.0.1',
		'7.1', '7.2', '7.2.5', '7.2.6', '7.2.7', '7.3', '7.4.2',
		'7.5.2', '7.5.4', '7.6', '7.6.10', '7.7.5', '7.8.4', '7.8.6',
		'7.9', '7.9', '7.10', '7.10', '7.10', '8.0', '8.0',
		'8.0.3', '8.1', '8.1', '8.1.5', '8.2.0', '8.2', '8.3',
		'8.3.5', '8.4', '8.4.2', '8.5', '8.5.8', '8.6', '8.6.8',
		'8.6.9', '8.7', '8.8.8', '8.10', '9.0', '9.0.1', '9.0.7',
		'9.1', '9.1.1', '9.1.5', '9.1.5', '9.2.9', '9.3.2', '9.3.10',
		'9.4', '9.4', '9.4.4', '9.5', '9.5', '9.5.6', '9.5.8',
		'9.6', '9.6.7', '9.7', '9.7.0', '9.7', '9.8.6', '9.10.1',
		'10.0', '10.0.5', '10.1.8', '10.2.0', '10.3', '10.4.1', '10.4.3',
		'10.5.2', '10.6', '10.6.2', '10.7.8', '10.8', '10.8.2', '10.9']
