import os
import sys
import re

import collections
import threading
import datetime
import sys
import multiprocessing
import traceback
import hashlib
import time
import boto
import k.aws.config
from cStringIO import StringIO
from math import ceil, floor
from threading import Thread
from multiprocessing.pool import ThreadPool
from multiprocessing import Pool
from multiprocessing.synchronize import BoundedSemaphore
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.resumable_download_handler import ResumableDownloadHandler
from k.aws.config import AwsCreds, connection_hash
from k.aws.config import RegionAwsCreds, region_connection_hash

ManualS3Options = collections.namedtuple('ManualS3Options', ["bucket"])

class FileNameException(Exception):
	pass

class ConflictException(Exception):
	pass

def is_valid_dns_name(bucket_name):
	if re.match(r'^[a-z0-9]+[a-z0-9.-]*[a-z0-9]$', bucket_name):
		if not re.match(r'^([0-9]{1,3}\.){3}[0-9]{1,3}$', bucket_name):
			return True
	return False

def connect(creds, bucket_name=None, ordinary=False):
	"""
	Connect to s3 using a k.aws.config.AwsCreds named tuple
	"""
	kwargs = {}
	if ordinary:
		kwargs['calling_format'] = OrdinaryCallingFormat()
	if bucket_name and not is_valid_dns_name(bucket_name):
		kwargs['calling_format'] = OrdinaryCallingFormat()
	if isinstance(creds, AwsCreds):
		kwargs.update(connection_hash(creds))
	elif isinstance(creds, RegionAwsCreds):
		kwargs.update(region_connection_hash(creds))
		del kwargs['region_name']

	conn = boto.connect_s3(**kwargs)
	return conn

def get_bucket_name(options):
	bucket_name = None
	if os.environ.has_key("S3_BUCKET"):
		bucket_name = os.environ["S3_BUCKET"]
	if options.bucket:
		bucket_name = options.bucket
	return bucket_name

def get_bucket(conn, options):
	bucket_name = get_bucket_name(options)
	if not bucket_name:
		sys.stderr.write("No bucket name passed in \n")
		sys.exit(1)

	# Validation breaks on some IAMs when it tries to perform an
	# get_all_keys() as part of the get_bucket().  That's not
	# necessary for the common case of getting a known key.	 In
	# e.g. s3-get, it won't hurt either because it catches errors
	# over both the connect and the get, which would perform any
	# listings, etc. 20120830 -PN
	bucket = conn.lookup(bucket_name, validate=False)
	return bucket

def get_key(bucket, key):
	key = bucket.get_key(key)
	return key.get_contents_as_string()

def put_key(bucket, key, doc):
	key = bucket.new_key(key)
	return key.set_contents_from_string(doc)

def split_and_put_multipart_key(bucket, key, filename, creds, mpsize,
		mpcount, debug, integrity_check=True):
	"""
	Splits a file into ${mpsize}MB-sized pieces and stores it in bucket with
	key using boto's multipart upload. Can also perform an MD5 check for data
	integrity.

	Reference for concurrent uploading:
	https://github.com/mumrah/s3-multipart/blob/master/s3-mp-upload.py
	"""
	multipart = bucket.initiate_multipart_upload(key)

	# Logic for adjusting chunk size or if there are more processes than chunks
	mpsize *= 10**6
	filesize = os.path.getsize(filename)
	piececount = int(floor(filesize / mpsize))
	excess = filesize - (piececount * mpsize)
	if excess > 0:
		mpsize += int(ceil(excess / float(piececount)))
	if piececount < mpcount:
		mpcount = piececount

	def _pool_info(pindices):
		for pindex in pindices:
			yield (bucket.name, filename, multipart.id, pindex, mpsize,
					creds, debug, integrity_check)

	# Creating and running worker pool on various file pieces. We wait for the
	# workers to finish, then check for success.
	pindices = range(1, piececount+1)			# noninclusive range endpoints
	workers = Pool(processes=mpcount)
	results = workers.map_async(put_multipart_piece, _pool_info(pindices))
	_ = results.get()
	results.successful()

	multipart.complete_upload()
	if integrity_check:
		_check_multipart_sizes(filesize, bucket, key)

def put_multipart_piece(args):
	"""
	Method called by mp.Pool workers to submit pieces concurrently.

	args ==
		str bname      name of bucket
		str filename   name of file to upload
		str mpu_id     id of active MultiPartUpload
		int pieceidx   index of piece to upload from file
		int piecesize  size of piece to upload
		creds          credentials used to re-connect to S3
		debug        bool; print to stderr if True
		md5check       bool; if True, perform MD5 check

	"""
	bname, filename, mpu_id, pieceidx, piecesize, creds, debug, md5check = args

	try:
		# Cannot pickle MultiPartUpload object or S3Connection, so we reconnect
		# and retrieve the MPU here.
		conn = connect(creds)
		bucket = conn.get_bucket(bname)
		multipart = _find_multipart_upload(bucket.list_multipart_uploads(), mpu_id)

		# Upload piece and run MD5 check if necessary.
		strbuf = _get_file_piece(pieceidx, piecesize, filename)
		multipart.upload_part_from_file(strbuf, pieceidx)
		if debug:
			sys.stderr.write("\tProcess #%i successfully uploaded %iMB.\n" %
					(pieceidx, piecesize/10**6))
		if md5check:
			localhash = hashlib.md5(strbuf.getvalue()).hexdigest()
			_check_multipart_hash(localhash, multipart, pieceidx)
	except TypeError, err:
		sys.stderr.write("Process #%i has failed to upload its chunk.\n\n")
		sys.stderr.write("Error: %s\n" % err)
		sys.stderr.write("Details:\n")
		sys.stderr.write("\tSize: %i; Buffer length: %i\n" % (piecesize,
				 len(strbuf.getvalue())))
		sys.stderr.write("\tHash: %s\n" % localhash)
		sys.exit(1)

def _get_file_piece(pieceidx, piecesize, filename):
	"""
	Retrieves a $piecesize-sized piece of $filename.
	"""
	filepos = piecesize * (pieceidx - 1)
	with open(filename, 'rb') as f:
		f.seek(filepos)
		piece = StringIO(f.read(piecesize))
	return piece

def _find_multipart_upload(mpus, mpu_id):
	"""
	Helper method for finding desired MultiPartUpload in a list.
	"""
	for mpu in mpus:
		if mpu.id == mpu_id:
			return mpu
	raise Exception("Could not find desired MultiPartUpload (id: %s)"
			% mpu_id)

def put_multipart_key(bucket, key, filename, suffixlen):
	"""
	Store group of files in bucket with key using boto's multipart upload.
	Assumes file has previously been split using

	split -d -a$suffixlen $filename $filename

	str filename  name of original file (before split)
	int suffixlen digit len for split file suffix, e.g. -a3 ==> suffixlen=3

	Use this method only if your file has already been split (i.e. if you're
	working with older Cassandra dbs). For single files, use
	split_and_put_multipart_key() instead.
	"""
	multipart = bucket.initiate_multipart_upload(key)
	dirname = os.path.dirname(filename) + "/"
	basename = os.path.basename(filename)

	# Generating list of file pieces produced by split.
	pieces = [piece for piece in os.listdir(dirname) if
			piece[-1*suffixlen:].isdigit() and
			piece.find(basename) != -1]

	for filepiece in pieces:
		with open(dirname + filepiece, 'rb') as f:
			pieceidx = int(filepiece[-1*suffixlen:]) + 1
			multipart.upload_part_from_file(f, pieceidx)

	multipart.complete_upload()

def delete_key(bucket, prefix):
	keys = bucket.list(prefix=prefix)
	for key in keys:
		bucket.delete_key(key)

def key_exists(bucket, name):
	if bucket.get_key(name):
		return True
	return False

def copy_error_key(bucket, key, application, exception, tback,
		write_stderr=False, delete_key=False):
	"""
	When an exception happens processing the contents of a bucket, you may at
	times wish to move certain files out of the way because they are erroring,
	without actually removing those files.  This function will take in a key
	along with an exception and a traceback, and copy that key to the bucket
	represented by the bucket object passed in.
	The Key will be copied to Errors/<old key name> and the traceback and
	exception will be copied to
	Errors/<old key name>_context/exception_traceback.log

	If the write_stderr is set, a note will be written to stderr.
	If delete_key is set, the original key will be deleted.

	bucket - boto.s3 bucket object
	key - boto.s3 key object
	application - Name of the application within which the exception was thrown
	exception - the exception
	tback - the traceback
	write_stderr - Write to stderr <True/False> default False
	delete_key - Delete the key after backing it up <True/False> default False
	"""
	traceback_contents = traceback.format_exc()
	new_name = '/'.join([
		"Errors", application, exception.__class__.__name__, key.name])
	if write_stderr:
		sys.stderr.write("%s\n" % new_name)
	new_key = key.copy(bucket.name, new_name)
	error_name = '/'.join(["%s_context" % new_name, "exception_traceback.log"])
	error_key = bucket.new_key(error_name)
	new_error_key = error_key.set_contents_from_string(traceback_contents)
	if new_key and new_error_key and delete_key:
		key.delete()
	return new_key == None

def check_bucket(conn, bucket_name, throw=True):
	exists = True
	try:
		bucket = conn.get_bucket(bucket_name)
	except boto.exception.S3ResponseError, e:
		if not e.code == "NoSuchBucket":
			if throw:
				sys.stderr.write(bucket_name + " " + str(e))
				raise e
			else:
				sys.stderr.write(bucket_name + " " + str(e.message))
		exists = False
	return exists

def create_bucket(conn, bucket_name, region=None):
	if region:
		conn.create_bucket(bucket_name, location=region)
	else:
		conn.create_bucket(bucket_name)

def delete_bucket(conn, bucket_name):
	conn.delete_bucket(bucket_name)

def get_s3_region_options(parser):
	parser.add_option(
		"-o", dest="ordinary", default=False, action="store_true",
		help="Use Ordinary Calling Format.")
	return parser

def get_s3_options(parser):
	get_s3_region_options(parser)
	get_s3_bucket_options(parser)

def get_s3_bucket_options(parser):
	parser.add_option(
		"-b", "--bucket", dest="bucket",
		help="S3 Bucket Name (uses S3_BUCKET environment variable if not set)")
	return parser

def sync_local(bucket, localdir, prefix='', debug=False):
	""" Syncs a local directory with an S3 bucket. If prefix is specified, only
	keys with the prefix will be synced locally.

	boto.s3.bucket.Bucket bucket: obj representing S3 bucket
	str localdir:                 target directory to be synced
	str prefix:                   prefix to use when syncing keys
	bool debug:                   debug output
	"""
	keys = bucket.list(prefix=prefix)
	for key in keys:
		try:
			## If key is a directory, check whether that directory exists
			## locally. If key is a file, write that file to the local dir.
			outname = _build_outfile_name(localdir, prefix, key.name)
			_create_key_directory(outname, debug)
			if os.path.basename(outname):
				write_key_to_filename(key, outname, debug)
		except IOError, err:
			sys.stderr.write("IOError: %s\n" % err)
			raise IOError(err)

def write_key_to_filename(key, outname, debug=False):
	""" Writes contents of key to file called $outname. Also reports on
	download status, as defined in key_write_status()

	boto.s3.key.Key key: key obj with info to be written
	str outname:         name of file to write to
	bool debug:          debug output

	Note: downloads will be retried via a
	boto.s3.resumable_download_handler.ResumableDownloadHandler
	"""
	if debug:
		sys.stderr.write("Saving %s...\n\t" % outname)
	key.get_contents_to_filename(outname,
			res_download_handler=ResumableDownloadHandler(num_retries=10),
			cb=key_write_status(debug))
	if debug:
		sys.stderr.write("\ndone.\n")

def key_write_status(debug):
	def func(written, total):
		""" Reports status of key-writing to stdout. Useful for identifying
		s3-get or s3-put timeouts.

		int written: number of bytes written so far
		int total:   total file size

		Returns nothing, reports progress to stderr.
		"""
		total /= 1000000.0		# scaling to MBs
		written /= 1000000.0	# scaling to MBs
		if total != 0.0:
			percent = 100 * written / float(total)
		else:
			percent = 100

		if written > 1000:
			written /= 1000.0
			unit = "GB"
		else:
			unit = "MB"

		if debug:
			sys.stderr.write("{percent:.1f}% ({written:.1f}{unit})  ".format(
				percent=percent, written=written, unit=unit))

	return func

def _build_outfile_name(localdir, prefix, keyname):
	""" Builds outfile name from the S3 key name and local directory.

	str localdir: target directory for outfile
	str prefix:   prefix used to find key
	str keyname:  name of key to sync locally

	Strips prefix from keyname, then returns concatenation of localdir
	and keyname.

	NOTE: assumes user terminates directory prefixes with '/', as specified
	in the options for bin/s3-sync-local.
	"""
	if localdir[-1] != '/':
		localdir += '/'

	if not prefix or prefix[-1] == "/":
		outname = localdir + keyname.replace(prefix, "")
	else:
		outname = localdir + os.path.basename(keyname)

	return outname


def _create_key_directory(keyname, debug=False):
	""" Checks whether there is a local directory that matches the keyname's
	directory.

	str keyname: name of key to check locally
	bool debug:  debug output

	Returns nothing, but creates directory locally if the key's directory
	doesn't exist.
	"""
	keydir = os.path.dirname(keyname)
	if not os.path.exists(keydir):
		if debug:
			sys.stderr.write("Creating local directory: %s\n" % keydir)
		os.makedirs(keydir)

def _check_multipart_hash(localhash, multipart, pieceidx):
	"""
	Helper method for verifying hashes of multipart uploads.
	Finds MD5 hash for remote piece, compares it to local piece.
	"""

	# Iterate through multipart pieces, get desired part.
	remotehash = [part.etag.strip('"') for part in multipart if
			part.part_number == pieceidx][0]

	if localhash != remotehash:
		raise Exception("Data integrity could not be confirmed. Please "
				"check the data uploaded to S3.\n\n"
				"Local MD5:\t\t%s\nRemote MD5:\t\t%s\n" %
				(localhash, remotehash))

def _check_multipart_sizes(localsize, bucket, key):
	"""
	Helper method for verifying file size of multipart uploads. Will query
	bucket for the MultiPartUpload's resulting file size.
	"""

	remotesize = bucket.get_key(key).size
	if localsize != remotesize:
		raise Exception("Data integrity could not be confirmed. Please "
				"check the data uploaded to S3.\n\n"
				"Local file size:\t\t%s\nRemote file size:\t\t%s\n" %
				(localsize, remotesize))

def _copy_key_part_with_retry(args):
	"""Args is an iterable, passed in by copy_key().  It gets
	extracted as would positional arguments.

	start          : int, offset to the first byte to be copied in this chunk
	end	       : int, offset to the last byte to be copied in this chunk.
	retry_per_part : int, how many attempts at re-trying should be made
	mp	       : boto.s3.MultiPartUpload object
	src_key        : boto.s3.Key, source of the copy
	part_count     : int, chunk number to be copied for collation by s3

	Raises boto.exception.S3CopyError on failure to copy.  On success, returns the
	number of retries that were attempted.
	"""
	# OK, this has gotten out of hand.	A
	#  dict may be better
	# in the future
	retry_count = 0
	start = args[0]
	end = args[1]
	retry_per_part = args[2]
	mp = args[3]
	src_key = args[4]
	part_count = args[5]

	while retry_count < retry_per_part:
		try:
			# print "Working on {0} - {1} (retry: {2})".format(start,
			#	 end, retry_count)
			mp.copy_part_from_key(src_key.bucket.name, src_key.name,
				part_count, start, end)
		except boto.exception.S3CopyError as s3c:
			retry_count += 1
			next
		else:
			break
		if retry_count == retry_per_part:
			raise boto.exception.S3CopyError, "Couldn't copy the key - retried {0} times".format(retry_count)
	return retry_count

def copy_key(src_key, dst_bucket, dst_key_name, part_size=500000000, retry_per_part=2, parallel=1, verbose=False):
	"""A boto copy will only copy keys < 5GB.  Otherwise the key
	needs to be broken up into parts.  By default part_size is a
	bit less than 500MB that to make the arithmetic works on round
	numbers, and so the chunks aren't so high-stakes.  Re-tries
	will be common, unfortunately.  Chunks closer to 5gb seem to have
	a high failure rate.

	If the parallel argument >1, then that many proceses will be spawned
	to parallelize the copy.

	The template for how to do this comes from
	https://github.com/boto/boto/pull/425

	src_key        : boto.s3.Key, the key that will be used as the source of the copy
	dst_bucket     : boto.s3.Bucket, the bucket that will be copied to
	dst_key_name   : str, the name of the key that will be created/overwritten
	part_size      : int, defaults to 500,000,000 bytes, or about 500MB
	retry_per_part : int, number of retries before throwing in the towel on a part
	parallel       : int, 1 means serialize, more means that many parallel threads
	verbose        : Bool, print extra junk to stderr

        Returns the (int) number of chunks that were copied.
	"""
	ranges = list()
	start_time = time.time()
	if src_key.size < part_size:
		# smaller than the limit, a single copy will work.
		if verbose:
			sys.stderr.write("The key is smaller than part_size, using a normal copy.\n")
		dst_bucket.copy_key(dst_key_name, src_key.bucket.name, src_key.name)
		#src_key.copy(dst_bucket, dst_key_name)
		return 1
	else:
		mp = dst_bucket.initiate_multipart_upload(dst_key_name)
		bytes_left = src_key.size
		bytes_assigned = 0 # marker for the next byte to copy
		part_count = 1 # the api starts the count at 1
		while True:
			start = bytes_assigned
			if bytes_left < part_size-1:
				chunk = bytes_left
			else:
				chunk = part_size
			end = bytes_assigned + chunk - 1

			ranges.append((start, end, retry_per_part, mp, src_key, part_count))
			bytes_assigned += chunk
			bytes_left -= chunk
			part_count += 1
			if bytes_left <= 1:
				break
	if parallel == 1:
		if verbose:
			sys.stderr.write("parallel is 1, copying serially\n")
		for r in ranges:
			_copy_key_part_with_retry(r)
	else:
		p = ThreadPool(processes=parallel)
		p.map(_copy_key_part_with_retry, ranges, 1)
	end_time = time.time()
	if verbose:
		sys.stderr.write("Elapsed time with {0} threads is {1}\n".format(end_time - start_time, parallel))

	mp.complete_upload()
	if verbose:
		sys.stderr.write("And completion time is {0}\n".format(time.time() - end_time))
		return len(ranges)

def parallel_copy_bucket(creds, src_bucket_name, dst_bucket_name,
		src_ordinary=False, dst_ordinary=False,
		prefix=None, threads=10, timeout=300, verbose=False):
	status = True
	src_conn = k.aws.s3.connect(
		creds, bucket_name=src_bucket_name, ordinary=src_ordinary)
	src_bucket = src_conn.get_bucket(src_bucket_name)
	if prefix:
		rs = src_bucket.list(prefix)
	else:
		rs = src_bucket.list()
	key_copy_thread_list = []
	pool_sema = BoundedSemaphore(value=threads)
	total_keys = 0

	for key in rs:
		total_keys += 1
		if verbose:
			sys.stderr.write("%s : Requesting copy thread for %s\n" % (
				datetime.datetime.now(), key.name))
		current = CopyKey(pool_sema, creds, src_bucket_name, key.name,
			dst_bucket_name, key.name,
			src_ordinary=src_ordinary, dst_ordinary=dst_ordinary)
		key_copy_thread_list.append(current)
		current.start()

		if len(threading.enumerate()) >= threads:
			while 1:
				if len(threading.enumerate()) < threads:
					result = cull_threads(key_copy_thread_list, verbose)
					if not result:
						status = False
					break
				time.sleep(0.05)

	for thread in key_copy_thread_list:
		thread.join(timeout)
		if thread.is_alive():
			if verbose:
				sys.stderr.write("%s : TIMEOUT on key %s\n" % (
					datetime.datetime.now(), thread.src_key_name))
				status = False
			continue
		if verbose:
			sys.stderr.write("%s\n" % thread.status)
		result = print_thread(thread)
		if not result:
			status = False
	if verbose:
		sys.stderr.write("%s : Complete : %s Total Keys Requested\n" % (
			datetime.datetime.now(), total_keys))
	return status

def print_thread(thread):
	if thread.status.find("Error") > 1:
		print "ERROR: %s" % thread.key_name
		return False
	elif thread.status.find("Success") > 1:
		print thread.key_name
		return True

def cull_threads(thread_list, verbose):
	status = True
	for thread in thread_list:
		if not thread.is_alive():
			result = print_thread(thread)
			if not result:
				status = False
			if verbose:
				sys.stderr.write("%s\n" % thread.status)
			thread.join()
			thread_list.remove(thread)
	return status

class CopyKey(Thread):
	def __init__(self, pool_sema, creds,
			src_bucket_name, src_key_name,
			dst_bucket_name, dst_key_name,
			src_ordinary=False, dst_ordinary=False,
			part_size=50000000, retry_per_part=2):
		Thread.__init__(self)
		self.pool_sema = pool_sema
		self.creds = creds
		self.src_bucket_name = src_bucket_name
		self.src_key_name = src_key_name
		self.key_name = src_key_name
		self.dst_bucket_name = dst_bucket_name
		self.dst_key_name = dst_key_name
		self.src_ordinary = src_ordinary
		self.dst_ordinary = dst_ordinary
		self.part_size = part_size
		self.retry_per_part = retry_per_part
		self.status = False

	def run(self):
		src_conn = k.aws.s3.connect(self.creds,
			bucket_name=self.src_bucket_name, ordinary=self.src_ordinary)
		src_bucket = src_conn.get_bucket(self.src_bucket_name)
		src_key = src_bucket.get_key(self.src_key_name)

		dst_conn = k.aws.s3.connect(self.creds,
			bucket_name=self.dst_bucket_name, ordinary=self.dst_ordinary)
		dst_bucket = dst_conn.get_bucket(self.dst_bucket_name)
		dst_key = dst_bucket.get_key(self.dst_key_name)
		if not dst_key or dst_key.size != src_key.size:
			self.pool_sema.acquire()
			self.status = "%s : Semaphore Acquired, Copy Next" % (
				datetime.datetime.now())
			try:
				copy_key(src_key, dst_bucket, self.dst_key_name,
					part_size=self.part_size,
					retry_per_part=self.retry_per_part,
					parallel=1, verbose=False)
				self.status = "%s : Copy Success : %s" % (
					datetime.datetime.now(), self.src_key_name)
			except:
				self.status = "%s : Copy Error: %s : %s" % (
					datetime.datetime.now(), self.src_key_name, sys.exc_info())
				exc_class, exc, tback = sys.exc_info()
				sys.stderr.write(str(exc_class) + "\n")
				traceback.print_tb(tback)
			finally:
				self.pool_sema.release()
		else:
			self.status = "%s : Key Exists: %s : Will not overwrite." % (
				datetime.datetime.now(), self.src_key_name)

def parallel_delete_bucket(creds, bucket_name, ordinary=False, prefix=None,
		threads=10, timeout=300, verbose=False):
	status = True
	conn = k.aws.s3.connect(
		creds, bucket_name=bucket_name, ordinary=ordinary)
	bucket = conn.get_bucket(bucket_name)
	if prefix:
		rs = bucket.list(prefix)
	else:
		rs = bucket.list()
	thread_list = []
	pool_sema = BoundedSemaphore(value=threads)
	total_keys = 0

	for key in rs:
		total_keys += 1
		if verbose:
			sys.stderr.write("%s : Requesting delete thread for %s\n" % (
				datetime.datetime.now(), key.name))
		current = DeleteKey(
			pool_sema, creds, bucket_name, key.name, ordinary=ordinary)
		thread_list.append(current)
		current.start()

		if len(threading.enumerate()) >= threads:
			while 1:
				if len(threading.enumerate()) < threads:
					result = cull_threads(thread_list, verbose)
					if not result:
						status = False
					break
				time.sleep(0.05)

	for thread in thread_list:
		thread.join(timeout)
		if thread.is_alive():
			if verbose:
				sys.stderr.write("%s : TIMEOUT on key %s\n" % (
					datetime.datetime.now(), thread.key_name))
				status = False
			continue
		if verbose:
			sys.stderr.write("%s\n" % thread.status)
		result = print_thread(thread)
		if not result:
			status = False
	if verbose:
		sys.stderr.write("%s : Complete : %s Total Keys Requested\n" % (
			datetime.datetime.now(), total_keys))
	return status

class DeleteKey(Thread):
	def __init__(self, pool_sema, creds, bucket_name, key_name, ordinary=False):
		Thread.__init__(self)
		self.pool_sema = pool_sema
		self.creds = creds
		self.bucket_name = bucket_name
		self.key_name = key_name
		self.ordinary = ordinary
		self.status = False

	def run(self):
		conn = k.aws.s3.connect(self.creds,
			bucket_name=self.bucket_name, ordinary=self.ordinary)

		self.pool_sema.acquire()
		self.status = "%s : Semaphore Acquired, Delete Next" % (
			datetime.datetime.now())
		bucket = conn.get_bucket(self.bucket_name)
		try:
			bucket.delete_key(self.key_name)
			self.status = "%s : Delete Success : %s" % (
				datetime.datetime.now(), self.key_name)
		except:
			self.status = "%s : Delete Error: %s : %s" % (
				datetime.datetime.now(), self.key_name, sys.exc_info())
			exc_class, exc, tback = sys.exc_info()
			sys.stderr.write(str(exc_class) + "\n")
			traceback.print_tb(tback)
		finally:
			self.pool_sema.release()
