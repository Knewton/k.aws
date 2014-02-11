from k.aws.config import K_AWS_PATH
import os
import glob

TOOL_HOME = K_AWS_PATH + "/tools"
CONFIG_HOME = TOOL_HOME + "/config"

class ToolBase(object):
	def name(self):
		raise NotImplementedError()

	def installed(self):
		raise NotImplementedError()

	def download_tool(self):
		raise NotImplementedError()

	def install_tool(self):
		raise NotImplementedError()

	def path(self):
		return []

	def add_variables(self, region_aws_creds):
		return {}

	def file_config(self, aws_creds):
		return []

	def sts_works(self):
		return True

	def rm_cmd_files(self, home):
		files = glob.glob(home + "/bin/*.cmd")
		for fn in files:
			os.remove(fn)
