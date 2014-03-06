"""
Configuration instructions are too long to repeat here:
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
"""

import sh
import os.path
from k.aws.tools.base import ToolBase
from k.aws.tools.base import ToolBase, TOOL_HOME, CONFIG_HOME

S3CMD_HOME = TOOL_HOME + "/s3cmd"

class S3CmdTool(ToolBase):
	def name(self):
		return "s3cmd"

	def download_tool(self):
		# have to use a patched s3cmd that can use env vars and knows
		# how to deal with sts tokens
		pass

	def install_tool(self):
		sh.rm("-rf", S3CMD_HOME)
		sh.rm("-f", TOOL_HOME + "/bin/s3cmd")
		sh.mkdir("-p", S3CMD_HOME)
		sh.virtualenv(S3CMD_HOME)
		sh.mkdir("-p", TOOL_HOME + "/bin")
		pip = sh.Command(S3CMD_HOME + "/bin/pip")
		pip("install", "s3cmd")
		sh.ln("-s", S3CMD_HOME + "/bin/s3cmd", TOOL_HOME + "/bin/s3cmd")

	def installed(self):
		return os.path.exists(S3CMD_HOME)

	def paths(self):
		return ["$TOOL_HOME/bin"]

	def add_variables(self, region_aws_creds):
		aws_creds = region_aws_creds.creds
		region = region_aws_creds.region_name
		return {
			'TOOL_HOME': TOOL_HOME,
			'S3CMD_CONFIG': "%s/%s/s3cfg" % (
				CONFIG_HOME, aws_creds.env),
		}

	def file_config(self, region_aws_creds):
		aws_creds = region_aws_creds.creds
		sh.mkdir("-p", "%s/%s" % (CONFIG_HOME, aws_creds.env))
		filename = "%s/%s/s3cfg" % (
			CONFIG_HOME, aws_creds.env)
		contents_list = ["[default]",
			"access_key = " + aws_creds.access,
			"secret_key = " + aws_creds.secret
		]
		if aws_creds.token:
			contents_list.append("security_token = " + aws_creds.token)

		contents = '\n'.join(contents_list)
		return [{'filename': filename, 'contents': contents}]
