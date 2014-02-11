"""
Configuration instructions are too long to repeat here:
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
"""

import sh
import os.path
from k.aws.tools.base import ToolBase
from k.aws.tools.base import ToolBase, TOOL_HOME, CONFIG_HOME

AWS_CLI_HOME = TOOL_HOME + "/AwsCli"

class AwsCliTool(ToolBase):
	def name(self):
		return "AWS CLI"

	def download_tool(self):
		# AWS CLI does not have a zip to download, so this step is
		# intentionally left blank
		pass

	def install_tool(self):
		sh.rm("-rf", AWS_CLI_HOME)
		sh.rm("-f", TOOL_HOME + "/bin/aws")
		sh.mkdir("-p", AWS_CLI_HOME)
		sh.virtualenv(AWS_CLI_HOME)
		sh.mkdir("-p", TOOL_HOME + "/bin")
		pip = sh.Command(AWS_CLI_HOME + "/bin/pip")
		pip("install", "awscli")
		sh.ln("-s", AWS_CLI_HOME + "/bin/aws", TOOL_HOME + "/bin/aws")

	def installed(self):
		return os.path.exists(AWS_CLI_HOME)

	def paths(self):
		return ["$TOOL_HOME/bin"]

	def add_variables(self, region_aws_creds):
		aws_creds = region_aws_creds.creds
		region = region_aws_creds.region_name
		return {
			'TOOL_HOME': TOOL_HOME,
			'AWS_ACCESS_KEY_ID': aws_creds.access,
			'AWS_SECRET_ACCESS_KEY': aws_creds.secret,
			'AWS_SECURITY_TOKEN': aws_creds.token,
			'AWS_DEFAULT_REGION': region
		}
