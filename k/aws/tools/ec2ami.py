"""
EC2 AMI tools support environment variables from the EC2 API tools
EC2 AMI tools are downloadable from:
     http://s3.amazonaws.com/ec2-downloads/ec2-ami-tools.zip
"""

import sh
import os.path
from k.aws.tools.base import ToolBase
from k.aws.tools.base import ToolBase, TOOL_HOME, CONFIG_HOME

DOWNLOAD = TOOL_HOME + "/ec2-ami-tools.zip"
AMI_HOME = TOOL_HOME + "/ec2-ami-tools"

class Ec2AmiTool(ToolBase):
	def name(self):
		return "Ec2 ami tools"

	def download_tool(self):
		sh.mkdir("-p", TOOL_HOME)
		sh.rm("-f", DOWNLOAD)
		results = sh.wget("--no-check-certificate",
			"http://s3.amazonaws.com/ec2-downloads/ec2-ami-tools.zip",
			"-O", DOWNLOAD)

	def _return_path_bit(self, path):
		for p in path.split('/'):
			if p.startswith('ec2-ami-tools'):
				return p

	def install_tool(self):
		results = sh.unzip("-o", DOWNLOAD, "-d", TOOL_HOME)
		parts = results.split('\n')
		for part in parts:
			if part.find("inflating") > -1:
				path = self._return_path_bit(part.strip().split(" ")[1])
				break
		sh.rm("-f", AMI_HOME)
		sh.ln("-s", TOOL_HOME + "/" + path, AMI_HOME)

	def installed(self):
		return os.path.exists(AMI_HOME)

	def paths(self):
		return ["$EC2_AMITOOL_HOME/bin"]

	def add_variables(self, region_aws_creds):
		aws_creds = region_aws_creds.creds
		region = region_aws_creds.region_name
		return {
			'AWS_ACCESS_KEY': aws_creds.access,
			'AWS_SECRET_KEY': aws_creds.secret,
			'AWS_DELEGATION_TOKEN': aws_creds.token,
			'EC2_AMITOOL_HOME': AMI_HOME,
			'REGION': region
			}
