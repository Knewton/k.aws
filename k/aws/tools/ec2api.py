"""
EC2 API tools support the following environment variables (from -h)
     -O, --aws-access-key KEY
          AWS Access Key ID. Defaults to the value of the AWS_ACCESS_KEY
          environment variable (if set).

     -W, --aws-secret-key KEY
          AWS Secret Access Key. Defaults to the value of the AWS_SECRET_KEY
          environment variable (if set).

     -T, --security-token TOKEN
          AWS delegation token. Defaults to the value of the AWS_DELEGATION_TOKEN

     --region REGION

EC2 API tools are downloadable from:
     http://s3.amazonaws.com/ec2-downloads/ec2-api-tools.zip
"""

import sh
import os.path
from k.aws.tools.base import ToolBase
from k.aws.tools.base import ToolBase, TOOL_HOME, CONFIG_HOME

DOWNLOAD = TOOL_HOME + "/ec2-api-tools.zip"
EC2_HOME = TOOL_HOME + "/ec2-api-tools"

class Ec2ApiTool(ToolBase):
	def name(self):
		return "Ec2 api tools"

	def download_tool(self):
		sh.mkdir("-p", TOOL_HOME)
		sh.rm("-f", DOWNLOAD)
		results = sh.wget("--no-check-certificate",
			"http://s3.amazonaws.com/ec2-downloads/ec2-api-tools.zip",
			"-O", DOWNLOAD)

	def _return_path_bit(self, path):
		for p in path.split('/'):
			if p.startswith('ec2-api-tools'):
				return p

	def install_tool(self):
		results = sh.unzip("-o", DOWNLOAD, "-d", TOOL_HOME)
		parts = results.split('\n')
		for part in parts:
			if part.find("inflating") > -1:
				path = self._return_path_bit(part.strip().split(" ")[1])
				break
		sh.rm("-f", EC2_HOME)
		sh.ln("-s", TOOL_HOME + "/" + path, EC2_HOME)
		self.rm_cmd_files(EC2_HOME)

	def installed(self):
		return os.path.exists(EC2_HOME)

	def paths(self):
		return ["$EC2_HOME/bin"]

	def add_variables(self, region_aws_creds):
		aws_creds = region_aws_creds.creds
		region = region_aws_creds.region_name
		return {
			'AWS_ACCESS_KEY': aws_creds.access,
			'AWS_SECRET_KEY': aws_creds.secret,
			'AWS_DELEGATION_TOKEN': aws_creds.token,
			'EC2_HOME': EC2_HOME,
			'REGION': region
			}
