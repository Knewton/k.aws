from k.aws.tools.aws import AwsCliTool
from k.aws.tools.autoscale import AutoScaleTool
from k.aws.tools.cloudformation import CloudFormationTool
from k.aws.tools.cloudwatch import CloudWatchTool
from k.aws.tools.ec2ami import Ec2AmiTool
from k.aws.tools.ec2api import Ec2ApiTool
from k.aws.tools.elasticache import ElasticacheTool
from k.aws.tools.elb import ElbTool
from k.aws.tools.iam import IamTool
from k.aws.tools.rds import RdsTool
from k.aws.tools.s3cmd import S3CmdTool

def get_tools():
	return [
		AwsCliTool(),
		AutoScaleTool(),
		CloudFormationTool(),
		CloudWatchTool(),
		Ec2AmiTool(),
		Ec2ApiTool(),
		ElasticacheTool(),
		ElbTool(),
		IamTool(),
		RdsTool(),
		S3CmdTool()]

