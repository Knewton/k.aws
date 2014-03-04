# k.aws

k.aws is a library and a number of tools designed for making operations in aws simpler.  There's a strong focus on making working with multiple aws accounts easier.

## key configuration

To access AWS service, you need access and secret keys.  One pair per billing environment/ AWS account.  Setting these keys up as below enables you to use all of the k.aws tools tools, and via k.aws tools, most of the Amazon authored tools as well (ec2-tools, aws-cli, etc)

You will need to create files for your credentials.
> mkdir -p ~/.k.aws

In that directory, you need a yml file for each aws account, and they must be named for the account
Platform

>    production.yml
>    staging.yml
>    ....

You can also create other files with names of your own choice for any iams that are created for specific purposes.

### Contents

> access_key: [YOUR ACCESS KEY ID]
> secret_key: [YOUR SECRET ACCESS KEY]  # Secret Access Key

If you happen to have additional iam credentials, you can create additional files with any name you choose.  When using a k.aws based program or program based on k.aws's configuration, the -e flag is specifying which of these files to read.  e.g. -e staging reads from staging.yml

## k.aws tools

### About

k.aws has a number of useful tools for working with amazon.  It also offers a tool downloader and configurator for amazon supplied tools.

### Setup
You want to make the tools available, regardless of what virtualenv you are using.
mkdir -p ~/bin
/opt/virtualenvs/k.aws/bin/k.aws-tool-link.sh

This will symlink all the k.aws tools into ~/bin.

#### Add to path

You will also need to add ~/bin to your path. Edit your .bash_profile (OS X) or .bashrc or equivalent (Ubuntu) to add:
export PATH=$PATH:$HOME/bin
source /opt/virtualenvs/k.aws/bin/aws-env-aliases.sh

You may also want to add the current knewton aws environment to your prompt. Edit your .bash_profile (OS X) or .bashrc or equivalent (Ubuntu) to add:
export PS1="$AWS_ACCOUNT $PS1"

#### aws-env-aliases

The aws-env-aliases script creates shell aliases named paws, saws onto production and staging.  If you run this command, it will create environment variables to configure your usage to the k.aws tools to execute against that account.  You no longer need to pass in -e once this has been done unless you wish to access an account other then the one currently sourced.  Finally, if you install the ec2 tools below, these aliases will properly configure all the ec2 tools by setting environment variables and writing config files to make these tools work for the currently sourced env.
aws keys

## ec2 tools

To install a variety of aws tools, run aws-tool-setup

This will download the tools to ~/.k.aws/tools.  Their usage can be configured by using the p|s|u|uat|analaws aliases created above.  This will install the following ec2 tools:

*  ec2 api tools
*  ec2 ami tools
*  AutoScaling tools
*  ElastiCache tools
*  CloudFormation tools
*  CloudWatch tools
*  ElasticLoadBalancing tools
*  IAM tools
*  RDS tools

If you want additional tools to be included, contact the systems engineering group.

### Note

If you do not normally have JAVA_HOME in your environment or wish to use a different jvm for the ec2 tools then your normal jvm, you will need to add an extra config file.  You will need to create the file .k.aws/tools.yml with the following contents:

> variables:
>     JAVA_HOME: /usr/lib/jvm/java-7-oracle # Obviously this needs to point at the path of your java installation.  This path is for mine

### Usage

With the tools installed, use aws-env to set the current aws account.  You can then use all the tools listed.
