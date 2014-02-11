#!/bin/bash

set -e

DIR=$(cd "$(dirname $0)";pwd)
CONFIG_DIR=$DIR/../config

TARGET_DIR="$1"
if [[ "x$TARGET_DIR" == "x" ]]; then
	TARGET_DIR="~/bin"
fi
eval TARGET_DIR="$TARGET_DIR"
if [ ! -d "$TARGET_DIR" ]; then
	echo "$TARGET_DIR does not exist"
	exit 1
fi

scripts=( \
	asg-change-key \
	asg-from-instance \
	aws-env \
	aws-env-aliases.sh \
	aws-tool-setup \
	ec2-describe-snapshots \
	ec2-instance-count \
	ec2-list-groups \
	ec2-list-instances \
	ec2-list-tags \
	ec2-metadata \
	elb-cull-dead-instances \
	elb-dns-from-ids \
	elbs-from-instance-id \
	emr-get-log \
	emr-list \
	emr-new-job \
	emr-terminate \
	find-cfn-resource \
	iam-list-users \
	k.aws-tool-link.sh \
	raw-data-backup \
	s3-clean \
	s3-compare-listings \
	s3-copy-bucket \
	s3-copy-key \
	s3-create-bucket \
	s3-delete \
	s3-delete-bucket \
	s3-describe-bucket \
	s3-destroy-bucket \
	s3-get \
	s3-list \
	s3-list-buckets \
	s3-put \
	s3-set-acl \
	s3-sync \
	s3-sync-local \
	sdb-backup \
	sdb-create-domain \
	sdb-create-item \
	sdb-delete-item \
	sdb-get-item \
	sdb-list-domains \
	sdb-query \
	sdb-update-item \
	sqs-get-message \
	sqs-list-queues \
	sqs-post-message \
)

for script in ${scripts[@]}; do
	cmd="ln -sf $DIR/$script $TARGET_DIR"
	#echo "$cmd"
	$cmd
done
