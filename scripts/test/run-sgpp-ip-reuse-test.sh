#!/usr/bin/env bash

# Script to run SGPP IP reuse scale tests
# This script performs continuous nodegroup scaling while validating IP reuse behavior

set -euoE pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BUILD_TEST_DIR="$SCRIPT_DIR/../../build"
SECONDS=0

# Scaling configuration (can be overridden)
: "${SGPP_MIN_CAPACITY:=3}"
: "${SGPP_MAX_CAPACITY:=6}"
: "${SGPP_SCALE_UP_WAIT:=2m}"
: "${SGPP_SCALE_DOWN_WAIT:=2m}"
: "${SGPP_TEST_DURATION:=30m}"
: "${SGPP_VALIDATION_INTERVAL:=30s}"
: "${EXTRA_GINKGO_FLAGS:=""}"

source "$SCRIPT_DIR"/lib/cluster.sh

cleanup(){
  if [[ $? == 0 ]]; then
    echo "Successfully ran SGPP IP reuse scale test in $(($SECONDS / 60)) minutes and $(($SECONDS % 60)) seconds"
  else
    echo "[Error] SGPP IP reuse scale test failed"
  fi
}

trap cleanup EXIT

function run_sgpp_ip_reuse_tests(){
  echo "Running SGPP IP reuse scale test with configuration:"
  echo "  Min Capacity: $SGPP_MIN_CAPACITY"
  echo "  Max Capacity: $SGPP_MAX_CAPACITY"
  echo "  Scale Up Wait: $SGPP_SCALE_UP_WAIT"
  echo "  Scale Down Wait: $SGPP_SCALE_DOWN_WAIT"
  echo "  Test Duration: $SGPP_TEST_DURATION"
  echo "  Validation Interval: $SGPP_VALIDATION_INTERVAL"
  
  (cd $BUILD_TEST_DIR/scale && CGO_ENABLED=0 ginkgo $EXTRA_GINKGO_FLAGS -v --timeout="$SGPP_TEST_DURATION" -- -cluster-kubeconfig=$KUBE_CONFIG_PATH -cluster-name=$CLUSTER_NAME --aws-region=$REGION --aws-vpc-id $VPC_ID)
}

echo "Running SGPP IP Reuse Scale Test with the following variables
KUBE CONFIG: $KUBE_CONFIG_PATH
CLUSTER_NAME: $CLUSTER_NAME
REGION: $REGION"

load_cluster_details
attach_controller_policy_cluster_role
set_env_aws_node "ENABLE_POD_ENI" "true"
run_sgpp_ip_reuse_tests
set_env_aws_node "ENABLE_POD_ENI" "false"
detach_controller_policy_cluster_role