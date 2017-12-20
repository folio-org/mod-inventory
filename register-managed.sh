#!/usr/bin/env bash

tenant_id=${1:-demo_tenant}
okapi_proxy_address=${2:-http://localhost:9130}
module_id=${3:-mod-inventory-6.0.1-SNAPSHOT}

./okapi-registration/managed-deployment/register.sh \
  ${module_id} \
  ${okapi_proxy_address} \
  ${tenant_id}

