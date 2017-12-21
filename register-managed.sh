#!/usr/bin/env bash

tenant_id=${1:-demo_tenant}
okapi_proxy_address=${2:-http://localhost:9130}

./okapi-registration/managed-deployment/register.sh \
  ${okapi_proxy_address} \
  ${tenant_id}

