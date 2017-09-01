#!/usr/bin/env bash

instance_id=${1:-localhost-9403}
tenant_id=${2:-demo_tenant}
module_id=${3:-inventory-5.1.2-SNAPSHOT}

./okapi-registration/unmanaged-deployment/unregister.sh \
  ${instance_id} \
  ${module_id} \
  ${tenant_id}
