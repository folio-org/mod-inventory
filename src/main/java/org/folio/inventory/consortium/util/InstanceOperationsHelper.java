package org.folio.inventory.consortium.util;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.folio.inventory.dataimport.util.DataImportConstants.ALREADY_EXISTS_ERROR_MSG;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.inventory.consortium.exceptions.StorageOperationException;
import org.folio.inventory.consortium.handlers.TenantProvider;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.OptimisticLockingException;
import org.folio.kafka.exception.DuplicateEventException;

public class InstanceOperationsHelper {

  private static final Logger LOGGER = LogManager.getLogger(InstanceOperationsHelper.class);

  public Future<Instance> addInstance(Instance instance, TenantProvider tenantProvider) {
    var tenantId = tenantProvider.tenantId();
    var instanceId = instance.getId();
    LOGGER.info("addInstance :: Adding instance with InstanceId={} to tenant={}", instanceId, tenantId);

    Promise<Instance> promise = Promise.promise();
    tenantProvider.instanceCollection().add(instance, insertSuccess -> promise.complete(insertSuccess.result()),
      insertFailure -> {
        //This is a temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(insertFailure.reason()) && insertFailure.reason()
          .contains(String.format(ALREADY_EXISTS_ERROR_MSG, instanceId))) {
          LOGGER.info("addInstance :: Duplicated event received by InstanceId={}. Ignoring...", instanceId);
          promise.fail(new DuplicateEventException(format("Duplicated event by InstanceId=%s", instanceId)));
        } else {
          LOGGER.error(format("addInstance :: Error adding instance with InstanceId=%s cause %s, status code %s",
            instanceId, insertFailure.reason(), insertFailure.statusCode()));
          promise.fail(new StorageOperationException(insertFailure));
        }
      });
    return promise.future();
  }

  public Future<Instance> getInstanceById(String instanceId, TenantProvider tenantProvider) {
    var tenantId = tenantProvider.tenantId();
    LOGGER.info("getInstanceById :: Get instance by InstanceId={} from tenant={}", instanceId, tenantId);
    Promise<Instance> promise = Promise.promise();
    tenantProvider.instanceCollection()
      .findById(instanceId, success -> {
        if (success.result() == null) {
          String errorMessage =
            format("Can't find instance by InstanceId=%s on tenant=%s.", instanceId, tenantId);
          LOGGER.warn("getInstanceById:: {}", errorMessage);
          promise.fail(new NotFoundException(errorMessage));
        } else {
          LOGGER.debug("getInstanceById :: Instance with InstanceId={} is present on tenant={}.", instanceId,
            tenantId);
          promise.complete(success.result());
        }
      }, failure -> {
        LOGGER.error(
          format("getInstanceById :: Error retrieving instance by InstanceId=%s from tenant=%s - %s, status code %s",
            instanceId, tenantId, failure.reason(), failure.statusCode()));
        promise.fail(new StorageOperationException(failure));
      });
    return promise.future();
  }

  public Future<String> updateInstance(Instance instance, TenantProvider tenantProvider) {
    var tenantId = tenantProvider.tenantId();
    var instanceId = instance.getId();
    LOGGER.info("updateInstanceInStorage :: Updating instance with InstanceId={} on tenant={}",
      instanceId, tenantId);
    Promise<String> promise = Promise.promise();
    tenantProvider.instanceCollection().update(instance, updateSuccess -> promise.complete(instanceId),
      updateFailure -> {
        if (updateFailure.statusCode() == HttpStatus.SC_CONFLICT) {
          promise.fail(new OptimisticLockingException(updateFailure.reason()));
        } else {
          LOGGER.error(format("Error updating instance with InstanceId=%s. Reason: %s. Status code %s",
            instanceId, updateFailure.reason(), updateFailure.statusCode()));
          promise.fail(new StorageOperationException(updateFailure));
        }
      });
    return promise.future();
  }
}
