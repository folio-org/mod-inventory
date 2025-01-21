package org.folio.inventory.consortium.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.consortium.handlers.TenantProvider;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.kafka.exception.DuplicateEventException;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.folio.inventory.dataimport.util.DataImportConstants.ALREADY_EXISTS_ERROR_MSG;

public class InstanceOperationsHelper {

  private static final Logger LOGGER = LogManager.getLogger(InstanceOperationsHelper.class);

  public Future<Instance> addInstance(Instance instance, TenantProvider tenantProvider) {

    LOGGER.info("addInstance :: Adding instance with InstanceId={} to tenant={}", instance.getId(),
      tenantProvider.getTenantId());

    Promise<Instance> promise = Promise.promise();
    tenantProvider.getInstanceCollection().add(instance, insertSuccess -> promise.complete(insertSuccess.getResult()),
      insertFailure -> {
        try {
          //This is a temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
          if (isNotBlank(insertFailure.getReason()) && insertFailure.getReason().contains(String.format(ALREADY_EXISTS_ERROR_MSG, instance.getId()))) {
            LOGGER.info("addInstance :: Duplicated event received by InstanceId={}. Ignoring...", instance.getId());
            promise.fail(new DuplicateEventException(format("Duplicated event by InstanceId=%s", instance.getId())));
          } else {
            LOGGER.error(format("addInstance :: Error adding instance with InstanceId=%s cause %s, status code %s",
              instance.getId(), insertFailure.getReason(), insertFailure.getStatusCode()));
            promise.fail(insertFailure.getReason());
          }
        } catch (Exception ex) {
          String errorMessage = format("Error processing insert instance with InstanceId=%s on tenant=%s failure. Error: %s",
            instance.getId(), tenantProvider.getTenantId(), ex.getCause());
          LOGGER.error("addInstance :: {}", errorMessage, ex);
          promise.fail(errorMessage);
        }
      });
    return promise.future();
  }

  public Future<Instance> getInstanceById(String instanceId, TenantProvider tenantProvider) {
    LOGGER.info("getInstanceById :: Get instance by InstanceId={} from tenant={}", instanceId, tenantProvider.getTenantId());
    Promise<Instance> promise = Promise.promise();
    tenantProvider.getInstanceCollection().findById(instanceId, success -> {
        if (success.getResult() == null) {
          String errorMessage = format("Can't find instance by InstanceId=%s on tenant=%s.", instanceId, tenantProvider.getTenantId());
          LOGGER.warn("getInstanceById:: {}", errorMessage);
          promise.fail(new NotFoundException(format(errorMessage)));
        } else {
          LOGGER.debug("getInstanceById :: Instance with InstanceId={} is present on tenant={}.", instanceId, tenantProvider.getTenantId());
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.error(format("getInstanceById :: Error retrieving instance by InstanceId=%s from tenant=%s - %s, status code %s",
          instanceId, tenantProvider.getTenantId(), failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  public Future<String> updateInstance(Instance instance, TenantProvider tenantProvider) {
    LOGGER.info("updateInstanceInStorage :: Updating instance with InstanceId={} on tenant={}",
      instance.getId(), tenantProvider.getTenantId());
    Promise<String> promise = Promise.promise();
    tenantProvider.getInstanceCollection().update(instance, updateSuccess -> promise.complete(instance.getId()),
      updateFailure -> {
        try {
          if (updateFailure.getStatusCode() == HttpStatus.SC_CONFLICT) {
            promise.fail(new OptimisticLockingException(updateFailure.getReason()));
          } else {
            LOGGER.error(format("Error updating instance with InstanceId=%s. Reason: %s. Status code %s",
              instance.getId(), updateFailure.getReason(), updateFailure.getStatusCode()));
            promise.fail(updateFailure.getReason());
          }
        } catch (Exception ex) {
          String errorMessage = format("Error processing update instance with InstanceId=%s on tenant=%s failure.",
            instance.getId(), tenantProvider.getTenantId());
          LOGGER.error(errorMessage, ex);
          promise.fail(ex);
        }
      });
    return promise.future();
  }

}
