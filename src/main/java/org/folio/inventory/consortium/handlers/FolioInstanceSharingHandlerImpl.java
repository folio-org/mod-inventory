package org.folio.inventory.consortium.handlers;

import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.domain.instances.Instance;

public class FolioInstanceSharingHandlerImpl implements InstanceSharingHandler {

  private static final Logger LOGGER = LogManager.getLogger(FolioInstanceSharingHandlerImpl.class);

  private final InstanceOperationsHelper instanceOperations;

  public FolioInstanceSharingHandlerImpl(InstanceOperationsHelper instanceOperations) {
    this.instanceOperations = instanceOperations;
  }

  public Future<String> publishInstance(Instance instance, SharingInstance sharingInstanceMetadata,
                                        SourceTenantProvider sourceTenantProvider,
                                        TargetTenantProvider targetTenantProvider, Map<String, String> kafkaHeaders) {

    String instanceId = instance.getId();
    String sourceTenantId = sharingInstanceMetadata.getSourceTenantId();
    String targetTenantId = sharingInstanceMetadata.getTargetTenantId();

    LOGGER.info("publishInstance:: Publishing instance with InstanceId={} from tenant={} to tenant={}.",
      instanceId, sourceTenantId, targetTenantId);

    // Remove HRID_KEY from the instance JSON
    JsonObject jsonInstance = new JsonObject(instance.getJsonForStorage().encode());
    jsonInstance.remove(HRID_KEY);

    // Add instance to the targetInstanceCollection
    return instanceOperations.addInstance(Instance.fromJson(jsonInstance), targetTenantProvider)
      .compose(targetInstance -> {
        JsonObject jsonInstanceToPublish = new JsonObject(instance.getJsonForStorage().encode());
        jsonInstanceToPublish.put(SOURCE_KEY, CONSORTIUM_FOLIO.getValue());
        jsonInstanceToPublish.put(HRID_KEY, targetInstance.getHrid());

        // Update instance in sourceInstanceCollection
        return instanceOperations.updateInstance(Instance.fromJson(jsonInstanceToPublish), sourceTenantProvider)
          .recover(cause -> rollbackSharedInstance(instanceId, sourceTenantProvider, targetTenantProvider, cause));
      });
  }

  /**
   * Removes the instance added to the target tenant and re-saves the source instance so that it gets back into
   * the search index, then re-fails with the original cause.
   */
  private Future<String> rollbackSharedInstance(String instanceId, SourceTenantProvider sourceTenantProvider,
                                                TargetTenantProvider targetTenantProvider, Throwable cause) {
    String targetTenantId = targetTenantProvider.tenantId();
    LOGGER.warn("rollbackSharedInstance:: Rolling back instance: {} shared to target tenant: {}",
      instanceId, targetTenantId, cause);

    return instanceOperations.deleteInstance(instanceId, targetTenantProvider)
      .transform(ar -> {
        if (ar.failed()) {
          LOGGER.error("rollbackSharedInstance:: Failed to delete instance: {} on target tenant: {}.",
            instanceId, targetTenantId, ar.cause());
        }
        return instanceOperations.republishInstance(instanceId, sourceTenantProvider);
      })
      .transform(ar -> {
        if (ar.failed()) {
          LOGGER.error("rollbackSharedInstance:: Failed to re-save instance: {} on source tenant: {}.",
            instanceId, sourceTenantProvider.tenantId(), ar.cause());
        }
        return Future.failedFuture(cause);
      });
  }
}
