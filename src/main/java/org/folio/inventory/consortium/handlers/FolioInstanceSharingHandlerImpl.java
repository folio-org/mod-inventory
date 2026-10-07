package org.folio.inventory.consortium.handlers;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.domain.instances.Instance;

import java.util.Map;

import static org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler.SOURCE;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.items.Item.HRID_KEY;

public class FolioInstanceSharingHandlerImpl implements InstanceSharingHandler {

  private static final Logger LOGGER = LogManager.getLogger(FolioInstanceSharingHandlerImpl.class);

  private final InstanceOperationsHelper instanceOperations;

  public FolioInstanceSharingHandlerImpl(InstanceOperationsHelper instanceOperations) {
    this.instanceOperations = instanceOperations;
  }

  public Future<String> publishInstance(Instance instance, SharingInstance sharingInstanceMetadata,
                                        Source source, Target target, Map<String, String> kafkaHeaders) {

    String instanceId = instance.getId();
    String sourceTenantId = sharingInstanceMetadata.getSourceTenantId();
    String targetTenantId = sharingInstanceMetadata.getTargetTenantId();

    LOGGER.info("publishInstanceWithFolioSource :: Publishing instance with InstanceId={} from tenant={} to tenant={}.",
      instanceId, sourceTenantId, targetTenantId);

    // Remove HRID_KEY from the instance JSON
    JsonObject jsonInstance = new JsonObject(instance.getJsonForStorage().encode());
    jsonInstance.remove(HRID_KEY);

    // Add instance to the targetInstanceCollection
    return instanceOperations.addInstance(Instance.fromJson(jsonInstance), target)
      .compose(targetInstance -> {
        JsonObject jsonInstanceToPublish = new JsonObject(instance.getJsonForStorage().encode());
        jsonInstanceToPublish.put(SOURCE, CONSORTIUM_FOLIO.getValue());
        jsonInstanceToPublish.put(HRID_KEY, targetInstance.getHrid());

        // Update instance in sourceInstanceCollection
        return instanceOperations.updateInstance(Instance.fromJson(jsonInstanceToPublish), source)
          .recover(cause -> rollbackSharedInstance(instanceId, source, target, cause));
      });
  }

  /**
   * Removes the instance added to the target tenant and re-saves the source instance so that it gets back into
   * the search index, then re-fails with the original cause. If the instance cannot be deleted it is left as is.
   */
  private Future<String> rollbackSharedInstance(String instanceId, Source source, Target target, Throwable cause) {
    String targetTenantId = target.getTenantId();
    LOGGER.warn("rollbackSharedInstance:: Rolling back instance: {} shared to target tenant: {}",
      instanceId, targetTenantId, cause);

    return instanceOperations.deleteInstance(instanceId, target)
      .compose(v -> instanceOperations.republishInstance(instanceId, source)
        .transform(ar -> {
          if (ar.failed()) {
            LOGGER.error("rollbackSharedInstance:: Failed to re-save instance: {} on source tenant: {}.",
              instanceId, source.getTenantId(), ar.cause());
          }
          return Future.succeededFuture();
        }))
      .transform(ar -> {
        if (ar.failed()) {
          LOGGER.error("rollbackSharedInstance:: Failed to delete instance: {} on target tenant: {}. "
                       + "The instance is left as is.", instanceId, targetTenantId, ar.cause());
        }
        return Future.failedFuture(cause);
      });
  }

}
