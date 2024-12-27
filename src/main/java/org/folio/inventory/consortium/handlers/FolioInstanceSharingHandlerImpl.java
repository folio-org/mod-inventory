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

    // 4
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
        return instanceOperations.updateInstance(Instance.fromJson(jsonInstanceToPublish), source);
      });
  }

}
