package org.folio.inventory.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsUpdateOwnership;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import java.lang.invoke.MethodHandles;

import static org.folio.inventory.support.EndpointFailureHandler.handleFailure;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.UpdateOwnershipValidator.updateOwnershipHasRequiredFields;

public class UpdateOwnershipApi extends AbstractInventoryResource {
  private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());
  public static final String CONSORTIUM = "CONSORTIUM";

  public UpdateOwnershipApi(Storage storage, HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register(Router router) {
    router.post("/inventory/items/update-ownership")
      .handler(this::updateItemsOwnership);
    router.post("/inventory/holdings/update-ownership")
      .handler(this::updateHoldingsOwnership);
  }

  private void updateHoldingsOwnership(RoutingContext routingContext) {
    try {
      final var context = new WebContext(routingContext);
      final var updateOwnershipRequest = routingContext.body().asJsonObject();

      final var validationError = updateOwnershipHasRequiredFields(updateOwnershipRequest, HoldingsUpdateOwnership.class);

      if (validationError.isPresent()) {
        unprocessableEntity(routingContext.response(), validationError.get());
        return;
      }
      var holdingsUpdateOwnership = updateOwnershipRequest.mapTo(HoldingsUpdateOwnership.class);

      LOGGER.info("updateHoldingsOwnership:: Started updating ownership of holdings record: {}, to tenant: {}", holdingsUpdateOwnership.getHoldingsRecordIds(),
        holdingsUpdateOwnership.getTargetTenantId());

      storage.getInstanceCollection(context)
        .findById(holdingsUpdateOwnership.getToInstanceId())
        .thenAccept(instance -> {
          if (instance != null) {
            if (instance.getSource().startsWith(CONSORTIUM)) {
              handleFailure(new Throwable("not implemented yet"), routingContext);
            } else {
              JsonResponse.unprocessableEntity(routingContext.response(),
                String.format("Instance with id: %s is not shared", holdingsUpdateOwnership.getToInstanceId()));
            }
          } else {
            JsonResponse.unprocessableEntity(routingContext.response(),
              String.format("Instance with id: %s not found at source tenant, tenant: %s", holdingsUpdateOwnership.getToInstanceId(), context.getTenantId()));
          }
        })
        .exceptionally(e -> {
          ServerErrorResponse.internalError(routingContext.response(), e);
          return null;
        });
    } catch (Exception e) {
      LOGGER.warn(e);
      handleFailure(e, routingContext);
    }
  }

  private void updateItemsOwnership(RoutingContext routingContext) {
    // should be implemented in MODINV-1031
  }
}
