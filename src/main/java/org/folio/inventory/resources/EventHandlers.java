package org.folio.inventory.resources;

import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.UpdateInstanceEventHandler;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.SuccessResponse;
import org.folio.processing.events.utils.ZIPArchiver;

import java.util.HashMap;
import java.util.Map;

public class EventHandlers {

  private static final String INSTANCES_EVENT_HANDLER_PATH = "/inventory/handlers/instances";

  private final Storage storage;

  public EventHandlers(final Storage storage) {
    this.storage = storage;
  }

  public void register(Router router) {
    router
      .post(INSTANCES_EVENT_HANDLER_PATH)
      .handler(BodyHandler.create())
      .handler(this::handleInstanceUpdate);
  }

  private void handleInstanceUpdate(RoutingContext routingContext) {
    try {
      HashMap<String, String> eventPayload = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(routingContext.getBodyAsString()), HashMap.class);
      InstanceUpdateDelegate updateInstanceDelegate = new InstanceUpdateDelegate(storage);
      new UpdateInstanceEventHandler(updateInstanceDelegate, new WebContext(routingContext)).handle(eventPayload, getOkapiHeaders(routingContext), routingContext.vertx());
      SuccessResponse.noContent(routingContext.response());
    } catch (Exception e) {
      ServerErrorResponse.internalError(routingContext.response(), e);
    }
  }

  private Map<String, String> getOkapiHeaders(RoutingContext rc) {
    Map<String, String> okapiHeaders = new HashMap<>();
    rc.request().headers().forEach(headerEntry -> {
      String headerKey = headerEntry.getKey().toLowerCase();
      if (headerKey.startsWith("x-okapi")) {
        okapiHeaders.put(headerKey, headerEntry.getValue());
      }
    });
    return okapiHeaders;
  }

}
