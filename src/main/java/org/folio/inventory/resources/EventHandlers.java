package org.folio.inventory.resources;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.handlers.action.CreateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.matching.InstanceLoader;
import org.folio.inventory.dataimport.handlers.matching.MatchInstanceEventHandler;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.SuccessResponse;
import org.folio.processing.events.EventManager;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.writer.holding.HoldingsWriterFactory;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;

public class EventHandlers {

  private static final String DATA_IMPORT_EVENT_HANDLER_PATH = "/inventory/handlers/data-import";

  public EventHandlers(final Storage storage) {
    MatchValueLoaderFactory.register(new InstanceLoader(storage));
    EventManager.registerEventHandler(new MatchInstanceEventHandler());
    EventManager.registerEventHandler(new CreateHoldingEventHandler(storage));
    MappingManager.registerWriterFactory(new HoldingsWriterFactory());
    MatchValueReaderFactory.register(new MarcValueReaderImpl());
  }

  public void register(Router router) {
    router
      .post(DATA_IMPORT_EVENT_HANDLER_PATH)
      .handler(BodyHandler.create())
      .handler(this::handleEvent);
  }

  private void handleEvent(RoutingContext routingContext) {
    try {
      JsonObject requestBody = routingContext.getBodyAsJson();
      DataImportEventPayload eventPayload = requestBody.mapTo(DataImportEventPayload.class);
      routingContext.vertx().executeBlocking(blockingFuture -> EventManager.handleEvent(eventPayload), null);
      SuccessResponse.noContent(routingContext.response());
    } catch (Exception e) {
      ServerErrorResponse.internalError(routingContext.response(), e);
    }

  }
}
