package org.folio.inventory.resources;

import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.HashMap;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.HoldingLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.InstanceLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.ItemLoader;
import org.folio.inventory.dataimport.handlers.actions.UpdateInstanceEventHandler;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.SuccessResponse;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.rest.tools.utils.ObjectMapperTool;

public class EventHandlers {

  private static final String DATA_IMPORT_EVENT_HANDLER_PATH = "/inventory/handlers/data-import";
  private static final String INSTANCES_EVENT_HANDLER_PATH = "/inventory/handlers/instances";

  private WorkerExecutor executor;
  private Storage storage;

  public EventHandlers(final Storage storage) {
    Vertx vertx = Vertx.vertx();
    this.storage = storage;
    this.executor = vertx.createSharedWorkerExecutor("di-event-handling-thread-pool");
    MatchValueLoaderFactory.register(new InstanceLoader(storage, vertx));
    MatchValueLoaderFactory.register(new ItemLoader(storage, vertx));
    MatchValueLoaderFactory.register(new HoldingLoader(storage, vertx));

    MatchValueReaderFactory.register(new MarcValueReaderImpl());

    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    EventManager.registerEventHandler(new MatchInstanceEventHandler());
    EventManager.registerEventHandler(new MatchItemEventHandler());
    EventManager.registerEventHandler(new MatchHoldingEventHandler());
    EventManager.registerEventHandler(new CreateItemEventHandler(storage));
    EventManager.registerEventHandler(new CreateHoldingEventHandler(storage));
    EventManager.registerEventHandler(new CreateInstanceEventHandler(storage));
  }

  public void register(Router router) {
    router
      .post(DATA_IMPORT_EVENT_HANDLER_PATH)
      .handler(BodyHandler.create())
      .handler(this::handleDataImportEvent);
    router
      .post(INSTANCES_EVENT_HANDLER_PATH)
      .handler(BodyHandler.create())
      .handler(this::handleInstanceUpdate);
  }

  private void handleDataImportEvent(RoutingContext routingContext) {
    try {
      DataImportEventPayload eventPayload = new JsonObject(ZIPArchiver.unzip(routingContext.getBodyAsString())).mapTo(DataImportEventPayload.class);
      executor.executeBlocking(blockingFuture -> EventManager.handleEvent(eventPayload), null);
      SuccessResponse.noContent(routingContext.response());
    } catch (Exception e) {
      ServerErrorResponse.internalError(routingContext.response(), e);
    }
  }

  private void handleInstanceUpdate(RoutingContext routingContext) {
    try {
      HashMap<String, String> eventPayload = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(routingContext.getBodyAsString()), HashMap.class);
      new UpdateInstanceEventHandler(storage, new WebContext(routingContext)).handle(eventPayload);
      SuccessResponse.noContent(routingContext.response());
    } catch (Exception e) {
      ServerErrorResponse.internalError(routingContext.response(), e);
    }
  }

}
