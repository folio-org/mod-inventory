package org.folio.inventory.resources;


import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.ext.web.client.WebClient;

import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateItemEventHandler;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.MarcBibModifiedPostProcessingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.HoldingLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.InstanceLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.ItemLoader;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.EventManager;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.matching.reader.StaticValueReaderImpl;

public class EventHandlers {

  private static final int DEFAULT_HTTP_TIMEOUT_IN_MILLISECONDS = 3000;

  public EventHandlers(final Storage storage) {
    Vertx vertx = Vertx.vertx();
    HttpClientOptions params = new HttpClientOptions().setConnectTimeout(DEFAULT_HTTP_TIMEOUT_IN_MILLISECONDS);
    HttpClient client = vertx.createHttpClient(params);
    MatchValueLoaderFactory.register(new InstanceLoader(storage, vertx));
    MatchValueLoaderFactory.register(new ItemLoader(storage, vertx));
    MatchValueLoaderFactory.register(new HoldingLoader(storage, vertx));

    MatchValueReaderFactory.register(new MarcValueReaderImpl());
    MatchValueReaderFactory.register(new StaticValueReaderImpl());

    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    EventManager.registerEventHandler(new MatchInstanceEventHandler());
    EventManager.registerEventHandler(new MatchItemEventHandler());
    EventManager.registerEventHandler(new MatchHoldingEventHandler());
    EventManager.registerEventHandler(new CreateItemEventHandler(storage));
    EventManager.registerEventHandler(new CreateHoldingEventHandler(storage));
    EventManager.registerEventHandler(new CreateInstanceEventHandler(storage, WebClient.wrap(client)));
    EventManager.registerEventHandler(new UpdateItemEventHandler(storage));
    EventManager.registerEventHandler(new UpdateHoldingEventHandler(storage));
    EventManager.registerEventHandler(new ReplaceInstanceEventHandler(storage, WebClient.wrap(client)));
    EventManager.registerEventHandler(new MarcBibModifiedPostProcessingEventHandler(new InstanceUpdateDelegate(storage)));
  }

}
