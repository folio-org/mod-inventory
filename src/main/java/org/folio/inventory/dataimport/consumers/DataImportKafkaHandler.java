package org.folio.inventory.dataimport.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.folio.DataImportEventPayload;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateItemEventHandler;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.MarcBibMatchedPostProcessingEventHandler;
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
import org.folio.kafka.AsyncRecordHandler;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.matching.reader.StaticValueReaderImpl;
import org.folio.rest.jaxrs.model.Event;

import java.io.IOException;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_ERROR;

public class DataImportKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(DataImportKafkaHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();

  private Vertx vertx;

  public DataImportKafkaHandler(Vertx vertx, Storage storage, HttpClient client) {
    this.vertx = vertx;
    registerDataImportProcessingHandlers(storage, client);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      Event event = OBJECT_MAPPER.readValue(record.value(), Event.class);
      DataImportEventPayload eventPayload = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(DataImportEventPayload.class);
      LOGGER.info(format("Data import event payload has been received with event type: %s", eventPayload.getEventType()));

      EventManager.handleEvent(eventPayload).whenComplete((processedPayload, throwable) -> {
        if (throwable != null) {
          promise.fail(throwable);
        } else if (DI_ERROR.value().equals(processedPayload.getEventType())) {
          promise.fail("Failed to process data import event payload");
        } else {
          promise.complete(record.key());
        }
      });
      return promise.future();
    } catch (IOException e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), e);
      return Future.failedFuture(e);
    }
  }

  private void registerDataImportProcessingHandlers(Storage storage, HttpClient client) {
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
    EventManager.registerEventHandler(new MarcBibMatchedPostProcessingEventHandler(storage));
  }
}
