package org.folio.inventory.dataimport.consumers;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_ERROR;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateItemEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateMarcAuthorityEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateMarcHoldingsEventHandler;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.MarcBibMatchedPostProcessingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.MarcBibModifiedPostProcessingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.HoldingLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.InstanceLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.ItemLoader;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.EventManager;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcHoldingsReaderFactory;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.matching.reader.StaticValueReaderImpl;
import org.folio.rest.jaxrs.model.Event;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

public class DataImportKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(DataImportKafkaHandler.class);
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String PROFILE_SNAPSHOT_ID_KEY = "JOB_PROFILE_SNAPSHOT_ID";

  private KafkaInternalCache kafkaInternalCache;
  private Vertx vertx;
  private ProfileSnapshotCache profileSnapshotCache;
  private MappingMetadataCache mappingMetadataCache;


  public DataImportKafkaHandler(Vertx vertx, Storage storage, HttpClient client, KafkaInternalCache kafkaInternalCache,
                                ProfileSnapshotCache profileSnapshotCache, MappingMetadataCache mappingMetadataCache) {
    this.vertx = vertx;
    this.kafkaInternalCache = kafkaInternalCache;
    this.profileSnapshotCache = profileSnapshotCache;
    this.mappingMetadataCache = mappingMetadataCache;
    registerDataImportProcessingHandlers(storage, client);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      Event event = Json.decodeValue(record.value(), Event.class);
      if (!kafkaInternalCache.containsByKey(event.getId())) {
        kafkaInternalCache.putToCache(event.getId());
        DataImportEventPayload eventPayload = Json.decodeValue(event.getEventPayload(), DataImportEventPayload.class);
        String recordId = extractRecordId(record.headers());
        LOGGER.info("Data import event payload has been received with event type: {}, recordId: {}", eventPayload.getEventType(), recordId);
        eventPayload.getContext().put(RECORD_ID_HEADER, recordId);

        Context context = EventHandlingUtil.constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl());
        String jobProfileSnapshotId = eventPayload.getContext().get(PROFILE_SNAPSHOT_ID_KEY);
        profileSnapshotCache.get(jobProfileSnapshotId, context)
          .toCompletionStage()
          .thenCompose(snapshotOptional -> snapshotOptional
            .map(profileSnapshot -> EventManager.handleEvent(eventPayload, profileSnapshot))
            .orElse(CompletableFuture.failedFuture(new EventProcessingException(format("Job profile snapshot with id '%s' does not exist", jobProfileSnapshotId)))))
          .whenComplete((processedPayload, throwable) -> {
            if (throwable != null) {
              promise.fail(throwable);
            } else if (DI_ERROR.value().equals(processedPayload.getEventType())) {
              promise.fail("Failed to process data import event payload");
            } else {
              promise.complete(record.key());
            }
          });
        return promise.future();
      }
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), e);
      return Future.failedFuture(e);
    }
    return Future.succeededFuture();
  }

  private void registerDataImportProcessingHandlers(Storage storage, HttpClient client) {
    MatchValueLoaderFactory.register(new InstanceLoader(storage, vertx));
    MatchValueLoaderFactory.register(new ItemLoader(storage, vertx));
    MatchValueLoaderFactory.register(new HoldingLoader(storage, vertx));

    MatchValueReaderFactory.register(new MarcValueReaderImpl());
    MatchValueReaderFactory.register(new StaticValueReaderImpl());

    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    MappingManager.registerReaderFactory(new MarcHoldingsReaderFactory());
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(WebClient.wrap(client));
    EventManager.registerEventHandler(new MatchInstanceEventHandler(mappingMetadataCache));
    EventManager.registerEventHandler(new MatchItemEventHandler(mappingMetadataCache));
    EventManager.registerEventHandler(new MatchHoldingEventHandler(mappingMetadataCache));
    EventManager.registerEventHandler(new CreateItemEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new CreateHoldingEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new CreateInstanceEventHandler(storage, precedingSucceedingTitlesHelper, mappingMetadataCache));
    EventManager.registerEventHandler(new CreateMarcHoldingsEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new CreateMarcAuthorityEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new UpdateItemEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new UpdateHoldingEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new ReplaceInstanceEventHandler(storage, precedingSucceedingTitlesHelper, mappingMetadataCache));
    EventManager.registerEventHandler(new MarcBibModifiedPostProcessingEventHandler(new InstanceUpdateDelegate(storage), precedingSucceedingTitlesHelper, mappingMetadataCache));
    EventManager.registerEventHandler(new MarcBibMatchedPostProcessingEventHandler(storage));
  }

  private String extractRecordId(List<KafkaHeader> headers) {
    return headers.stream()
      .filter(header -> header.key().equals(RECORD_ID_HEADER))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
