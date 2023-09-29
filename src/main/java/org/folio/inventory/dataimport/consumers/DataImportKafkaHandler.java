package org.folio.inventory.dataimport.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.client.OrdersClient;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.dao.EntityIdStorageDaoImpl;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.HoldingsItemMatcherFactory;
import org.folio.inventory.dataimport.HoldingsMapperFactory;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.ItemsMapperFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.dataimport.handlers.actions.CreateAuthorityEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateItemEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateMarcHoldingsEventHandler;
import org.folio.inventory.dataimport.handlers.actions.DeleteAuthorityEventHandler;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.MarcBibMatchedPostProcessingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.MarcBibModifiedPostProcessingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateAuthorityEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateItemEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateMarcHoldingsEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchAuthorityEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.AuthorityLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.HoldingLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.InstanceLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.ItemLoader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.HoldingsPreloader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.InstancePreloader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.ItemPreloader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.OrdersPreloaderHelper;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.services.OrderHelperService;
import org.folio.inventory.dataimport.services.OrderHelperServiceImpl;
import org.folio.inventory.services.AuthorityIdStorageService;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.services.HoldingsIdStorageService;
import org.folio.inventory.services.InstanceIdStorageService;
import org.folio.inventory.services.ItemIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcHoldingsReaderFactory;
import org.folio.processing.matching.MatchingManager;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.matching.reader.StaticValueReaderImpl;
import org.folio.rest.jaxrs.model.Event;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_ERROR;

public class DataImportKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(DataImportKafkaHandler.class);
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String PROFILE_SNAPSHOT_ID_KEY = "JOB_PROFILE_SNAPSHOT_ID";

  private Vertx vertx;
  private ProfileSnapshotCache profileSnapshotCache;
  private MappingMetadataCache mappingMetadataCache;
  private KafkaConfig kafkaConfig;
  private OrderHelperService orderHelperService;
  private ConsortiumService consortiumService;

  public DataImportKafkaHandler(Vertx vertx, Storage storage, HttpClient client,
                                ProfileSnapshotCache profileSnapshotCache,
                                KafkaConfig kafkaConfig,
                                MappingMetadataCache mappingMetadataCache,
                                ConsortiumDataCache consortiumDataCache) {
    this.vertx = vertx;
    this.profileSnapshotCache = profileSnapshotCache;
    this.mappingMetadataCache = mappingMetadataCache;
    this.kafkaConfig = kafkaConfig;
    orderHelperService = new OrderHelperServiceImpl(profileSnapshotCache);
    consortiumService = new ConsortiumServiceImpl(client, consortiumDataCache);
    registerDataImportProcessingHandlers(storage, client);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      Event event = Json.decodeValue(record.value(), Event.class);
      DataImportEventPayload eventPayload = Json.decodeValue(event.getEventPayload(), DataImportEventPayload.class);
      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      String recordId = headersMap.get(RECORD_ID_HEADER);
      String chunkId = headersMap.get(CHUNK_ID_HEADER);
      String jobExecutionId = eventPayload.getJobExecutionId();
      LOGGER.info("Data import event payload has been received with event type: {}, recordId: {} by jobExecution: {} and chunkId: {}", eventPayload.getEventType(), recordId, jobExecutionId, chunkId);
      eventPayload.getContext().put(RECORD_ID_HEADER, recordId);
      eventPayload.getContext().put(CHUNK_ID_HEADER, chunkId);

      Context context = EventHandlingUtil.constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl());
      String jobProfileSnapshotId = eventPayload.getContext().get(PROFILE_SNAPSHOT_ID_KEY);
      profileSnapshotCache.get(jobProfileSnapshotId, context)
        .toCompletionStage()
        .thenCompose(snapshotOptional -> snapshotOptional
          .map(profileSnapshot -> EventManager.handleEvent(eventPayload, profileSnapshot))
          .orElse(CompletableFuture.failedFuture(new EventProcessingException(format("Job profile snapshot with id '%s' does not exist", jobProfileSnapshotId)))))
        .whenComplete((processedPayload, throwable) -> {
          if (throwable != null) {
            LOGGER.error(throwable.getMessage());
            promise.fail(throwable);
          } else if (DI_ERROR.value().equals(processedPayload.getEventType())) {
            LOGGER.warn("Failed to process data import event payload: {}", processedPayload.getEventType());
            promise.fail("Failed to process data import event payload");
          } else {
            promise.complete(record.key());
          }
        });
      return promise.future();
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), e);
      return Future.failedFuture(e);
    }
  }

  private void registerDataImportProcessingHandlers(Storage storage, HttpClient client) {
    OrdersClient ordersClient = new OrdersClient(WebClient.wrap(client));
    OrdersPreloaderHelper ordersPreloaderHelper = new OrdersPreloaderHelper(ordersClient);
    InstancePreloader instancePreloader = new InstancePreloader(ordersPreloaderHelper);
    HoldingsPreloader holdingsPreloader = new HoldingsPreloader(ordersPreloaderHelper);
    ItemPreloader itemPreloader = new ItemPreloader(ordersPreloaderHelper);

    MatchValueLoaderFactory.register(new InstanceLoader(storage, vertx, instancePreloader));
    MatchValueLoaderFactory.register(new ItemLoader(storage, vertx, itemPreloader));
    MatchValueLoaderFactory.register(new HoldingLoader(storage, vertx, holdingsPreloader));
    MatchValueLoaderFactory.register(new AuthorityLoader(storage, vertx));

    MatchValueReaderFactory.register(new MarcValueReaderImpl());
    MatchValueReaderFactory.register(new StaticValueReaderImpl());

    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    MappingManager.registerReaderFactory(new MarcHoldingsReaderFactory());
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerWriterFactory(new InstanceWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());
    MappingManager.registerMapperFactory(new ItemsMapperFactory());
    MatchingManager.registerMatcherFactory(new HoldingsItemMatcherFactory());

    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(WebClient.wrap(client));
    EventManager.registerEventHandler(new MatchInstanceEventHandler(mappingMetadataCache));
    EventManager.registerEventHandler(new MatchItemEventHandler(mappingMetadataCache));
    EventManager.registerEventHandler(new MatchHoldingEventHandler(mappingMetadataCache));
    EventManager.registerEventHandler(new MatchAuthorityEventHandler(mappingMetadataCache));
    EventManager.registerEventHandler(new CreateItemEventHandler(storage, mappingMetadataCache, new ItemIdStorageService(new EntityIdStorageDaoImpl(new PostgresClientFactory(vertx))), orderHelperService));
    EventManager.registerEventHandler(new CreateHoldingEventHandler(storage, mappingMetadataCache, new HoldingsIdStorageService(new EntityIdStorageDaoImpl(new PostgresClientFactory(vertx))), orderHelperService, consortiumService));
    EventManager.registerEventHandler(new CreateInstanceEventHandler(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, new InstanceIdStorageService(new EntityIdStorageDaoImpl(new PostgresClientFactory(vertx))), orderHelperService));
    EventManager.registerEventHandler(new CreateMarcHoldingsEventHandler(storage, mappingMetadataCache, new HoldingsIdStorageService(new EntityIdStorageDaoImpl(new PostgresClientFactory(vertx))), new HoldingsCollectionService()));
    EventManager.registerEventHandler(new UpdateMarcHoldingsEventHandler(storage, mappingMetadataCache, new KafkaEventPublisher(kafkaConfig, vertx, 100)));
    EventManager.registerEventHandler(new CreateAuthorityEventHandler(storage, mappingMetadataCache, new AuthorityIdStorageService(new EntityIdStorageDaoImpl(new PostgresClientFactory(vertx)))));
    EventManager.registerEventHandler(new UpdateAuthorityEventHandler(storage, mappingMetadataCache, new KafkaEventPublisher(kafkaConfig, vertx, 100)));
    EventManager.registerEventHandler(new DeleteAuthorityEventHandler(storage));
    EventManager.registerEventHandler(new UpdateItemEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new UpdateHoldingEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new ReplaceInstanceEventHandler(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, client, consortiumService));
    EventManager.registerEventHandler(new MarcBibModifiedPostProcessingEventHandler(new InstanceUpdateDelegate(storage), precedingSucceedingTitlesHelper, mappingMetadataCache));
    EventManager.registerEventHandler(new MarcBibMatchedPostProcessingEventHandler(storage));
  }
}
