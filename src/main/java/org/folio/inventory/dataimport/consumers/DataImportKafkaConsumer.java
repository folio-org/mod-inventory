package org.folio.inventory.dataimport.consumers;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.DataImportHeaders;
import org.folio.inventory.client.InstanceLinkClient;
import org.folio.inventory.client.OrdersClient;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.dao.EntityIdStorageDao;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.HoldingsItemMatcherFactory;
import org.folio.inventory.dataimport.HoldingsMapperFactory;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.ItemsMapperFactory;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.inventory.dataimport.cache.DeleteRuleFor999FieldCache;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateItemEventHandler;
import org.folio.inventory.dataimport.handlers.actions.CreateMarcHoldingsEventHandler;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.MarcBibModifiedPostProcessingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateItemEventHandler;
import org.folio.inventory.dataimport.handlers.actions.UpdateMarcHoldingsEventHandler;
import org.folio.inventory.dataimport.handlers.actions.modify.MarcBibModifyEventHandler;
import org.folio.inventory.dataimport.handlers.matching.CommonMatchEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MarcBibliographicMatchEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchAuthorityEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.matching.MatchItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.HoldingLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.InstanceLoader;
import org.folio.inventory.dataimport.handlers.matching.loaders.ItemLoader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.HoldingsPreloader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.InstancePreloader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.ItemPreloader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.OrdersPreloaderHelper;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.services.OrderHelperService;
import org.folio.inventory.dataimport.services.OrderHelperServiceImpl;
import org.folio.inventory.dataimport.services.SnapshotService;
import org.folio.inventory.dataimport.util.LoggerUtil;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.services.HoldingsIdStorageService;
import org.folio.inventory.services.InstanceIdStorageService;
import org.folio.inventory.services.ItemIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.okapi.common.XOkapiHeaders;
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

public class DataImportKafkaConsumer implements AsyncRecordHandler<String, String> {

  public static final String PROFILE_SNAPSHOT_ID_KEY = "JOB_PROFILE_SNAPSHOT_ID";
  private static final Logger LOGGER = LogManager.getLogger(DataImportKafkaConsumer.class);

  private static final Set<String> CANCELLED_JOB_ALLOWED_EVENTS = Set.of(
    DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value()
  );

  private final Vertx vertx;
  private final ProfileSnapshotCache profileSnapshotCache;
  private final MappingMetadataCache mappingMetadataCache;
  private final DeleteRuleFor999FieldCache deleteRuleFor999FieldCache;
  private final KafkaConfig kafkaConfig;
  private final OrderHelperService orderHelperService;
  private final ConsortiumService consortiumService;
  private final CancelledJobsIdsCache cancelledJobsIdCache;

  public DataImportKafkaConsumer(Vertx vertx, Storage storage, HttpClient client, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.profileSnapshotCache = ProfileSnapshotCache.getInstance(vertx, client);
    this.mappingMetadataCache = MappingMetadataCache.getInstance(vertx);
    this.deleteRuleFor999FieldCache = DeleteRuleFor999FieldCache.getInstance(vertx);
    this.kafkaConfig = kafkaConfig;
    this.cancelledJobsIdCache = CancelledJobsIdsCache.getInstance();
    this.orderHelperService = new OrderHelperServiceImpl(this.profileSnapshotCache);
    this.consortiumService = new ConsortiumServiceImpl(client, ConsortiumDataCache.getInstance(vertx, client));
    registerDataImportProcessingHandlers(storage, client);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> kafkaRecord) {
    var kafkaTopic = kafkaRecord.topic();
    try {
      DataImportEventPayload eventPayload = Json.decodeValue(
        Json.decodeValue(kafkaRecord.value(), Event.class).getEventPayload(), DataImportEventPayload.class);
      Map<String, String> headersMap = extractHeaders(kafkaRecord);
      String recordId = headersMap.get(DataImportHeaders.RECORD_ID);
      String chunkId = headersMap.get(DataImportHeaders.CHUNK_ID);
      String jobExecutionId = eventPayload.getJobExecutionId();
      var eventType = eventPayload.getEventType();
      var tenant = eventPayload.getTenant();

      LOGGER.info("Data import event payload has been received with event type: {}, recordId: {} "
                  + "by jobExecution: {} and chunkId: {}", eventType, recordId, jobExecutionId, chunkId);

      if (shouldSkipEventProcessing(eventPayload)) {
        LOGGER.info("Skip processing of event, topic: '{}', tenantId: '{}', jobExecutionId: '{}' recordId: '{}'"
                    + " because the job has been cancelled", kafkaTopic, tenant, jobExecutionId, recordId);
        return Future.succeededFuture(kafkaRecord.key());
      }

      String userId = extractUserId(eventPayload, headersMap);
      String requestId = headersMap.get(XOkapiHeaders.REQUEST_ID.toLowerCase());
      if (isNull(userId)) {
        LOGGER.error("Data import event payload has been received with userId is null "
                     + "jobExecutionId: '{}' recordId: '{}'", jobExecutionId, recordId);
      }

      populatePayloadContext(eventPayload, headersMap, recordId, chunkId, userId, requestId);

      Context context = EventHandlingUtil.constructContext(tenant, eventPayload.getToken(),
        eventPayload.getOkapiUrl(), userId, requestId);

      Promise<String> promise = Promise.promise();
      processEvent(eventPayload, context, jobExecutionId, recordId, kafkaRecord.key(), promise);
      return promise.future();
    } catch (Exception e) {
      LOGGER.error("Failed to process data import kafka record from topic: {}", kafkaTopic, e);
      return Future.failedFuture(e);
    }
  }

  private Map<String, String> extractHeaders(KafkaConsumerRecord<String, String> kafkaRecord) {
    Map<String, String> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    headersMap.putAll(KafkaHeaderUtils.kafkaHeadersToMap(kafkaRecord.headers()));
    return headersMap;
  }

  private void populatePayloadContext(DataImportEventPayload eventPayload, Map<String, String> headersMap,
                                      String recordId, String chunkId, String userId, String requestId) {
    eventPayload.getContext().put(DataImportHeaders.RECORD_ID, recordId);
    eventPayload.getContext().put(DataImportHeaders.CHUNK_ID, chunkId);
    eventPayload.getContext().put(DataImportHeaders.USER_ID, userId);
    eventPayload.getContext().put(XOkapiHeaders.REQUEST_ID.toLowerCase(), requestId);
    populateWithPermissionsHeader(eventPayload, headersMap);
  }

  private void processEvent(DataImportEventPayload eventPayload, Context context,
                            String jobExecutionId, String recordId, String recordKey, Promise<String> promise) {
    String jobProfileSnapshotId = eventPayload.getContext().get(PROFILE_SNAPSHOT_ID_KEY);
    profileSnapshotCache.get(jobProfileSnapshotId, context)
      .onFailure(e -> sendPayloadWithDiError(eventPayload))
      .toCompletionStage()
      .thenCompose(snapshotOptional -> snapshotOptional
        .map(profileSnapshot -> EventManager.handleEvent(eventPayload, profileSnapshot))
        .orElse(CompletableFuture.failedFuture(new EventProcessingException(
          format("Job profile snapshot with id '%s' does not exist", jobProfileSnapshotId)))))
      .whenComplete((processedPayload, throwable) -> {
        if (throwable != null) {
          LOGGER.error("jobExecutionId: {} recordId: {} {}", jobExecutionId, recordId, throwable.getMessage());
          promise.fail(throwable);
        } else if (DI_ERROR.value().equals(processedPayload.getEventType())) {
          LOGGER.warn("Failed to process data import event payload: {} jobExecutionId: {} recordId: {}",
            processedPayload.getEventType(), jobExecutionId, recordId);
          promise.fail("Failed to process data import event payload");
        } else {
          promise.complete(recordKey);
        }
      });
  }

  private String extractUserId(DataImportEventPayload eventPayload, Map<String, String> headersMap) {
    String userId = headersMap.get(DataImportHeaders.USER_ID);
    if (isNull(userId)) {
      if (eventPayload.getAdditionalProperties().get(DataImportHeaders.USER_ID) != null) {
        userId = String.valueOf(eventPayload.getAdditionalProperties().get(DataImportHeaders.USER_ID));
      } else if (eventPayload.getContext().get("USER_ID") != null) {
        userId = eventPayload.getContext().get("USER_ID");
      } else {
        userId = headersMap.get(XOkapiHeaders.USER_ID.toLowerCase());
      }
    }
    return userId;
  }

  private void sendPayloadWithDiError(DataImportEventPayload eventPayload) {
    eventPayload.setEventType(DI_ERROR.value());
    try (var eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      eventPublisher.publish(eventPayload);
      var eventType = eventPayload.getEventType();
      var recordId = LoggerUtil.extractRecordId(eventPayload);
      LOGGER.warn("publish:: {}_Producer send error for event: '{}' by jobExecutionId: '{}' recordId: '{}' ",
        eventType, eventType, eventPayload.getJobExecutionId(), recordId);
    } catch (Exception e) {
      LOGGER.error("Error closing kafka publisher: {}", e.getMessage());
    }
  }

  private void registerDataImportProcessingHandlers(Storage storage, HttpClient client) {
    registerPreloaders(storage, client);
    registerMatchValueReaders();
    registerMappingManager();
    MatchingManager.registerMatcherFactory(new HoldingsItemMatcherFactory());
    registerEventHandlers(storage, client);
  }

  private void registerEventHandlers(Storage storage, HttpClient client) {
    InstanceLinkClient instanceLinkClient = new InstanceLinkClient(WebClient.wrap(client));
    SnapshotService snapshotService = new SnapshotService(client);
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper =
      new PrecedingSucceedingTitlesHelper(WebClient.wrap(client));
    EventManager.registerEventHandler(new CommonMatchEventHandler(List.of(
      new MatchInstanceEventHandler(mappingMetadataCache, consortiumService),
      new MatchHoldingEventHandler(mappingMetadataCache, consortiumService),
      new MatchItemEventHandler(mappingMetadataCache, consortiumService),
      new MarcBibliographicMatchEventHandler(consortiumService, client, storage)
    )));

    EventManager.registerEventHandler(new MatchAuthorityEventHandler(mappingMetadataCache, consortiumService));
    EventManager.registerEventHandler(new CreateItemEventHandler(storage, mappingMetadataCache,
      new ItemIdStorageService(new EntityIdStorageDao(postgresClientFactory)), orderHelperService));
    EventManager.registerEventHandler(new CreateHoldingEventHandler(storage, mappingMetadataCache,
      new HoldingsIdStorageService(new EntityIdStorageDao(postgresClientFactory)), orderHelperService,
      consortiumService));
    EventManager.registerEventHandler(
      new CreateInstanceEventHandler(storage, precedingSucceedingTitlesHelper, mappingMetadataCache,
        new InstanceIdStorageService(new EntityIdStorageDao(postgresClientFactory)), orderHelperService,
        snapshotService, client));
    EventManager.registerEventHandler(new CreateMarcHoldingsEventHandler(storage, mappingMetadataCache,
      new HoldingsIdStorageService(new EntityIdStorageDao(postgresClientFactory)), new HoldingsCollectionService(),
      consortiumService));
    EventManager.registerEventHandler(new UpdateMarcHoldingsEventHandler(storage, mappingMetadataCache,
      new KafkaEventPublisher(kafkaConfig, vertx, 100)));
    EventManager.registerEventHandler(new UpdateItemEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(new UpdateHoldingEventHandler(storage, mappingMetadataCache));
    EventManager.registerEventHandler(
      new ReplaceInstanceEventHandler(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, client,
        consortiumService, instanceLinkClient, snapshotService));
    EventManager.registerEventHandler(new MarcBibModifiedPostProcessingEventHandler(new InstanceUpdateDelegate(storage),
      precedingSucceedingTitlesHelper, mappingMetadataCache));
    EventManager.registerEventHandler(new MarcBibModifyEventHandler(mappingMetadataCache, deleteRuleFor999FieldCache,
      new InstanceUpdateDelegate(storage), precedingSucceedingTitlesHelper, client));
  }

  private void registerMappingManager() {
    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    MappingManager.registerReaderFactory(new MarcHoldingsReaderFactory());
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerWriterFactory(new InstanceWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());
    MappingManager.registerMapperFactory(new ItemsMapperFactory());
  }

  private void registerMatchValueReaders() {
    MatchValueReaderFactory.register(new MarcValueReaderImpl());
    MatchValueReaderFactory.register(new StaticValueReaderImpl());
  }

  private void registerPreloaders(Storage storage, HttpClient client) {
    OrdersClient ordersClient = new OrdersClient(WebClient.wrap(client));

    OrdersPreloaderHelper ordersPreloaderHelper = new OrdersPreloaderHelper(ordersClient);
    InstancePreloader instancePreloader = new InstancePreloader(ordersPreloaderHelper);
    HoldingsPreloader holdingsPreloader = new HoldingsPreloader(ordersPreloaderHelper);
    ItemPreloader itemPreloader = new ItemPreloader(ordersPreloaderHelper);

    MatchValueLoaderFactory.register(new InstanceLoader(storage, instancePreloader));
    MatchValueLoaderFactory.register(new ItemLoader(storage, itemPreloader));
    MatchValueLoaderFactory.register(new HoldingLoader(storage, holdingsPreloader));
  }

  private boolean shouldSkipEventProcessing(DataImportEventPayload eventPayload) {
    return cancelledJobsIdCache.contains(eventPayload.getJobExecutionId())
           && !CANCELLED_JOB_ALLOWED_EVENTS.contains(eventPayload.getEventType());
  }

  private void populateWithPermissionsHeader(DataImportEventPayload eventPayload, Map<String, String> headersMap) {
    String permissions = headersMap.getOrDefault(PERMISSIONS, headersMap.get(PERMISSIONS.toLowerCase()));
    if (isNotBlank(permissions)) {
      eventPayload.getContext().put(PERMISSIONS, permissions);
    }
  }
}
