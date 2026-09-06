package org.folio.inventory.consortium.consumers;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.inventory.consortium.entities.SharingInstanceEventType.SHARING_COMPLETE;
import static org.folio.inventory.consortium.entities.SharingStatus.COMPLETE;
import static org.folio.inventory.consortium.handlers.InstanceSharingHandlerFactory.getInstanceSharingHandler;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingStatus;
import org.folio.inventory.consortium.exceptions.ConsortiumException;
import org.folio.inventory.consortium.handlers.InstanceSharingHandlerFactory;
import org.folio.inventory.consortium.handlers.SourceTenantProvider;
import org.folio.inventory.consortium.handlers.TargetTenantProvider;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.services.EventIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SimpleKafkaProducerManager;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.folio.okapi.common.XOkapiHeaders;

public class ConsortiumInstanceSharingConsumer implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(ConsortiumInstanceSharingConsumer.class);

  private static final String EVENT_RECEIVED_MSG =
    "handle:: Event CONSORTIUM_INSTANCE_SHARING_INIT received for instanceId={}, sourceTenant={}, targetTenant={}.";
  private static final String CHECK_TARGET_INSTANCE_SUCCESS_MSG =
    "handle:: Checking if instance exists on target tenant completed successfully for instanceId={}, "
    + "sourceTenant={}, targetTenant={}.";
  private static final String CHECK_TARGET_INSTANCE_FAILED_MSG =
    "handle:: Checking if instance exists on target tenant failed for instanceId={}, sourceTenant={}, "
    + "targetTenant={}, error={}.";
  private static final String DUPLICATE_EVENT_MSG =
    "handle:: Duplicate event received for instanceId={}, sourceTenant={}, targetTenant={}.";
  private static final String ERROR_STORING_EVENT_ID_MSG =
    "handle:: Error storing eventId for instanceId={}, sourceTenant={}, targetTenant={}, error={}.";
  private static final String FAILED_TO_PROCESS_KAFKA_RECORD_MSG =
    "handle:: Failed to process kafka record from topic={}.";

  private static final String CHECKING_INSTANCE_ON_TARGET_TENANT_MSG =
    "publishInstanceIfNeeded:: Checking if instance exists on target tenant for instanceId={}, targetTenant={}.";
  private static final String INSTANCE_PRESENT_ON_TARGET_TENANT_MSG =
    "Instance with instanceId=%s is present on target tenant=%s.";
  private static final String INSTANCE_NOT_FOUND_ON_TARGET_TENANT_MSG =
    "publishInstanceIfNeeded:: Instance does not exist on target tenant for instanceId={}, targetTenant={}.";
  private static final String INSTANCE_CANNOT_BE_SHARED_MSG =
    "publishInstanceIfNeeded:: Instance cannot be shared on target tenant for instanceId={}, targetTenant={}, "
    + "error={}.";

  private static final String PUBLISHING_INSTANCE_MSG =
    "publishInstance:: Publishing instance for instanceId={}, sourceTenant={}, targetTenant={}.";
  // Below three: text is asserted verbatim by ConsortiumInstanceSharingConsumerTest via the failed future's message.
  private static final String INSTANCE_NOT_FOUND_ON_SOURCE_TENANT_MSG =
    "Error sharing Instance with InstanceId=%s to the target tenant %s. "
    + "Because the instance is not found on the source tenant %s";
  private static final String SHARING_FAILED_MSG =
    "Sharing instance with InstanceId=%s to the target tenant %s. Error: %s";
  private static final String UNSUPPORTED_SOURCE_TYPE_MSG =
    "Error sharing Instance with InstanceId=%s to the target tenant %s. Error: Unsupported source type: %s";
  private static final String INSTANCE_SHARED_MSG =
    "Instance with instanceId=%s has been shared to target tenant=%s.";

  private static final String HANDLE_ERROR_PREFIX_MSG = "handle:: {}";

  private static final String SENDING_KAFKA_EVENT_MSG =
    "sendCompleteEventToKafka:: Sending event for instanceId={}, tenantId={}, status={}, message={}.";
  private static final String KAFKA_EVENT_SENT_MSG =
    "sendCompleteEventToKafka:: Event sent for instanceId={}, eventType={}.";
  private static final String KAFKA_EVENT_SEND_FAILED_MSG =
    "sendCompleteEventToKafka:: Failed to send event for instanceId={}, eventType={}, cause={}.";
  private static final String KAFKA_EVENT_SEND_ERROR_MSG =
    "sendCompleteEventToKafka:: Error sending event for instanceId={}, eventType={}.";

  private static final String CREATING_PRODUCER_RECORD_MSG =
    "createProducerRecord:: Creating producer record for instanceId={}, status={}, topic={}, message={}.";
  private static final String CREATING_PRODUCER_MSG =
    "createProducer:: Creating producer for tenantId={}, topicName={}.";

  private final Vertx vertx;
  private final HttpClient httpClient;
  private final Storage storage;
  private final KafkaConfig kafkaConfig;
  private final InstanceOperationsHelper instanceOperations;
  private final EventIdStorageService eventIdStorageService;

  public ConsortiumInstanceSharingConsumer(Vertx vertx, HttpClient httpClient, Storage storage, KafkaConfig kafkaConfig,
                                           EventIdStorageService eventIdStorageService) {
    this.vertx = vertx;
    this.httpClient = httpClient;
    this.storage = storage;
    this.kafkaConfig = kafkaConfig;
    this.instanceOperations = new InstanceOperationsHelper();
    this.eventIdStorageService = eventIdStorageService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> event) {
    try {
      SharingInstance sharingInstanceMetadata = parseSharingInstance(event.value());

      Map<String, String> kafkaHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      kafkaHeaders.putAll(KafkaHeaderUtils.kafkaHeadersToMap(event.headers()));
      String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();

      var sourceTenantId = sharingInstanceMetadata.getSourceTenantId();
      var targetTenantId = sharingInstanceMetadata.getTargetTenantId();
      LOGGER.info(EVENT_RECEIVED_MSG, instanceId, sourceTenantId, targetTenantId);

      return eventIdStorageService.store(event.key(), targetTenantId)
        .compose(r -> publishInstanceIfNeeded(sharingInstanceMetadata, kafkaHeaders),
          processSharingError(instanceId, sourceTenantId, targetTenantId));
    } catch (Exception ex) {
      LOGGER.error(FAILED_TO_PROCESS_KAFKA_RECORD_MSG, event.topic(), ex);
      return Future.failedFuture(ex);
    }
  }

  private Function<Throwable, Future<String>> processSharingError(String instanceId,
                                                                  String sourceTenantId,
                                                                  String targetTenantId) {
    return throwable -> {
      if (throwable instanceof DuplicateEventException) {
        LOGGER.info(DUPLICATE_EVENT_MSG, instanceId, sourceTenantId, targetTenantId);
        return Future.succeededFuture(instanceId);
      } else {
        LOGGER.warn(ERROR_STORING_EVENT_ID_MSG, instanceId, sourceTenantId, targetTenantId, throwable);
        return Future.failedFuture(throwable);
      }
    };
  }

  private Future<String> publishInstanceIfNeeded(SharingInstance sharingInstanceMetadata,
                                                 Map<String, String> kafkaHeaders) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
    String targetTenant = sharingInstanceMetadata.getTargetTenantId();

    var source = new SourceTenantProvider(sourceTenant, getTenantSpecificSourceCollection(sourceTenant, kafkaHeaders));
    var target = new TargetTenantProvider(targetTenant, getTenantSpecificSourceCollection(targetTenant, kafkaHeaders));

    LOGGER.info(CHECKING_INSTANCE_ON_TARGET_TENANT_MSG, instanceId, targetTenant);

    return instanceOperations.getInstanceById(instanceId, target)
      .compose(instance -> {
        String message = format(INSTANCE_PRESENT_ON_TARGET_TENANT_MSG, instanceId, targetTenant);
        sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, message, kafkaHeaders);
        return Future.succeededFuture(instanceId);
      })
      .recover(throwable -> {
        if (throwable.getClass().equals(NotFoundException.class)) {
          LOGGER.info(INSTANCE_NOT_FOUND_ON_TARGET_TENANT_MSG, instanceId, targetTenant);
          return publishInstance(sharingInstanceMetadata, source, target, kafkaHeaders);
        } else {
          LOGGER.error(INSTANCE_CANNOT_BE_SHARED_MSG, instanceId, targetTenant, throwable);
          return Future.failedFuture(throwable);
        }
      })
      .onSuccess(result -> LOGGER.info(CHECK_TARGET_INSTANCE_SUCCESS_MSG, instanceId, sourceTenant, targetTenant))
      .onFailure(cause -> LOGGER.warn(CHECK_TARGET_INSTANCE_FAILED_MSG, instanceId, sourceTenant, targetTenant, cause));
  }

  private Future<String> publishInstance(SharingInstance sharingInstanceMetadata,
                                         SourceTenantProvider sourceTenantProvider,
                                         TargetTenantProvider targetTenantProvider, Map<String, String> kafkaHeaders) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
    String targetTenant = sharingInstanceMetadata.getTargetTenantId();

    LOGGER.info(PUBLISHING_INSTANCE_MSG, instanceId, sourceTenant, targetTenant);

    return instanceOperations.getInstanceById(instanceId, sourceTenantProvider)
      .recover(cause -> Future.failedFuture(new ConsortiumException(
        format(INSTANCE_NOT_FOUND_ON_SOURCE_TENANT_MSG, instanceId, targetTenant, sourceTenant), cause)))
      .compose(instance -> checkSourceType(instance.getSource())
        .map(sourceType -> getInstanceSharingHandler(sourceType, instanceOperations, storage, vertx, httpClient)
          .publishInstance(instance, sharingInstanceMetadata, sourceTenantProvider, targetTenantProvider, kafkaHeaders)
          .recover(cause -> Future.failedFuture(new ConsortiumException(
            format(SHARING_FAILED_MSG, instanceId, targetTenant, cause.getMessage()), cause))))
        .orElseGet(() -> Future.failedFuture(new ConsortiumException(
          format(UNSUPPORTED_SOURCE_TYPE_MSG, instanceId, targetTenant, instance.getSource())))))
      .compose(handlerResult -> {
          String completeMessage = format(INSTANCE_SHARED_MSG, instanceId, targetTenant);
          sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, completeMessage, kafkaHeaders);
          return Future.succeededFuture(instanceId);
        },
        cause -> {
          sendErrorResponseAndPrintLogMessage(cause.getMessage(), sharingInstanceMetadata, kafkaHeaders);
          return Future.failedFuture(cause);
        });
  }

  private static Optional<InstanceSharingHandlerFactory> checkSourceType(String source) {
    return Stream.of(InstanceSharingHandlerFactory.values())
      .filter(value -> value.name().equalsIgnoreCase(source))
      .findFirst();
  }

  private void sendErrorResponseAndPrintLogMessage(String errorMessage, SharingInstance sharingInstance,
                                                   Map<String, String> kafkaHeaders) {
    LOGGER.error(HANDLE_ERROR_PREFIX_MSG, errorMessage);
    sendCompleteEventToKafka(sharingInstance, SharingStatus.ERROR, errorMessage, kafkaHeaders);
  }

  private void sendCompleteEventToKafka(SharingInstance sharingInstance, SharingStatus status, String errorMessage,
                                        Map<String, String> kafkaHeaders) {
    try {
      String tenantId = kafkaHeaders.get(XOkapiHeaders.TENANT);
      List<KafkaHeader> kafkaHeadersList = convertKafkaHeadersMap(kafkaHeaders);

      LOGGER.info(SENDING_KAFKA_EVENT_MSG, sharingInstance.getInstanceIdentifier(), tenantId, status.getValue(),
        errorMessage);

      KafkaProducerRecord<String, String> kafkaRecord =
        createProducerRecord(getTopicName(tenantId),
          sharingInstance,
          status,
          errorMessage,
          kafkaHeadersList);
      KafkaProducer<String, String> producer = createProducer(tenantId, SHARING_COMPLETE.value());

      producer.send(kafkaRecord)
        .<Void>mapEmpty()
        .eventually(producer::flush)
        .eventually(producer::close)
        .onSuccess(res -> LOGGER.info(KAFKA_EVENT_SENT_MSG, sharingInstance.getInstanceIdentifier(),
          SHARING_COMPLETE.value()))
        .onFailure(err -> LOGGER.error(KAFKA_EVENT_SEND_FAILED_MSG, sharingInstance.getInstanceIdentifier(),
          SHARING_COMPLETE.value(), err.getCause()));
    } catch (Exception e) {
      LOGGER.error(KAFKA_EVENT_SEND_ERROR_MSG, sharingInstance.getInstanceIdentifier(), SHARING_COMPLETE.value(), e);
    }
  }

  private KafkaProducerRecord<String, String> createProducerRecord(String topicName, SharingInstance sharingInstance,
                                                                   SharingStatus status, String message,
                                                                   List<KafkaHeader> kafkaHeaders) {

    LOGGER.info(CREATING_PRODUCER_RECORD_MSG, sharingInstance.getInstanceIdentifier(), status, topicName, message);

    sharingInstance.setStatus(status);
    if (SharingStatus.ERROR == sharingInstance.getStatus()) {
      sharingInstance.setError(message);
    } else {
      sharingInstance.setError(EMPTY);
    }

    return new KafkaProducerRecordBuilder<String, Object>(sharingInstance.getTargetTenantId())
      .key(sharingInstance.getInstanceIdentifier().toString())
      .value(sharingInstance)
      .topic(topicName)
      .build()
      .addHeaders(kafkaHeaders);
  }

  private String getTopicName(String tenantId) {
    return KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, SHARING_COMPLETE.value());
  }

  private KafkaProducer<String, String> createProducer(String tenantId, String topicName) {
    LOGGER.info(CREATING_PRODUCER_MSG, tenantId, topicName);
    return new SimpleKafkaProducerManager(vertx, kafkaConfig).createShared(topicName);
  }

  private SharingInstance parseSharingInstance(String eventValue) {
    return Json.decodeValue(eventValue, SharingInstance.class);
  }

  private InstanceCollection getTenantSpecificSourceCollection(String tenantId, Map<String, String> kafkaHeaders) {
    return storage.getInstanceCollection(
      EventHandlingUtil.constructContext(
        tenantId,
        kafkaHeaders.get(XOkapiHeaders.TOKEN),
        kafkaHeaders.get(XOkapiHeaders.URL),
        kafkaHeaders.get(XOkapiHeaders.USER_ID),
        kafkaHeaders.get(XOkapiHeaders.REQUEST_ID))
    );
  }

  private List<KafkaHeader> convertKafkaHeadersMap(Map<String, String> kafkaHeaders) {
    return Stream.of(XOkapiHeaders.URL, XOkapiHeaders.TENANT, XOkapiHeaders.TOKEN, XOkapiHeaders.USER_ID,
        XOkapiHeaders.REQUEST_ID)
      .filter(key -> kafkaHeaders.get(key) != null)
      .map(key -> KafkaHeader.header(key, kafkaHeaders.get(key)))
      .toList();
  }
}
