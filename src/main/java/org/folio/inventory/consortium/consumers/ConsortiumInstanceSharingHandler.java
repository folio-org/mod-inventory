package org.folio.inventory.consortium.consumers;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingInstanceEventType;
import org.folio.inventory.consortium.entities.SharingStatus;
import org.folio.inventory.consortium.handlers.InstanceSharingHandlerFactory;
import org.folio.inventory.consortium.handlers.Source;
import org.folio.inventory.consortium.handlers.Target;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.KafkaTopicNameHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.folio.inventory.consortium.entities.SharingInstanceEventType.CONSORTIUM_INSTANCE_SHARING_COMPLETE;
import static org.folio.inventory.consortium.entities.SharingStatus.COMPLETE;
import static org.folio.inventory.consortium.handlers.InstanceSharingHandlerFactory.getInstanceSharingHandler;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class ConsortiumInstanceSharingHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(ConsortiumInstanceSharingHandler.class);
  public static final String SOURCE = "source";

  public static final String ID = "id";
  private final Vertx vertx;
  private final Storage storage;
  private final KafkaConfig kafkaConfig;
  private final InstanceOperationsHelper instanceOperations;

  public ConsortiumInstanceSharingHandler(Vertx vertx, Storage storage, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.storage = storage;
    this.kafkaConfig = kafkaConfig;
    this.instanceOperations = new InstanceOperationsHelper();
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> event) {
    try {
      SharingInstance sharingInstanceMetadata = parseSharingInstance(event.value());

      Map<String, String> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersToMap(event.headers());
      String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();

      LOGGER.info("Event CONSORTIUM_INSTANCE_SHARING_INIT has been received for InstanceId={}, sourceTenant={}, targetTenant={}",
        instanceId, sharingInstanceMetadata.getSourceTenantId(), sharingInstanceMetadata.getTargetTenantId());

      return publishInstanceIfNeeded(sharingInstanceMetadata, kafkaHeaders);
    } catch (Exception ex) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", event.topic()), ex);
      return Future.failedFuture(ex);
    }
  }

  private Future<String> publishInstanceIfNeeded(SharingInstance sharingInstanceMetadata, Map<String, String> kafkaHeaders) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
    String targetTenant = sharingInstanceMetadata.getTargetTenantId();

    Source source = new Source(sourceTenant, getTenantSpecificSourceCollection(sourceTenant, kafkaHeaders));
    Target target = new Target(targetTenant, getTenantSpecificSourceCollection(targetTenant, kafkaHeaders));

    LOGGER.info("Checking if instance with InstanceId={} exists on target tenant={}", instanceId, targetTenant);

    return instanceOperations.getInstanceById(instanceId, target)
      .compose(instance -> {
        String warningMessage = String.format("Instance with InstanceId=%s is present on target tenant: %s", instanceId, targetTenant);
        sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, warningMessage, kafkaHeaders);
        return Future.succeededFuture(warningMessage);
      })
      .onFailure(throwable -> {
        String warningMessage = String.format("Instance with InstanceId=%s is not exists on target tenant: %s.", instanceId, targetTenant);
        LOGGER.info(warningMessage);
      })
      .recover(throwable -> publishInstance(sharingInstanceMetadata, source, target, kafkaHeaders));
  }

  private Future<String> publishInstance(SharingInstance sharingInstanceMetadata, Source source,
                                         Target target, Map<String, String> kafkaHeaders) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
    String targetTenant = sharingInstanceMetadata.getTargetTenantId();

    LOGGER.info("publishInstance :: Publishing instance with InstanceId={} from tenant {} to tenant {}.",
      instanceId, sourceTenant, targetTenant);

    Promise<String> promise = Promise.promise();

    try {
      instanceOperations.getInstanceById(instanceId, source).onComplete(result -> {
        if (result.succeeded()) {
          Instance instance = result.result();

          getInstanceSharingHandler(InstanceSharingHandlerFactory.valueOf(instance.getSource()), instanceOperations, vertx)
            .publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders)
            .onComplete(publishResult -> handleSharingResult(sharingInstanceMetadata, kafkaHeaders, promise, publishResult));

        } else {
          String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant=%s. " +
            "Because the instance is not found on the source tenant=%s", instanceId, targetTenant, sourceTenant);
          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
          promise.fail(errorMessage);
        }
      });
    } catch (Exception ex) {
      LOGGER.error(format("Failed to import instance with importId to  %s", instanceId), ex);
      promise.fail(ex);
    }
    return promise.future();
  }

  private void handleSharingResult(SharingInstance sharingInstanceMetadata, Map<String, String> kafkaHeaders, Promise<String> promise,
                                   AsyncResult<String> result) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String targetTenant = sharingInstanceMetadata.getTargetTenantId();

    if (result.succeeded()) {
      String completeMessage = format("Instance with InstanceId=%s has been shared to the target tenant %s", instanceId, targetTenant);
      sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, completeMessage, kafkaHeaders);
      promise.complete(completeMessage);
    } else {
      String errorMessage = format("Sharing instance with InstanceId=%s to the target tenant %s. Error: %s",
        instanceId, targetTenant, result.cause().getMessage());
      sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
      promise.fail(errorMessage);
    }
  }

  private void sendErrorResponseAndPrintLogMessage(String errorMessage, SharingInstance sharingInstance, Map<String, String> kafkaHeaders) {
    LOGGER.error("handle:: {}", errorMessage);
    sendCompleteEventToKafka(sharingInstance, SharingStatus.ERROR, errorMessage, kafkaHeaders);
  }

  private void sendCompleteEventToKafka(SharingInstance sharingInstance, SharingStatus status, String errorMessage,
                                        Map<String, String> kafkaHeaders) {

    SharingInstanceEventType evenType = CONSORTIUM_INSTANCE_SHARING_COMPLETE;

    try {
      String tenantId = kafkaHeaders.get(OKAPI_TENANT_HEADER);
      List<KafkaHeader> kafkaHeadersList = convertKafkaHeadersMap(kafkaHeaders);

      LOGGER.info("sendEventToKafka :: tenantId={}, instance with InstanceId={}, status={}, message={}",
        tenantId, sharingInstance.getInstanceIdentifier(), status.getValue(), errorMessage);

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, evenType.value());

      KafkaProducerRecord<String, String> kafkaRecord = createProducerRecord(topicName, sharingInstance, status, errorMessage, kafkaHeadersList);
      createProducer(tenantId, topicName).write(kafkaRecord, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("Event with type {}, was sent to kafka", evenType.value());
        } else {
          var cause = ar.cause();
          LOGGER.error("Failed to sent event {}, cause: {}", evenType.value(), cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to send an event for eventType {}, cause {}", evenType.value(), e);
    }
  }

  private KafkaProducerRecord<String, String> createProducerRecord(String topicName, SharingInstance sharingInstance,
                                                                   SharingStatus status, String errorMessage, List<KafkaHeader> kafkaHeaders) {
    LOGGER.info("createKafkaMessage :: Instance with InstanceId={}, status: {}, {}topicName: {}",
      sharingInstance.getInstanceIdentifier(), status,
      status.equals(SharingStatus.ERROR) ? " message: " + errorMessage + ", " : EMPTY, topicName);

    sharingInstance.setStatus(status);
    if (sharingInstance.getStatus().equals(SharingStatus.ERROR)) {
      sharingInstance.setError(errorMessage);
    } else {
      sharingInstance.setError(EMPTY);
    }

    String eventPayload = Json.encode(sharingInstance);
    return KafkaProducerRecord.create(topicName, sharingInstance.getInstanceIdentifier().toString(), eventPayload).addHeaders(kafkaHeaders);
  }

  private KafkaProducer<String, String> createProducer(String tenantId, String topicName) {
    LOGGER.info("createProducer :: tenantId: {}, topicName: {}", tenantId, topicName);
    return KafkaProducer.createShared(vertx, topicName + "_Producer", kafkaConfig.getProducerProps());
  }

  private SharingInstance parseSharingInstance(String eventValue) {
    return Json.decodeValue(eventValue, SharingInstance.class);
  }

  private InstanceCollection getTenantSpecificSourceCollection(String tenantId, Map<String, String> kafkaHeaders) {
    return storage.getInstanceCollection(
      EventHandlingUtil.constructContext(
        tenantId,
        kafkaHeaders.get(OKAPI_TOKEN_HEADER),
        kafkaHeaders.get(OKAPI_URL_HEADER))
    );
  }

  private List<KafkaHeader> convertKafkaHeadersMap(Map<String, String> kafkaHeaders) {
    return new ArrayList<>(List.of(
      KafkaHeader.header(OKAPI_URL_HEADER, kafkaHeaders.get(OKAPI_URL_HEADER)),
      KafkaHeader.header(OKAPI_TENANT_HEADER, kafkaHeaders.get(OKAPI_TENANT_HEADER)),
      KafkaHeader.header(OKAPI_TOKEN_HEADER, kafkaHeaders.get(OKAPI_TOKEN_HEADER)))
    );
  }

}
