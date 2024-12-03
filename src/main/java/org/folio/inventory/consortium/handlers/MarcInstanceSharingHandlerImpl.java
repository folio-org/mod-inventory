package org.folio.inventory.consortium.handlers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Authority;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.Record;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.exceptions.ConsortiumException;
import org.folio.inventory.services.EntitiesLinksService;
import org.folio.inventory.services.EntitiesLinksServiceImpl;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.consortium.util.MarcRecordUtil;
import org.folio.inventory.consortium.util.RestDataImportHelper;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.storage.Storage;
import org.folio.rest.client.SourceStorageRecordsClient;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler.SOURCE;
import static org.folio.inventory.consortium.util.MarcRecordUtil.removeFieldFromMarcRecord;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.INSTANCE_ID_TYPE;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.domain.items.Item.HRID_KEY;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class MarcInstanceSharingHandlerImpl implements InstanceSharingHandler {

  private static final Logger LOGGER = LogManager.getLogger(MarcInstanceSharingHandlerImpl.class);
  private static final String CONSORTIUM_PREFIX = "CONSORTIUM-";
  public static final String SRS_RECORD_ID_TYPE = "SRS_RECORD";
  private final RestDataImportHelper restDataImportHelper;
  private final InstanceOperationsHelper instanceOperations;
  private final EntitiesLinksService entitiesLinksService;
  private final Storage storage;
  private final Vertx vertx;

  public MarcInstanceSharingHandlerImpl(InstanceOperationsHelper instanceOperations, Storage storage, Vertx vertx, HttpClient httpClient) {
    this.vertx = vertx;
    this.instanceOperations = instanceOperations;
    this.restDataImportHelper = new RestDataImportHelper(vertx);
    this.entitiesLinksService = new EntitiesLinksServiceImpl(vertx, httpClient);
    this.storage = storage;
  }

  public Future<String>  publishInstance(Instance instance, SharingInstance sharingInstanceMetadata,
                                         Source source, Target target, Map<String, String> kafkaHeaders) {
    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();

    SourceStorageRecordsClient sourceStorageClient = getSourceStorageRecordsClient(sourceTenant, kafkaHeaders);
    Context context = constructContext(sourceTenant, kafkaHeaders.get(OKAPI_TOKEN_HEADER), kafkaHeaders.get(OKAPI_URL_HEADER));

    // Get source MARC by instance ID
    return getSourceMARCByInstanceId(instanceId, sourceTenant, sourceStorageClient)
      .compose(marcRecord -> detachLocalAuthorityLinksIfNeeded(marcRecord, instanceId, context, sharingInstanceMetadata, storage))
      .compose(marcRecord -> {
        // Publish instance with MARC source
        removeFieldFromMarcRecord(marcRecord, "001");
        return restDataImportHelper.importMarcRecord(marcRecord, sharingInstanceMetadata, kafkaHeaders)
          .compose(result -> {
            if ("COMMITTED".equals(result)) {
              return updateTargetInstanceWithNonMarcControlledFields(instance, target, kafkaHeaders)
                // Delete source record by record ID if the result is "COMMITTED"
                .compose(targetInstance -> deleteSourceRecordByRecordId(marcRecord.getId(), instanceId, sourceTenant, sourceStorageClient)
                  .map(targetInstance))
                .compose(targetInstance -> {
                  // Update JSON instance to include SOURCE=CONSORTIUM-MARC
                  JsonObject jsonInstanceToPublish = new JsonObject(instance.getJsonForStorage().encode());
                  jsonInstanceToPublish.put(SOURCE, CONSORTIUM_MARC.getValue());
                  jsonInstanceToPublish.put(HRID_KEY, targetInstance.getHrid());
                  // Update instance in sourceInstanceCollection
                  return instanceOperations.updateInstance(Instance.fromJson(jsonInstanceToPublish), source);
                });
            } else {
              // If the result is not "COMMITTED", skip the deletion and update steps and return the result directly
              return Future.failedFuture(String.format("DI status is %s", result));
            }
          });
      });
  }

  private Future<Instance> updateTargetInstanceWithNonMarcControlledFields(Instance sourceInstance, Target targetTenantProvider,
                                                                           Map<String, String> kafkaHeaders) {
    return instanceOperations.getInstanceById(sourceInstance.getId(), targetTenantProvider)
      .map(targetInstance -> populateTargetInstanceWithNonMarcControlledFields(targetInstance, sourceInstance))
      .compose(targetInstance -> instanceOperations.updateInstance(targetInstance, targetTenantProvider).map(targetInstance))
      .compose(targetInstance -> updateSuppressFromDiscoveryFlagIfNeeded(targetInstance, targetTenantProvider, kafkaHeaders));
  }

  private Future<Instance> updateSuppressFromDiscoveryFlagIfNeeded(Instance targetInstance, Target targetTenantProvider,
                                                                   Map<String, String> kafkaHeaders) {
    if (targetInstance.getDiscoverySuppress()) {
      SourceStorageRecordsClient sourceStorageClient = getSourceStorageRecordsClient(targetTenantProvider.getTenantId(), kafkaHeaders);
      return updateSourceRecordSuppressFromDiscoveryByInstanceId(targetInstance.getId(), targetInstance.getDiscoverySuppress(), sourceStorageClient)
        .map(targetInstance);
    }
    return Future.succeededFuture(targetInstance);
  }

  private Instance populateTargetInstanceWithNonMarcControlledFields(Instance targetInstance, Instance sourceInstance) {
    targetInstance.setStaffSuppress(sourceInstance.getStaffSuppress());
    targetInstance.setDiscoverySuppress(sourceInstance.getDiscoverySuppress());
    targetInstance.setCatalogedDate(sourceInstance.getCatalogedDate());
    targetInstance.setStatusId(sourceInstance.getStatusId());
    targetInstance.setStatisticalCodeIds(sourceInstance.getStatisticalCodeIds());
    targetInstance.setAdministrativeNotes(sourceInstance.getAdministrativeNotes());
    targetInstance.setNatureOfContentTermIds(sourceInstance.getNatureOfContentTermIds());
    return targetInstance;
  }


  private Future<Record> detachLocalAuthorityLinksIfNeeded(Record marcRecord, String instanceId, Context context,
                                                           SharingInstance sharingInstanceMetadata, Storage storage) {
    return entitiesLinksService.getInstanceAuthorityLinks(context, instanceId)
      .compose(entityLinks -> {
        if (entityLinks.isEmpty()) {
          LOGGER.debug("unlinkAuthorityLinksIfNeeded:: Not found linked authorities for instance id: {} and tenant: {}", instanceId, context.getTenantId());
          return Future.succeededFuture(marcRecord);
        }
        AuthorityRecordCollection authorityRecordCollection = storage.getAuthorityRecordCollection(context);
        return entitiesLinksService.getLinkingRules(context)
          .compose(linkingRules -> relinkAuthorities(entityLinks, linkingRules, marcRecord, instanceId, context, sharingInstanceMetadata, authorityRecordCollection));
      });
  }

  private Future<Record> relinkAuthorities(List<Link> entityLinks, List<LinkingRuleDto> linkingRules, Record marcRecord,
                                                String instanceId, Context context, SharingInstance sharingInstanceMetadata,
                                                AuthorityRecordCollection authorityRecordCollection) {

    return getLocalAuthoritiesIdsList(entityLinks, authorityRecordCollection)
      .compose(localAuthoritiesIds -> {
        if (!localAuthoritiesIds.isEmpty()) {
          List<String> fields = linkingRules.stream().map(LinkingRuleDto::getBibField).toList();
          LOGGER.debug("relinkAuthorities:: Unlinking local authorities: {} from instance: {}, tenant: %s {}",
            localAuthoritiesIds, instanceId, context.getTenantId());
          /*
           * Removing $9 subfields containing local authorities ids from fields specified at linking-rules
           */
          try {
            MarcRecordUtil.removeSubfieldsThatContainsValues(marcRecord, fields, '9', localAuthoritiesIds);
          } catch (Exception e) {
            LOGGER.warn(format("unlinkLocalAuthorities:: Error during remove of 9 subfields from record: %s", marcRecord.getId()), e);
            return Future.failedFuture(new ConsortiumException("Error of unlinking local authorities from marc record during Instance sharing process"));
          }
        }

        List<Link> sharedAuthorityLinks = localAuthoritiesIds.isEmpty() ? entityLinks : getSharedAuthorityLinks(entityLinks, localAuthoritiesIds);
        if (!sharedAuthorityLinks.isEmpty()) {
          /*
           * Updating instance-authority links to contain only links to shared authority, as far as instance will be shared
           */
          Context targetTenantContext = constructContext(sharingInstanceMetadata.getTargetTenantId(), context.getToken(), context.getOkapiLocation());
          LOGGER.debug("relinkAuthorities:: Linking shared authorities: {} to instance: {}, tenant: %s {}",
            sharedAuthorityLinks, instanceId, targetTenantContext.getTenantId());
          return entitiesLinksService.putInstanceAuthorityLinks(targetTenantContext, instanceId, sharedAuthorityLinks).map(marcRecord);
        }
        return Future.succeededFuture(marcRecord);
      });
  }

  private Future<List<String>> getLocalAuthoritiesIdsList(List<Link> entityLinks, AuthorityRecordCollection authorityRecordCollection) {
    Promise<List<String>> promise = Promise.promise();
    try {
      authorityRecordCollection.findByCql(format("id==(%s)", getQueryParamForMultipleAuthorities(entityLinks)), PagingParameters.defaults(),
        findResults -> {
          List<String> localEntitiesIds = findResults.getResult().records.stream()
            .filter(source -> !source.getSource().value().startsWith(CONSORTIUM_PREFIX))
            .map(Authority::getId).toList();
          promise.complete(localEntitiesIds);
        },
        failure -> promise.fail(failure.getReason()));
    } catch (UnsupportedEncodingException e) {
      promise.fail(e);
    }
    return promise.future();
  }

  private static List<Link> getSharedAuthorityLinks(List<Link> entityLinks, List<String> localAuthoritiesIds) {
    return entityLinks.stream().filter(entityLink -> !localAuthoritiesIds.contains(entityLink.getAuthorityId())).toList();
  }

  private static String getQueryParamForMultipleAuthorities(List<Link> entityLinks) {
    return entityLinks.stream().map(Link::getAuthorityId).collect(Collectors.joining(" OR "));
  }

  Future<Record> getSourceMARCByInstanceId(String instanceId, String sourceTenant, SourceStorageRecordsClient client) {

    LOGGER.info("getSourceMARCByInstanceId:: Getting source MARC record for instance with InstanceId={} from tenant={}.",
      instanceId, sourceTenant);

    Promise<Record> promise = Promise.promise();
    client.getSourceStorageRecordsFormattedById(instanceId, INSTANCE_ID_TYPE).onComplete(responseResult -> {
        try {
          if (responseResult.succeeded()) {
            int statusCode = responseResult.result().statusCode();
            if (statusCode == HttpStatus.SC_OK) {
              String bodyAsString = responseResult.result().bodyAsString();
              LOGGER.debug("MARC source for instance with InstanceId={} from tenant={}. Record={}.", instanceId, sourceTenant, bodyAsString);
              promise.complete(responseResult.result().bodyAsJson(Record.class));
            } else {
              String errorMessage = String.format("Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s. " +
                "Status message: %s. Status code: %s", instanceId, sourceTenant, responseResult.result().statusMessage(), statusCode);
              LOGGER.error(errorMessage);
              promise.fail(errorMessage);
            }
          } else {
            String errorMessage = String.format("Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s. " +
              "Error message: %s", instanceId, sourceTenant, responseResult.cause().getMessage());
            LOGGER.error(errorMessage);
            promise.fail(responseResult.cause());
          }
        } catch (Exception ex) {
          LOGGER.error("Error processing MARC record retrieval.", ex);
          promise.fail("Error processing MARC record retrieval.");
        }
      });
    return promise.future();
  }

  Future<String> deleteSourceRecordByRecordId(String recordId, String instanceId, String tenantId, SourceStorageRecordsClient client) {
    LOGGER.info("deleteSourceRecordByRecordId :: Delete source record with recordId={} for instance by InstanceId={} from tenant {}",
      recordId, instanceId, tenantId);

    return client.deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE)
      .onFailure(e -> LOGGER.error("deleteSourceRecordByRecordId:: Error deleting source record with recordId={} by InstanceId={} from tenant {}",
        recordId, instanceId, tenantId, e))
      .compose(response -> {
        if (response.statusCode() == SC_NO_CONTENT) {
          LOGGER.info("deleteSourceRecordByRecordId:: Source record with recordId={} for instance with InstanceId={} from tenant {} has been deleted.",
            recordId, instanceId, tenantId);
          return Future.succeededFuture(instanceId);
        } else {
          String msg = format("Error deleting source record with recordId=%s by InstanceId=%s from tenant %s, responseStatus=%s, body=%s",
            recordId, instanceId, tenantId, response.statusCode(), response.bodyAsString());
          LOGGER.error("deleteSourceRecordByRecordId:: {}", msg);
          return Future.failedFuture(msg);
        }
      });
  }

  Future<String> updateSourceRecordSuppressFromDiscoveryByInstanceId(String instanceId, boolean suppress,
                                                                     SourceStorageRecordsClient sourceStorageClient) {
    LOGGER.info("updateSourceRecordSuppressFromDiscoveryByInstanceId:: Updating suppress from discovery flag for record in SRS, instanceId: {}, suppressFromDiscovery: {}",
      instanceId, suppress);

    return sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(instanceId,
        INSTANCE_ID_TYPE, suppress)
      .compose(response -> {
        if (response.statusCode() == org.folio.HttpStatus.HTTP_OK.toInt()) {
          LOGGER.info("updateSourceRecordSuppressFromDiscoveryByInstanceId:: Suppress from discovery flag was successfully updated for record in SRS, instanceId: {}, suppressFromDiscovery: {}",
            instanceId, suppress);
          return Future.succeededFuture(instanceId);
        } else {
          String errorMessage = format("Cannot update suppress from discovery flag for SRS record, instanceId: %s, statusCode: %s, suppressFromDiscovery: %s",
            instanceId, response.statusCode(), suppress);
          LOGGER.warn(format("updateSourceRecordSuppressFromDiscoveryByInstanceId:: %s", errorMessage));
          return Future.failedFuture(errorMessage);
        }
      });
  }

  public SourceStorageRecordsClient getSourceStorageRecordsClient(String tenant, Map<String, String> kafkaHeaders) {
    LOGGER.info("getSourceStorageRecordsClient :: Creating SourceStorageRecordsClient for tenant={}", tenant);
    return new SourceStorageRecordsClient(
      kafkaHeaders.get(OKAPI_URL_HEADER),
      tenant,
      kafkaHeaders.get(OKAPI_TOKEN_HEADER),
      vertx.createHttpClient());
  }

}
