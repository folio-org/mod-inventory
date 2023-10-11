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
import static org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler.SOURCE;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.INSTANCE_ID_TYPE;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.domain.items.Item.HRID_KEY;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class MarcInstanceSharingHandlerImpl implements InstanceSharingHandler {

  private static final Logger LOGGER = LogManager.getLogger(MarcInstanceSharingHandlerImpl.class);
  private static final String CONSORTIUM_PREFIX = "CONSORTIUM-";
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
      .compose(marcRecord -> detachLocalAuthorityLinksIfNeeded(marcRecord, instanceId, context, storage))
      .compose(marcRecord -> {
        // Publish instance with MARC source
        return restDataImportHelper.importMarcRecord(marcRecord, sharingInstanceMetadata, kafkaHeaders)
          .compose(result -> {
            if ("COMMITTED".equals(result)) {
              // Delete source record by instance ID if the result is "COMMITTED"
              return deleteSourceRecordByInstanceId(marcRecord.getId(), instanceId, sourceTenant, sourceStorageClient)
                .compose(deletedInstanceId ->
                  instanceOperations.getInstanceById(instanceId, target))
                .compose(targetInstance -> {
                  // Update JSON instance to include SOURCE=CONSORTIUM-MARC
                  JsonObject jsonInstanceToPublish = instance.getJsonForStorage();
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

  private Future<Record> detachLocalAuthorityLinksIfNeeded(Record marcRecord, String instanceId, Context context, Storage storage) {
    return entitiesLinksService.getInstanceAuthorityLinks(context, instanceId)
      .compose(entityLinks -> {
        if (entityLinks.isEmpty()) {
          LOGGER.debug("unlinkAuthorityLinksIfNeeded:: Not found linked authorities for instance id: {} and tenant: {}", instanceId, context.getTenantId());
          return Future.succeededFuture(marcRecord);
        }
        AuthorityRecordCollection authorityRecordCollection = storage.getAuthorityRecordCollection(context);
        return entitiesLinksService.getLinkingRules(context)
          .compose(linkingRules -> unlinkLocalAuthorities(entityLinks, linkingRules, marcRecord, instanceId, context, authorityRecordCollection));
      });
  }

  private Future<Record> unlinkLocalAuthorities(List<Link> entityLinks, List<LinkingRuleDto> linkingRules, Record marcRecord,
                                                String instanceId, Context context, AuthorityRecordCollection authorityRecordCollection) {

    return getLocalAuthoritiesIdsList(entityLinks, authorityRecordCollection)
      .compose(localAuthoritiesIds -> {
        if (localAuthoritiesIds.isEmpty()) {
          return Future.succeededFuture(marcRecord);
        }
        List<String> fields = linkingRules.stream().map(LinkingRuleDto::getBibField).toList();
        LOGGER.debug("unlinkLocalAuthorities:: Unlinking from tenant: {} local authorities: {}", context.getTenantId(), localAuthoritiesIds);
        /*
         * Removing $9 subfields containing local authorities ids from fields specified at linking-rules
         */
        try {
          MarcRecordUtil.removeSubfieldsThatContainsValues(marcRecord, fields, '9', localAuthoritiesIds);
          /*
           * Updating instance-authority links to contain only links to shared authority, as far as instance will be shared
           */
          List<Link> sharedAuthorityLinks = getSharedAuthorityLinks(entityLinks, localAuthoritiesIds);
          return entitiesLinksService.putInstanceAuthorityLinks(context, instanceId, sharedAuthorityLinks).map(marcRecord);
        } catch (Exception e) {
          LOGGER.warn(format("unlinkLocalAuthorities:: Error during remove of 9 subfields from record: %s", marcRecord.getId()), e);
          return Future.failedFuture(new ConsortiumException("Error of unlinking local authorities from marc record during Instance sharing process"));
        }
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

  Future<String> deleteSourceRecordByInstanceId(String recordId, String instanceId, String tenantId, SourceStorageRecordsClient client) {
    LOGGER.info("deleteSourceRecordByInstanceId :: Delete source record with recordId={} for instance by InstanceId={} from tenant {}",
      recordId, instanceId, tenantId);
    Promise<String> promise = Promise.promise();
    client.deleteSourceStorageRecordsById(recordId).onComplete(responseResult -> {
      try {
        if (responseResult.failed()) {
          LOGGER.error("deleteSourceRecordByInstanceId:: Error deleting source record with recordId={} by InstanceId={} from tenant {}",
            recordId, instanceId, tenantId, responseResult.cause());
          promise.fail(responseResult.cause());
        } else {
          LOGGER.info("TEST: resp status: {}, body: {}", responseResult.result().statusCode(), responseResult.result().bodyAsString());
          LOGGER.info("deleteSourceRecordByInstanceId:: Source record with recordId={} for instance with InstanceId={} from tenant {} has been deleted.",
            recordId, instanceId, tenantId);
          promise.complete(instanceId);
        }
      } catch (Exception ex) {
        String errorMessage = String.format("Error processing source record with recordId={} deletion for instance with InstanceId=%s from tenant=%s. Error message: %s",
          recordId, instanceId, tenantId, responseResult.cause());
        LOGGER.error("deleteSourceRecordByInstanceId:: {}", errorMessage, ex);
        promise.fail(errorMessage);
      }
    });
    return promise.future();
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
