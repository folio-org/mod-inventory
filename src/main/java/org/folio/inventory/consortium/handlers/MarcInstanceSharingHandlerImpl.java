package org.folio.inventory.consortium.handlers;

import static java.lang.String.format;
import static org.folio.dataimport.util.marc.MarcConstants.FIELD_001;
import static org.folio.dataimport.util.marc.MarcConstants.SUBFIELD_9;
import static org.folio.dataimport.util.marc.MarcRecordEditor.removeSubfieldsThatContainsValues;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Authority;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.Record;
import org.folio.dataimport.util.marc.MarcRecordEditor;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.PagingParameters;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.exceptions.ConsortiumException;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.consortium.util.RestDataImportHelper;
import org.folio.inventory.consortium.util.SourceStorageHelper;
import org.folio.inventory.dataimport.util.FolioRecordHolder;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceSource;
import org.folio.inventory.services.EntitiesLinksService;
import org.folio.inventory.storage.Storage;
import org.folio.okapi.common.XOkapiHeaders;

public class MarcInstanceSharingHandlerImpl implements InstanceSharingHandler {

  private static final Logger LOGGER = LogManager.getLogger(MarcInstanceSharingHandlerImpl.class);
  private static final String COMMITTED_STATUS = "COMMITTED";

  private final RestDataImportHelper restDataImportHelper;
  private final InstanceOperationsHelper instanceOperations;
  private final EntitiesLinksService entitiesLinksService;
  private final SourceStorageHelper sourceStorageHelper;
  private final Storage storage;

  public MarcInstanceSharingHandlerImpl(InstanceOperationsHelper instanceOperations, Storage storage,
                                        RestDataImportHelper restDataImportHelper,
                                        EntitiesLinksService entitiesLinksService,
                                        SourceStorageHelper sourceStorageHelper) {
    this.instanceOperations = instanceOperations;
    this.restDataImportHelper = restDataImportHelper;
    this.entitiesLinksService = entitiesLinksService;
    this.sourceStorageHelper = sourceStorageHelper;
    this.storage = storage;
  }

  public Future<String> publishInstance(Instance instance, SharingInstance sharingInstanceMetadata,
                                        SourceTenantProvider sourceTenantProvider,
                                        TargetTenantProvider targetTenantProvider, Map<String, String> kafkaHeaders) {
    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
    Context context =
      constructContext(sourceTenant, kafkaHeaders.get(XOkapiHeaders.TOKEN), kafkaHeaders.get(XOkapiHeaders.URL),
        kafkaHeaders.get(XOkapiHeaders.USER_ID), kafkaHeaders.get(XOkapiHeaders.REQUEST_ID));

    return sourceStorageHelper.getSourceRecordByInstanceId(instanceId, sourceTenant, kafkaHeaders)
      .compose(marcRecord -> detachLocalAuthorityLinksIfNeeded(marcRecord, instanceId, context, sharingInstanceMetadata,
        storage))
      .compose(marcRecord -> importAndCommit(marcRecord, instance, sharingInstanceMetadata, sourceTenantProvider,
        targetTenantProvider, kafkaHeaders));
  }

  private Future<String> importAndCommit(Record marcRecord, Instance instance, SharingInstance sharingInstanceMetadata,
                                         SourceTenantProvider sourceTenantProvider,
                                         TargetTenantProvider targetTenantProvider, Map<String, String> kafkaHeaders) {
    MarcRecordEditor.removeFieldFromMarcRecord(new FolioRecordHolder(marcRecord), FIELD_001);
    return restDataImportHelper.importMarcRecord(marcRecord, sharingInstanceMetadata, kafkaHeaders)
      .compose(importStatus -> commitIfImportSucceeded(importStatus, marcRecord, instance, sharingInstanceMetadata,
        sourceTenantProvider, targetTenantProvider, kafkaHeaders));
  }

  private Future<String> commitIfImportSucceeded(String importStatus, Record marcRecord, Instance instance,
                                                 SharingInstance sharingInstanceMetadata,
                                                 SourceTenantProvider sourceTenantProvider,
                                                 TargetTenantProvider targetTenantProvider,
                                                 Map<String, String> kafkaHeaders) {
    if (!COMMITTED_STATUS.equals(importStatus)) {
      return Future.failedFuture(format("DI status is %s", importStatus));
    }

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();

    return updateTargetInstanceWithNonMarcControlledFields(instance, targetTenantProvider, kafkaHeaders)
      .compose(targetInstance -> sourceStorageHelper
        .deleteSourceRecordByRecordId(marcRecord.getId(), instanceId, sourceTenant, kafkaHeaders)
        .map(deletedRecordId -> targetInstance))
      .compose(targetInstance -> updateSourceInstanceAsShared(instance, targetInstance, sourceTenantProvider));
  }

  private Future<String> updateSourceInstanceAsShared(Instance instance, Instance targetInstance,
                                                      SourceTenantProvider sourceTenantProvider) {
    JsonObject jsonInstanceToPublish = new JsonObject(instance.getJsonForStorage().encode());
    jsonInstanceToPublish.put(SOURCE_KEY, CONSORTIUM_MARC.getValue());
    jsonInstanceToPublish.put(HRID_KEY, targetInstance.getHrid());
    return instanceOperations.updateInstance(Instance.fromJson(jsonInstanceToPublish), sourceTenantProvider);
  }

  private Future<Instance> updateTargetInstanceWithNonMarcControlledFields(Instance sourceInstance,
                                                                           TargetTenantProvider targetTenantProvider,
                                                                           Map<String, String> kafkaHeaders) {
    return instanceOperations.getInstanceById(sourceInstance.getId(), targetTenantProvider)
      .map(targetInstance -> populateTargetInstanceWithNonMarcControlledFields(targetInstance, sourceInstance))
      .compose(
        targetInstance -> instanceOperations.updateInstance(targetInstance, targetTenantProvider).map(targetInstance))
      .compose(
        targetInstance -> updateSuppressFromDiscoveryFlagIfNeeded(targetInstance, targetTenantProvider, kafkaHeaders));
  }

  private Future<Instance> updateSuppressFromDiscoveryFlagIfNeeded(Instance targetInstance,
                                                                   TargetTenantProvider targetTenantProvider,
                                                                   Map<String, String> kafkaHeaders) {
    if (Boolean.TRUE.equals(targetInstance.getDiscoverySuppress())) {
      return sourceStorageHelper.updateSourceRecordSuppressFromDiscovery(targetInstance.getId(),
          targetInstance.getDiscoverySuppress(), targetTenantProvider.tenantId(), kafkaHeaders)
        .map(targetInstance);
    }
    return Future.succeededFuture(targetInstance);
  }

  private Instance populateTargetInstanceWithNonMarcControlledFields(Instance targetInstance, Instance sourceInstance) {
    targetInstance.setStaffSuppress(sourceInstance.getStaffSuppress());
    targetInstance.setDiscoverySuppress(sourceInstance.getDiscoverySuppress());
    targetInstance.setDeleted(sourceInstance.getDeleted());
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
          LOGGER.debug("unlinkAuthorityLinksIfNeeded:: Not found linked authorities for instance id: {} and tenant: {}",
            instanceId, context.getTenantId());
          return Future.succeededFuture(marcRecord);
        }
        AuthorityRecordCollection authorityRecordCollection = storage.getAuthorityRecordCollection(context);
        return entitiesLinksService.getLinkingRules(context)
          .compose(linkingRules -> relinkAuthorities(entityLinks, linkingRules, marcRecord, instanceId, context,
            sharingInstanceMetadata, authorityRecordCollection));
      });
  }

  private Future<Record> relinkAuthorities(List<Link> entityLinks, List<LinkingRuleDto> linkingRules, Record marcRecord,
                                           String instanceId, Context context, SharingInstance sharingInstanceMetadata,
                                           AuthorityRecordCollection authorityRecordCollection) {

    return updateLinksForSourceTenant(List.of(), instanceId, context, sharingInstanceMetadata)
      .compose(v -> getLocalAuthoritiesIdsList(entityLinks, authorityRecordCollection)
        .compose(localAuthoritiesIds -> {
          var unlinkLocalAuthoritiesFuture = localAuthoritiesIds.isEmpty()
                                             ? Future.succeededFuture()
                                             : unlinkLocalAuthorities(linkingRules, marcRecord, instanceId, context,
                                               localAuthoritiesIds);

          return unlinkLocalAuthoritiesFuture.compose(v2 -> {
            var sharedAuthorityLinks = localAuthoritiesIds.isEmpty()
                                       ? entityLinks
                                       : getSharedAuthorityLinks(entityLinks, localAuthoritiesIds);

            return sharedAuthorityLinks.isEmpty()
                   ? Future.succeededFuture(marcRecord)
                   : linkSharedAuthoritiesToTargetTenantInstance(marcRecord, instanceId, context,
                     sharingInstanceMetadata, sharedAuthorityLinks);
          });
        })
        .recover(
          cause -> rollbackAuthorityLinksForSourceTenant(entityLinks, instanceId, context, sharingInstanceMetadata,
            cause))
      );
  }

  private Future<Record> rollbackAuthorityLinksForSourceTenant(List<Link> entityLinks, String instanceId,
                                                               Context context, SharingInstance sharingInstanceMetadata,
                                                               Throwable cause) {
    LOGGER.warn("Rollback authority links update for source tenant: {} and instance: {}",
      sharingInstanceMetadata.getSourceTenantId(), instanceId);

    updateLinksForSourceTenant(entityLinks, instanceId, context, sharingInstanceMetadata)
      .onFailure(
        e -> LOGGER.error("Error during rollback authority links update for source tenant: {} and instance: {}",
          sharingInstanceMetadata.getSourceTenantId(), instanceId, e));

    return Future.failedFuture(
      new ConsortiumException(cause != null ? cause.getMessage() : "Error updating shared authorities in MARC record"));
  }

  private Future<Void> unlinkLocalAuthorities(List<LinkingRuleDto> linkingRules, Record marcRecord, String instanceId,
                                              Context context, List<String> localAuthoritiesIds) {
    var fields = linkingRules.stream().map(LinkingRuleDto::getBibField).toList();
    LOGGER.debug("unlinkLocalAuthorities:: Unlinking local authorities: {} from instance: {}, tenant: {}",
      localAuthoritiesIds, instanceId, context.getTenantId());

    try {
      removeSubfieldsThatContainsValues(new FolioRecordHolder(marcRecord), fields, SUBFIELD_9, localAuthoritiesIds);
      return Future.succeededFuture();
    } catch (Exception e) {
      LOGGER.warn("unlinkLocalAuthorities:: Error removing $9 subfields from record: {}", marcRecord.getId(), e);
      return Future.failedFuture(new ConsortiumException("Error unlinking local authorities during instance sharing"));
    }
  }

  private Future<Record> linkSharedAuthoritiesToTargetTenantInstance(Record marcRecord, String instanceId,
                                                                     Context context,
                                                                     SharingInstance sharingInstanceMetadata,
                                                                     List<Link> sharedAuthorityLinks) {

    var targetTenantContext = getTenantContext(context, sharingInstanceMetadata.getTargetTenantId());
    LOGGER.debug(
      "linkSharedAuthoritiesToTargetTenantInstance:: Linking shared authorities: {} to instance: {}, tenant: {}",
      sharedAuthorityLinks, instanceId, targetTenantContext.getTenantId());
    return entitiesLinksService.putInstanceAuthorityLinks(targetTenantContext, instanceId, sharedAuthorityLinks)
      .map(marcRecord);
  }

  private Future<Void> updateLinksForSourceTenant(List<Link> entityLinks, String instanceId, Context context,
                                                  SharingInstance sharingInstanceMetadata) {
    var sourceTenantContext = getTenantContext(context, sharingInstanceMetadata.getSourceTenantId());
    LOGGER.debug("updateLinksForSourceTenant:: Updating authority links for source tenant: {} and instance: {}",
      sourceTenantContext.getTenantId(), instanceId);
    return entitiesLinksService.putInstanceAuthorityLinks(sourceTenantContext, instanceId, entityLinks);
  }

  private Context getTenantContext(Context context, String tenantId) {
    return constructContext(tenantId, context.getToken(), context.getOkapiLocation(), context.getUserId(),
      context.getRequestId());
  }

  private Future<List<String>> getLocalAuthoritiesIdsList(List<Link> entityLinks,
                                                          AuthorityRecordCollection authorityRecordCollection) {
    Promise<List<String>> promise = Promise.promise();
    try {
      authorityRecordCollection.findByCql(format("id==(%s)", getQueryParamForMultipleAuthorities(entityLinks)),
        PagingParameters.defaults(),
        findResults -> {
          List<String> localEntitiesIds = findResults.result().records().stream()
            .filter(source -> !source.getSource().value().startsWith(InstanceSource.CONSORTIUM_PREFIX))
            .map(Authority::getId).toList();
          promise.complete(localEntitiesIds);
        },
        failure -> promise.fail(failure.reason()));
    } catch (UnsupportedEncodingException e) {
      promise.fail(e);
    }
    return promise.future();
  }

  private static List<Link> getSharedAuthorityLinks(List<Link> entityLinks, List<String> localAuthoritiesIds) {
    return entityLinks.stream().filter(entityLink -> !localAuthoritiesIds.contains(entityLink.getAuthorityId()))
      .toList();
  }

  private static String getQueryParamForMultipleAuthorities(List<Link> entityLinks) {
    return entityLinks.stream().map(Link::getAuthorityId).collect(Collectors.joining(" OR "));
  }
}
