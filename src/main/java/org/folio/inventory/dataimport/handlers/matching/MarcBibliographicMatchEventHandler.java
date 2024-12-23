package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil.AdditionalSubfields;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordMatchingDto;

import java.io.UnsupportedEncodingException;
import java.util.List;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.getTenant;

public class MarcBibliographicMatchEventHandler extends AbstractMarcMatchEventHandler {

  private static final String HOLDINGS_LOADING_ERROR_MSG = "Failed to load holdings by instanceId: '%s' for matched MARC-BIB, jobExecutionId: '%s'";
  private static final String INSTANCES_IDS_KEY = "INSTANCES_IDS";
  private final Storage storage;

  public MarcBibliographicMatchEventHandler(ConsortiumService consortiumService, HttpClient httpClient, Storage storage) {
    super(consortiumService, DI_SRS_MARC_BIB_RECORD_MATCHED, DI_SRS_MARC_BIB_RECORD_NOT_MATCHED, httpClient);
    this.storage = storage;
  }

  @Override
  protected RecordMatchingDto.RecordType getMatchedRecordType() {
    return RecordMatchingDto.RecordType.MARC_BIB;
  }

  @Override
  protected String getMarcType() {
    return MARC_BIBLIOGRAPHIC.value();
  }

  @Override
  protected boolean isMatchingOnCentralTenantRequired() {
    return true;
  }

  @Override
  protected String getMultiMatchResultKey() {
    return INSTANCES_IDS_KEY;
  }

  @Override
  protected Future<Void> ensureRelatedEntities(List<Record> records, DataImportEventPayload eventPayload) {
    if (records.size() == 1) {
      Record matchedRecord = records.get(0);
      String instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(matchedRecord.getParsedRecord(), AdditionalSubfields.I);
      String matchedRecordTenantId = getTenant(eventPayload);
      Context context = EventHandlingUtil.constructContext(matchedRecordTenantId, eventPayload.getToken(), eventPayload.getOkapiUrl(), eventPayload.getContext().get(EventHandlingUtil.USER_ID));
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);

      if (isBlank(instanceId)) {
        LOG.info("ensureRelatedEntities:: Skipping instance loading for matched MARC-BIB record because the matched MARC-BIB does not contain instanceId");
        return Future.succeededFuture();
      }

      return Future.fromCompletionStage(instanceCollection.findById(instanceId))
        .compose(instance -> {
          eventPayload.getContext().put(INSTANCE.value(), Json.encode(instance));
          return consortiumService.getConsortiumConfiguration(context);
        })
        .compose(consortiumConfigurationOptional -> {
          if (consortiumConfigurationOptional.isEmpty() || !consortiumConfigurationOptional.get().getCentralTenantId().equals(matchedRecordTenantId)) {
            return loadHoldingsRecordByInstanceId(instanceId, eventPayload, context).mapEmpty();
          }
          return Future.succeededFuture();
        });
    }
    return Future.succeededFuture();
  }

  private Future<Void> loadHoldingsRecordByInstanceId(String instanceId, DataImportEventPayload eventPayload, Context context) {
    return getHoldingsByInstanceId(instanceId, eventPayload, context)
      .compose(holdingsRecords -> {
        if (holdingsRecords.size() > 1) {
          LOG.info("loadHoldingsRecordByInstanceId:: Found multiple holdings records by instanceId: '{}' for matched MARC-BIB record, jobExecutionId: '{}'",
            instanceId, eventPayload.getJobExecutionId());
        } else if (holdingsRecords.size() == 1) {
          LOG.info("loadHoldingsRecordByInstanceId:: Found holdings record with id: '{}' by instanceId: '{}' for matched MARC-BIB record, jobExecutionId: '{}'",
            holdingsRecords.get(0).getId(), instanceId, eventPayload.getJobExecutionId());
          eventPayload.getContext().put(HOLDINGS.value(), Json.encode(holdingsRecords.get(0)));
        }
        return Future.succeededFuture();
      });
  }

  private Future<List<HoldingsRecord>> getHoldingsByInstanceId(String instanceId, DataImportEventPayload eventPayload, Context context) {
    Promise<List<HoldingsRecord>> promise = Promise.promise();
    HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);

    try {
      holdingsRecordCollection.findByCql(format("instanceId=%s", instanceId), PagingParameters.defaults(),
        findResult -> {
          if (findResult.getResult() != null && findResult.getResult().totalRecords == 1) {
            eventPayload.getContext().put(HOLDINGS.value(), Json.encode(findResult.getResult().records.get(0)));
          }
          promise.complete(findResult.getResult().records);
        },
        failure -> {
          String msg = format("Error loading inventory holdings for matched MARC-BIB, instanceId: '%s' statusCode: '%s', message: '%s'", instanceId, failure.getStatusCode(), failure.getReason());
          LOG.warn("getHoldingsByInstanceId:: {}", msg);
          promise.fail(msg);
        });
    } catch (UnsupportedEncodingException e) {
      String msg = format(HOLDINGS_LOADING_ERROR_MSG, instanceId, eventPayload.getJobExecutionId());
      LOG.warn("getHoldingsByInstanceId:: {}", msg);
      promise.fail(msg);
    }
    return promise.future();
  }

}
