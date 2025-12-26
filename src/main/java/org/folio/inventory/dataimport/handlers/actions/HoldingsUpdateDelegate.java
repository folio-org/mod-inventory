package org.folio.inventory.dataimport.handlers.actions;

import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersUpdateDelegate;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Holdings;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.support.HoldingsRecordUtil;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

public class HoldingsUpdateDelegate {

  private static final Logger LOGGER = LogManager.getLogger(HoldingsUpdateDelegate.class);

  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String MARC_FORMAT = "MARC_HOLDINGS";
  private static final String SOURCE_NAME = "MARC";

  // This mapper is needed to keep null values in serialized JSON to correct merge JsonObject objects.
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    MAPPER.configOverride(HoldingsRecord.class)
      .setInclude(JsonInclude.Value.construct(JsonInclude.Include.ALWAYS, JsonInclude.Include.ALWAYS));
    MAPPER.configOverride(Holdings.class)
      .setInclude(JsonInclude.Value.construct(JsonInclude.Include.ALWAYS, JsonInclude.Include.ALWAYS));
  }

  private final Storage storage;
  private final HoldingsCollectionService holdingsCollectionService;

  public HoldingsUpdateDelegate(Storage storage, HoldingsCollectionService holdingsCollectionService) {
    this.storage = storage;
    this.holdingsCollectionService = holdingsCollectionService;
  }

  public Future<HoldingsRecord> handle(Map<String, String> eventPayload, Record marcRecord, Context context) {
    logParametersUpdateDelegate(LOGGER, eventPayload, marcRecord, context);
    try {
      var mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      var mappingParameters = new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);

      var parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      var holdingsId = marcRecord.getExternalIdsHolder().getHoldingsId();
      LOGGER.info("Holdings update with holdingId: {}", holdingsId);
      var recordMapper = RecordMapperBuilder.<Holdings>buildMapper(MARC_FORMAT);
      var mappedHoldings = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      var holdingsRecordCollection = storage.getHoldingsRecordCollection(context);

      return holdingsCollectionService.getById(holdingsId, holdingsRecordCollection)
        .compose(existingHoldingsRecord -> findSourceId(context)
          .map(sourceId -> mergeRecords(existingHoldingsRecord, mappedHoldings, sourceId)))
        .compose(modifiedRecord -> holdingsCollectionService.update(modifiedRecord, holdingsRecordCollection));
    } catch (Exception e) {
      LOGGER.error("Error updating inventory holdings", e);
      return Future.failedFuture(e);
    }
  }

  private JsonObject retrieveParsedContent(ParsedRecord parsedRecord) {
    return parsedRecord.getContent() instanceof String
           ? new JsonObject(parsedRecord.getContent().toString())
           : JsonObject.mapFrom(parsedRecord.getContent());
  }

  public static HoldingsRecord mergeRecords(HoldingsRecord existingRecord, Holdings mappedRecord, String sourceId) {
    try {
      mappedRecord.setId(existingRecord.getId());
      mappedRecord.setVersion(existingRecord.getVersion());
      mappedRecord.setSourceId(sourceId);
      if (mappedRecord.getInstanceId() != null) {
        mappedRecord.setInstanceId(existingRecord.getInstanceId());
      }
      var existing = new JsonObject(MAPPER.writeValueAsString(existingRecord));
      var mapped = new JsonObject(MAPPER.writeValueAsString(mappedRecord));
      var merged = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);
      return merged.mapTo(HoldingsRecord.class);
    } catch (Exception e) {
      throw new IllegalStateException("Error updating holdings", e);
    }
  }

  private Future<String> findSourceId(Context context) {
    var sourceCollection = storage.getHoldingsRecordsSourceCollection(context);
    return holdingsCollectionService.findSourceIdByName(sourceCollection, SOURCE_NAME);
  }
}
