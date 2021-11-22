package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;

import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.Authority;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

public class AuthorityUpdateDelegate {

  private static final Logger LOGGER = LogManager.getLogger(AuthorityUpdateDelegate.class);

  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String QM_RELATED_RECORD_VERSION_KEY = "RELATED_RECORD_VERSION";
  private static final String MARC_FORMAT = "MARC_AUTHORITY";

  private final Storage storage;

  public AuthorityUpdateDelegate(Storage storage) {
    this.storage = storage;
  }

  public Future<Authority> handle(Map<String, String> eventPayload, Record marcRecord, Context context) {
    try {
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters =
        new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);

      JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String authorityId = marcRecord.getId();
      RecordMapper<Authority> recordMapper = RecordMapperBuilder.buildMapper(MARC_FORMAT);
      var mappedAuthority = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      AuthorityRecordCollection authorityRecordCollection = storage.getAuthorityRecordCollection(context);

      return getAuthorityRecordById(authorityId, authorityRecordCollection)
        .onSuccess(existingAuthorityRecord -> fillVersion(existingAuthorityRecord, eventPayload))
        .compose(existingAuthorityRecord -> mergeRecords(existingAuthorityRecord, mappedAuthority))
        .compose(updatedAuthorityRecord -> updateAuthorityRecord(updatedAuthorityRecord, authorityRecordCollection));
    } catch (Exception e) {
      LOGGER.error("Error updating inventory authority", e);
      return Future.failedFuture(e);
    }
  }

  private JsonObject retrieveParsedContent(ParsedRecord parsedRecord) {
    return parsedRecord.getContent() instanceof String
      ? new JsonObject(parsedRecord.getContent().toString())
      : JsonObject.mapFrom(parsedRecord.getContent());
  }

  private Future<Authority> getAuthorityRecordById(String authorityId,
                                                   AuthorityRecordCollection authorityRecordCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityRecordCollection.findById(authorityId, success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error(format("Error retrieving Authority by id %s - %s, status code %s", authorityId, failure.getReason(),
          failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private void fillVersion(Authority existingAuthorityRecord, Map<String, String> eventPayload) {
    if (eventPayload.containsKey(QM_RELATED_RECORD_VERSION_KEY)) {
      existingAuthorityRecord.setVersion(Integer.parseInt(eventPayload.get(QM_RELATED_RECORD_VERSION_KEY)));
    }
  }

  private Future<Authority> mergeRecords(Authority existingRecord, Authority mappedRecord) {
    try {
      mappedRecord.setId(existingRecord.getId());
      JsonObject existing = JsonObject.mapFrom(existingRecord);
      JsonObject mapped = JsonObject.mapFrom(mappedRecord);
      JsonObject merged = existing.mergeIn(mapped);
      Authority mergedAuthorityRecord = merged.mapTo(Authority.class);
      return Future.succeededFuture(mergedAuthorityRecord);
    } catch (Exception e) {
      LOGGER.error("Error updating authority", e);
      return Future.failedFuture(e);
    }
  }

  private Future<Authority> updateAuthorityRecord(Authority authorityRecord,
                                                  AuthorityRecordCollection authorityRecordCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityRecordCollection.update(authorityRecord, success -> promise.complete(authorityRecord),
      failure -> {
        LOGGER.error(format("Error updating Authority - %s, status code %s", failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
