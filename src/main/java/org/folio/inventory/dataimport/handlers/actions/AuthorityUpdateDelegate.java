package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile.FolioRecord;
import org.folio.Authority;
import org.folio.AuthorityExtended;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.DataImportException;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

import java.util.Map;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.handlers.actions.AbstractAuthorityEventHandler.getIsAuthorityExtended;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersUpdateDelegate;

public class AuthorityUpdateDelegate {

  private static final Logger LOGGER = LogManager.getLogger(AuthorityUpdateDelegate.class);

  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String MARC_FORMAT = "MARC_AUTHORITY";

  private final Storage storage;

  public AuthorityUpdateDelegate(Storage storage) {
    this.storage = storage;
  }

  public Future<Authority> handle(Map<String, String> eventPayload, Record marcRecord, Context context) {
    logParametersUpdateDelegate(LOGGER, eventPayload, marcRecord, context);
    try {
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters =
        new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);

      JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String authorityId = marcRecord.getExternalIdsHolder().getAuthorityId();
      LOGGER.info("Authority update with authorityId: {}", authorityId);
      RecordMapper<Authority> recordMapper = getIsAuthorityExtended()
        ? RecordMapperBuilder.buildMapper(FolioRecord.MARC_AUTHORITY_EXTENDED.value())
        : RecordMapperBuilder.buildMapper(MARC_FORMAT);
      var mappedAuthority = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      AuthorityRecordCollection authorityRecordCollection = storage.getAuthorityRecordCollection(context);

      return getAuthorityRecordById(authorityId, authorityRecordCollection)
        .compose(existingAuthorityRecord -> mergeRecords(existingAuthorityRecord, mappedAuthority))
        .compose(updatedAuthorityRecord -> updateAuthorityRecord(updatedAuthorityRecord, authorityRecordCollection));
    } catch (Exception e) {
      LOGGER.error("Error updating Authority", e);
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
    authorityRecordCollection.findById(authorityId, success -> {
        var result = success.getResult();
        if (result == null) {
          completeExceptionally(promise, format("Error retrieving Authority by id %s - Record was not found", authorityId));
        } else {
          promise.complete(result);
        }
      },
      failure -> completeExceptionally(promise,
        format("Error retrieving Authority by id %s - %s, status code %s",
          authorityId,
          failure.getReason(),
          failure.getStatusCode()
        )
      ));
    return promise.future();
  }

  private void completeExceptionally(Promise<?> promise, String message) {
    LOGGER.error(message);
    promise.fail(new DataImportException(message));
  }

  private Future<Authority> mergeRecords(Authority existingRecord, Authority mappedRecord) {
    try {
      mappedRecord.setId(existingRecord.getId());
      mappedRecord.setVersion(existingRecord.getVersion());
      JsonObject mapped = JsonObject.mapFrom(mappedRecord);
      Authority mergedAuthorityRecord = getIsAuthorityExtended()
        ? mapped.mapTo(AuthorityExtended.class)
        : mapped.mapTo(Authority.class);
      mergedAuthorityRecord.setSource(Authority.Source.MARC);
      return Future.succeededFuture(mergedAuthorityRecord);
    } catch (Exception e) {
      LOGGER.error("Error updating authority", e);
      return Future.failedFuture(e);
    }
  }

  private Future<Authority> updateAuthorityRecord(Authority authorityRecord, AuthorityRecordCollection authorityRecordCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityRecordCollection.update(authorityRecord, success -> promise.complete(authorityRecord),
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          promise.fail(new OptimisticLockingException(failure.getReason()));
        } else {
          LOGGER.error(format("Error updating authority record: %s, status code: %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
    });
    return promise.future();
  }
}
