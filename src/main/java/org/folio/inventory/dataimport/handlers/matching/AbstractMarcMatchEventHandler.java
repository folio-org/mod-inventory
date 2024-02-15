package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.DataImportEventTypes;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.reader.util.MarcValueReaderUtil;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.Filter;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordIdentifiersDto;
import org.folio.rest.jaxrs.model.RecordMatchingDto;
import org.folio.rest.jaxrs.model.RecordsIdentifiersCollection;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.folio.processing.value.Value.ValueType.MISSING;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

public abstract class AbstractMarcMatchEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger(AbstractMarcMatchEventHandler.class);

  protected static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";
  private static final String PAYLOAD_HAS_NO_DATA_MESSAGE = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE = "Found multiple records matching specified conditions";
  private static final String RECORDS_NOT_FOUND_MESSAGE = "Can`t find records matching specified conditions";
  private static final String MATCH_DETAIL_IS_NOT_VALID = "Match detail is not valid: %s";
  private static final String USER_ID_HEADER = "userId";

  private final ConsortiumService consortiumService;
  private final DataImportEventTypes matchedEventType;
  private final DataImportEventTypes notMatchedEventType;
  private final HttpClient httpClient;

  protected AbstractMarcMatchEventHandler(ConsortiumService consortiumService,
                                          DataImportEventTypes matchedEventType,
                                          DataImportEventTypes notMatchedEventType,
                                          HttpClient httpClient) {
    this.consortiumService = consortiumService;
    this.matchedEventType = matchedEventType;
    this.notMatchedEventType = notMatchedEventType;
    this.httpClient = httpClient;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    try {
      payload.setEventType(notMatchedEventType.value());
      HashMap<String, String> context = payload.getContext();

      if (MapUtils.isEmpty(context) || isEmpty(payload.getContext().get(getMarcType())) || Objects.isNull(payload.getCurrentNode()) || Objects.isNull(payload.getEventsChain())) {
        LOG.warn(PAYLOAD_HAS_NO_DATA_MESSAGE);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MESSAGE));
      }
      payload.getEventsChain().add(payload.getEventType());
      payload.setAdditionalProperty(USER_ID_HEADER, context.get(USER_ID_HEADER));
      SourceStorageRecordsClient sourceStorageRecordsClient = new SourceStorageRecordsClient(payload.getOkapiUrl(), payload.getTenant(), payload.getToken(), httpClient);

      String recordAsString = context.get(getMarcType());
      MatchDetail matchDetail = retrieveMatchDetail(payload);

      if (!isValidMatchDetail(matchDetail)) {
        constructError(payload, String.format(MATCH_DETAIL_IS_NOT_VALID, matchDetail));
        return CompletableFuture.completedFuture(payload);
      }

      Value<?> value = MarcValueReaderUtil.readValueFromRecord(recordAsString, matchDetail.getIncomingMatchExpression());
      if (value.getType() == MISSING) {
        LOG.info("Could not find records by matching criteria because incoming record does not contain specified field");
        return CompletableFuture.completedFuture(payload);
      }

      RecordMatchingDto recordMatchingDto = buildRecordsMatchingRequest(matchDetail, value);
      return retrieveMarcRecords(recordMatchingDto, sourceStorageRecordsClient, payload)
        .compose(localMatchedRecords -> {
          if (isMatchingOnCentralTenantRequired()) {
            return matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords(recordMatchingDto, payload, localMatchedRecords);
          }
          return Future.succeededFuture(localMatchedRecords.stream().toList());
        })
        .compose(recordList -> processSucceededResult(recordList, payload))
        .onFailure(e -> LOG.warn("handle:: Failed to process event for MARC record matching, jobExecutionId: '{}'", payload.getJobExecutionId(), e))
        .recover(throwable -> Future.failedFuture(mapToMatchException(throwable)))
        .toCompletionStage().toCompletableFuture();
    } catch (Exception e) {
      LOG.warn("handle:: Error while processing event for MARC record matching, jobExecutionId: '{}'", payload.getJobExecutionId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private Future<Optional<Record>> retrieveMarcRecords(RecordMatchingDto recordMatchingDto, SourceStorageRecordsClient sourceStorageRecordsClient,
                                                       DataImportEventPayload payload) {
    return getMatchedRecordsIdentifiers(recordMatchingDto, payload, sourceStorageRecordsClient)
      .compose(recordsIdentifiersCollection -> {
        if (recordsIdentifiersCollection.getIdentifiers().size() > 1) {
          return Future.failedFuture(FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE);
        } else if (recordsIdentifiersCollection.getIdentifiers().size() == 1) {
          return getRecordById(recordsIdentifiersCollection.getIdentifiers().get(0), sourceStorageRecordsClient, payload);
        }
        return Future.succeededFuture(Optional.empty());
      });
  }

  private Future<RecordsIdentifiersCollection> getMatchedRecordsIdentifiers(RecordMatchingDto recordMatchingDto, DataImportEventPayload payload,
                                                                            SourceStorageRecordsClient sourceStorageRecordsClient) {
    return sourceStorageRecordsClient.postSourceStorageRecordsMatching(recordMatchingDto)
      .compose(response -> {
        if (response.statusCode() == SC_OK) {
          return Future.succeededFuture(response.bodyAsJson(RecordsIdentifiersCollection.class));
        }
        String msg = String.format("Failed to request records identifiers by matching criteria, responseStatus: '%s', body: '%s', jobExecutionId: '%s', tenant: '%s'",
          response.statusCode(), response.bodyAsString(), payload.getJobExecutionId(), payload.getTenant());
        return Future.failedFuture(msg);
      });
  }

  private Future<Optional<Record>> getRecordById(RecordIdentifiersDto recordIdentifiersDto, SourceStorageRecordsClient sourceStorageRecordsClient,
                                                 DataImportEventPayload payload) {
    String recordId = recordIdentifiersDto.getRecordId();

    return sourceStorageRecordsClient.getSourceStorageRecordsById(recordId)
      .compose(response -> {
        if (response.statusCode() == SC_OK) {
          return Future.succeededFuture(Optional.of(response.bodyAsJson(Record.class)));
        } else if (response.statusCode() == SC_NOT_FOUND) {
          return Future.succeededFuture(Optional.empty());
        }
        String msg = String.format("Failed to retrieve record by id: '%s', responseStatus: '%s', body: '%s', jobExecutionId: '%s', tenant: '%s'",
          recordId, response.statusCode(), response.bodyAsString(), payload.getJobExecutionId(), payload.getTenant());
        return Future.failedFuture(msg);
      });
  }

  private RecordMatchingDto buildRecordsMatchingRequest(MatchDetail matchDetail, Value<?> value) {
    List<Field> matchDetailFields = matchDetail.getExistingMatchExpression().getFields();
    String field = matchDetailFields.get(0).getValue();
    String ind1 = matchDetailFields.get(1).getValue();
    String ind2 = matchDetailFields.get(2).getValue();
    String subfield = matchDetailFields.get(3).getValue();

    List<String> values = switch (value.getType()) {
      case STRING -> List.of(((StringValue) value).getValue());
      case LIST -> ((ListValue) value).getValue();
      default -> Collections.emptyList();
    };

    Filter filter = new Filter()
      .withValues(values)
      .withField(field)
      .withIndicator1(ind1)
      .withIndicator2(ind2)
      .withSubfield(subfield);

    return new RecordMatchingDto()
      .withRecordType(getMatchedRecordType())
      .withFilters(List.of(filter))
      .withReturnTotalRecordsCount(false);
  }

  protected abstract String getMarcType();

  protected abstract RecordMatchingDto.RecordType getMatchedRecordType();

  private Future<List<Record>> matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords(RecordMatchingDto recordMatchingDto, DataImportEventPayload payload,
                                                                                           Optional<Record> localMatchedRecord) {
    Context context = EventHandlingUtil.constructContext(payload.getTenant(), payload.getToken(), payload.getOkapiUrl());
    return consortiumService.getConsortiumConfiguration(context)
      .compose(consortiumConfigurationOptional -> {
        if (consortiumConfigurationOptional.isPresent() && !consortiumConfigurationOptional.get().getCentralTenantId().equals(payload.getTenant())) {
          LOG.debug("matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords:: Matching on centralTenant with id: {}",
            consortiumConfigurationOptional.get().getCentralTenantId());
          SourceStorageRecordsClient sourceStorageRecordsClient =
            new SourceStorageRecordsClient(payload.getOkapiUrl(), consortiumConfigurationOptional.get().getCentralTenantId(), payload.getToken(), httpClient);

          return retrieveMarcRecords(recordMatchingDto, sourceStorageRecordsClient, payload)
            .map(centralRecordOptional -> {
              centralRecordOptional.ifPresent(r -> payload.getContext().put(CENTRAL_TENANT_ID_KEY, consortiumConfigurationOptional.get().getCentralTenantId()));
              return Stream.concat(localMatchedRecord.stream(), centralRecordOptional.stream()).toList();
            });
        }
        return Future.succeededFuture(localMatchedRecord.stream().toList());
      });
  }

  private static Throwable mapToMatchException(Throwable throwable) {
    return throwable instanceof MatchingException ? throwable : new MatchingException(throwable);
  }

  abstract boolean isMatchingOnCentralTenantRequired();

  private boolean isValidMatchDetail(MatchDetail matchDetail) {
    if (matchDetail.getExistingMatchExpression() != null
      && matchDetail.getExistingMatchExpression().getDataValueType() == VALUE_FROM_RECORD) {
      List<Field> fields = matchDetail.getExistingMatchExpression().getFields();

      return fields != null && fields.size() == 4
        && matchDetail.getIncomingRecordType() == EntityType.fromValue(getMarcType())
        && matchDetail.getExistingRecordType() == EntityType.fromValue(getMarcType());
    }
    return false;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null
      && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      var matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);
      return isEligibleMatchProfile(matchProfile);
    }
    return false;
  }

  /* Verifies whether the given {@link MatchProfile} is suitable for {@link EventHandler} */
  private boolean isEligibleMatchProfile(MatchProfile matchProfile) {
    return matchProfile.getIncomingRecordType() == EntityType.fromValue(getMarcType())
      && matchProfile.getExistingRecordType() == EntityType.fromValue(getMarcType());
  }

  /**
   * @return the key under which a matched record is put into event payload context
   */
  protected String getMatchedMarcKey() {
    return "MATCHED_" + getMarcType();
  }

  /* Retrieves a {@link MatchDetail} from the given {@link DataImportEventPayload} */
  private MatchDetail retrieveMatchDetail(DataImportEventPayload dataImportEventPayload) {
    MatchProfile matchProfile;
    ProfileSnapshotWrapper matchingProfileWrapper = dataImportEventPayload.getCurrentNode();
    if (matchingProfileWrapper.getContent() instanceof Map) {
      matchProfile = new JsonObject((Map) matchingProfileWrapper.getContent()).mapTo(MatchProfile.class);
    } else {
      matchProfile = (MatchProfile) matchingProfileWrapper.getContent();
    }
    return matchProfile.getMatchDetails().get(0);
  }

  /**
   * Prepares {@link DataImportEventPayload} for the further processing based on the number of retrieved records in {@link RecordCollection}
   */
  private Future<DataImportEventPayload> processSucceededResult(List<Record> records, DataImportEventPayload payload) {
    if (records.size() == 1) {
      payload.setEventType(matchedEventType.toString());
      payload.getContext().put(getMatchedMarcKey(), Json.encode(records.get(0)));
      LOG.debug("processSucceededResult:: Matched 1 record for tenant with id {}", payload.getTenant());
      return Future.succeededFuture(payload);
    }
    if (records.size() > 1) {
      constructError(payload, FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE);
      LOG.warn("processSucceededResult:: Matched multiple record for tenant with id {}", payload.getTenant());
      return Future.failedFuture(new MatchingException(FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE));
    }
    constructError(payload, RECORDS_NOT_FOUND_MESSAGE);
    return Future.succeededFuture(payload);
  }

  private void constructError(DataImportEventPayload payload, String errorMessage) {
    LOG.warn(errorMessage);
    payload.setEventType(notMatchedEventType.toString());
  }

}
