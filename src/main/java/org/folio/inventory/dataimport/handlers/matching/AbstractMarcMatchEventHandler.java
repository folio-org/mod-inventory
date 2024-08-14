package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.DataImportEventTypes;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.kafka.SimpleConfigurationReader;
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
import org.folio.rest.jaxrs.model.Qualifier;
import org.folio.rest.jaxrs.model.Record;
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

import static java.lang.String.format;
import static org.apache.http.HttpStatus.SC_OK;
import static org.folio.processing.value.Value.ValueType.MISSING;
import static org.folio.rest.jaxrs.model.Filter.*;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;

public abstract class AbstractMarcMatchEventHandler implements EventHandler {

  protected static final Logger LOG = LogManager.getLogger();

  protected static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";
  protected static final String RECORDS_IDENTIFIERS_FETCH_LIMIT_PARAM = "inventory.di.records.identifiers.fetch.limit";
  private static final String PAYLOAD_HAS_NO_DATA_MESSAGE = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE = "Found multiple records matching specified conditions";
  private static final String RECORDS_NOT_FOUND_MESSAGE = "Could not find records matching specified conditions";
  private static final String MATCH_DETAIL_IS_INVALID_MESSAGE = "Match detail is invalid";
  private static final String MATCH_RESULT_KEY_PREFIX = "MATCHED_%s";
  private static final String USER_ID_HEADER = "userId";
  private static final int EXPECTED_MATCH_EXPRESSION_FIELDS_NUMBER = 4;
  private static final String DEFAULT_RECORDS_IDENTIFIERS_LIMIT = "5000";

  protected final ConsortiumService consortiumService;
  private final DataImportEventTypes matchedEventType;
  private final DataImportEventTypes notMatchedEventType;
  private final HttpClient httpClient;
  private final int recordsIdentifiersLimit;

  protected AbstractMarcMatchEventHandler(ConsortiumService consortiumService,
                                          DataImportEventTypes matchedEventType,
                                          DataImportEventTypes notMatchedEventType,
                                          HttpClient httpClient) {
    this.consortiumService = consortiumService;
    this.matchedEventType = matchedEventType;
    this.notMatchedEventType = notMatchedEventType;
    this.httpClient = httpClient;

    this.recordsIdentifiersLimit = Integer.parseInt(SimpleConfigurationReader.getValue(
      RECORDS_IDENTIFIERS_FETCH_LIMIT_PARAM, DEFAULT_RECORDS_IDENTIFIERS_LIMIT));
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    try {
      payload.setEventType(notMatchedEventType.value());
      HashMap<String, String> context = payload.getContext();

      if (isNotValidPayload(payload)) {
        LOG.warn("handle:: {}", PAYLOAD_HAS_NO_DATA_MESSAGE);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MESSAGE));
      }
      payload.getEventsChain().add(payload.getEventType());
      payload.setAdditionalProperty(USER_ID_HEADER, context.get(USER_ID_HEADER));

      String recordAsString = context.get(getMarcType());
      MatchDetail matchDetail = retrieveMatchDetail(payload);

      if (!isValidMatchDetail(matchDetail)) {
        LOG.warn("handle:: Match detail is invalid, jobExecutionId: '{}', match detail: '{}'", payload.getJobExecutionId(), matchDetail);
        return CompletableFuture.failedFuture(new EventProcessingException(MATCH_DETAIL_IS_INVALID_MESSAGE));
      }

      Value<?> value = MarcValueReaderUtil.readValueFromRecord(recordAsString, matchDetail.getIncomingMatchExpression());
      if (value.getType() == MISSING) {
        LOG.info("handle:: Could not find records by matching criteria because incoming record does not contain specified field, jobExecutionId: '{}'",
          payload.getJobExecutionId());
        return CompletableFuture.completedFuture(payload);
      }

      RecordMatchingDto recordMatchingDto = buildRecordsMatchingRequest(matchDetail, value);
      return retrieveMarcRecords(recordMatchingDto, payload.getTenant(), payload)
        .compose(localMatchedRecords -> {
          if (isMatchingOnCentralTenantRequired()) {
            return matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords(recordMatchingDto, payload, localMatchedRecords);
          }
          return Future.succeededFuture(localMatchedRecords.stream().toList());
        })
        .compose(recordList -> ensureRelatedEntities(recordList, payload).map(recordList))
        .compose(recordList -> processSucceededResult(recordList, payload))
        .onFailure(e -> LOG.warn("handle:: Failed to process event for MARC record matching, jobExecutionId: '{}'", payload.getJobExecutionId(), e))
        .toCompletionStage().toCompletableFuture();
    } catch (Exception e) {
      LOG.warn("handle:: Error while processing event for MARC record matching, jobExecutionId: '{}'",
        payload.getJobExecutionId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  protected abstract String getMarcType();

  protected abstract RecordMatchingDto.RecordType getMatchedRecordType();

  protected abstract boolean isMatchingOnCentralTenantRequired();

  protected abstract String getMultiMatchResultKey();

  @SuppressWarnings("squid:S1172")
  protected Future<Void> ensureRelatedEntities(List<Record> records, DataImportEventPayload eventPayload) {
    return Future.succeededFuture();
  }

  private boolean isNotValidPayload(DataImportEventPayload payload) {
    HashMap<String, String> context = payload.getContext();
    return MapUtils.isEmpty(context)
      || StringUtils.isEmpty(payload.getContext().get(getMarcType()))
      || Objects.isNull(payload.getCurrentNode())
      || Objects.isNull(payload.getEventsChain());
  }

  /**
   * Retrieves a {@link MatchDetail} from the given {@link DataImportEventPayload}
   *
   * @param dataImportEventPayload event payload to retrieve from
   * @return {@link MatchDetail}
   */
  @SuppressWarnings("squid:S3740")
  private MatchDetail retrieveMatchDetail(DataImportEventPayload dataImportEventPayload) {
    MatchProfile matchProfile;
    ProfileSnapshotWrapper matchingProfileWrapper = dataImportEventPayload.getCurrentNode();
    if (matchingProfileWrapper.getContent() instanceof Map profileAsMap) {
      matchProfile = new JsonObject(profileAsMap).mapTo(MatchProfile.class);
    } else {
      matchProfile = (MatchProfile) matchingProfileWrapper.getContent();
    }
    return matchProfile.getMatchDetails().get(0);
  }

  private boolean isValidMatchDetail(MatchDetail matchDetail) {
    if (matchDetail.getExistingMatchExpression() != null
      && matchDetail.getExistingMatchExpression().getDataValueType() == VALUE_FROM_RECORD) {
      List<Field> fields = matchDetail.getExistingMatchExpression().getFields();

      return fields != null && fields.size() == EXPECTED_MATCH_EXPRESSION_FIELDS_NUMBER
        && matchDetail.getIncomingRecordType() == EntityType.fromValue(getMarcType())
        && matchDetail.getExistingRecordType() == EntityType.fromValue(getMarcType());
    }
    return false;
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

    Qualifier qualifier = matchDetail.getExistingMatchExpression().getQualifier();
    Filter.Qualifier qualifierFilterType = null;
    ComparisonPartType comparisonPartType = null;
    String qualifierValue = null;

    if (qualifier != null) {
      qualifierFilterType = Filter.Qualifier.valueOf(qualifier.getQualifierType().toString());
      qualifierValue = qualifier.getQualifierValue();
      comparisonPartType = ComparisonPartType.valueOf(qualifier.getComparisonPart().toString());
    }

    return new RecordMatchingDto()
      .withRecordType(getMatchedRecordType())
      .withFilters(List.of(new Filter()
        .withValues(values)
        .withField(field)
        .withIndicator1(ind1)
        .withIndicator2(ind2)
        .withSubfield(subfield)
        .withQualifier(qualifierFilterType)
        .withQualifierValue(qualifierValue)
        .withComparisonPartType(comparisonPartType)))
      .withReturnTotalRecordsCount(true);
  }

  private Future<Optional<Record>> retrieveMarcRecords(RecordMatchingDto recordMatchingDto, String tenantId,
                                                       DataImportEventPayload payload) {
    SourceStorageRecordsClient sourceStorageRecordsClient =
      new SourceStorageRecordsClient(payload.getOkapiUrl(), tenantId, payload.getToken(), httpClient);

    return getAllMatchedRecordsIdentifiers(recordMatchingDto, payload, sourceStorageRecordsClient)
      .compose(recordsIdentifiersCollection -> {
        if (recordsIdentifiersCollection.getIdentifiers().size() > 1) {
          populatePayloadWithExternalIdentifiers(recordsIdentifiersCollection, payload);
          return Future.succeededFuture(Optional.empty());
        } else if (recordsIdentifiersCollection.getIdentifiers().size() == 1) {
          return getRecordById(recordsIdentifiersCollection.getIdentifiers().get(0), sourceStorageRecordsClient, payload);
        }
        LOG.info("retrieveMarcRecords:: {}, jobExecutionId: '{}', tenantId: '{}'",
          RECORDS_NOT_FOUND_MESSAGE, payload.getJobExecutionId(), tenantId);
        return Future.succeededFuture(Optional.empty());
      });
  }

  private Future<RecordsIdentifiersCollection> getAllMatchedRecordsIdentifiers(RecordMatchingDto recordMatchingDto,
                                                                               DataImportEventPayload payload,
                                                                               SourceStorageRecordsClient sourceStorageRecordsClient) {
    return getMatchedRecordsIdentifiers(recordMatchingDto, payload, sourceStorageRecordsClient)
      .compose(recordsIdentifiersCollection -> {
        if (recordsIdentifiersCollection.getIdentifiers().size() < recordsIdentifiersCollection.getTotalRecords()) {
          return getRemainingRecordsIdentifiers(recordMatchingDto, payload, sourceStorageRecordsClient, recordsIdentifiersCollection);
        }
        return Future.succeededFuture(recordsIdentifiersCollection);
      });
  }

  private Future<RecordsIdentifiersCollection> getRemainingRecordsIdentifiers(RecordMatchingDto recordMatchingDto,
                                                                              DataImportEventPayload payload,
                                                                              SourceStorageRecordsClient sourceStorageRecordsClient,
                                                                              RecordsIdentifiersCollection recordsIdentifiersCollection) {
    RecordMatchingDto matchingRequest = JsonObject.mapFrom(recordMatchingDto).mapTo(RecordMatchingDto.class);
    Future<RecordsIdentifiersCollection> future = Future.succeededFuture();

    for (int offset = recordsIdentifiersLimit; offset < recordsIdentifiersCollection.getTotalRecords(); offset += recordsIdentifiersLimit) {
      matchingRequest.setOffset(offset);
      future = future.compose(v -> getMatchedRecordsIdentifiers(matchingRequest, payload, sourceStorageRecordsClient)
        .map(identifiersCollection -> {
          recordsIdentifiersCollection.getIdentifiers().addAll(identifiersCollection.getIdentifiers());
          return recordsIdentifiersCollection;
        }));
    }
    return future;
  }

  private Future<RecordsIdentifiersCollection> getMatchedRecordsIdentifiers(RecordMatchingDto recordMatchingDto,
                                                                            DataImportEventPayload payload,
                                                                            SourceStorageRecordsClient sourceStorageRecordsClient) {
    return sourceStorageRecordsClient.postSourceStorageRecordsMatching(recordMatchingDto)
      .compose(response -> {
        if (response.statusCode() == SC_OK) {
          return Future.succeededFuture(response.bodyAsJson(RecordsIdentifiersCollection.class));
        }
        String msg = format("Failed to request records identifiers by matching criteria, responseStatus: '%s', body: '%s', jobExecutionId: '%s', tenant: '%s'",
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
        }
        String msg = format("Failed to retrieve record by id: '%s', responseStatus: '%s', body: '%s', jobExecutionId: '%s', tenant: '%s'",
          recordId, response.statusCode(), response.bodyAsString(), payload.getJobExecutionId(), payload.getTenant());
        return Future.failedFuture(msg);
      });
  }

  /**
   * Populates payload with identifiers of external entities associated to the records
   * that meet criteria from match profile.
   * For example for MARC-BIB records it would be identifiers of the associated instances.
   * These external identifiers represent multiple match result returned by this handler
   * and can be used and deleted during further matching processing by other match handlers.
   *
   * @param recordsIdentifiersCollection identifiers collection to extract external identifiers
   * @param payload                      event payload to populate
   */
  private void populatePayloadWithExternalIdentifiers(RecordsIdentifiersCollection recordsIdentifiersCollection,
                                                      DataImportEventPayload payload) {
    List<String> externalEntityIds = recordsIdentifiersCollection.getIdentifiers()
      .stream()
      .map(RecordIdentifiersDto::getExternalId)
      .toList();

    payload.getContext().put(getMultiMatchResultKey(), Json.encode(externalEntityIds));
  }

  private Future<List<Record>> matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords(RecordMatchingDto recordMatchingDto, DataImportEventPayload payload,
                                                                                           Optional<Record> localMatchedRecord) {
    Context context = EventHandlingUtil.constructContext(payload.getTenant(), payload.getToken(), payload.getOkapiUrl());
    return consortiumService.getConsortiumConfiguration(context)
      .compose(consortiumConfigurationOptional -> {
        if (consortiumConfigurationOptional.isPresent() && !consortiumConfigurationOptional.get().getCentralTenantId().equals(payload.getTenant())) {
          LOG.debug("matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords:: Matching on centralTenant with id: {}",
            consortiumConfigurationOptional.get().getCentralTenantId());

          return retrieveMarcRecords(recordMatchingDto, consortiumConfigurationOptional.get().getCentralTenantId(), payload)
            .map(centralRecordOptional -> {
              centralRecordOptional.ifPresent(r -> payload.getContext().put(CENTRAL_TENANT_ID_KEY, consortiumConfigurationOptional.get().getCentralTenantId()));
              return Stream.concat(localMatchedRecord.stream(), centralRecordOptional.stream()).toList();
            });
        }
        return Future.succeededFuture(localMatchedRecord.stream().toList());
      });
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

  private boolean isEligibleMatchProfile(MatchProfile matchProfile) {
    return matchProfile.getIncomingRecordType() == EntityType.fromValue(getMarcType())
      && matchProfile.getExistingRecordType() == EntityType.fromValue(getMarcType());
  }

  /**
   * Prepares {@link DataImportEventPayload} for the further processing
   * based on the number of specified records in {@code records} list
   *
   * @param records records retrieved during matching processing
   * @param payload event payload to prepare
   * @return Future of {@link DataImportEventPayload} with result of matching
   */
  private Future<DataImportEventPayload> processSucceededResult(List<Record> records, DataImportEventPayload payload) {
    if (records.size() == 1) {
      payload.setEventType(matchedEventType.toString());
      payload.getContext().put(getMatchedMarcKey(), Json.encode(records.get(0)));
      LOG.debug("processSucceededResult:: Matched 1 record for jobExecutionId: '{}', tenantId: '{}'",
        payload.getJobExecutionId(), payload.getTenant());
      return Future.succeededFuture(payload);
    }
    if (records.size() > 1) {
      LOG.warn("processSucceededResult:: Matched multiple records, jobExecutionId: '{}', tenantId: '{}'",
        payload.getJobExecutionId(), payload.getTenant());
      return Future.failedFuture(new MatchingException(FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE));
    }
    if (payload.getContext().containsKey(getMultiMatchResultKey())) {
      LOG.info("processSucceededResult:: Multiple records were found which match criteria, jobExecutionId: '{}', tenantId: '{}'",
        payload.getJobExecutionId(), payload.getTenant());
      payload.setEventType(matchedEventType.toString());
      return Future.succeededFuture(payload);
    }
    LOG.info("processSucceededResult:: {}, jobExecutionId: '{}', tenantId: '{}'",
      RECORDS_NOT_FOUND_MESSAGE, payload.getJobExecutionId(), payload.getTenant());
    return Future.succeededFuture(payload);
  }

  private String getMatchedMarcKey() {
    return format(MATCH_RESULT_KEY_PREFIX, getMarcType());
  }

}
