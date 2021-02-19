package org.folio.inventory.storage.external;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;

import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.BatchResult;
import org.folio.inventory.domain.Metadata;
import org.folio.inventory.domain.instances.AlternativeTitle;
import org.folio.inventory.domain.instances.Classification;
import org.folio.inventory.domain.instances.Contributor;
import org.folio.inventory.domain.instances.Identifier;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.Note;
import org.folio.inventory.domain.instances.Publication;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;
import org.folio.inventory.support.http.client.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;

class ExternalStorageModuleInstanceCollection
  extends ExternalStorageModuleCollection<Instance>
  implements InstanceCollection {

  private final String batchAddress;

  ExternalStorageModuleInstanceCollection(
    Vertx vertx,
    String baseAddress,
    String tenant,
    String token,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "instance-storage/instances"),
      tenant, token, "instances", client);
    batchAddress = String.format("%s/%s", baseAddress, "instance-storage/batch/instances");
  }

  @Override
  protected JsonObject mapToRequest(Instance instance) {
    JsonObject instanceToSend = new JsonObject();
    //TODO: Review if this shouldn't be defaulting here
    instanceToSend.put("id", instance.getId() != null
      ? instance.getId()
      : UUID.randomUUID().toString());
    instanceToSend.put(Instance.HRID_KEY, instance.getHrid());
    includeIfPresent(instanceToSend, Instance.SOURCE_KEY, instance.getSource());
    instanceToSend.put(Instance.MATCH_KEY_KEY, instance.getMatchKey());
    instanceToSend.put(Instance.TITLE_KEY, instance.getTitle());
    instanceToSend.put(Instance.INDEX_TITLE_KEY, instance.getIndexTitle());
    instanceToSend.put(Instance.ALTERNATIVE_TITLES_KEY, instance.getAlternativeTitles());
    instanceToSend.put(Instance.EDITIONS_KEY, instance.getEditions());
    instanceToSend.put(Instance.SERIES_KEY, instance.getSeries());
    instanceToSend.put(Instance.IDENTIFIERS_KEY, instance.getIdentifiers());
    instanceToSend.put(Instance.CONTRIBUTORS_KEY, instance.getContributors());
    instanceToSend.put(Instance.SUBJECTS_KEY, instance.getSubjects());
    instanceToSend.put(Instance.CLASSIFICATIONS_KEY, instance.getClassifications());
    instanceToSend.put(Instance.PUBLICATION_KEY, instance.getPublication());
    instanceToSend.put(Instance.PUBLICATION_FREQUENCY_KEY, instance.getPublicationFrequency());
    instanceToSend.put(Instance.PUBLICATION_RANGE_KEY, instance.getPublicationRange());
    instanceToSend.put(Instance.ELECTRONIC_ACCESS_KEY, instance.getElectronicAccess());
    includeIfPresent(instanceToSend, Instance.INSTANCE_TYPE_ID_KEY, instance.getInstanceTypeId());
    instanceToSend.put(Instance.INSTANCE_FORMAT_IDS_KEY, instance.getInstanceFormatIds());
    instanceToSend.put(Instance.PHYSICAL_DESCRIPTIONS_KEY, instance.getPhysicalDescriptions());
    instanceToSend.put(Instance.LANGUAGES_KEY, instance.getLanguages());
    instanceToSend.put(Instance.NOTES_KEY, instance.getNotes());
    instanceToSend.put(Instance.MODE_OF_ISSUANCE_ID_KEY, instance.getModeOfIssuanceId());
    instanceToSend.put(Instance.CATALOGED_DATE_KEY, instance.getCatalogedDate());
    instanceToSend.put(Instance.PREVIOUSLY_HELD_KEY, instance.getPreviouslyHeld());
    instanceToSend.put(Instance.STAFF_SUPPRESS_KEY, instance.getStaffSuppress());
    instanceToSend.put(Instance.DISCOVERY_SUPPRESS_KEY, instance.getDiscoverySuppress());
    instanceToSend.put(Instance.STATISTICAL_CODE_IDS_KEY, instance.getStatisticalCodeIds());
    includeIfPresent(instanceToSend, Instance.SOURCE_RECORD_FORMAT_KEY, instance.getSourceRecordFormat());
    instanceToSend.put(Instance.STATUS_ID_KEY, instance.getStatusId());
    instanceToSend.put(Instance.STATUS_UPDATED_DATE_KEY, instance.getStatusUpdatedDate());
    instanceToSend.put(Instance.TAGS_KEY, new JsonObject().put(Instance.TAG_LIST_KEY, new JsonArray(instance.getTags() == null ? Collections.emptyList() : instance.getTags())));
    instanceToSend.put(Instance.NATURE_OF_CONTENT_TERM_IDS_KEY, instance.getNatureOfContentTermIds());

    return instanceToSend;
  }

  @Override
  protected Instance mapFromJson(JsonObject instanceFromServer) {
    List<JsonObject> identifiers = toList(
      instanceFromServer.getJsonArray(Instance.IDENTIFIERS_KEY, new JsonArray()));

    List<Identifier> mappedIdentifiers = identifiers.stream()
      .map(Identifier::new)
      .collect(Collectors.toList());

    List<JsonObject> alternativeTitles = toList(
      instanceFromServer.getJsonArray(Instance.ALTERNATIVE_TITLES_KEY, new JsonArray()));

    List<AlternativeTitle> mappedAlternativeTitles = alternativeTitles.stream()
      .map(AlternativeTitle::new)
      .collect(Collectors.toList());

    List<JsonObject> contributors = toList(
      instanceFromServer.getJsonArray(Instance.CONTRIBUTORS_KEY, new JsonArray()));

    List<Contributor> mappedContributors = contributors.stream()
      .map(Contributor::new)
      .collect(Collectors.toList());

    List<JsonObject> classifications = toList(
      instanceFromServer.getJsonArray(Instance.CLASSIFICATIONS_KEY, new JsonArray()));

    List<Classification> mappedClassifications = classifications.stream()
      .map(Classification::new)
      .collect(Collectors.toList());

    List<JsonObject> publications = toList(
      instanceFromServer.getJsonArray(Instance.PUBLICATION_KEY, new JsonArray()));

    List<Publication> mappedPublications = publications.stream()
      .map(Publication::new)
      .collect(Collectors.toList());

    List<JsonObject> electronicAccess = toList(
      instanceFromServer.getJsonArray(Instance.ELECTRONIC_ACCESS_KEY, new JsonArray()));

    List<ElectronicAccess> mappedElectronicAccess = electronicAccess.stream()
      .map(ElectronicAccess::new)
      .collect(Collectors.toList());

    List<JsonObject> notes = toList(
      instanceFromServer.getJsonArray(Instance.NOTES_KEY, new JsonArray()));

    List<Note> mappedNotes = notes.stream()
      .map(Note::new)
      .collect(Collectors.toList());

    List<String> tags = instanceFromServer.containsKey(Instance.TAGS_KEY)
      ? jsonArrayAsListOfStrings(instanceFromServer.getJsonObject(Instance.TAGS_KEY), Instance.TAG_LIST_KEY)
      : new ArrayList<>();

    JsonObject metadataJson = instanceFromServer.getJsonObject(Instance.METADATA_KEY);

    return new Instance(
      instanceFromServer.getString("id"),
      instanceFromServer.getString("hrid"),
      instanceFromServer.getString(Instance.SOURCE_KEY),
      instanceFromServer.getString(Instance.TITLE_KEY),
      instanceFromServer.getString(Instance.INSTANCE_TYPE_ID_KEY))
      .setMatchKey(instanceFromServer.getString(Instance.MATCH_KEY_KEY))
      .setIndexTitle(instanceFromServer.getString(Instance.INDEX_TITLE_KEY))
      .setAlternativeTitles(mappedAlternativeTitles)
      .setEditions(jsonArrayAsListOfStrings(instanceFromServer, Instance.EDITIONS_KEY))
      .setSeries(jsonArrayAsListOfStrings(instanceFromServer, Instance.SERIES_KEY))
      .setIdentifiers(mappedIdentifiers)
      .setContributors(mappedContributors)
      .setSubjects(jsonArrayAsListOfStrings(instanceFromServer, Instance.SUBJECTS_KEY))
      .setClassifications(mappedClassifications)
      .setPublication(mappedPublications)
      .setPublicationFrequency(jsonArrayAsListOfStrings(instanceFromServer, Instance.PUBLICATION_FREQUENCY_KEY))
      .setPublicationRange(jsonArrayAsListOfStrings(instanceFromServer, Instance.PUBLICATION_RANGE_KEY))
      .setElectronicAccess(mappedElectronicAccess)
      .setInstanceFormatIds(jsonArrayAsListOfStrings(instanceFromServer, Instance.INSTANCE_FORMAT_IDS_KEY))
      .setPhysicalDescriptions(jsonArrayAsListOfStrings(instanceFromServer, Instance.PHYSICAL_DESCRIPTIONS_KEY))
      .setLanguages(jsonArrayAsListOfStrings(instanceFromServer, Instance.LANGUAGES_KEY))
      .setNotes(mappedNotes)
      .setModeOfIssuanceId(instanceFromServer.getString(Instance.MODE_OF_ISSUANCE_ID_KEY))
      .setCatalogedDate(instanceFromServer.getString(Instance.CATALOGED_DATE_KEY))
      .setPreviouslyHeld(instanceFromServer.getBoolean(Instance.PREVIOUSLY_HELD_KEY))
      .setStaffSuppress(instanceFromServer.getBoolean(Instance.STAFF_SUPPRESS_KEY))
      .setDiscoverySuppress(instanceFromServer.getBoolean(Instance.DISCOVERY_SUPPRESS_KEY))
      .setStatisticalCodeIds(jsonArrayAsListOfStrings(instanceFromServer, Instance.STATISTICAL_CODE_IDS_KEY))
      .setSourceRecordFormat(instanceFromServer.getString(Instance.SOURCE_RECORD_FORMAT_KEY))
      .setStatusId(instanceFromServer.getString(Instance.STATUS_ID_KEY))
      .setStatusUpdatedDate(instanceFromServer.getString(Instance.STATUS_UPDATED_DATE_KEY))
      .setMetadata(new Metadata(metadataJson))
      .setTags(tags)
      .setNatureOfContentTermIds(jsonArrayAsListOfStrings(instanceFromServer, Instance.NATURE_OF_CONTENT_TERM_IDS_KEY));
  }

  @Override
  protected String getId(Instance record) {
    return record.getId();
  }

  private List<String> jsonArrayAsListOfStrings(JsonObject source, String propertyName) {
    return source.containsKey(propertyName)
      ? toListOfStrings(source.getJsonArray(propertyName))
      : new ArrayList<>();
  }

  @Override
  public void addBatch(List<Instance> items,
    Consumer<Success<BatchResult<Instance>>> resultCallback, Consumer<Failure> failureCallback) {

    List<JsonObject> jsonList = items.stream()
      .map(this::mapToRequest)
      .collect(Collectors.toList());

    JsonObject batchRequest = new JsonObject()
      .put("instances", new JsonArray(jsonList))
      .put("totalRecords", jsonList.size());

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.postAbs(batchAddress));

    request.sendJsonObject(batchRequest, futureResponse::complete);

    futureResponse
      .thenCompose(this::mapAsyncResultToCompletionStage)
      .thenAccept(response -> {
        if (isBatchResponse(response)) {
          JsonObject batchResponse = response.getJson();
          JsonArray createdInstances = batchResponse.getJsonArray("instances");

          List<Instance> instancesList = new ArrayList<>();
          for (int i = 0; i < createdInstances.size(); i++) {
            instancesList.add(mapFromJson(createdInstances.getJsonObject(i)));
          }
          BatchResult<Instance> batchResult = new BatchResult<>();
          batchResult.setBatchItems(instancesList);
          batchResult.setErrorMessages(batchResponse.getJsonArray("errorMessages").getList());

          resultCallback.accept(new Success<>(batchResult));
        } else {
          failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
        }
      });
  }

  private boolean isBatchResponse(Response response) {
    int statusCode = response.getStatusCode();
    String contentHeaderValue = response.getContentType();
    return statusCode == SC_CREATED
      || (statusCode == SC_INTERNAL_SERVER_ERROR && APPLICATION_JSON.equals(contentHeaderValue));
  }
}
