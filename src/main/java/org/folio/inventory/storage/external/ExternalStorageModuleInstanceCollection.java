package org.folio.inventory.storage.external;

import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.inventory.domain.Metadata;
import org.folio.inventory.domain.instances.Classification;
import org.folio.inventory.domain.instances.Contributor;
import org.folio.inventory.domain.instances.ElectronicAccess;
import org.folio.inventory.domain.instances.Identifier;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.Publication;
import org.folio.inventory.domain.instances.StatisticalCode;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

class ExternalStorageModuleInstanceCollection
  extends ExternalStorageModuleCollection<Instance>
  implements InstanceCollection {

  ExternalStorageModuleInstanceCollection(
    Vertx vertx,
    String baseAddress,
    String tenant,
    String token) {

    super(vertx, String.format("%s/%s", baseAddress, "instance-storage/instances"),
      tenant, token, "instances");
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
    instanceToSend.put(Instance.TITLE_KEY, instance.getTitle());
    instanceToSend.put(Instance.ALTERNATIVE_TITLES_KEY, instance.getAlternativeTitles());
    includeIfPresent(instanceToSend, Instance.EDITION_KEY, instance.getEdition());
    instanceToSend.put(Instance.SERIES_KEY, instance.getSeries());
    instanceToSend.put(Instance.IDENTIFIERS_KEY, instance.getIdentifiers());
    instanceToSend.put(Instance.CONTRIBUTORS_KEY, instance.getContributors());
    instanceToSend.put(Instance.SUBJECTS_KEY, instance.getSubjects());
    instanceToSend.put(Instance.CLASSIFICATIONS_KEY, instance.getClassifications());
    instanceToSend.put(Instance.PUBLICATION_KEY, instance.getPublication());
    instanceToSend.put(Instance.PUBLICATION_FREQUENCY_KEY, instance.getPublicationFrequency());
    instanceToSend.put(Instance.PUBLICATION_RANGE_KEY, instance.getPublicationRange());
    instanceToSend.put(Instance.ELECTRONIC_ACCESS_KEY, instance.getElectronicAccess());
    instanceToSend.put(Instance.URLS_KEY, instance.getUrls());
    includeIfPresent(instanceToSend, Instance.INSTANCE_TYPE_ID_KEY, instance.getInstanceTypeId());
    includeIfPresent(instanceToSend, Instance.INSTANCE_FORMAT_ID_KEY, instance.getInstanceFormatId());
    instanceToSend.put(Instance.PHYSICAL_DESCRIPTIONS_KEY, instance.getPhysicalDescriptions());
    instanceToSend.put(Instance.LANGUAGES_KEY, instance.getLanguages());
    instanceToSend.put(Instance.NOTES_KEY, instance.getNotes());
    instanceToSend.put(Instance.MODE_OF_ISSUANCE_ID_KEY, instance.getModeOfIssuanceId());
    instanceToSend.put(Instance.CATALOGED_DATE_KEY, instance.getCatalogedDate());
    instanceToSend.put(Instance.PREVIOUSLY_HELD_KEY, instance.getPreviouslyHeld());
    instanceToSend.put(Instance.STAFF_SUPPRESS_KEY, instance.getStaffSuppress());
    instanceToSend.put(Instance.DISCOVERY_SUPPRESS_KEY, instance.getDiscoverySuppress());
    instanceToSend.put(Instance.STATISTICAL_CODES_KEY, instance.getStatisticalCodes());
    includeIfPresent(instanceToSend, Instance.SOURCE_RECORD_FORMAT_KEY, instance.getSourceRecordFormat());
    instanceToSend.put(Instance.STATUS_ID_KEY, instance.getStatusId());
    instanceToSend.put(Instance.STATUS_UPDATED_DATE_KEY, instance.getStatusUpdatedDate());

    return instanceToSend;
  }

  @Override
  protected Instance mapFromJson(JsonObject instanceFromServer) {
    List<JsonObject> identifiers = toList(
      instanceFromServer.getJsonArray(Instance.IDENTIFIERS_KEY, new JsonArray()));

    List<Identifier> mappedIdentifiers = identifiers.stream()
      .map(it -> new Identifier(it))
      .collect(Collectors.toList());

    List<JsonObject> contributors = toList(
      instanceFromServer.getJsonArray(Instance.CONTRIBUTORS_KEY, new JsonArray()));

    List<Contributor> mappedContributors = contributors.stream()
      .map(it -> new Contributor(it))
      .collect(Collectors.toList());

    List<JsonObject> classifications = toList(
      instanceFromServer.getJsonArray(Instance.CLASSIFICATIONS_KEY, new JsonArray()));

    List<Classification> mappedClassifications = classifications.stream()
      .map(it -> new Classification(it))
      .collect(Collectors.toList());

    List<JsonObject> publications = toList(
      instanceFromServer.getJsonArray(Instance.PUBLICATION_KEY, new JsonArray()));

    List<Publication> mappedPublications = publications.stream()
      .map(it -> new Publication(it))
      .collect(Collectors.toList());

    List<JsonObject> electronicAccess = toList(
      instanceFromServer.getJsonArray(Instance.ELECTRONIC_ACCESS_KEY, new JsonArray()));

    List<ElectronicAccess> mappedElectronicAccess = electronicAccess.stream()
      .map(it -> new ElectronicAccess(it))
      .collect(Collectors.toList());

    List<JsonObject> statisticalCodes = toList(
      instanceFromServer.getJsonArray(Instance.STATISTICAL_CODES_KEY, new JsonArray()));

    List<StatisticalCode> mappedStatisticalCodes = statisticalCodes.stream()
      .map(it -> new StatisticalCode(it))
      .collect(Collectors.toList());


    JsonObject metadataJson = instanceFromServer.getJsonObject(Instance.METADATA_KEY);

    return new Instance(
      instanceFromServer.getString("id"),
      instanceFromServer.getString("hrid"),
      instanceFromServer.getString(Instance.SOURCE_KEY),
      instanceFromServer.getString(Instance.TITLE_KEY),
      instanceFromServer.getString(Instance.INSTANCE_TYPE_ID_KEY))
      .setAlternativeTitles(jsonArrayAsListOfStrings(instanceFromServer, Instance.ALTERNATIVE_TITLES_KEY))
      .setEdition(instanceFromServer.getString(Instance.EDITION_KEY))
      .setSeries(jsonArrayAsListOfStrings(instanceFromServer, Instance.SERIES_KEY))
      .setIdentifiers(mappedIdentifiers)
      .setContributors(mappedContributors)
      .setSubjects(jsonArrayAsListOfStrings(instanceFromServer, Instance.SUBJECTS_KEY))
      .setClassifications(mappedClassifications)
      .setPublication(mappedPublications)
      .setPublicationFrequency(jsonArrayAsListOfStrings(instanceFromServer, Instance.PUBLICATION_FREQUENCY_KEY))
      .setPublicationRange(jsonArrayAsListOfStrings(instanceFromServer, Instance.PUBLICATION_RANGE_KEY))
      .setElectronicAccess(mappedElectronicAccess)
      .setUrls(jsonArrayAsListOfStrings(instanceFromServer, Instance.URLS_KEY))
      .setInstanceFormatId(instanceFromServer.getString(Instance.INSTANCE_FORMAT_ID_KEY))
      .setPhysicalDescriptions(jsonArrayAsListOfStrings(instanceFromServer, Instance.PHYSICAL_DESCRIPTIONS_KEY))
      .setLanguages(jsonArrayAsListOfStrings(instanceFromServer, Instance.LANGUAGES_KEY))
      .setNotes(jsonArrayAsListOfStrings(instanceFromServer, Instance.NOTES_KEY))
      .setModeOfIssuanceId(instanceFromServer.getString(Instance.MODE_OF_ISSUANCE_ID_KEY))
      .setCatalogedDate(instanceFromServer.getString(Instance.CATALOGED_DATE_KEY))
      .setPreviouslyHeld(instanceFromServer.getBoolean(Instance.PREVIOUSLY_HELD_KEY))
      .setStaffSuppress(instanceFromServer.getBoolean(Instance.STAFF_SUPPRESS_KEY))
      .setDiscoverySuppress(instanceFromServer.getBoolean(Instance.DISCOVERY_SUPPRESS_KEY))
      .setStatisticalCodes(mappedStatisticalCodes)
      .setSourceRecordFormat(instanceFromServer.getString(Instance.SOURCE_RECORD_FORMAT_KEY))
      .setStatusId(instanceFromServer.getString(Instance.STATUS_ID_KEY))
      .setStatusUpdatedDate(instanceFromServer.getString(Instance.STATUS_UPDATED_DATE_KEY))
      .setMetadata(new Metadata(metadataJson));
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

}
