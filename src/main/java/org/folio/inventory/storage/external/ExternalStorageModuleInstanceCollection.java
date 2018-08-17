package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import org.folio.inventory.domain.instances.Contributor;
import org.folio.inventory.domain.instances.Identifier;
import org.folio.inventory.domain.instances.Classification;
import org.folio.inventory.domain.instances.Publication;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.folio.inventory.domain.Metadata;

import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;

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
    instanceToSend.put(Instance.URLS_KEY, instance.getUrls());
    includeIfPresent(instanceToSend, Instance.INSTANCE_TYPE_ID_KEY, instance.getInstanceTypeId());
    includeIfPresent(instanceToSend, Instance.INSTANCE_FORMAT_ID_KEY, instance.getInstanceFormatId());
    instanceToSend.put(Instance.PHYSICAL_DESCRIPTIONS_KEY, instance.getPhysicalDescriptions());
    instanceToSend.put(Instance.LANGUAGES_KEY, instance.getLanguages());
    instanceToSend.put(Instance.NOTES_KEY, instance.getNotes());
    includeIfPresent(instanceToSend, Instance.SOURCE_RECORD_FORMAT_KEY, instance.getSourceRecordFormat());

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

    JsonObject metadataJson = instanceFromServer.getJsonObject(Instance.METADATA_KEY);

    return new Instance(
      instanceFromServer.getString("id"),
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
      .setUrls(jsonArrayAsListOfStrings(instanceFromServer, Instance.URLS_KEY))
      .setInstanceFormatId(instanceFromServer.getString(Instance.INSTANCE_FORMAT_ID_KEY))
      .setPhysicalDescriptions(jsonArrayAsListOfStrings(instanceFromServer, Instance.PHYSICAL_DESCRIPTIONS_KEY))
      .setLanguages(jsonArrayAsListOfStrings(instanceFromServer, Instance.LANGUAGES_KEY))
      .setNotes(jsonArrayAsListOfStrings(instanceFromServer, Instance.NOTES_KEY))
      .setSourceRecordFormat(instanceFromServer.getString(Instance.SOURCE_RECORD_FORMAT_KEY))
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
