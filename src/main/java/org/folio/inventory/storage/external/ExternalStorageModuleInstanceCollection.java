package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import org.folio.inventory.domain.Contributor;
import org.folio.inventory.domain.Identifier;
import org.folio.inventory.domain.Classification;
import org.folio.inventory.domain.Publication;
import org.folio.inventory.domain.Instance;
import org.folio.inventory.domain.InstanceCollection;

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
    instanceToSend.put("id", instance.id != null
      ? instance.id
      : UUID.randomUUID().toString());
    includeIfPresent(instanceToSend, Instance.SOURCE, instance.source);
    instanceToSend.put(Instance.TITLE, instance.title);
    instanceToSend.put(Instance.ALTERNATIVE_TITLES, instance.alternativeTitles);
    includeIfPresent(instanceToSend, Instance.EDITION, instance.edition);
    instanceToSend.put(Instance.SERIES, instance.series);
    instanceToSend.put(Instance.IDENTIFIERS, instance.identifiers);
    instanceToSend.put(Instance.CONTRIBUTORS, instance.contributors);
    instanceToSend.put(Instance.SUBJECTS, instance.subjects);
    instanceToSend.put(Instance.CLASSIFICATIONS, instance.classifications);
    instanceToSend.put(Instance.PUBLICATION, instance.publication);
    instanceToSend.put(Instance.URLS, instance.urls);
    includeIfPresent(instanceToSend, Instance.INSTANCE_TYPE_ID, instance.instanceTypeId);
    includeIfPresent(instanceToSend, Instance.INSTANCE_FORMAT_ID, instance.instanceFormatId);
    instanceToSend.put(Instance.PHYSICAL_DESCRIPTIONS, instance.physicalDescriptions);
    instanceToSend.put(Instance.LANGUAGES, instance.languages);
    instanceToSend.put(Instance.NOTES, instance.notes);
    includeIfPresent(instanceToSend, Instance.SOURCE_RECORD_FORMAT, instance.sourceRecordFormat);

    return instanceToSend;
  }

  @Override
  protected Instance mapFromJson(JsonObject instanceFromServer) {
    List<JsonObject> identifiers = toList(
      instanceFromServer.getJsonArray(Instance.IDENTIFIERS, new JsonArray()));

    List<Identifier> mappedIdentifiers = identifiers.stream()
      .map(it -> new Identifier(it))
      .collect(Collectors.toList());

    List<JsonObject> contributors = toList(
      instanceFromServer.getJsonArray(Instance.CONTRIBUTORS, new JsonArray()));

    List<Contributor> mappedContributors = contributors.stream()
      .map(it -> new Contributor(it))
      .collect(Collectors.toList());

    List<JsonObject> classifications = toList(
      instanceFromServer.getJsonArray(Instance.CLASSIFICATIONS, new JsonArray()));

    List<Classification> mappedClassifications = classifications.stream()
      .map(it -> new Classification(it))
      .collect(Collectors.toList());

    List<JsonObject> publications = toList(
      instanceFromServer.getJsonArray(Instance.PUBLICATION, new JsonArray()));

    List<Publication> mappedPublications = publications.stream()
      .map(it -> new Publication(it))
      .collect(Collectors.toList());

    JsonObject metadataJson = instanceFromServer.getJsonObject(Instance.METADATA);

    Instance response = new Instance(
      instanceFromServer.getString("id"),
      instanceFromServer.getString(Instance.SOURCE),
      instanceFromServer.getString(Instance.TITLE),
      instanceFromServer.getString(Instance.INSTANCE_TYPE_ID))
      .setAlternativeTitles(jsonArrayAsListOfStrings(instanceFromServer, Instance.ALTERNATIVE_TITLES))
      .setEdition(instanceFromServer.getString(Instance.EDITION))
      .setSeries(jsonArrayAsListOfStrings(instanceFromServer, Instance.SERIES))
      .setIdentifiers(mappedIdentifiers)
      .setContributors(mappedContributors)
      .setSubjects(jsonArrayAsListOfStrings(instanceFromServer, Instance.SUBJECTS))
      .setClassifications(mappedClassifications)
      .setPublication(mappedPublications)
      .setUrls(jsonArrayAsListOfStrings(instanceFromServer, Instance.URLS))
      .setInstanceFormatId(instanceFromServer.getString(Instance.INSTANCE_FORMAT_ID))
      .setPhysicalDescriptions(jsonArrayAsListOfStrings(instanceFromServer, Instance.PHYSICAL_DESCRIPTIONS))
      .setLanguages(jsonArrayAsListOfStrings(instanceFromServer, Instance.LANGUAGES))
      .setNotes(jsonArrayAsListOfStrings(instanceFromServer, Instance.NOTES))
      .setSourceRecordFormat(instanceFromServer.getString(Instance.SOURCE_RECORD_FORMAT))
      .setMetadata(new Metadata(metadataJson));
    return response;
  }

  @Override
  protected String getId(Instance record) {
    return record.id;
  }

  private List<String> jsonArrayAsListOfStrings(JsonObject source, String propertyName) {
    return source.containsKey(propertyName)
      ? toListOfStrings(source.getJsonArray(propertyName))
      : new ArrayList<>();
  }

}
