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

    instanceToSend.put("title", instance.title);
    includeIfPresent(instanceToSend, "instanceTypeId", instance.instanceTypeId);
    includeIfPresent(instanceToSend, "source", instance.source);
    instanceToSend.put("identifiers", instance.identifiers);
    instanceToSend.put("contributors", instance.contributors);

    return instanceToSend;
  }

  @Override
  protected Instance mapFromJson(JsonObject instanceFromServer) {
    List<JsonObject> identifiers = toList(
      instanceFromServer.getJsonArray("identifiers", new JsonArray()));

    List<Identifier> mappedIdentifiers = identifiers.stream()
      .map(it -> new Identifier(it.getString("identifierTypeId"), it.getString("value")))
      .collect(Collectors.toList());

    List<JsonObject> contributors = toList(
      instanceFromServer.getJsonArray("contributors", new JsonArray()));

    List<Contributor> mappedContributors = contributors.stream()
      .map(it -> new Contributor(it.getString("contributorNameTypeId"), it.getString("name"), it.getString("contributorTypeId"), it.getString("contributorTypeText")))
      .collect(Collectors.toList());

    List<JsonObject> classifications = toList(
      instanceFromServer.getJsonArray("classifications", new JsonArray()));

    List<Classification> mappedClassifications = classifications.stream()
      .map(it -> new Classification(it.getString("classificationTypeId"), it.getString("classificationNumber")))
      .collect(Collectors.toList());

    List<JsonObject> publications = toList(
      instanceFromServer.getJsonArray("publication", new JsonArray()));

    List<Publication> mappedPublications = publications.stream()
      .map(it -> new Publication(it.getString("publisher"), it.getString("place"), it.getString("dateOfPublication")))
      .collect(Collectors.toList());

    return new Instance(
      instanceFromServer.getString("id"),
      instanceFromServer.getString("source"),
      instanceFromServer.getString("title"),
      instanceFromServer.getString("instanceTypeId"))
      .setAlternativeTitles(jsonArrayAsListOfStrings(instanceFromServer, "alternativeTitles"))
      .setEdition(instanceFromServer.getString("edition"))
      .setSeries(jsonArrayAsListOfStrings(instanceFromServer, "series"))
      .setIdentifiers(mappedIdentifiers)
      .setContributors(mappedContributors)
      .setSubjects(jsonArrayAsListOfStrings(instanceFromServer, "subjects"))
      .setClassifications(mappedClassifications)
      .setPublication(mappedPublications)
      .setUrls(jsonArrayAsListOfStrings(instanceFromServer, "urls"))
      .setInstanceFormatId(instanceFromServer.getString("instanceFormatId"))
      .setPhysicalDescriptions(jsonArrayAsListOfStrings(instanceFromServer, "physicalDescriptions"))
      .setLanguages(jsonArrayAsListOfStrings(instanceFromServer, "languages"))
      .setNotes(jsonArrayAsListOfStrings(instanceFromServer, "notes"))
      .setSourceRecordFormat(instanceFromServer.getString("sourceRecordFormat"));
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
