package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.Contributor;
import org.folio.inventory.domain.Identifier;
import org.folio.inventory.domain.Instance;
import org.folio.inventory.domain.InstanceCollection;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.folio.inventory.domain.Instance.CONTRIBUTORS_PROPERTY_NAME;
import static org.folio.inventory.domain.Instance.ID_NAME;
import static org.folio.inventory.domain.Instance.IDENTIFIER_PROPERTY_NAME;
import static org.folio.inventory.domain.Instance.INSTANCE_TYPE_ID_NAME;
import static org.folio.inventory.domain.Instance.SOURCE_BINARY_BASE64_NAME;
import static org.folio.inventory.domain.Instance.SOURCE_BINARY_FORMAT_NAME;
import static org.folio.inventory.domain.Instance.SOURCE_NAME;
import static org.folio.inventory.domain.Instance.TITLE_PROPERTY_NAME;
import static org.folio.inventory.support.JsonArrayHelper.toList;

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
    instanceToSend.put(ID_NAME, instance.id != null
      ? instance.id
      : UUID.randomUUID().toString());

    instanceToSend.put(TITLE_PROPERTY_NAME, instance.title);
    includeIfPresent(instanceToSend, INSTANCE_TYPE_ID_NAME, instance.instanceTypeId);
    includeIfPresent(instanceToSend, SOURCE_NAME, instance.source);
    includeIfPresent(instanceToSend, SOURCE_BINARY_BASE64_NAME, instance.sourceBinaryBase64);
    includeIfPresent(instanceToSend, SOURCE_BINARY_FORMAT_NAME, instance.sourceBinaryFormat);
    instanceToSend.put(IDENTIFIER_PROPERTY_NAME, instance.identifiers);
    instanceToSend.put(CONTRIBUTORS_PROPERTY_NAME, instance.contributors);

    return instanceToSend;
  }

  @Override
  protected Instance mapFromJson(JsonObject instanceFromServer) {
    List<JsonObject> identifiers = toList(
      instanceFromServer.getJsonArray(IDENTIFIER_PROPERTY_NAME, new JsonArray()));

    List<Identifier> mappedIdentifiers = identifiers.stream()
      .map(it -> new Identifier(it.getString("identifierTypeId"), it.getString("value")))
      .collect(Collectors.toList());

    List<JsonObject> contributors = toList(
      instanceFromServer.getJsonArray(CONTRIBUTORS_PROPERTY_NAME, new JsonArray()));

    List<Contributor> mappedContributors = contributors.stream()
      .map(it -> new Contributor(it.getString("contributorNameTypeId"), it.getString("name")))
      .collect(Collectors.toList());

    return new Instance(
      instanceFromServer.getString(ID_NAME),
      instanceFromServer.getString(TITLE_PROPERTY_NAME),
      mappedIdentifiers,
      instanceFromServer.getString(SOURCE_NAME),
      instanceFromServer.getString(INSTANCE_TYPE_ID_NAME),
      mappedContributors,
      instanceFromServer.getString(SOURCE_BINARY_BASE64_NAME),
      instanceFromServer.getString(SOURCE_BINARY_FORMAT_NAME));
  }

  @Override
  protected String getId(Instance record) {
    return record.id;
  }
}
