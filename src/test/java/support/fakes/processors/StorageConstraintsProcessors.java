package support.fakes.processors;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.instances.InstanceRelationship;
import org.folio.inventory.domain.instances.RelatedInstance;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.ValidationError;
import org.folio.util.StringUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static api.ApiTestSuite.createOkapiHttpClient;
import static api.support.http.StorageInterfaceUrls.instanceRelationshipTypeUrl;
import static api.support.http.StorageInterfaceUrls.instancesStorageUrl;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static org.folio.inventory.support.JsonArrayHelper.toList;

public final class StorageConstraintsProcessors {

  private StorageConstraintsProcessors() {
  }

  public static CompletableFuture<JsonObject> relatedInstancesConstraints(
    @SuppressWarnings("unused") JsonObject oldRelatedInstance, JsonObject newRelatedInstance) throws MalformedURLException {

    final RelatedInstance relatedInstance = new RelatedInstance(newRelatedInstance);

    return getInstanceByIds(relatedInstance.instanceId, relatedInstance.relatedInstanceId)
      .thenCombine(get(instanceRelationshipTypeUrl(
        "/" + relatedInstance.relatedInstanceTypeId)), (relationships, relationshipType) -> {

        if (relationshipType.getStatusCode() != 200) {
          throw new UnprocessableEntityException(new ValidationError(
            "Related instance type does not exist", "relatedInstanceTypeId",
            relatedInstance.relatedInstanceTypeId));
        }

        if (!relationships.containsKey(relatedInstance.instanceId)) {
          throw new UnprocessableEntityException(new ValidationError(
            "Instance does not exist", "instanceId", relatedInstance.instanceId));
        }

        if (!relationships.containsKey(relatedInstance.relatedInstanceId)) {
          throw new UnprocessableEntityException(new ValidationError(
            "Related instance does not exist", "relatedInstanceId", relatedInstance.relatedInstanceId));
        }

        return newRelatedInstance;
      });
  }

  public static CompletableFuture<JsonObject> instanceRelationshipsConstraints(
    @SuppressWarnings("unused") JsonObject oldRelationship, JsonObject newRelationship) throws MalformedURLException {

    final InstanceRelationship relationship = new InstanceRelationship(newRelationship);

    return getInstanceByIds(relationship.subInstanceId, relationship.superInstanceId)
      .thenCombine(get(instanceRelationshipTypeUrl(
        "/" + relationship.instanceRelationshipTypeId)), (relationships, relationshipType) -> {

        if (relationshipType.getStatusCode() != 200) {
          throw new UnprocessableEntityException(new ValidationError(
            "Relationship type does not exist", "instanceRelationshipTypeId",
            relationship.instanceRelationshipTypeId));
        }

        if (!relationships.containsKey(relationship.subInstanceId)) {
          throw new UnprocessableEntityException(new ValidationError(
            "Sub instance does not exist", "subInstanceId", relationship.subInstanceId));
        }

        if (!relationships.containsKey(relationship.superInstanceId)) {
          throw new UnprocessableEntityException(new ValidationError(
            "Super instance does not exist", "superInstanceId", relationship.superInstanceId));
        }

        return newRelationship;
      });
  }

  public static CompletableFuture<JsonObject> instancePrecedingSucceedingTitleConstraints(
    @SuppressWarnings("unused") JsonObject oldRelationship, JsonObject newRelationship) throws MalformedURLException {

    final PrecedingSucceedingTitle relationship = PrecedingSucceedingTitle.from(newRelationship);

    if (relationship.precedingInstanceId == null && relationship.succeedingInstanceId == null) {
      throw new UnprocessableEntityException(
        new ValidationError("Either preceding or succeeding id must be set",
          "succeedingInstanceId", null));
    }

    return getInstanceByIds(relationship.precedingInstanceId, relationship.succeedingInstanceId)
      .thenCompose(instancesMap -> {
        if (relationship.precedingInstanceId != null
          && !instancesMap.containsKey(relationship.precedingInstanceId)) {

          throw new UnprocessableEntityException(new ValidationError(
            "Preceding instance does not exist", "precedingInstanceId",
            relationship.precedingInstanceId));
        }

        if (relationship.succeedingInstanceId != null
          && !instancesMap.containsKey(relationship.succeedingInstanceId)) {

          throw new UnprocessableEntityException(new ValidationError(
            "Succeeding instance does not exist", "succeedingInstanceId",
            relationship.succeedingInstanceId));
        }

        return completedFuture(newRelationship);
      });
  }

  private static CompletableFuture<Map<String, JsonObject>> getInstanceByIds(String... ids)
    throws MalformedURLException {

    final String fullQuery = String.format("?query=id==(%s)&limit=%s",
      StringUtil.urlEncode(Arrays.stream(ids)
        .filter(Objects::nonNull)
        .map(id -> '"' + id + '"')
        .collect(Collectors.joining(" or "))),
      ids.length);

    return get(instancesStorageUrl(fullQuery))
      .thenApply(response -> toList(response.getJson().getJsonArray("instances")))
      .thenApply(instances -> instances.stream()
        .collect(Collectors.toMap(instance -> instance.getString("id"), identity())));
  }

  private static CompletableFuture<Response> get(URL url) throws MalformedURLException {
    return createOkapiHttpClient().get(url).toCompletableFuture();
  }
}
