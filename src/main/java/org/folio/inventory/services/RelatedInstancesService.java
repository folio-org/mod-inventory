package org.folio.inventory.services;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.RelatedInstance;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;

import io.vertx.core.json.JsonObject;

public class RelatedInstancesService {
  private final MultipleRecordsFetchClient relatedInstancesFetchClient;

  public RelatedInstancesService(CollectionResourceClient relatedInstancesClient) {
    this.relatedInstancesFetchClient =
      createRelatedInstancesFetchClient(relatedInstancesClient);
  }

  public CompletableFuture<List<JsonObject>> fetchRelatedInstances(List<String> instanceIds) {
    return relatedInstancesFetchClient.find(instanceIds, this::fetchRelatedInstancesCql);
  }

  private CqlQuery fetchRelatedInstancesCql(List<String> instanceIds) {
    return CqlQuery.exactMatchAny(RelatedInstance.INSTANCE_ID_KEY, instanceIds)
      .or(CqlQuery.exactMatchAny(RelatedInstance.RELATED_INSTANCE_ID_KEY, instanceIds));
  }

  private MultipleRecordsFetchClient createRelatedInstancesFetchClient(
    CollectionResourceClient relatedInstancesClient) {

    return MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName(Instance.RELATED_INSTANCES_KEY)
      .withExpectedStatus(200)
      .withCollectionResourceClient(relatedInstancesClient)
      .build();
  }
}
