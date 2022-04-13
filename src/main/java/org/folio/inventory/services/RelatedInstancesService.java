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
  private final MultipleRecordsFetchClient fetchClient;

  public RelatedInstancesService(CollectionResourceClient fetchClient) {
    this.fetchClient = createFetchClient(fetchClient);
  }

  public CompletableFuture<List<JsonObject>> fetchRelatedInstances(List<String> instanceIds) {
    return fetchClient.find(instanceIds, this::fetchRelatedInstancesCql);
  }

  private CqlQuery fetchRelatedInstancesCql(List<String> instanceIds) {
    return CqlQuery.exactMatchAny(RelatedInstance.RELATED_INSTANCE_ID_KEY, instanceIds);
  }

  private MultipleRecordsFetchClient createFetchClient(
    CollectionResourceClient relatedInstancesClient) {

    return MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName(Instance.RELATED_INSTANCES_KEY)
      .withExpectedStatus(200)
      .withCollectionResourceClient(relatedInstancesClient)
      .build();
  }

}
