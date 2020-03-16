package org.folio.inventory.services;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;

import io.vertx.core.json.JsonObject;

public class InstanceRelationshipsService {
  private final MultipleRecordsFetchClient relationshipsFetchClient;
  private final MultipleRecordsFetchClient precedingSucceedingFetchClient;

  public InstanceRelationshipsService(CollectionResourceClient relationshipsClient,
    CollectionResourceClient precedingSucceedingTitleClient) {

    this.relationshipsFetchClient =
      createInstanceRelationshipsFetchClient(relationshipsClient);
    this.precedingSucceedingFetchClient =
      createPrecedingSucceedingTitlesFetchClient(precedingSucceedingTitleClient);
  }

  public CompletableFuture<List<JsonObject>> fetchInstanceRelationships(List<String> instanceIds) {
    return relationshipsFetchClient.find(instanceIds, this::fetchRelatedInstancesCql);
  }

  public CompletableFuture<List<JsonObject>> fetchInstancePrecedingSucceedingTitles(List<String> instanceIds) {
    return precedingSucceedingFetchClient.find(instanceIds, this::fetchPrecedingSucceedingTitleCql);
  }

  private CqlQuery fetchRelatedInstancesCql(List<String> instanceIds) {
    return CqlQuery.exactMatchAny("subInstanceId", instanceIds)
      .or(CqlQuery.exactMatchAny("superInstanceId", instanceIds));
  }

  private CqlQuery fetchPrecedingSucceedingTitleCql(List<String> instanceIds) {
    return CqlQuery.exactMatchAny("succeedingInstanceId", instanceIds)
      .or(CqlQuery.exactMatchAny("precedingInstanceId", instanceIds));
  }

  private MultipleRecordsFetchClient createInstanceRelationshipsFetchClient(
    CollectionResourceClient relationshipsClient) {

    return MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName("instanceRelationships")
      .withExpectedStatus(200)
      .withCollectionResourceClient(relationshipsClient)
      .build();
  }

  private MultipleRecordsFetchClient createPrecedingSucceedingTitlesFetchClient(
    CollectionResourceClient relationshipsClient) {

    return MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName("precedingSucceedingTitles")
      .withExpectedStatus(200)
      .withCollectionResourceClient(relationshipsClient)
      .build();
  }
}
