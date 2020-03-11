package api;

import static api.support.fixtures.InstanceRequestExamples.nod;
import static org.folio.inventory.storage.external.MultipleRecordsFetchClient.builder;
import static org.folio.inventory.storage.external.MultipleRecordsFetchClient.forInstanceRelationships;
import static org.folio.inventory.storage.external.MultipleRecordsFetchClient.forPrecedingSucceedingTitles;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.util.function.Consumer;

import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import api.support.ApiTests;
import io.vertx.core.json.JsonObject;

@RunWith(PowerMockRunner.class)
@PrepareForTest(MultipleRecordsFetchClient.class)
public class InstanceRelationshipsTest extends ApiTests {

  @Test
  public void canForwardInstanceRelationshipsFetchFailure() throws Exception {
    final MultipleRecordsFetchClient precedingSucceedingTitlesClient =
      getRecordFetchClient("precedingSucceedingTitles");
    final MultipleRecordsFetchClient instanceRelationshipsClient =
      getRecordFetchClient("instanceRelationships");

    instancesClient.create(nod());
    instancesClient.create(nod());
    instancesClient.create(nod());

    // Inject mocked CollectionResource which will emulate failures
    mockStatic(MultipleRecordsFetchClient.class);
    when(forInstanceRelationships(any(CollectionResourceClient.class)))
      .thenReturn(instanceRelationshipsClient);
    when(forPrecedingSucceedingTitles(any(CollectionResourceClient.class)))
      .thenReturn(precedingSucceedingTitlesClient);

    Response response = instancesClient.attemptGetMany("title=\"\"", 3);

    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getContentType(), is("application/json"));
    assertThat(response.getJson().getString("message"),
      is("Internal server error"));
  }

  private MultipleRecordsFetchClient getRecordFetchClient(String collectionProperty) {
    return builder()
      .withCollectionPropertyName(collectionProperty)
      .withExpectedStatus(200)
      .withCollectionResourceClient(failOnGetManyCollectionResourceClient())
      .build();
  }

  private CollectionResourceClient failOnGetManyCollectionResourceClient() {
    return new CollectionResourceClient(null, null) {
      @Override
      public void getMany(String cqlQuery, Integer pageLimit, Integer pageOffset,
        Consumer<Response> responseHandler) {

        final JsonObject error = new JsonObject().put("message", "Internal server error");
        Response response = new Response(500, error.toString(), "application/json", "");
        responseHandler.accept(response);
      }
    };
  }
}
