package api.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ControlledVocabularyPreparation {
  private final OkapiHttpClient client;
  private final URL controlledVocabularyRoot;
  private final String collectionWrapperProperty;

  public ControlledVocabularyPreparation(
    OkapiHttpClient client,
    URL controlledVocabularyRoot,
    String collectionWrapperProperty) {

    this.controlledVocabularyRoot = controlledVocabularyRoot;
    this.client = client;
    this.collectionWrapperProperty = collectionWrapperProperty;
  }

  public String createOrReferenceTerm(String name)
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    client.get(controlledVocabularyRoot, ResponseHandler.json(getCompleted));

    Response response = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat("Controlled vocabulary API unavailable",
      response.getStatusCode(), is(200));

    List<JsonObject> existingTerms = JsonArrayHelper.toList(
      response.getJson().getJsonArray(this.collectionWrapperProperty));

    if (doesNotExist(existingTerms, name)) {
      JsonObject vocabularyEntryRequest = new JsonObject().put("name", name);

      CompletableFuture<Response> postCompleted = new CompletableFuture<>();

      client.post(controlledVocabularyRoot,
        vocabularyEntryRequest, ResponseHandler.json(postCompleted));

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

      assertThat("Failed to create reference record",
        postResponse.getStatusCode(), is(201));

      return postResponse.getJson().getString("id");

    } else {
      return existingTerms.stream()
        .filter(it -> it.getString("name").equals(name))
        .findFirst().get().getString("id");
    }
  }

  private boolean doesNotExist(List<JsonObject> existingTerms, String name) {
    return existingTerms.stream()
      .noneMatch(it -> it.getString("name").equals(name));
  }
}
