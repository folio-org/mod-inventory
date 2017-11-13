package api.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;

import java.net.MalformedURLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ItemApiClient {
  public static JsonObject createItem(
    OkapiHttpClient client,
    JsonObject newItemRequest)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    client.post(ApiRoot.items(), newItemRequest,
      ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    assertThat("Failed to create item",
      postResponse.getStatusCode(), is(201));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    client.get(postResponse.getLocation(), ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat("Failed to get item",
      getResponse.getStatusCode(), is(200));

    return getResponse.getJson();
  }
}
