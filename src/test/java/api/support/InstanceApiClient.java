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

public class InstanceApiClient {
  public static JsonObject createInstance(
    OkapiHttpClient client,
    JsonObject newInstanceRequest)
    throws MalformedURLException,
      InterruptedException,
      ExecutionException,
      TimeoutException {

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    client.post(ApiRoot.instances(), newInstanceRequest,
      ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    assertThat("Failed to create instance",
      postResponse.getStatusCode(), is(201));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    client.get(postResponse.getLocation(), ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat("Failed to get instance",
      getResponse.getStatusCode(), is(200));

    return getResponse.getJson();
  }

  public static JsonObject updateInstance(
    OkapiHttpClient client, String instanceId, JsonObject updatedInstance)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {
    final String instancesUrl = ApiRoot.instances() + "/" + instanceId;

    CompletableFuture<Response> updateCompleted = new CompletableFuture<>();
    client.put(instancesUrl, updatedInstance,
      ResponseHandler.any(updateCompleted));

    Response updateResponse = updateCompleted.get(5, TimeUnit.SECONDS);

    assertThat("Failed to update instance",
      updateResponse.getStatusCode(), is(204));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();
    client.get(instancesUrl, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat("Failed to get instance",
      getResponse.getStatusCode(), is(200));

    return getResponse.getJson();
  }
}
