package api.support;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import io.vertx.core.json.JsonObject;

public class InstanceApiClient {
  public static JsonObject createInstance(
    OkapiHttpClient client,
    JsonObject newInstanceRequest)
    throws MalformedURLException,
      InterruptedException,
      ExecutionException,
      TimeoutException {

    final var postCompleted = client.post(ApiRoot.instances(), newInstanceRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat("Failed to create instance",
      postResponse.getStatusCode(), is(201));

    final var getCompleted = client.get(postResponse.getLocation());

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat("Failed to get instance",
      getResponse.getStatusCode(), is(200));

    return getResponse.getJson();
  }
}
