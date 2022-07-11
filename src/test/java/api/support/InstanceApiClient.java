package api.support;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import api.InstancesApiExamples;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import io.vertx.core.json.JsonObject;

public class InstanceApiClient {
  private static final Logger LOGGER = LogManager.getLogger(InstanceApiClient.class);

  public static JsonObject createInstance(
    OkapiHttpClient client,
    JsonObject newInstanceRequest)
    throws MalformedURLException,
      InterruptedException,
      ExecutionException,
      TimeoutException {

    final var postCompleted = client.post(ApiRoot.instances(), newInstanceRequest);

    Response postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    LOGGER.error("!!!! body:", postResponse.getBody());
    LOGGER.error("!!!! json:", postResponse.getJson());

    assertThat("Failed to create instance",
      postResponse.getStatusCode(), is(201));

    final var getCompleted = client.get(postResponse.getLocation());

    Response getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat("Failed to get instance",
      getResponse.getStatusCode(), is(200));

    return getResponse.getJson();
  }
}
