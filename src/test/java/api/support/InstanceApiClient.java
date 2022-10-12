package api.support;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.folio.inventory.support.http.client.OkapiHttpClient;

import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;

public class InstanceApiClient {
  private InstanceApiClient() { }

  @SneakyThrows
  public static JsonObject createInstance(
    OkapiHttpClient client,
    JsonObject newInstanceRequest) {

    final var postCompleted = client.post(ApiRoot.instances(), newInstanceRequest);

    final var postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat("Failed to create instance",
      postResponse.getStatusCode(), is(201));

    final var getCompleted = client.get(postResponse.getLocation());

    final var getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat("Failed to get instance",
      getResponse.getStatusCode(), is(200));

    return getResponse.getJson();
  }
}
