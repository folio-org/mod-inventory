package support;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import org.folio.inventory.support.http.client.OkapiHttpClient;

public final class InstanceApiClient {

  private InstanceApiClient() { }

  @SneakyThrows
  public static JsonObject createInstance(OkapiHttpClient client, JsonObject newInstanceRequest) {
    final var postCompleted = client.post(ApiRoot.instances(), newInstanceRequest);
    final var postResponse = postCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat("Failed to create instance", postResponse.statusCode(), is(201));

    final var getCompleted = client.get(postResponse.location());
    final var getResponse = getCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat("Failed to get instance", getResponse.statusCode(), is(200));
    return getResponse.getJson();
  }
}
