package api;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;
import support.ApiRoot;
import support.ApiTests;

public class AdminApiTest extends ApiTests {

  @Test
  void health() throws Exception {
    var response = okapiClient.get(ApiRoot.health()).toCompletableFuture().get(10, SECONDS);
    assertThat(response.statusCode(), is(200));
    assertThat(response.contentType(), is("text/plain"));
  }
}
