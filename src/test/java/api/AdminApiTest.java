package api;

import api.support.ApiRoot;
import api.support.ApiTests;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AdminApiTest extends ApiTests {

  @Test
  public void health() throws Exception {
    var response = okapiClient.get(ApiRoot.health()).toCompletableFuture().get(10, SECONDS);
    assertThat(response.getStatusCode(), is(200));
  }
}
