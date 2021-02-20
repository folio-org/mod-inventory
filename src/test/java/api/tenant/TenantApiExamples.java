package api.tenant;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import api.support.ApiRoot;
import api.support.ApiTests;
import io.netty.handler.codec.http.HttpResponseStatus;

public class TenantApiExamples extends ApiTests {

  public TenantApiExamples() {
    super();
  }

  @Test
  public void shouldReturnNoContent()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    final var postCompleted = okapiClient.post(ApiRoot.tenant(), (String)null);
    Response postResponse = postCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(postResponse.getStatusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
  }
}
