package api.tenant;

import api.support.ApiRoot;
import api.support.ApiTests;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TenantApiExamples extends ApiTests {

  public TenantApiExamples() throws MalformedURLException {
    super();
  }

  @Test
  public void shouldReturnNoContent()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.tenant(), null, ResponseHandler.any(postCompleted));
    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postResponse.getStatusCode(), is(HttpResponseStatus.NO_CONTENT.code()));
  }
}
