package api.support;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.util.PercentCodec;

public class Preparation {
  private final OkapiHttpClient client;

  public Preparation(OkapiHttpClient client) {
    this.client = client;
  }

  public void deleteInstances()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    deleteAll(ApiRoot.instances());
  }

  public void deleteItems()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    deleteAll(ApiRoot.items());
  }

  private void deleteAll(URL root)
    throws InterruptedException, ExecutionException, TimeoutException {

    final var deleteCompleted = client.delete(
        root + "?query=" + PercentCodec.encode("cql.allRecords=1"));

    Response response = deleteCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat("Failed to delete all records", response.getStatusCode(), is(204));
  }
}
