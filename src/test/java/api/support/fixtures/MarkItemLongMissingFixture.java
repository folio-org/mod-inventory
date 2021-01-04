package api.support.fixtures;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static api.support.http.BusinessLogicInterfaceUrls.markLongMissingUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markMissingUrl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.inventory.support.http.client.ResponseHandler.any;

@RequiredArgsConstructor
public class MarkItemLongMissingFixture {
  private final OkapiHttpClient okapiClient;

  @SneakyThrows
  public Response markLongMissing(IndividualResource item) {
    return markLongMissing(item.getId());
  }

  @SneakyThrows
  public Response markLongMissing(UUID id) {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    okapiClient.post(markLongMissingUrl(id.toString()), null, any(future));

    return future.get(5, SECONDS);
  }
}
