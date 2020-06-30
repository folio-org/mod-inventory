package api.support.fixtures;

import static api.support.http.BusinessLogicInterfaceUrls.markMissingUrl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.inventory.support.http.client.ResponseHandler.any;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class MarkItemMissingFixture {
  private final OkapiHttpClient okapiClient;

  @SneakyThrows
  public Response markMissing(IndividualResource item) {
    return markMissing(item.getId());
  }

  @SneakyThrows
  public Response markMissing(UUID id) {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    okapiClient.post(markMissingUrl(id.toString()), null, any(future));

    return future.get(5, SECONDS);
  }
}
