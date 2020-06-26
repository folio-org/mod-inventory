package api.support.fixtures;

import static api.support.http.BusinessLogicInterfaceUrls.markWithdrawnUrl;
import static org.folio.inventory.support.http.client.ResponseHandler.any;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class ItemMarkWithdrawnFixture {
  private final OkapiHttpClient okapiClient;

  @SneakyThrows
  public Response markWithdrawn(IndividualResource item) {
    return markWithdrawn(item.getId());
  }

  @SneakyThrows
  public Response markWithdrawn(UUID id) {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    okapiClient.post(markWithdrawnUrl(id.toString()), null, any(future));

    return future.get(5, TimeUnit.SECONDS);
  }
}
