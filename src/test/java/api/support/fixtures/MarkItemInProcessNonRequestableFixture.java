package api.support.fixtures;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static api.support.http.BusinessLogicInterfaceUrls.markInProcessNonRequestableUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markInProcessUrl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.inventory.support.http.client.ResponseHandler.any;

@RequiredArgsConstructor
public class MarkItemInProcessNonRequestableFixture {
  private final OkapiHttpClient okapiClient;

  @SneakyThrows
  public Response markInProcessNonRequestable(UUID id) {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    okapiClient.post(markInProcessNonRequestableUrl(id.toString()), null, any(future));

    return future.get(5, SECONDS);
  }
}
