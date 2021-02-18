package api.support.fixtures;

import static api.support.http.BusinessLogicInterfaceUrls.markInProcessNonRequestableUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markInProcessUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markIntellectualItemUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markLongMissingUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markMissingUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markRestrictedUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markUnavailableUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markUnknownUrl;
import static api.support.http.BusinessLogicInterfaceUrls.markWithdrawnUrl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.inventory.support.http.client.ResponseHandler.any;

import java.net.URL;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class MarkItemFixture {
  private final OkapiHttpClient okapiClient;

  public Response markInProcess(UUID id) {
    return markItem(markInProcessUrl(id.toString()));
  }

  public Response markIntellectualItem(UUID id) {
    return markItem(markIntellectualItemUrl(id.toString()));
  }

  public Response markLongMissing(UUID id) {
    return markItem(markLongMissingUrl(id.toString()));
  }

  public Response markMissing(UUID id) {
    return markItem(markMissingUrl(id.toString()));
  }

  public Response markRestricted(UUID id) {
    return markItem(markRestrictedUrl(id.toString()));
  }

  public Response markUnavailable(UUID id) {
    return markItem(markUnavailableUrl(id.toString()));
  }

  public Response markUnknown(UUID id) {
    return markItem(markUnknownUrl(id.toString()));
  }

  public Response markWithdrawn(UUID id) {
    return markItem(markWithdrawnUrl(id.toString()));
  }

  public Response markInProcessNonRequestable(UUID id) {
    return markItem(markInProcessNonRequestableUrl(id.toString()));
  }

  @SneakyThrows
  private Response markItem(URL url) {
    final var future = okapiClient.post(url, (String) null);

    return future.toCompletableFuture().get(5, SECONDS);
  }
}
