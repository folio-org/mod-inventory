package org.folio.inventory.storage.external;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.user.Personal;
import org.folio.inventory.domain.user.User;
import org.folio.inventory.domain.user.UserCollection;

class ExternalStorageModuleUserCollection
  extends ExternalStorageModuleCollection<User>
  implements UserCollection {

  ExternalStorageModuleUserCollection(
    String baseAddress,
    String tenant,
    String token,
    String userId,
    String requestId,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "users"),
      tenant, token, userId, requestId, "users", client);
  }

  @Override
  protected JsonObject mapToRequest(User user) {
    Personal personal = user.personal();
    JsonObject personalJson = new JsonObject()
      .put("lastName", personal.lastName())
      .put("firstName", personal.firstName());

    return new JsonObject()
      .put("id", user.id())
      .put("personal", personalJson);
  }

  @Override
  protected User mapFromJson(JsonObject userJson) {
    JsonObject personalJson = userJson.getJsonObject("personal");
    Personal personal = new Personal(personalJson.getString("lastName"),
      personalJson.getString("firstName"));

    return new User(userJson.getString("id"), personal);
  }

  @Override
  protected String getId(User entity) {
    return entity.id();
  }
}
