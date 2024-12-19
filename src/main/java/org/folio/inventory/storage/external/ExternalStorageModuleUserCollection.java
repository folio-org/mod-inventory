package org.folio.inventory.storage.external;

import org.folio.inventory.domain.user.Personal;
import org.folio.inventory.domain.user.User;
import org.folio.inventory.domain.user.UserCollection;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

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
  protected User mapFromJson(JsonObject userJson) {
    JsonObject personalJson = userJson.getJsonObject("personal");
    Personal personal = new Personal(personalJson.getString("lastName"),
                                     personalJson.getString("firstName"));

    return new User(userJson.getString("id"), personal);
  }

  @Override
  protected String getId(User record) {
    return record.getId();
  }

  @Override
  protected JsonObject mapToRequest(User user) {
    Personal personal = user.getPersonal();
    JsonObject personalJson = new JsonObject()
      .put("lastName", personal.getLastName())
      .put("firstName", personal.getFirstName());

    return new JsonObject()
      .put("id", user.getId())
      .put("personal", personalJson);
  }
}
