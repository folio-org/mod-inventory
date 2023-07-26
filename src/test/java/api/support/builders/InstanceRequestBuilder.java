package api.support.builders;

import api.ApiTestSuite;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static api.support.InstanceSamples.INSTANCE_SOURCE;

public class InstanceRequestBuilder extends AbstractBuilder {
  private final String title;
  private final String contributor;
  private final UUID id;

  public InstanceRequestBuilder(String title, String contributor) {
    id = UUID.randomUUID();
    this.title = title;
    this.contributor = contributor;
  }

  public InstanceRequestBuilder(UUID id, String title, String contributor) {
    this.id = id;
    this.title = title;
    this.contributor = contributor;
  }

  @Override
  public JsonObject create() {
    return new JsonObject()
      .put("id", id.toString())
      .put("title", title)
      .put("contributors", new JsonArray().add(new JsonObject()
        .put("contributorNameTypeId", ApiTestSuite.getPersonalContributorNameType())
        .put("name", contributor)))
      .put("source", INSTANCE_SOURCE)
      .put("instanceTypeId", ApiTestSuite.getTextInstanceType());
  }

  public InstanceRequestBuilder withId(UUID id) {
    return new InstanceRequestBuilder(
      id,
      this.title,
      this.contributor);
  }

  public InstanceRequestBuilder withTitle(String title) {
    return new InstanceRequestBuilder(
      this.id,
      title,
      this.contributor);
  }
}
