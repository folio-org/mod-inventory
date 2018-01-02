package org.folio.inventory.support;

import java.io.IOException;

import org.folio.util.ResourceUtil;

import io.vertx.core.json.JsonObject;

/**
 * Create JsonObject from a resource file.
 */
public final class JsonHelper {
  private JsonHelper() {
    throw new UnsupportedOperationException("Invoking the constructor of a utility class is forbidden");
  }

  /**
   * Return a resource file as JsonObject.
   * @param resourcePath  file to read
   * @return the JsonObject
   * @throws IOException  on file read error
   */
  public static JsonObject getJsonFileAsJsonObject(String resourcePath) throws IOException {
    return new JsonObject(ResourceUtil.asString(resourcePath));
  }
}
