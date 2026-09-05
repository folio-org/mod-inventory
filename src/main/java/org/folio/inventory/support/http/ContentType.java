package org.folio.inventory.support.http;

import lombok.experimental.UtilityClass;

//Apache content type constants include character encodings which are stricter than we expect
@UtilityClass
public class ContentType {
  public static final String APPLICATION_JSON = "application/json";
  public static final String TEXT_PLAIN = "text/plain";
}
