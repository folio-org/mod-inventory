package support.http;

import java.net.URL;
import lombok.SneakyThrows;

public class UrlHelper {

  @SneakyThrows
  public static URL joinPath(URL base, String additionalPath) {
    return new URL(
      base.getProtocol(),
      base.getHost(),
      base.getPort(),
      base.getPath() + additionalPath);
  }
}
