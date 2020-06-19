package api.support.http;

import java.net.MalformedURLException;
import java.net.URL;

import static api.ApiTestSuite.apiRoot;

public class BusinessLogicInterfaceUrls {
  public static URL items(String subPath) {
    return getUrl("/inventory/items", subPath);
  }

  public static URL markWithdrawn(String subPath) {
    return items("/" + subPath + "/mark-withdrawn");
  }

  public static URL instances(String subPath) {
    return getUrl("/inventory/instances", subPath);
  }

  public static URL instancesBatch(String subPath) {
    return getUrl("/inventory/instances/batch", subPath);
  }

  public static URL isbns(String subPath) {
    return getUrl("/isbn", subPath);
  }

  public static URL move(String subPath) {
    return getUrl("/inventory/items/move", subPath);
  }

  private static URL getUrl(String basePath, String subPath) {
    try {
      return URLHelper.joinPath(new URL(apiRoot()), String.format(
        "%s%s", basePath, subPath));
    } catch (MalformedURLException e) {
      return null;
    }
  }

}
