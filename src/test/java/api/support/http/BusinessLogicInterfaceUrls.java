package api.support.http;

import java.net.MalformedURLException;
import java.net.URL;

import static api.ApiTestSuite.apiRoot;

public class BusinessLogicInterfaceUrls {
  public static URL items(String subPath) {
    return getUrl("/inventory/items", subPath);
  }

  public static URL markInProcessUrl(String subPath) {
    return items("/"+subPath+"/mark-inprocess");
  }

  public static URL markInProcessNonRequestableUrl(String subPath) {
    return items("/"+subPath+"/mark-inprocess-non-requestable");
  }

  public static URL markIntellectualItemUrl(String subPath) {
    return items("/" + subPath + "/mark-intellectual-item");
  }

  public static URL markLongMissingUrl(String subPath) {
    return items("/" + subPath + "/mark-long-missing");
  }


  public static URL markMissingUrl(String subPath) {
    return items("/" + subPath + "/mark-missing");
  }

  public static URL markRestrictedUrl(String subPath) {
    return items("/" + subPath + "/mark-restricted");
  }

  public static URL markUnknownUrl(String subPath) {
    return items("/" + subPath + "/mark-unknown");
  }

  public static URL markWithdrawnUrl(String subPath) {
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

  private static URL getUrl(String basePath, String subPath) {
    try {
      return URLHelper.joinPath(new URL(apiRoot()), String.format(
        "%s%s", basePath, subPath));
    } catch (MalformedURLException e) {
      return null;
    }
  }

}
