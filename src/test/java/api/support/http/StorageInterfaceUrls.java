package api.support.http;

import java.net.MalformedURLException;
import java.net.URL;

import static api.ApiTestSuite.storageOkapiUrl;

public class StorageInterfaceUrls {
  public static URL holdingStorageUrl(String subPath) {
    try {
      return URLHelper.joinPath(new URL(storageOkapiUrl()), String.format(
        "/holdings-storage/holdings%s", subPath));
    } catch (MalformedURLException e) {
      return null;
    }
  }

  public static URL itemsStorageUrl(String subPath) {
    try {
      return URLHelper.joinPath(new URL(storageOkapiUrl()), String.format(
        "/item-storage/items%s", subPath));
    } catch (MalformedURLException e) {
      return null;
    }
  }
}
