package api.support.http;

import java.net.MalformedURLException;
import java.net.URL;

import static api.ApiTestSuite.storageOkapiUrl;

public class StorageInterfaceUrls {
  public static URL holdingStorageUrl(String subPath) {
    return viaOkapiURL(String.format("/holdings-storage/holdings%s", subPath));
  }

  public static URL itemsStorageUrl(String subPath) {
    return viaOkapiURL(String.format("/item-storage/items%s", subPath));
  }

  public static URL instancesStorageUrl(String subPath) {
    return viaOkapiURL(String.format("/instance-storage/instances%s", subPath));
  }

  public static URL institutionsStorageUrl(String subPath) {
    return viaOkapiURL("/location-units/institutions" + subPath);
  }

  public static URL campusesStorageUrl(String subPath) {
    return viaOkapiURL("/location-units/campuses" + subPath);
  }

  public static URL librariesStorageUrl(String subPath) {
    return viaOkapiURL("/location-units/libraries" + subPath);
  }

  public static URL locationsStorageUrl(String subPath) {
    return viaOkapiURL("/locations" + subPath);
  }

  public static URL usersStorageUrl(String subPath) {
    return viaOkapiURL("/users" + subPath);
  }
  public static URL natureOfContentTermsStorageUrl(String subPath) {
    return viaOkapiURL("/nature-of-content-terms" + subPath);
  }

  public static URL precedingSucceedingTitlesUrl(String subPath) {
    return viaOkapiURL("/preceding-succeeding-titles" + subPath);
  }

  public static URL relatedInstancesUrl(String subPath) {
    return viaOkapiURL("/instance-storage/related-instances" + subPath);
  }

  public static URL relatedInstanceTypeUrl(String subPath) {
    return viaOkapiURL("/related-instance-types" + subPath);
  }

  public static URL instanceRelationshipUrl(String subPath) {
    return viaOkapiURL("/instance-storage/instance-relationships" + subPath);
  }

  public static URL instanceRelationshipTypeUrl(String subPath) {
    return viaOkapiURL("/instance-relationship-types" + subPath);
  }

  public static URL requestStorageUrl(String subPath) {
    return viaOkapiURL("/request-storage/requests" + subPath);
  }

  public static URL boundWithPartsUrl(String subPath) {
    return viaOkapiURL( "/inventory-storage/bound-with-parts" + subPath );
  }

  private static URL viaOkapiURL(String path) {
    try {
      return URLHelper.joinPath(new URL(storageOkapiUrl()), path);
    } catch (MalformedURLException e) {
      return null;
    }
  }
}
