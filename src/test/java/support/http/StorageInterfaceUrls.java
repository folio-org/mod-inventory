package support.http;

import static api.ApiTestSuite.storageOkapiUrl;

import java.net.URI;
import java.net.URL;
import lombok.SneakyThrows;

public class StorageInterfaceUrls {
  public static URL holdingStorageUrl(String subPath) {
    return viaOkapiUrl(String.format("/holdings-storage/holdings%s", subPath));
  }

  public static URL itemsStorageUrl(String subPath) {
    return viaOkapiUrl(String.format("/item-storage/items%s", subPath));
  }

  public static URL instancesStorageUrl(String subPath) {
    return viaOkapiUrl(String.format("/instance-storage/instances%s", subPath));
  }

  public static URL institutionsStorageUrl(String subPath) {
    return viaOkapiUrl("/location-units/institutions" + subPath);
  }

  public static URL campusesStorageUrl(String subPath) {
    return viaOkapiUrl("/location-units/campuses" + subPath);
  }

  public static URL librariesStorageUrl(String subPath) {
    return viaOkapiUrl("/location-units/libraries" + subPath);
  }

  public static URL locationsStorageUrl(String subPath) {
    return viaOkapiUrl("/locations" + subPath);
  }

  public static URL usersStorageUrl(String subPath) {
    return viaOkapiUrl("/users" + subPath);
  }

  public static URL userTenantsStorageUrl(String subPath) {
    return viaOkapiUrl("/user-tenants" + subPath);
  }

  public static URL natureOfContentTermsStorageUrl(String subPath) {
    return viaOkapiUrl("/nature-of-content-terms" + subPath);
  }

  public static URL precedingSucceedingTitlesUrl(String subPath) {
    return viaOkapiUrl("/preceding-succeeding-titles" + subPath);
  }

  public static URL instanceRelationshipUrl(String subPath) {
    return viaOkapiUrl("/instance-storage/instance-relationships" + subPath);
  }

  public static URL instanceRelationshipTypeUrl(String subPath) {
    return viaOkapiUrl("/instance-relationship-types" + subPath);
  }

  public static URL requestStorageUrl(String subPath) {
    return viaOkapiUrl("/request-storage/requests" + subPath);
  }

  public static URL sourceRecordStorageUrl(String subPath) {
    return viaOkapiUrl("/source-storage/records" + subPath);
  }

  public static URL holdingRecordSourcesUrl(String subPath) {
    return viaOkapiUrl("/holdings-sources" + subPath);
  }

  public static URL boundWithPartsUrl(String subPath) {
    return viaOkapiUrl("/inventory-storage/bound-with-parts" + subPath);
  }

  @SneakyThrows
  private static URL viaOkapiUrl(String path) {
    return UrlHelper.joinPath(new URI(storageOkapiUrl()).toURL(), path);
  }
}
