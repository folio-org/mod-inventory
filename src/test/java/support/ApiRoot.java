package support;

import api.ApiTestSuite;
import java.net.URI;
import java.net.URL;
import java.util.UUID;
import lombok.SneakyThrows;

public class ApiRoot {

  private ApiRoot() { }

  public static String inventory() {
    return String.format("%s/inventory", ApiTestSuite.apiRoot());
  }

  @SneakyThrows
  public static URL instances() {
    return new URI(String.format("%s/instances", inventory())).toURL();
  }

  @SneakyThrows
  public static URL holdings() {
    return new URI(String.format("%s/holdings", inventory())).toURL();
  }

  @SneakyThrows
  public static URL instancesBatch() {
    return new URI(String.format("%s/instances/batch", inventory())).toURL();
  }

  @SneakyThrows
  public static URL instances(String query) {
    return new URI(String.format("%s/instances?%s", inventory(), query)).toURL();
  }

  @SneakyThrows
  public static URL instance(UUID id) {
    return instance(id.toString());
  }

  @SneakyThrows
  public static URL instance(String id) {
    return new URI(String.format("%s/%s", instances(), id)).toURL();
  }

  @SneakyThrows
  public static URL items() {
    return new URI(String.format("%s/items", inventory())).toURL();
  }

  @SneakyThrows
  public static URL moveItems() {
    return new URI(String.format("%s/items/move", inventory())).toURL();
  }

  @SneakyThrows
  public static URL moveHoldingsRecords() {
    return new URI(String.format("%s/holdings/move", inventory())).toURL();
  }

  @SneakyThrows
  public static URL updateItemsOwnership() {
    return new URI(String.format("%s/items/update-ownership", inventory())).toURL();
  }

  @SneakyThrows
  public static URL updateHoldingsRecordsOwnership() {
    return new URI(String.format("%s/holdings/update-ownership", inventory())).toURL();
  }

  @SneakyThrows
  public static URL items(String query) {
    return new URI(String.format("%s/items?%s", inventory(), query)).toURL();
  }

  @SneakyThrows
  public static URL itemsRetrieve() {
    return new URI(String.format("%s/items/retrieve", inventory())).toURL();
  }

  @SneakyThrows
  public static URL tenantItems() {
    return new URI(String.format("%s/tenant-items", inventory())).toURL();
  }

  @SneakyThrows
  public static String isbn() {
    return String.format("%s/isbn", ApiTestSuite.apiRoot());
  }

  @SneakyThrows
  public static URL isbnValidate(String query) {
    return new URI(String.format("%s/validator?%s", isbn(), query)).toURL();
  }

  @SneakyThrows
  public static URL isbnConvertTo10(String query) {
    return new URI(String.format("%s/convertTo10?%s", isbn(), query)).toURL();
  }

  @SneakyThrows
  public static URL isbnConvertTo13(String query) {
    return new URI(String.format("%s/convertTo13?%s", isbn(), query)).toURL();
  }

  @SneakyThrows
  public static URL instanceBlockedFieldsConfig() {
    return new URI(String.format("%s/inventory/config/instances/blocked-fields", ApiTestSuite.apiRoot())).toURL();
  }

  @SneakyThrows
  public static URL holdingsBlockedFieldsConfig() {
    return new URI(String.format("%s/inventory/config/holdings/blocked-fields", ApiTestSuite.apiRoot())).toURL();
  }

  @SneakyThrows
  public static URL tenant() {
    return new URI(String.format("%s/_/tenant", ApiTestSuite.apiRoot())).toURL();
  }

  @SneakyThrows
  public static URL health() {
    return new URI(String.format("%s/admin/health", ApiTestSuite.apiRoot())).toURL();
  }
}
