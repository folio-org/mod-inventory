package api.support;

import java.net.MalformedURLException;
import java.net.URL;

import api.ApiTestSuite;
import lombok.SneakyThrows;

public class ApiRoot {
  private ApiRoot() { }

  public static String inventory() {
    return String.format("%s/inventory", ApiTestSuite.apiRoot());
  }

  @SneakyThrows
  public static URL instances() {
    return new URL(String.format("%s/instances", inventory()));
  }

  public static URL holdings()
    throws MalformedURLException {

    return new URL(String.format("%s/holdings", inventory()));
  }

  public static URL instancesBatch()
    throws MalformedURLException {
    return new URL(String.format("%s/instances/batch", inventory()));
  }

  public static URL instances(String query)
    throws MalformedURLException {

    return new URL(String.format("%s/instances?%s", inventory(), query));
  }

  public static URL items()
    throws MalformedURLException {

    return new URL(String.format("%s/items", inventory()));
  }

  public static URL moveItems()
    throws MalformedURLException {

    return new URL(String.format("%s/items/move", inventory()));
  }

  public static URL moveHoldingsRecords()
    throws MalformedURLException {

    return new URL(String.format("%s/holdings/move", inventory()));
  }

  public static URL updateItemsOwnership()
    throws MalformedURLException {

    return new URL(String.format("%s/items/update-ownership", inventory()));
  }

  public static URL updateHoldingsRecordsOwnership()
    throws MalformedURLException {

    return new URL(String.format("%s/holdings/update-ownership", inventory()));
  }

  public static URL items(String query)
    throws MalformedURLException {

    return new URL(String.format("%s/items?%s", inventory(), query));
  }

  public static URL itemsRetrieve()
          throws MalformedURLException {

    return new URL(String.format("%s/items/retrieve", inventory()));
  }

  public static URL tenantItems()
    throws MalformedURLException {

    return new URL(String.format("%s/tenant-items", inventory()));
  }

  public static String isbn() {
    return String.format("%s/isbn", ApiTestSuite.apiRoot());
  }

  public static URL isbnValidate(String query) throws MalformedURLException {

    return new URL(String.format("%s/validator?%s", isbn(), query));
  }

  public static URL isbnConvertTo10(String query) throws MalformedURLException {

    return new URL(String.format("%s/convertTo10?%s", isbn(), query));
  }

  public static URL isbnConvertTo13(String query) throws MalformedURLException {

    return new URL(String.format("%s/convertTo13?%s", isbn(), query));
  }

  public static URL instanceBlockedFieldsConfig() throws MalformedURLException {
    return new URL(String.format("%s/inventory/config/instances/blocked-fields", ApiTestSuite.apiRoot()));
  }

  public static URL holdingsBlockedFieldsConfig() throws MalformedURLException {
    return new URL(String.format("%s/inventory/config/holdings/blocked-fields", ApiTestSuite.apiRoot()));
  }

  public static URL tenant()
    throws MalformedURLException {
    return new URL(String.format("%s/_/tenant", ApiTestSuite.apiRoot()));
  }

  public static URL health() throws MalformedURLException {
    return new URL(String.format("%s/admin/health", ApiTestSuite.apiRoot()));
  }
}
