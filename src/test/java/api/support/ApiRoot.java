package api.support;

import api.ApiTestSuite;

import java.net.MalformedURLException;
import java.net.URL;

public class ApiRoot {
  public static String inventory() {
    return String.format("%s/inventory", ApiTestSuite.apiRoot());
  }

  public static URL instances()
    throws MalformedURLException {

    return new URL(String.format("%s/instances", inventory()));
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

  public static URL items(String query)
    throws MalformedURLException {

    return new URL(String.format("%s/items?%s", inventory(), query));
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

  public static URL blockedFieldsConfig() throws MalformedURLException {
    return new URL(String.format("%s/inventory/config/instances/blocked-fields", ApiTestSuite.apiRoot()));
  }

  public static URL tenant()
    throws MalformedURLException {
    return new URL(String.format("%s/_/tenant", ApiTestSuite.apiRoot()));
  }

}
