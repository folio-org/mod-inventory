package api.support;

import api.ApiTestSuite;

import java.net.MalformedURLException;
import java.net.URL;

public class ApiRoot {
  public static String inventory() {
    return String.format("%s/inventory", ApiTestSuite.apiRoot());
  }

  public static String inventoryConfig() {return String.format("%s/inventory/config", ApiTestSuite.apiRoot());}

  public static String instancesConfig() {return String.format("%s/instances", inventoryConfig());}

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

  public static URL items(String query)
    throws MalformedURLException {

    return new URL(String.format("%s/items?%s", inventory(), query));
  }

  public static URL blockedFieldsConfig() throws MalformedURLException {
    return new URL(String.format("%s/blockedFields", instancesConfig()));
  }
}
