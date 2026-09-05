package support.fixtures;

import support.builders.ItemRequestBuilder;

public class ItemRequestFixture {
  public static ItemRequestBuilder basedUponSmallAngryPlanet() {
    return new ItemRequestBuilder()
      .withBarcode("036000291452");
  }

  public static ItemRequestBuilder basedUponTemeraire() {
    return new ItemRequestBuilder()
      .withBarcode("232142443432");
  }
}
