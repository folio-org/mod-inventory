package support.fixtures;

import support.builders.InstanceRequestBuilder;

public class InstanceRequestFixture {

  public static InstanceRequestBuilder smallAngryPlanet() {
    return create("The Long Way to a Small, Angry Planet", "Chambers, Becky");
  }

  public static InstanceRequestBuilder temeraire() {
    return create("Temeraire", "Novik, Naomi");
  }

  private static InstanceRequestBuilder create(String title, String contributor) {
    return new InstanceRequestBuilder(title, contributor);
  }
}
