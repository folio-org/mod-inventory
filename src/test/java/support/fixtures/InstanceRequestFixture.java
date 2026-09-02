package support.fixtures;

import support.builders.InstanceRequestBuilder;

public class InstanceRequestFixture {

  public static InstanceRequestBuilder smallAngryPlanet() {
    return create("The Long Way to a Small, Angry Planet", "Chambers, Becky");
  }

  public static InstanceRequestBuilder nod() {
    return create("Nod", "Barnes, Adrian");
  }

  public static InstanceRequestBuilder uprooted() {
    return create("Uprooted", "Novik, Naomi");
  }

  public static InstanceRequestBuilder temeraire() {
    return create("Temeraire", "Novik, Naomi");
  }

  public static InstanceRequestBuilder interestingTimes() {
    return create("Interesting Times", "Pratchett, Terry");
  }

  private static InstanceRequestBuilder create(String title, String contributor) {
    return new InstanceRequestBuilder(title, contributor);
  }
}
