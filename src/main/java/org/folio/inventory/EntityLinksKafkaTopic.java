package org.folio.inventory;


public enum EntityLinksKafkaTopic {
  LINKS_STATS("links.instance-authority-stats");

  private final String topic;

  EntityLinksKafkaTopic(String topic) {
    this.topic = topic;
  }

  public String topicName() {
    return topic;
  }
}
