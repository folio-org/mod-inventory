package org.folio.inventory;

import org.folio.kafka.services.KafkaTopic;

public enum EntityLinksKafkaTopic implements KafkaTopic {
  LINKS_STATS("instance-authority-stats");

  private final String topic;

  EntityLinksKafkaTopic(String topic) {
    this.topic = topic;
  }

  @Override
  public String moduleName() {
    return "links";
  }

  public String topicName() {
    return topic;
  }
}
