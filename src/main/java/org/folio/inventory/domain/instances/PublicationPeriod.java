package org.folio.inventory.domain.instances;

import static org.apache.commons.lang3.ObjectUtils.anyNotNull;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

import io.vertx.core.json.JsonObject;

public class PublicationPeriod {
  private final Integer start;
  private final Integer end;

  public PublicationPeriod(Integer start, Integer end) {
    this.start = start;
    this.end = end;
  }

  public Integer getStart() {
    return start;
  }

  public Integer getEnd() {
    return end;
  }

  public static JsonObject publicationPeriodToJson(PublicationPeriod period) {
    if (period == null || (period.getStart() == null && period.getEnd() == null)) {
      return null;
    }

    var json = new JsonObject();
    includeIfPresent(json, "start", period.getStart());
    includeIfPresent(json, "end", period.getEnd());

    return json;
  }

  public static PublicationPeriod publicationPeriodFromJson(JsonObject pubPeriodJson) {
    if (pubPeriodJson == null) {
      return null;
    }

    var start = pubPeriodJson.getInteger("start");
    var end = pubPeriodJson.getInteger("end");

    return anyNotNull(start, end) ? new PublicationPeriod(start, end) : null;
  }
}
