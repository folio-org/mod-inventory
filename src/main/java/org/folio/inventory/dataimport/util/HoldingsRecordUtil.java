package org.folio.inventory.dataimport.util;

import org.apache.commons.lang3.StringUtils;
import org.folio.HoldingsRecord;
import org.folio.Metadata;
import org.folio.inventory.common.Context;
import org.folio.okapi.common.OkapiToken;

public final class HoldingsRecordUtil {

  private HoldingsRecordUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  public static HoldingsRecord populateUpdatedByUserIdIfNeeded(HoldingsRecord holdingsRecord, Context context) {
    if (holdingsRecord.getMetadata() == null) {
      holdingsRecord.setMetadata(new Metadata());
    }

    if (StringUtils.isBlank(holdingsRecord.getMetadata().getUpdatedByUserId())) {
      holdingsRecord.getMetadata().setUpdatedByUserId(getUserId(context));
    }
    return holdingsRecord;
  }

  private static String getUserId(Context context) {
    return StringUtils.isNotBlank(context.getUserId()) ? context.getUserId()
      : new OkapiToken(context.getToken()).getUserIdWithoutValidation();
  }

}
