package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;

import static java.util.Set.of;

public class MissingTargetItemStatusValidator extends AbstractTargetItemStatusValidator {
  public MissingTargetItemStatusValidator() {
    super(ItemStatusName.MISSING, of(
      ItemStatusName.AVAILABLE,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.IN_PROCESS,
      ItemStatusName.IN_PROCESS_NON_REQUESTABLE,
      ItemStatusName.LONG_MISSING,
      ItemStatusName.PAGED,
      ItemStatusName.UNAVAILABLE,
      ItemStatusName.WITHDRAWN
    ));
  }
}
