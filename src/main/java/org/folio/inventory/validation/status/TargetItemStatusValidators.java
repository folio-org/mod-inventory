package org.folio.inventory.validation.status;

import java.util.EnumMap;
import org.folio.inventory.domain.items.ItemStatusName;

public class TargetItemStatusValidators {
  private static final EnumMap<ItemStatusName, AbstractTargetItemStatusValidator> VALIDATORS =
    new EnumMap<>(ItemStatusName.class);

  public TargetItemStatusValidators() {
    VALIDATORS.put(ItemStatusName.IN_PROCESS, new InProcessTargetItemStatusValidator());
    VALIDATORS.put(ItemStatusName.IN_PROCESS_NON_REQUESTABLE, new InProcessNonRequestableTargetItemStatusValidator());
    VALIDATORS.put(ItemStatusName.INTELLECTUAL_ITEM, new IntellectualItemTargetItemStatusValidator());
    VALIDATORS.put(ItemStatusName.LONG_MISSING, new LongMissingTargetItemStatusValidator());
    VALIDATORS.put(ItemStatusName.MISSING, new MissingTargetItemStatusValidator());
    VALIDATORS.put(ItemStatusName.RESTRICTED, new RestrictedTargetItemStatusValidator());
    VALIDATORS.put(ItemStatusName.UNAVAILABLE, new UnavailableTargetItemStatusValidator());
    VALIDATORS.put(ItemStatusName.UNKNOWN, new UnknownTargetItemStatusValidator());
    VALIDATORS.put(ItemStatusName.WITHDRAWN, new WithdrawnTargetItemStatusValidator());
  }

  public AbstractTargetItemStatusValidator getValidator(ItemStatusName itemStatusName) {
    return VALIDATORS.get(itemStatusName);
  }
}
