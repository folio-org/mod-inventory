package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.ItemStatusName;

import java.util.EnumMap;

public class TargetItemStatusValidators {
  private static final EnumMap<ItemStatusName, AbstractTargetItemStatusValidator> validators = new EnumMap<>(ItemStatusName.class);

  public TargetItemStatusValidators() {
    validators.put(ItemStatusName.IN_PROCESS, new InProcessTargetItemStatusValidator());
    validators.put(ItemStatusName.IN_PROCESS_NON_REQUESTABLE, new InProcessNonRequestableTargetItemStatusValidator());
    validators.put(ItemStatusName.INTELLECTUAL_ITEM, new IntellectualItemTargetItemStatusValidator());
    validators.put(ItemStatusName.LONG_MISSING, new LongMissingTargetItemStatusValidator());
    validators.put(ItemStatusName.MISSING, new MissingTargetItemStatusValidator());
    validators.put(ItemStatusName.RESTRICTED, new RestrictedTargetItemStatusValidator());
    validators.put(ItemStatusName.UNAVAILABLE, new UnavaliableTargetItemStatusValidator());
  }

  public AbstractTargetItemStatusValidator getValidator(ItemStatusName itemStatusName) {
    return validators.get(itemStatusName);
  }
}
