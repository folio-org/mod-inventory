package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.ItemStatusName;

import java.util.HashMap;
import java.util.Map;

public class TargetItemStatusValidator {
  private static final Map<ItemStatusName, TargetItemStatusValidatorInterface> validators = new HashMap<>();

  public TargetItemStatusValidator() {
    validators.put(InProcessTargetValidator.statusName,new InProcessTargetValidator());
    validators.put(InProcessNonRequestableTargetValidator.statusName,new InProcessNonRequestableTargetValidator());
  }

  public TargetItemStatusValidatorInterface getValidator(ItemStatusName itemStatusName) {
    return validators.get(itemStatusName);
  }

}
