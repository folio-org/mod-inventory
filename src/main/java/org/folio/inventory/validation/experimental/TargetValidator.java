package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.ItemStatusName;

import java.util.HashMap;
import java.util.Map;

public class TargetValidator {
  private static final Map<ItemStatusName,TargetValidatorInterface> validators = new HashMap<>();

  public TargetValidator() {
    validators.put(InProcessTargetValidator.statusName,new InProcessTargetValidator());
    validators.put(InProcessNonRequestableTargetValidator.statusName,new InProcessNonRequestableTargetValidator());
  }

  public TargetValidatorInterface getValidator(ItemStatusName itemStatusName) {
    return validators.get(itemStatusName);
  }

}
