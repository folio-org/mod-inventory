package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.ItemStatusName;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class GenerateItemStatusTransitionsMatrix {
  private static TargetItemStatusValidators targetItemStatusValidators;

  @BeforeClass
  public static void setUp() throws Exception {
    targetItemStatusValidators = new TargetItemStatusValidators();
  }

  @Test
  public void generate() {
    final String fieldDelim = "\t";
    final String stringDelim = "\"";
    final List<String> sortedStatusNames = Arrays.stream(ItemStatusName.values())
      .map(itemStatusName -> itemStatusName.value())
      .sorted()
      .collect(Collectors.toList());
    final Date dateNow = new Date();
    StringBuilder sb = new StringBuilder();
    // Create header
    sb.append("Generated at:" + dateNow);
    sb.append(System.lineSeparator());
    sb.append(stringDelim + "Initial\\Target" + stringDelim + fieldDelim);
    sortedStatusNames.stream().forEach(statusName -> sb.append(stringDelim + statusName + stringDelim + fieldDelim));
    sb.append(System.lineSeparator());
    // Create body
    sortedStatusNames.stream().forEach(initStatusName -> {
      sb.append(stringDelim + initStatusName + stringDelim + fieldDelim);
      sortedStatusNames.stream().forEach(targetStatusName -> {
        AbstractTargetItemStatusValidator validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetStatusName));
        sb.append(stringDelim);
        sb.append(validator == null ? "" : validator.getAllStatusesAllowedToMark().contains(ItemStatusName.forName(initStatusName)) ? "Y" : "");
        sb.append(stringDelim);
        sb.append(fieldDelim);
      });
      sb.append(System.lineSeparator());
    });
    try {
      String fileName = "./target/ItemStatusesAndTransitions.txt";
      System.out.println(fileName);
      System.out.println(sb.toString());
      BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
      writer.write(sb.toString());
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
