package api.support.dto;

import java.util.Date;
import java.util.UUID;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Request {
  @Builder.Default
  private String id = UUID.randomUUID().toString();
  private String status;
  private String itemId;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private Date holdShelfExpirationDate;
  private String requesterId;
  private String requestType;
  @Builder.Default
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private Date requestDate = DateTime.now(DateTimeZone.UTC).toDate();
  @Builder.Default
  private String fulfillmentPreference = "Hold Shelf";
}
