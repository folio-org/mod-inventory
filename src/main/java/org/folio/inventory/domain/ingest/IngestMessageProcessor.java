package org.folio.inventory.domain.ingest;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.CollectAll;
import org.folio.inventory.common.MessagingContext;
import org.folio.inventory.domain.*;
import org.folio.inventory.resources.ingest.IngestJob;
import org.folio.inventory.resources.ingest.IngestJobState;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.JsonArrayHelper;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class IngestMessageProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TITLE_PROPERTY = "title";
  private final Storage storage;

  public IngestMessageProcessor(final Storage storage) {
    this.storage = storage;
  }

  public void register(EventBus eventBus) {
    eventBus.consumer(Messages.START_INGEST.Address, recordsMessageHandler(eventBus));
    eventBus.consumer(Messages.INGEST_COMPLETED.Address, this::markIngestCompleted);
  }

  private Handler<Message<JsonObject>> recordsMessageHandler(EventBus eventBus) {
    return message -> processRecordsMessage(message, eventBus);
  }

  private void processRecordsMessage(Message<JsonObject> message, final EventBus eventBus) {
    final CollectAll<Item> allItems = new CollectAll<>();
    final CollectAll<Instance> allInstances = new CollectAll<>();

    final MessagingContext context = new MessagingContext(message.headers());
    final JsonObject body = message.body();

    IngestMessages.completed(context.getJobId(), context).send(eventBus);

    final List<JsonObject> records = JsonArrayHelper.toList(body.getJsonArray("records"));
    final JsonObject materialTypes = body.getJsonObject("materialTypes");
    final JsonObject loanTypes = body.getJsonObject("loanTypes");
    final JsonObject locations = body.getJsonObject("locations");
    final JsonObject instanceTypes = body.getJsonObject("instanceTypes");
    final JsonObject identifierTypes = body.getJsonObject("identifierTypes");
    final JsonObject creatorTypes = body.getJsonObject("creatorTypes");

    final InstanceCollection instanceCollection = storage.getInstanceCollection(context);
    final ItemCollection itemCollection = storage.getItemCollection(context);

    records.stream()
      .map(record -> {
        List<JsonObject> identifiersJson = JsonArrayHelper.toList(
          record.getJsonArray("identifiers"));

        List<Identifier> identifiers = identifiersJson.stream()
          .map(identifier -> new Identifier(
            identifierTypes.getString("ISBN"),
            identifier.getString("value")))
          .collect(Collectors.toList());

        List<JsonObject> creatorsJson = JsonArrayHelper.toList(
          record.getJsonArray("creators"));

        List<Creator> creators = creatorsJson.stream()
          .map(creator -> new Creator(
            creatorTypes.getString("Personal name"),
            creator.getString("name")))
          .collect(Collectors.toList());

        if(creators.isEmpty()) {
          creators.add(new Creator(
            creatorTypes.getString("Personal name"),
            "Unknown creator"));
        }

        return new Instance(UUID.randomUUID().toString(), record.getString(TITLE_PROPERTY),
          identifiers, "Local: MODS", instanceTypes.getString("Books"), creators);
      })
      .forEach(instance -> instanceCollection.add(instance, allInstances.receive(),
        failure -> log.error("Instance processing failed: " + failure.getReason())));

      allInstances.collect(instances ->
        records.stream().map(record -> {
          Optional<Instance> possibleInstance = instances.stream()
            .filter(instance ->
              StringUtils.equals(instance.title, record.getString(TITLE_PROPERTY)))
            .findFirst();

          String instanceId = possibleInstance.isPresent()
            ? possibleInstance.get().id
            : null;

          return new Item(null,
            record.getString(TITLE_PROPERTY),
            record.getString("barcode"),
            null, null, new ArrayList<>(), null,
            instanceId,
            new ArrayList<>(),
            "Available",
            materialTypes.getString("Book") != null
              ? materialTypes.getString("Book")
              : materialTypes.getString("book"),
            locations.getString("Main Library"),
            null,
            loanTypes.getString("Can Circulate") != null
              ? loanTypes.getString("Can Circulate")
              : loanTypes.getString("Can circulate"),
            null);
      })
      .forEach(item -> itemCollection.add(item, allItems.receive(),
        failure -> log.error("Item processing failed: " + failure.getReason()))));

    allItems.collect(items ->
      IngestMessages.completed(context.getJobId(), context).send(eventBus));
  }

  private void markIngestCompleted(Message<JsonObject> message) {
    final MessagingContext context = new MessagingContext(message.headers());

    storage.getIngestJobCollection(context).update(
      new IngestJob(context.getJobId(), IngestJobState.COMPLETED),
      v -> log.info(String.format("Ingest job %s completed", context.getJobId())),
      failure -> log.error(
        String.format("Updating ingest job failed: %s", failure.getReason())));
  }
}
