package org.folio.inventory.domain.ingest

import io.vertx.core.eventbus.EventBus
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import org.folio.inventory.common.CollectAll
import org.folio.inventory.common.MessagingContext
import org.folio.inventory.common.domain.Failure
import org.folio.inventory.domain.Creator
import org.folio.inventory.domain.Instance
import org.folio.inventory.domain.Item
import org.folio.inventory.domain.Messages
import org.folio.inventory.resources.ingest.IngestJob
import org.folio.inventory.resources.ingest.IngestJobState
import org.folio.inventory.storage.Storage
import org.folio.inventory.support.JsonArrayHelper

import java.util.stream.Collectors

class IngestMessageProcessor {
  private final Storage storage

  IngestMessageProcessor(final Storage storage) {
    this.storage = storage
  }

  void register(EventBus eventBus) {
    eventBus.consumer(Messages.START_INGEST.Address)
      .handler(this.&processRecordsMessage.rcurry(eventBus))

    eventBus.consumer(Messages.INGEST_COMPLETED.Address)
      .handler(this.&markIngestCompleted)
  }

  private void processRecordsMessage(Message message, EventBus eventBus) {
    def allItems = new CollectAll<Item>()
    def allInstances = new CollectAll<Instance>()

    def body = ((JsonObject)message.body()).map

    def records = JsonArrayHelper.toListOfMaps(body.records)
    Map materialTypes = body.materialTypes.map
    Map loanTypes = body.loanTypes.map
    Map locations = body.locations.map
    Map identifierTypes = body.identifierTypes.map
    Map instanceTypes = body.instanceTypes.map
    Map creatorTypes = body.creatorTypes.map

    def context = new MessagingContext(message.headers())

    def instanceCollection = storage.getInstanceCollection(context)
    def itemCollection = storage.getItemCollection(context)

    records.stream()
      .map({
        def creators = JsonArrayHelper.toList(it.creators)
          .stream()
          .map({ creator ->
            //Default all creators to personal name
            return new Creator(creatorTypes.get("Personal name").toString(),
            creator.getString("name"))
        })
        .collect(Collectors.toList())

        def identifiers = JsonArrayHelper.toList(it.identifiers)
          .stream()
          .map({ identifier ->
            def newIdentifier = new HashMap<String, Object>()

            //Default all identifiers to ISBN
            newIdentifier.put("identifierTypeId", identifierTypes.get("ISBN"))
            newIdentifier.put("value", identifier.getString("value"))

            return newIdentifier
          })
          .collect(Collectors.toList())

      new Instance(UUID.randomUUID().toString(), it.title,
        identifiers, "Local: MODS", instanceTypes.get("Books"), creators)
    })
    .forEach({ instanceCollection.add(it, allInstances.receive(),
      { Failure failure -> println("Ingest Creation Failed: ${failure.reason}") })
    })

    allInstances.collect ({ instances ->
      records.stream()
        .map({ record ->
          new Item(null, record.title, record.barcode,
            instances.find({ it.title == record.title })?.id,
            "Available", materialTypes.get("Book"), locations.get("Main Library"),
            null, loanTypes.get("Can Circulate"), null)
      })
      .forEach({ itemCollection.add(it, allItems.receive(),
        { Failure failure -> println("Ingest Creation Failed: ${failure.reason}") })
      })
    })

    allItems.collect({
      IngestMessages.completed(context.getHeader("jobId"), context)
        .send(eventBus)
    })
  }

  private void markIngestCompleted(Message message) {
    def context = new MessagingContext(message.headers())

    storage.getIngestJobCollection(context).update(
      new IngestJob(context.getHeader("jobId"), IngestJobState.COMPLETED),
      { },
      { Failure failure ->
        println("Updating ingest job failed: ${failure.reason}") })
  }
}
