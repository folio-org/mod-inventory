## 21.0.4 2024-12-xx
* Provide consistent handling with concurrency two or more Marc Bib Update events for the same bib record [MODINV-1100](https://folio-org.atlassian.net/browse/MODINV-1100)
* Missing x-okapi-user-id header in communications with inventory-storage [MODINV-1134](https://folio-org.atlassian.net/browse/MODINV-1134)
* Enable system user for data-import processes [MODINV-1115](https://folio-org.atlassian.net/browse/MODINV-1115)

## 21.0.3 2024-12-05
* suppressFromDiscovery value is not synchronized between member and centrlal tenants [MODINV-1123](https://issues.folio.org/browse/MODINV-1123)

## 21.0.2 2024-11-27
* Update dependencies fixing vulnerabilities [MODINV-1124](https://folio-org.atlassian.net/browse/MODINV-1124)

## 21.0.1 2024-11-18
* Missing interface dependencies in module descriptor [MODINV-1117](https://folio-org.atlassian.net/browse/MODINV-1117)

## 21.0.0 2024-10-29
* Existing "035" field is not retained the original position in imported record [MODINV-1049](https://folio-org.atlassian.net/browse/MODINV-1049)
* Update Data Import logic to normalize OCLC 035 values [MODINV-949](https://folio-org.atlassian.net/browse/MODINV-949)
* The sorting for Items on Instance details page is not worked [MODINV-1001](https://folio-org.atlassian.net/browse/MODINV-1001)
* "PMSystem" displayed as source in "quickmarc" view when record was created by "Non-matches" action of job profile [MODSOURCE-608](https://folio-org.atlassian.net/browse/MODSOURCE-608)
* The result table is not displayed in the file details log [MODINV-1003](https://folio-org.atlassian.net/browse/MODINV-1003)
* Invalid values (as it is) created in JSON when value is not matching accepted options provided in Instance field mapping for Nature of Content term [MODINV-1012](https://folio-org.atlassian.net/browse/MODINV-1012)
* Remove null values from electronicAccess object before returning item and instance [MODINV-1006](https://folio-org.atlassian.net/browse/MODINV-1006)
* 422 Unprocessable Content Error while updating  Instances and Items with electronic access without URI field populated. [MODINV-1024](https://folio-org.atlassian.net/browse/MODINV-1024)
* Error appears when edit via quickMARC MARC Instance shared from Member tenant [MODDATAIMP-1052](https://folio-org.atlassian.net/browse/MODDATAIMP-1052)
* Fix mod-inventory OOM issue [MODINV-1023](https://folio-org.atlassian.net/browse/MODINV-1023)
* Replace GET with POST request for fetching instances and holdings on /items endpoint to omit 414 error [MODINV-943](https://folio-org.atlassian.net/browse/MODINV-943)
* Call suppress-on-discovery for source record on holding update if discoverySuppress is true [MODINV-977](https://folio-org.atlassian.net/browse/MODINV-977)
* Requires `holdings-storage 2.0 3.0 4.0 5.0 6.0 7.0 8.0`
* Requires `bound-with-parts-storage 2.0`
* InstanceIngress create events consumption [MODINV-986](https://folio-org.atlassian.net/browse/MODINV-986)
* Additional Requirements - Update Data Import logic to normalize OCLC 035 values [MODINV-1044](https://folio-org.atlassian.net/browse/MODINV-1044)
* Implement endpoint to update ownership of Holdings [MODINV-1031](https://folio-org.atlassian.net/browse/MODINV-1031)
* Handle assignment of ids during update ownership of holdings [MODINV-1053](https://folio-org.atlassian.net/browse/MODINV-1053)
* Implement endpoint for updating ownership of Items [MODINV-955](https://folio-org.atlassian.net/browse/MODINV-955)
* Disallow updating holdings ownership with related boundwith [MODINV-1051](https://folio-org.atlassian.net/browse/MODINV-1051)
* Disallow updating ownership of boundwith item [MODINV-1052](https://folio-org.atlassian.net/browse/MODINV-1052)
* InstanceIngress update events consumption [MODINV-1008](https://folio-org.atlassian.net/browse/MODINV-1008)
* Apply new date type fields to instance schema [MODINV-1067](https://folio-org.atlassian.net/browse/MODINV-1067)
* Keep original UUIDs when updating ownership of Holdings/Items [MODINV-1074](https://folio-org.atlassian.net/browse/MODINV-1074)
* API version update [MODINV-1080](https://folio-org.atlassian.net/browse/MODINV-1080)
* Add `inventory-storage.instances.retrieve.collection.post` and `inventory-storage.holdings.retrieve.collection.post` permissions
* Fix inconsistencies in permission namings [MODINV-1072](https://folio-org.atlassian.net/browse/MODINV-1072)
* Update suppressDiscovery in the MARC file when creating an instance with “suppressDiscovery=true” when importing data [MODINV-1094](https://folio-org.atlassian.net/browse/MODINV-1094)

## 20.2.0 2023-03-20
* Inventory cannot process Holdings with virtual fields ([MODINV-941](https://issues.folio.org/browse/MODINV-941))
* Set instance record as deleted ([MODINV-883](https://issues.folio.org/browse/MODINV-883))
* Remove step of initial saving of incoming records to SRS [MODSOURMAN-1022](https://issues.folio.org/browse/MODSOURMAN-1022)
* Apply 005 logic before saving MARC Bib in SRS [MODINV-921](https://issues.folio.org/browse/MODINV-921)
* Remove extra fields from 'holdings/move' mechanism [MODINV-948](https://issues.folio.org/browse/MODINV-948)
* Allow to link local instance to shared instance [MODINV-901](https://issues.folio.org/browse/MODINV-901)
* OOM issue in mod-inventory (MODINV-944) ([MODINV-982](https://issues.folio.org/browse/MODINV-982))
* Make configurable params for instance sharing [MODINV-964](https://issues.folio.org/browse/MODINV-964)
* Create Kafka topics instead of relying on auto create in mod-inventory [MODINV-957](https://issues.folio.org/browse/MODINV-957)
* Remove post-processing for Instance Update action [MODINV-922](https://issues.folio.org/browse/MODINV-922)
* The import of file is completed with errors [MODINV-968](https://issues.folio.org/browse/MODINV-968)
* Move Marc-Bib matching event handler to inventory [MODINV-935](https://issues.folio.org/browse/MODINV-935)
* The duplicate record is created after updating action [MODINV-969](https://issues.folio.org/browse/MODINV-969)
* Upgrade vertx to 4.5.4 [MODDICORE-398](https://folio-org.atlassian.net/browse/MODDICORE-398)
* Add 999 validation for instance creation [MODDATAIMP-1001](https://folio-org.atlassian.net/browse/MODDATAIMP-1001)
* Adjust Match event handlers to take into account multiple search results from previous match [MODINV-936](https://issues.folio.org/browse/MODINV-936)
* SPIKE: Remove duplicated action DI_INVENTORY_INSTANCE_CREATED from Kafka event [MODINV-782](https://folio-org.atlassian.net/browse/MODINV-782)
* Allow to retrieve ITEM universally during HOLDINGS update [MODINV-987](https://folio-org.atlassian.net/browse/MODINV-987)
* Support same title being part of many (100+) bound-withs [MODINV-839](https://folio-org.atlassian.net/browse/MODINV-839)
* Provide event payload with incomingRecordId for logs [MODSOURMAN-1141](https://folio-org.atlassian.net/browse/MODSOURMAN-1141)
* Correct tenantId for retrieving record while instance updating [MODINV-972](https://folio-org.atlassian.net/browse/MODINV-972)
* Implement Modify MARC Bib handler in mod-inventory [MODINV-967](https://folio-org.atlassian.net/browse/MODINV-967)
* MARC bib record with only required fields cannot be imported [MODINV-951](https://folio-org.atlassian.net/browse/MODINV-951)
* Call delete endpoint in SRS when Instance is marked for deletion [MODINV-961](https://folio-org.atlassian.net/browse/MODINV-961)
* Incorrect tenantId installed in received DI_ERROR message [MODINV-952](https://folio-org.atlassian.net/browse/MODINV-952)
* Remove jsonld-java dependency fixing commons-io 2.5 CVE [MODINV-965](https://folio-org.atlassian.net/browse/MODINV-965)
* Fix exception when deciding to update Field 005 [FAT-9178](https://folio-org.atlassian.net/browse/FAT-9178)
* Extra Item for Orders Created during multiple import [MODINV-928](https://folio-org.atlassian.net/browse/MODINV-928)
* Adjust non-match by POL/VRN logic to not throw an exception [MODINV-953](https://folio-org.atlassian.net/browse/MODINV-953)
* Add new field "Display summary" for the item schema [MODINV-959](https://folio-org.atlassian.net/browse/MODINV-959)
* Add missed module permissions [MODINV-901](https://folio-org.atlassian.net/browse/MODINV-901)
* Fix exception when updating field 005 [FAT-9178](https://folio-org.atlassian.net/browse/FAT-9178)
* Remove extra fields from 'holdings/move' mechanism [MODINV-948](https://folio-org.atlassian.net/browse/MODINV-948)
* Move 001+003->035 logic to inventory and save SRS MARC by an HTTP request [MODINV-849](https://folio-org.atlassian.net/browse/MODINV-849)
* Preload orders not only with status Open [MODINV-876](https://folio-org.atlassian.net/browse/MODINV-876)
* Add subscription on DI_SRS_MARC_BIB_RECORD_UPDATED event about marc-bib update [MODSOURMAN-1106](https://folio-org.atlassian.net/browse/MODSOURMAN-1106)
* Consortial: Local HRID gets added as 035 when instance is shared [MODINV-918](https://folio-org.atlassian.net/browse/MODINV-918)
* Consortial: non-MARC data not saved when local source = MARC instance is promoted [MODINV-960](https://folio-org.atlassian.net/browse/MODINV-960)
* Keep order of MARC fields while Creating/Deriving/Editing MARC records [MODSOURMAN-1137](https://folio-org.atlassian.net/browse/MODSOURMAN-1137)

## 20.1.0 2023-10-13
* Update status when user attempts to update shared auth record from member tenant ([MODDATAIMP-926](https://issues.folio.org/browse/MODDATAIMP-926))
* Add cache to get and store consortium data configurations ([MODINV-872](https://issues.folio.org/browse/MODINV-872))
* Data Import 3rd update Action on Item Record Fails ([MODINV-842](https://issues.folio.org/browse/MODINV-842))
* Adjust Create Instance handler to use existing Instance UUID ([MODINV-840](https://issues.folio.org/browse/MODINV-840))
* Upgrade mod-inventory to Java 17 ([MODINV-826](https://issues.folio.org/browse/MODINV-826))
* Upgrade folio-kafka-wrapper to 3.0.0 version ([MODINV-834](https://issues.folio.org/browse/MODINV-834))
* Add deduplication mechanism for sharing Instance ([MODINV-866](https://issues.folio.org/browse/MODINV-866))
* Allow to overlay source 'MARC' instances without related MARC record ([MODINV-846](https://issues.folio.org/browse/MODINV-847))

## 20.0.0 2023-02-20
* Controlled Instance's field properly reflects updates made by user in MARC Authority's 1XX fields ([MODINV-773] (https://issues.folio.org/browse/MODINV-773))
* POLine ID now set correctly when present in batch imports ([MODINV-774] (https://issues.folio.org/browse/MODINV-774))
* Extend subjects, alternativeTitles, series with Authority ID ([MODINV-761](https://issues.folio.org/browse/MODINV-761))
* Provides `inventory 13.0`
* Provides `inventory-batch 2.0`
* Requires `instance-storage 10.0`
* Requires `instance-storage-batch 2.0`

## 19.0.0 2022-10-25

* Extend instance contributors schema with Authority ID ([MODINV-729](https://issues.folio.org/browse/MODINV-729))
* Delete instance/item by CQL ([MODINV-731](https://issues.folio.org/browse/MODINV-731))
* Removes MODS ingest process ([MODINV-735](https://issues.folio.org/browse/MODINV-735))
* Removes minimal dublin core support for instances ([MODINV-734](https://issues.folio.org/browse/MODINV-734))
* Removes self links from instances and items ([MODINV-734](https://issues.folio.org/browse/MODINV-734))
* Pre-set event type to payload before processing event for entities matching ([MODINV-723](https://issues.folio.org/browse/MODINV-723))
* Cannot add tags to Instance when unlinked preceding/succeeding titles are present ([MODINV-674](https://issues.folio.org/browse/MODINV-674))
* Support MARC-MARC Holdings update action ([MODINV-719](https://issues.folio.org/browse/MODINV-719))
* Use ExternalStorageModuleHoldingsRecordCollection for DI processes ([MODINV-740](https://issues.folio.org/browse/MODINV-740))
* Add new endpoint "/admin/health" ([MODINV-749](https://issues.folio.org/browse/MODINV-749))
* Provides `inventory 12.0`
* Requires `instance-storage 9.0`
* Requires `item-storage 10.0`
* Requires `holdings-storage 2.0 3.0 4.0 5.0 6.0`
* Requires `users 15.0 16.0`

## 18.2.0 2022-06-27

* Add source record preparation before mapping to instance for instance update ([MODINV-686](https://issues.folio.org/browse/MODINV-686))
* Fix http timeouts under huge loads when importing MARC files ([MODSOURMAN-712](https://issues.folio.org/browse/MODSOURMAN-712))
* Update instance import overwrites instance status update date when profile is not editing the instance status ([MODINV-715](https://issues.folio.org/browse/MODINV-715))
* Upgrade liquibase-core, httpclient, guava, and log4j (CVE-2022-0839, CVE-2020-13956) ([MODINV-692](https://issues.folio.org/browse/MODINV-692))
* Import profile that worked on Kiwi is not working on Lotus, OL-related ([MODINV-690](https://issues.folio.org/browse/MODINV-690))
* Instance null pointer exception due to missing _version causes import problems in Kiwi and Lotus ([MODINV-684](https://issues.folio.org/browse/MODINV-684))
* Fixed update of an Instance to not erase administrative note ([MODINV-679](https://issues.folio.org/browse/MODINV-679))
* The status of SRS MARC record is not updated after uploading MARC file with non-existing instanceId in 999ff$i field ([MODINV-678](https://issues.folio.org/browse/MODINV-678))
* Fix latency in creating holdings flow ([MODINV-669](https://issues.folio.org/browse/MODINV-669))
* Added permissions to search for item records to instance retrive by id endpoint ([MODINV-662](https://issues.folio.org/browse/MODINV-662))
* The error appears when save "MARC Holding" record after editing in quickMARC ([MODINV-661](https://issues.folio.org/browse/MODINV-661))
* Add parameter to allow deploying module without Kafka consumers ([MODINV-658](https://issues.folio.org/browse/MODINV-658))
* Fix creation of holdings via data import ([MODINV-657](https://issues.folio.org/browse/MODINV-657))
* Data Import Log > Authority toggle > JSON response does not show ([MODINV-656](https://issues.folio.org/browse/MODINV-656))
* Record matches are not decreased when additional match conditions are added to a job profile ([MODINV-652](https://issues.folio.org/browse/MODINV-652))
* Add logic to skip sending DI_ERROR from mod-inventory in case for duplicates ([MODINV-646](https://issues.folio.org/browse/MODINV-646))
* Delete MARC Authority: Create processor for Delete Authority record ([MODINV-635](https://issues.folio.org/browse/MODINV-635))
* Match logic for POL matches to Inventory records ([MODINV-578](https://issues.folio.org/browse/MODINV-578))
* Match logic for VRN matches to Inventory records ([MODINV-577](https://issues.folio.org/browse/MODINV-577))
* Bound-with-parts gets an empty collection ([MODINV-550](https://issues.folio.org/browse/MODINV-550))

## 18.1.0 2022-02-24

* added Marc Authority handler for entities created via MARC Authority Data Import ([MODINV-501](https://issues.folio.org/browse/MODINV-501))
* added support for new administrative notes field (MODINV-580, MODINV-581, [MODINV-582](https://issues.folio.org/browse/MODINV-582))
* Update Log4j to 2.16.0. (CVE-2021-44228) ([MODINV-599](https://issues.folio.org/browse/MODINV-599))
* Remove Kafka cache for QuickMarcKafkaHandler ([MODINV-614](https://issues.folio.org/browse/MODINV-614)) ([MODINV-599](https://issues.folio.org/browse/MODINV-599))
* Edit MARC Authorities via quickMARC | Update Inventory Authorities ([MODINV-503](https://issues.folio.org/browse/MODINV-503))
* Fixed incorrect optimistic locking behavior for instance update when uploading file via data import ([MODINV-546](https://issues.folio.org/browse/MODINV-546))
* Fixed holdings record created via MARC Bib Data Import shows incorrect source ([MODINV-549](https://issues.folio.org/browse/MODINV-549))
* Fixed put for mappingParams into dataImportEventPayload ([MODINV-571](https://issues.folio.org/browse/MODINV-571))

## 18.0.0 2021-10-06

* Adds `boundWithTitles` property to individual bound-with item ([MODINV-443](https://issues.folio.org/browse/MODINV-443))
* Removed explicit dependencies on mod-source-record-manager and mod-data-import-converter-storage clients ([MODINV-541](https://issues.folio.org/browse/MODINV-541))
* Provides `inventory 11.0`
* Provides `inventory-batch 1.0`
* Provides `inventory-config 0.2`
* Requires `item-storage 8.0 or 9.0`
* Requires `instance-storage 7.8 or 8.0`
* Requires `instance-storage-batch 0.2 or 1.0`
* Requires `holdings-storage 2.0, 3.0, 4.0 or 5.0`
* Optionally requires `mapping-metadata-provider 1.0`
* Optionally requires `data-import-converter-storage 1.2`

## 17.0.0 2021-06-15

* Sets `isBoundWith` flag on instances and items ([MODINV-388](https://issues.folio.org/browse/MODINV-388))
* Provides end-point `/inventory/items-by-holdings-id` ([MODINV-418](https://issues.folio.org/browse/MODINV-418))
* Introduces `publication period` for instances ([MODINV-428](https://issues.folio.org/browse/MODINV-428))
* QuickMARC updates now use Kafka instead of pub-sub ([MODINV-407](https://issues.folio.org/browse/MODINV-407))
* Includes fix for item update failure ([MODINV-404](https://issues.folio.org/browse/MODINV-404))
* No longer removes instance tags during MARC record import ([MODINV-419](https://issues.folio.org/browse/MODINV-419))
* Preceding/succeeding titles may be updated during MARC record import (MODINV-429, [MODINV-434](https://issues.folio.org/browse/MODINV-434))
* Introduces parameters to set a number of data import verticle instances (MODINV-393, [MODINV-423](https://issues.folio.org/browse/MODINV-423))
* Provides support for USER_CONTEXT in handling QM-update events ([MODINV-399](https://issues.folio.org/browse/MODINV-399))
* Provides `inventory 10.12`
* No longer provides `inventory-event-handlers 1.0`
* No longer provides `_tenant 1.2`
* No longer provides `pubsub-event-types 0.1`
* No longer provides `pubsub-publishers 0.1`
* No longer provides `pubsub-subscribers 0.1`
* No longer provides `pubsub-publish 0.1`
* Requires `instance-storage 7.8`
* Requires `bound-with-parts-storage 1.0`
* Requires `source-storage-records 3.0`

## 16.3.0 2021-03-26

* Introduces `contributorsNames` CQL index for searching `instances` ([MODINV-390](https://issues.folio.org/browse/MODINV-390))
* Tracks processed data import events to ensure events are only processed once ([MODINV-373](https://issues.folio.org/browse/MODINV-373))
* Provides `inventory 10.10`
* Requires `instance-storage 7.7`

## 16.2.0 2020-03-15

* Uses Kafka to coordinate data import process ([MODINV-326](https://issues.folio.org/browse/MODINV-326))
* Allows matching between MARC records during import ([MODINV-379](https://issues.folio.org/browse/MODINV-379))
* Can mark an item as `Unavailable` (MODINV-356, [MODINV-366](https://issues.folio.org/browse/MODINV-366))
* Can mark an item as `In process` (MODINV-356, [MODINV-366](https://issues.folio.org/browse/MODINV-366))
* Can mark an item as `In process (non-requestable)` (MODINV-356, [MODINV-366](https://issues.folio.org/browse/MODINV-366))
* Can mark an item as an `Intellectual item` (MODINV-356, [MODINV-366](https://issues.folio.org/browse/MODINV-366))
* Can mark an item as `Long missing` (MODINV-356, [MODINV-366](https://issues.folio.org/browse/MODINV-366))
* Can mark an item as `Restricted` (MODINV-356, [MODINV-366](https://issues.folio.org/browse/MODINV-366))
* Can mark an item as `Unknown` (MODINV-356, [MODINV-366](https://issues.folio.org/browse/MODINV-366))
* Includes `effective shelving order` property for items ([MODINV-155](https://issues.folio.org/browse/MODINV-155))
* Upgrades to vert.x 4.0.0 ([MODINV-371](https://issues.folio.org/browse/MODINV-371))
* Provides `inventory 10.9`
* Provides `inventory-event-handlers 1.0`
* Requires `item-storage 8.9`
* Requires `Kafka 2.6`

## 16.1.0 2020-10-12

* Provides the ability to move items between holdings ([MODINV-309](https://issues.folio.org/browse/MODINV-309))
* Provides the ability to move holdings between instances ([MODINV-309](https://issues.folio.org/browse/MODINV-309))
* Marking an item as missing changes request status ([MODINV-312](https://issues.folio.org/browse/MODINV-312))
* Introduces `match key` property for instances
* Introduces `Unknown` and `Aged to lost` item statuses ([MODINV-299](https://issues.folio.org/browse/MODINV-299))
* Can map public and staff holdings holdings statement notes during import ([MODINV-343](https://issues.folio.org/browse/MODINV-343))
* Restricts item status transitions during import ([MODINV-296](https://issues.folio.org/browse/MODINV-296))
* Fixes instance statistical code and nature of content import issues ([MODINV-336](https://issues.folio.org/browse/MODINV-336))
* Fixes problem with the repeatable check in/out notes field mapping actions ([MODINV-346](https://issues.folio.org/browse/MODINV-346))
* Saves source record state during quick MARC edit ([MODINV-324](https://issues.folio.org/browse/MODINV-324))
* Requires JDK 11 ([MODINV-335](https://issues.folio.org/browse/MODINV-335))
* Provides `inventory 10.7`
* Requires `item-storage 8.6`
* Requires `instance-storage 7.5`

## 16.0.0 2020-06-25

* Update SRS client requests for v4.0.0, change dependency on interface (from source-storage-suppress-discovery 1.0 to source-storage-records 2.0) ([MODINV-316](https://issues.folio.org/browse/MODINV-316))
* Fix adding/changing Instance notes triggered by quickMarc updates ([MODINV-311](https://issues.folio.org/browse/MODINV-311))

## 15.0.0 2020-06-11

* Updates instances based upon imported MARC records (MODINV-207, MODINV-243
* Updates holdings based upon imported MARC records (MODINV-232,
* Updates items based upon imported MARC records (MODINV-259,
* Can add preceding / succeeding titles during MARC record import (MODINV-302, [MODINV-294](https://issues.folio.org/browse/MODINV-294))
* Can mark items withdrawn (MODINV-216, MODINV-286, [MODINV-287](https://issues.folio.org/browse/MODINV-287))
* Introduces `Lost and paid` item status ([MODINV-291](https://issues.folio.org/browse/MODINV-291))
* Title is required for unconnected preceding / succeeding titles ([MODINV-218](https://issues.folio.org/browse/MODINV-218))
* Doubled recommended memory usage to support receiving pub-sub messages ([MODINV-279](https://issues.folio.org/browse/MODINV-279))
* Provides `inventory 10.5`
* Requires `item-storage 8.4`
* Requires `request-storage 3.3`
* Requires `source-storage-suppress-discovery 1.0`

## 14.1.0 2020-03-28

* Processes data import events to create or update inventory records (MODINV-182, MODINV-182, MODINV-183, MODINV-184, MODINV-201, [MODINV-204](https://issues.folio.org/browse/MODINV-204))
* Fetches preceding and succeeding titles in batches partitioned by `id` ([MODINV-212](https://issues.folio.org/browse/MODINV-212))

## 14.0.0 2020-03-09

* Makes item status required ([MODINV-189](https://issues.folio.org/browse/MODINV-189))
* Makes item status date read-only ([MODINV-189](https://issues.folio.org/browse/MODINV-189))
* Allows only one copy number for an item ([MODINV-189](https://issues.folio.org/browse/MODINV-189))
* Includes `effective call number` for items ([MODINV-177](https://issues.folio.org/browse/MODINV-177))
* Introduces `Claimed returned` item status (MODINV-194, [MODINV-200](https://issues.folio.org/browse/MODINV-200))
* Introduces preceding and succeeding titles ([MODINV-198](https://issues.folio.org/browse/MODINV-198))
* Includes `last check in` for items (MODINV-173, [MODINV-190](https://issues.folio.org/browse/MODINV-190))
* Disallows editing of mode of issuance when instance has underlying MARC record ([MODINV-192](https://issues.folio.org/browse/MODINV-192))
* Allows editing of cataloged date when instance has underlying MARC record ([MODINV-202](https://issues.folio.org/browse/MODINV-202))
* Registers event subscriptions with pub sub ([MODINV-181](https://issues.folio.org/browse/MODINV-181))
* Provides `inventory 10.2`
* Provides `inventory-batch 0.5`
* Provides `_tenant 1.2`
* Requires `item-storage 8.1`
* Requires `instance-preceding-succeeding-titles 0.1`
* Requires `pub-sub-event-types 0.1`
* Requires `pubsub-publishers 0.1`
* Requires `pubsub-subscribers 0.1`
* Requires `pubsub-publish 0.1`

## 13.1.0 2019-12-03

* Generates HRIDs for instance and item records (MODINV-160, [MODINV-162](https://issues.folio.org/browse/MODINV-162))
* Allow editing of instance relationships when source is MARC ([MODINV-163](https://issues.folio.org/browse/MODINV-163))
* Includes `effective location ID` property for instances ([MODINV-157](https://issues.folio.org/browse/MODINV-157))
* Includes `effective call number` properties for instances ([MODINV-174](https://issues.folio.org/browse/MODINV-174))
* Includes `last check in` properties for items ([MODINV-173](https://issues.folio.org/browse/MODINV-173))
* Forwards X-Okapi-Request-Id ([MODINV-156](https://issues.folio.org/browse/MODINV-156))
* Changes container memory management (MODINV-175, [FOLIO-2358](https://issues.folio.org/browse/FOLIO-2358))
* Provides `inventory 9.6`
* Requires `item-storage 7.8`
* Requires `holdings-storage 2.0, 3.0 or 4.0`
* No longer requires `source-record-storage 2.2`

## 13.0.0 2019-09-09

* Adds nature of content terms to `instances` ([MODINV-149](https://issues.folio.org/browse/MODINV-149))
* Adds tags to `items` ([MODINV-147](https://issues.folio.org/browse/MODINV-147))
* Adds tags to `instances` ([MODINV-146](https://issues.folio.org/browse/MODINV-146))
* `ISBN validation` uses 422 responses instead of 400 for errors ([MODINV-139](https://issues.folio.org/browse/MODINV-139))
* `Instance batch` API uses batch storage API ([MODINV-134](https://issues.folio.org/browse/MODINV-134))
* Provides `inventory 9.4` interface (MODINV-149, MODINV-146, [MODINV-147](https://issues.folio.org/browse/MODINV-147))
* Provides `inventory-batch 0.3` interface ([MODINV-149](https://issues.folio.org/browse/MODINV-149))
* Provides `isbn-utils 2.0` interface ([MODINV-139](https://issues.folio.org/browse/MODINV-139))
* Requires `item-storage 7.5` interface ([MODINV-147](https://issues.folio.org/browse/MODINV-147))
* Requires `instance-storage 7.2` interface ([MODINV-149](https://issues.folio.org/browse/MODINV-149))

## 12.0.0 2019-07-23

* Provides `inventory` interface 9.1 (MODINV-132, [MODINV-129](https://issues.folio.org/browse/MODINV-129))
* Provides `inventory-batch` interface 0.2 ([MODINV-132](https://issues.folio.org/browse/MODINV-132))
* Provides `inventory-config` interface 0.1 ([MODINV-121](https://issues.folio.org/browse/MODINV-121))
* Requires `item-storage` interface 7.4 ([MODINV-129](https://issues.folio.org/browse/MODINV-129))
* Requires `instance-storage` interface 7.0 ([MODINV-132](https://issues.folio.org/browse/MODINV-132))
* Propagates changed instance notes structure to/from storage ([MODINV-132](https://issues.folio.org/browse/MODINV-132))
* Adds configuration for protected instance fields ([MODINV-124](https://issues.folio.org/browse/MODINV-124))
* Generates source, date for circulation notes ([MODINV-129](https://issues.folio.org/browse/MODINV-129))

## 11.5.0 2019-06-11

* Provides `inventory-batch` interface 0.1 ([MODINV-119](https://issues.folio.org/browse/MODINV-119))

## 11.4.0 2019-05-09

* Adds Instance contributor names to Item representation ([MODINV-112](https://issues.folio.org/browse/MODINV-112))
* Adds missing module permissions for managing instance relationships ([MODINV-113](https://issues.folio.org/browse/MODINV-113))
* Propagates property `primary` of Instance contributors ([MODINV-117](https://issues.folio.org/browse/MODINV-117))

## 11.3.0 2019-03-15

* Optimizes use of HttpClients ([MODINV-102](https://issues.folio.org/browse/MODINV-102))
* Use Alpine docker container ([MODINV-100](https://issues.folio.org/browse/MODINV-100))
* Return new `Item` as JSON on POST ([MODINV-106](https://issues.folio.org/browse/MODINV-106))

## 11.2.0 2019-02-19

* Provides `inventory` interface 8.3 ([MODINV-104](https://issues.folio.org/browse/MODINV-104))
* Requires `item-storage` interface 7.3 ([MODINV-104](https://issues.folio.org/browse/MODINV-104))
* Adds `status.date` to `item` schema ([MODINV-104](https://issues.folio.org/browse/MODINV-104))

## 11.1.0 2019-02-01

* Provides `inventory` interface 8.2 ([MODINV-101](https://issues.folio.org/browse/MODINV-101) [MODINV-103](https://issues.folio.org/browse/MODINV-103))
* Requires `item-storage` interface 7.2 ([MODINV-101](https://issues.folio.org/browse/MODINV-101) [MODINV-103](https://issues.folio.org/browse/MODINV-103))
* Adds `circulationNotes' to `item` schema ([MODINV-101](https://issues.folio.org/browse/MODINV-101))
* Adds `purchaseOrderLineIdentifier` to `item` schema ([MODINV-103](https://issues.folio.org/browse/MODINV-103))

## 11.0.0 2018-11-30

* Provides `inventory` interface 8.0 ([MODINV-90](https://issues.folio.org/browse/MODINV-90))
* Requires `holdings-storage` interface 2.0 or 3.0 ([MODINV-93](https://issues.folio.org/browse/MODINV-93))
* Requires `instance-storage` interface 6.0 ([MODINV-90](https://issues.folio.org/browse/MODINV-90))
* Requires `item-storage` interface 7.0 ([MODINV-94](https://issues.folio.org/browse/MODINV-94))
* Changes structure of `alternativeTitles` ([MODINV-90](https://issues.folio.org/browse/MODINV-90))
* Renames `item.pieceIdentifiers` to `copyNumbers` ([MODINV-94](https://issues.folio.org/browse/MODINV-94))
* Changes structure of `item` `notes` ([MODINV-97](https://issues.folio.org/browse/MODINV-97))
* Renames, changes structure of `instance.statisticalCodes` ([MODINV-98](https://issues.folio.org/browse/MODINV-98))
* Adds 20 new properties to `item` ([MODINV-72](https://issues.folio.org/browse/MODINV-72))

## 10.0.0 2018-10-12

* Propagates `instance.indexTitle` ([MODINV-88](https://issues.folio.org/browse/MODINV-88))
* Requires `locations` interface 2.0 or 3.0 ([MODINV-89](https://issues.folio.org/browse/MODINV-89))
* Upgrades to RAML 1.0 ([MODINV-85](https://issues.folio.org/browse/MODINV-85))
* Removes `instance.edition` ([MODINV-78](https://issues.folio.org/browse/MODINV-78))
* Adds `instance.editions` ([MODINV-78](https://issues.folio.org/browse/MODINV-78))
* Removes `instanceFormatId` ([MODINV-86](https://issues.folio.org/browse/MODINV-86))
* Adds `instanceFormatIds` ([MODINV-86](https://issues.folio.org/browse/MODINV-86))
* Provides `inventory` interface 7.0 ([MODINV-83](https://issues.folio.org/browse/MODINV-83))
* Requires `instance-storage` interface version 5.0 ([MODINV-83](https://issues.folio.org/browse/MODINV-83))
* Removes property `instance.urls` ([MODINV-81](https://issues.folio.org/browse/MODINV-81))
* Removes property `categoryLevelId` ([MODINV-82](https://issues.folio.org/browse/MODINV-82))
* Renames `instance.electronicAccess.relationship` to `relationshipId` ([MODINV-84](https://issues.folio.org/browse/MODINV-84))

## 9.5.0 2018-09-13

* Propagates more properties from `instance-storage` ([MODINV-71](https://issues.folio.org/browse/MODINV-71))
* Provides `inventory` interface 6.4 ([MODINV-71](https://issues.folio.org/browse/MODINV-71))
* Requires `instance-storage` interface version 4.6 ([MODINV-71](https://issues.folio.org/browse/MODINV-71))

## 9.4.0 2018-09-10

* Implements Instance relationships ([MODINV-69](https://issues.folio.org/browse/MODINV-69))
* Adds optional Instance properties `parentInstances`, `childInstances` ([MODINV-69](https://issues.folio.org/browse/MODINV-69))
* Provides `inventory` interface 6.3 ([MODINV-69](https://issues.folio.org/browse/MODINV-69))
* Requires `instance-storage` interface version 4.5 ([MODINV-69](https://issues.folio.org/browse/MODINV-69))

## 9.3.0 2018-08-30

* Include `callNumber` from holding in item (if present, [MODINV-70](https://issues.folio.org/browse/MODINV-70))
* Provides `inventory` 6.2 interface ([MODINV-70](https://issues.folio.org/browse/MODINV-70))

## 9.2.0 2018-08-27

* Add properties `alternativeTitles`, `edition`, `series`, `subjects`, `classifications`,
  `publication`, `urls`, `instanceFormatId`, `physicalDescriptions`, `languages`,
  `notes`, `sourceRecordFormat`, `metadata` to `Instance` schema ([MODINV-68](https://issues.folio.org/browse/MODINV-68))

## 9.1.0 2018-07-10

* Reference record lookups use == CQL relation, instead of = ([MODINV-64](https://issues.folio.org/browse/MODINV-64))

## 9.0.0 2018-07-06

* Includes item `effectiveLocation` property for overall location of item ([MODINV-56](https://issues.folio.org/browse/MODINV-56))
* Changes item `permanentLocation` to be property from storage instead of derived ([MODINV-56](https://issues.folio.org/browse/MODINV-56))
* Provides `inventory` interface 6.0 ([MODINV-56](https://issues.folio.org/browse/MODINV-56))
* Requires `item-storage` interface 5.3 ([MODINV-56](https://issues.folio.org/browse/MODINV-56))
* Requires `holdings-storage` interface 1.3 ([MODINV-56](https://issues.folio.org/browse/MODINV-56))
* Requires `instance-types` interface 1.0 or 2.0 ([MODINV-60](https://issues.folio.org/browse/MODINV-60))

## 8.0.0 2018-05-15

* Expose change `metadata` property for items ([MODINV-47](https://issues.folio.org/browse/MODINV-47))
* Extend the offset and limit paging query parameters to allow maximum integer values ([MODINV-55](https://issues.folio.org/browse/MODINV-55))
* Provides `inventory` interface 5.2 (MODINV-47, [MODINV-55](https://issues.folio.org/browse/MODINV-55))
* Requires `locations` interface 1.0 or 2.0 ([MODINV-49](https://issues.folio.org/browse/MODINV-49))
* No longer requires `shelf-locations` interface ([MODINV-49](https://issues.folio.org/browse/MODINV-49))

## 7.1.0 2018-04-03

* Use exact match for barcode and ID lookups using CQL ([MODINV-43](https://issues.folio.org/browse/MODINV-43))
* Add optional field `contributorTypeText` to `instance.contributors` ([MODINV-52](https://issues.folio.org/browse/MODINV-52))
* Provides inventory interface version 5.1
* Requires instance-storage interface version 4.2

## 7.0.0 2018-01-09

* Require `holdingsRecordId` from `item` ([MODINV-39](https://issues.folio.org/browse/MODINV-39))
* Removes `title` from `item` ([MODINV-37](https://issues.folio.org/browse/MODINV-37))
* Removes `instanceId` from `item` ([MODINV-38](https://issues.folio.org/browse/MODINV-38))
* Removes `permanentLocationId` from `item` ([MODINV-37](https://issues.folio.org/browse/MODINV-37))
* Removes `creators` from `instance` ([MODINV-41](https://issues.folio.org/browse/MODINV-41))
* Adds `contributorNameTypeId` to `contributors` in `instances` ([MODINV-41](https://issues.folio.org/browse/MODINV-41))
* Provides inventory interface version 5.0
* Requires item-storage interface version 5.0
* Requires instance-storage interface version 4.0

## 6.0.0 2017-12-20

* Removes `location` property from `item` record
* `title` is now optional for an `item` ([MODINV-34](https://issues.folio.org/browse/MODINV-34))
* Adds `holdingsRecordId` to item ([MODINV-30](https://issues.folio.org/browse/MODINV-30))
* MODS ingest now creates holdings records ([MODINV-30](https://issues.folio.org/browse/MODINV-30))
* Introduces `enumeration`, `chronology`, `numberOfPieces`, `notes`, `pieceIdentifiers` ([MODINV-33](https://issues.folio.org/browse/MODINV-33))
* Introduces `permanentLocation` and `temporaryLocation` properties (which include the name fetched from `shelf-locations`)
* Provides inventory interface version 4.2
* Requires item-storage interface version 4.1
* Requires instance-storage interface version 3.0
* Requires holding-storage interface version 1.0
* Requires shelf-locations interface version 1.0
* Requires instance-types interface version 1.0
* Requires identifier-types interface version 1.0
* Requires contributor-name-types interface version 1.0
* Adds mod- prefix to names of the built artifacts ([FOLIO-813](https://issues.folio.org/browse/FOLIO-813))

## 5.1.1 2017-09-01

* Generates Descriptors at build time from templates in ./descriptors ([FOLIO-701](https://issues.folio.org/browse/FOLIO-701))
* Remove `module.items.enabled` and `inventory-storage.all` permissions from
`inventory.all` set, as part of moving permissions to UI modules ([MODINV-23](https://issues.folio.org/browse/MODINV-23))

## 5.1.0 2017-08-03

* [MODINVSTOR-12](https://issues.folio.org/browse/MODINVSTOR-12) Searching and sorting on material type properties (e.g. materialType.name)
* Provides inventory interface version 2.1 (notes additional CQL indexes in comments)
* Requires item-storage interface version 3.1
* Include implementation version in `id` in Module Descriptor
* [MODINV-18](https://issues.folio.org/browse/MODINV-18) - Fix for unique barcode check when changing barcode

## 5.0.0 2017-06-07

* Items do not require relating to an instance (instanceId is optional)
* Items do not require a barcode (no uniqueness check performed when not supplied)
* Items require a title
* Items require a reference to a material type
* Items require a reference to a permanent loan type
* Inventory.all permission set includes permissions for related UI tasks
* Requires item-storage interface version 3.0
* Requires instance-storage interface version 2.0
* Requires material-types interface version 2.0
* Requires loan-types interface version 2.0

## 4.4.0 2017-05-31

* Makes the all inventory permissions set visible (requires Okapi 1.3.0)
* Includes permission definition for enabling the items UI module (included in all permissions set)

## 4.3.0 2017-05-09

* Include total record count in item and instance collection resources

## 4.2.0 2017-05-08

* Provide permanent and temporary loan type associations for items
* Include name of loan type in item representation (by querying loan type controlled vocabulary)

## 4.0.0 2017-04-25

* Use UUID to reference material types

## 3.0.0 2017-04-04

* Required permissions for requests

## 2.0.0 2017-04-03

* Initial release
