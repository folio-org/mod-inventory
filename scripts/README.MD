# Scripts Information

## When Using deleted_synchronization.py script

This script is intended to synchronize deleted records between Inventory and SRS. It searches for records with leader `d`
or `DELETED` state in SRS and sets `deleted = true` for the corresponding Inventory instance. This allows OAI-PMH
harvesting to include deleted records. Also, this script logs successful login, login refresh, id of updated records,
total time taken, total records to update and any errors encountered during GET or PUT requests.

### Script location

`scripts/deleted_synchronization.py`

```bash
pip install requests psycopg2-binary
```
### Run (use your own values for env variables!)

```terminaloutput
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=folio
export DB_USER=folio
export DB_PASSWORD=postgres_password
export API_HOST=localhost
export API_PORT=8000
export USERNAME=diku_admin
export PASSWORD=admin
export TENANT=diku
python deleted_synchronization.py
```
### What the script updates

For each matched instance, the script does a GET and then PUT to `/inventory/instances/{id}` and sets:

- `deleted = true`
- `discoverySuppress = true`
- `staffSuppress = true`

### Recommended safety checks

- Run first in a non-production environment.
- Run the following script to check amount of records to be updated before executing the main script:

```sql
SELECT COUNT(*) FROM {TENANT}_mod_source_record_storage.records_lb record_lb
JOIN {TENANT}_mod_inventory_storage.instance inst
ON record_lb.external_id = inst.id 
WHERE inst.jsonb ->> 'deleted' != 'true' 
AND record_type = 'MARC_BIB'
  AND ((record_lb.leader_record_status = 'd' AND (record_lb.state = 'ACTUAL' OR record_lb.state = 'DELETED')) OR record_lb.state = 'DELETED');
```
- Validate DB/API credentials and tenant before execution.
- Review output logs for `GET failed` or `PUT failed` messages.

### Testing
The script was tested on https://folio-eperf-corsair-fs09000000.ci.folio.org and for 55k records it
took 2.1 hours.
