import json
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
    AuthProvider
)

from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig
)

from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseService,
    DatabaseServiceType,
    DatabaseConnection,
)

from metadata.generated.schema.entity.services.connections.database.snowflakeConnection import (
    SnowflakeConnection
)

from metadata.generated.schema.api.data.createDatabase import (
    CreateDatabaseRequest,
)
from metadata.generated.schema.type.entityReference import EntityReference

from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)

from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import (
    Column,
    DataType,
    Table,
)
from metadata.generated.schema.api.data.createLocation import CreateLocationRequest
from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.data.createTopic import CreateTopicRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.api.services.createStorageService import (
    CreateStorageServiceRequest,
)
from metadata.generated.schema.api.services.createPipelineService import (
    CreatePipelineServiceRequest,
)

from metadata.generated.schema.api.services.createMessagingService import (
    CreateMessagingServiceRequest,
)

from metadata.generated.schema.entity.services.connections.pipeline.airflowConnection import (AirflowConnection)
from metadata.generated.schema.entity.data.dashboard import Dashboard
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.location import Location
from metadata.generated.schema.entity.data.pipeline import Pipeline, PipelineStatus, Task
from metadata.generated.schema.entity.services.dashboardService import DashboardService
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.services.messagingService import MessagingService
from metadata.generated.schema.entity.services.mlmodelService import MlModelService
from metadata.generated.schema.entity.services.pipelineService import PipelineService
from metadata.generated.schema.entity.services.storageService import StorageService

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type import basic, entityReference, schema, tagLabel



# to connect to OpenMetadata Server
security_config = OpenMetadataJWTClientConfig(jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJpbmdlc3Rpb24tYm90IiwiaXNCb3QiOnRydWUsImlzcyI6Im9wZW4tbWV0YWRhdGEub3JnIiwiaWF0IjoxNjc3OTA3MDA5LCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyJ9.oufyMJVWNFv4jMfRhl49JMFW5cOvGJVMIbKuZwCBtowdct3L-pIS5O6cEQUitrIQS5MCFDms9F8zaiKCF1I8_OVgLAVRuhAzI6YrlA25E_Ge5x7AlaJuXpPJvgNh-SpjPnW3YS585CyAYhNRBIcEpQT4DLhqINFSa-46klitgpJ3tLVT4iNbrRgW-cKGZcwgShM2UkZ5dJbdGO95_1OqHLxZ-fy8FoYA6gkcsCTuPok_mBDgrYX3B667ETWOHxIaNF9SE79bYtTYbNFnkTWb29yX4bffOibqxjtNG7Hy1C02mxFiApxGdzPN1psKUeyQYdWukXaFaot34TxfnGFllg")
server_config = OpenMetadataConnection(hostPort="http://107.150.111.66:8585/api", securityConfig=security_config, authProvider=AuthProvider.openmetadata)
metadata = OpenMetadata(server_config)
metadata.health_check() # we are able to connect to OpenMetadata Server

print(metadata.health_check())

# Create DatabaseService
#
#
create_service = CreateDatabaseServiceRequest(
    name="web3_meta",
    serviceType=DatabaseServiceType.Snowflake,
    connection=DatabaseConnection(
        config=SnowflakeConnection(
            username="username",
            password="password",
            account="eth",
            warehouse="web3"
        )
    ),
)

service_entity = metadata.create_or_update(data=create_service)

# Create Database
#
create_db = CreateDatabaseRequest(
    name="crypto",
    service=EntityReference(id=service_entity.id, type="databaseService"),
)

db_entity = metadata.create_or_update(create_db)


create_schema = CreateDatabaseSchemaRequest(
    name="aave_v3",
    database=EntityReference(id=db_entity.id, type="database")
)

schema_entity = metadata.create_or_update(data=create_schema)

# We can prepare the EntityReference that will be needed
# in the next step!
schema_reference = EntityReference(
    id=schema_entity.id, name="aave_v3", type="databaseSchema"
)


# create_table = CreateTableRequest(
#     name="dim_orders",
#     databaseSchema=schema_reference,
#     columns=[Column(name="id", dataType=DataType.BIGINT)],
# )

# table_entity = metadata.create_or_update(create_table)

# Opening JSON file
f = open('manifest.json')
  
# returns JSON object as 
# a dictionary
data = json.load(f)

all_tables = {}

for k in data['nodes']:
    table_name = data['nodes'][k]['name']
    node_db_name = data['nodes'][k]['fqn'][1]
    node_schema = data['nodes'][k]['fqn'][2]
    node_type = data['nodes'][k]['resource_type']
    
    print("read table name: ", table_name)
    print("read db name: ", node_db_name)
    print("read schema name: ", node_schema)
    if node_type != 'model':
        continue
    
    # Create Database
    #
    create_db = CreateDatabaseRequest(
        name=node_db_name,
        service=EntityReference(id=service_entity.id, type="databaseService"),
    )

    db_entity = metadata.create_or_update(create_db)
    
    create_schema = CreateDatabaseSchemaRequest(
    name=node_schema,
    database=EntityReference(id=db_entity.id, type="database")
    )

    schema_entity = metadata.create_or_update(data=create_schema)

    # We can prepare the EntityReference that will be needed
    # in the next step!
    schema_reference = EntityReference(
        id=schema_entity.id, name=node_schema, type="databaseSchema"
    )
    
    columns = []
    for kk in data['nodes'][k]['columns']:
        columns.append(Column(name=data['nodes'][k]['columns'][kk]['name'], dataType=DataType.STRING))
    
    print('create table: ', table_name)
    create_table = CreateTableRequest(
    name=table_name,
    databaseSchema=schema_reference,
    columns=columns,
    )   

    table_entity = metadata.create_or_update(create_table)
    
    all_tables[k] = table_entity
    
    #test
    #break

print("load source type!")
for k in data['sources']:
    table_name = data['sources'][k]['name']
    node_db_name = data['sources'][k]['fqn'][1]
    node_schema = data['sources'][k]['fqn'][2]
    node_type = data['sources'][k]['resource_type']
    
    print("read table name: ", table_name)
    print("read db name: ", node_db_name)
    print("read schema name: ", node_schema)
    if node_type != 'source':
        continue
    
    # Create Database
    #
    create_db = CreateDatabaseRequest(
        name=node_db_name,
        service=EntityReference(id=service_entity.id, type="databaseService"),
    )

    db_entity = metadata.create_or_update(create_db)
    
    create_schema = CreateDatabaseSchemaRequest(
    name=node_schema,
    database=EntityReference(id=db_entity.id, type="database")
    )

    schema_entity = metadata.create_or_update(data=create_schema)

    # We can prepare the EntityReference that will be needed
    # in the next step!
    schema_reference = EntityReference(
        id=schema_entity.id, name=node_schema, type="databaseSchema"
    )
    
    columns = []
    for kk in data['sources'][k]['columns']:
        columns.append(Column(name=data['sources'][k]['columns'][kk]['name'], dataType=DataType.STRING))
    
    print('create table: ', table_name)
    create_table = CreateTableRequest(
    name=table_name,
    databaseSchema=schema_reference,
    columns=columns,
    )   

    table_entity = metadata.create_or_update(create_table)
    
    all_tables[k] = table_entity

print("load lineage!")
for k in data['nodes']:
    if node_type != 'model':
        continue
    depend_tables = data['nodes'][k]['depends_on']["nodes"]
    for kk in depend_tables:
    
        add_lineage_request = AddLineageRequest(
        description="lineage",
        edge=EntitiesEdge(
            fromEntity=EntityReference(id=all_tables[k].id, type="table"),
            toEntity=EntityReference(id=all_tables[kk].id, type="table"),
        ),
        )

        created_lineage = metadata.add_lineage(data=add_lineage_request)
 