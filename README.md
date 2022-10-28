# Microsoft SQL Server Data Layer

A Data Layer for Microsoft SQL Server (https://www.microsoft.com/en-ca/sql-server) that conforms to the Universal Data API specification (https://open.mimiro.io/specifications/uda/latest.html). This data layer can be used in conjunction with the MIMIRO data hub (https://github.com/mimiro-io/datahub) to create a modern data fabric. The Microsoft SQL Server data layer can be configured to expose tables and views from a MySQL database as a stream of changes or a current snapshot. Rows in a table are represented in JSON according the Entity Graph Data model that is described in the UDA specification. This data layer can be run as a standalone binary or as a docker container.

Releases of this data layer are published to docker hub in the repository: `mimiro/mssql-datalayer`

## Configuration

### General settings

The layer can take a few configuration options, that can (and should) be set via Environment variables.

```PROFILE=local```

The profile is used to determine how to set up logging. If set to "local", then a human readable log formatting is used, if set to anything else (like dev/prod/), it will log in Json format. Local profile is not suitable for production usage.

```SERVICE_NAME=mssql-datalayer```

This is used for logging and metrics purposes, and should be unique to recognize the service in your organization.

```SERVER_PORT=8080```

The server port the service is answering on. You should always set an Https proxy in front of this Datalayer, as it is not capable of terminating TLS connections.

```LOG_LEVEL=INFO```

What log level to log with. Supported levels are DEBUG, INFO, WARN and ERROR in that order. We recommend either to run with INFO or DEBUG in production.

```DD_AGENT_HOST=```

When set, this enables metrics using StatsD. You need to point this to your own operated StatsD service, we recommend deploying the Datalayer with a StatsD sidecar container. This is used together with the SERVICE_NAME to report the correct service.

### Dataset configuration

```CONFIG_LOCATION=```

This is the location of the configuration of the datasets. 2 protocols are supported:

 * file:// - reads a file from a local file system
 * http(s):// - reads a file from an http location

 A remote file must support the authentication scheme configured with the "AUTH0_*" confgiurations, and it must be in the following format:

 ```json
{
    "id": "config-name",
    "data": {
        // json blob in format described below
    }
}
 ```

```CONFIG_REFRESH_INTERVAL=@every 120s```

The datalayer supports dynamic reconfiguring, and this is the interval of how often it will attempt to refresh it's configuration.

### Securing the Datalayer

You need to secure your server.

```AUTHORIZATION_MIDDLEWARE=auth0```

This tells the server what security middleware to run. Currently supported middlewares are "noop" (no security at all), and "auth0". Currently the only supported security layer is Auth0 (which is compatible with Mimiro's auth solution as well)

The Datalayer also currently supports validating both machine tokens with scopes = ["datahub:r"] and user tokens with Adm = true set. This is likely to change in the future.

```TOKEN_WELL_KNOWN=https://auth.yoursite.io/jwks/.well-known/jwks.json```

This must point to the well known endpoint that contains the x.509 certificate you want to validate with.

```TOKEN_AUDIENCE=https://api.yoursite.io```

This is the audience you want to validate the token for.

```TOKEN_ISSUER=https://api.yoursite.io```

The issuer of the token you are validating.

```TOKEN_AUDIENCE_AUTH0=https://api.yourtokenissuer.io```

This is an alternative audience to validate Auth0 machine tokens against. This will most likelly be removed in the future.

```TOKEN_ISSUER_AUTH0=https://yourtokenissuer.auth0.com/```

This is an alternative issuer to validate Auth0 machine tokens with. This will most likelly be removed in the future.

### Outgoing authentication

If a datalayer configuration is fetched from the datahub, then Authentication against it is needed.
You can do this with using either an Auth0 token, or an Auth0 compatible one (for example Mimiro). It should be a machine token. It needs the scopes = ["datahub:r"] or adm = true.

```AUTH0_CLIENT_ID=```

Client Id of the machine token

```AUTH0_CLIENT_SECRET=```

Client secret of the machine token

```AUTH0_AUDIENCE=```

Audience of the token

```AUTH0_GRANT_TYPE=```

The grant type. Should be "client_credentials", but "app_credentials" are also supported.

```AUTH0_ENDPOINT=```

Auth0 (or Jwt) endpoint to call to get a token.

### Misc

```MSSQL_DB_USER=```

This is the user of the database you are connecting to.

```MSSQL_DB_PASSWORD=```

This is the password of the database you are connecting to. This should be injected from a secure store, for example AWS SSM.

## Configuration

The service is configured with either a local json file or a remote variant of the same.
It is strongly recommended to leave the Password and User fields empty.


```json
{
    "id": "test-import",
    "databaseServer": "test.testserver.io",
    "baseUri": "http://data.test.io/le-data/",
    "database": "my_test",
    "port": "1433",
    "instance":"namedInstance",
    "schema": "dbo",
    "user": "<user>",
    "password": "<password>",
    "baseNameSpace": "http://data.test.io/le-data/",
    "postMappings": [
        {
            "config": {
                "database": "MYDB1",
                "password": {
                    "key": "password",
                    "type": "env"
                },
                "user": {
                    "key": "username",
                    "type": "env"
                }
            },
            "datasetName": "postCustomers",
            "fieldMappings": [
                {
                    "fieldName": "Id",
                    "order": 1,
                    "dataType": "VARCHAR(100)",
                    "resolveNamespace": true
                },
                {
                    "fieldName": "Name",
                    "order": 2,
                    "dataType": "VARCHAR(100)"
                },
                {
                    "fieldName": "secretData",
                    "order": 3,
                    "dataType": "VARCHAR(100)"
                }
            ],
            "query": "mssql",
            "tableName": "Customers",
            "nullEmptyColumnValues": true,
            "idColumn": "Id"
        }],
    "tableMappings": [
        {
            "tableName": "Customers",
            "nameSpace": "customer",
            "query": "",
            "sinceColumn":"DateTimeColumn",
            "cdcEnabled": true,
            "entityIdConstructor": "customers/%s",
            "types": [
                "http://data.test.io/test/Customer"
            ],
            "columnMappings": [
                {
                    "fieldName": "Id",
                    "isIdColumn": true,
                    "propertyName": "id"
                },
                {
                    "fieldName": "Name",
                    "propertyName": "name",
                    "isReference": true,
                    "referenceTemplate": "http://data.test.io/test/name/%s"
                },
                {
                    "fieldName": "secretData",
                    "ignoreColumn": true
                }
            ]
        },
        {
            "tableName": "Products",
            "nameSpace": "product",
            "entityIdConstructor": "products/%s",
            "config": {
                "databaseServer": "server2",
                "database": "test2",
                "port": "4242",
                "schema": "test",
                "user": {
                    "type": "env",
                    "key": "TEST_USER"
                },
                "password": {
                    "type": "env",
                    "key": "TEST_PASSWORD"
                }
            },
            "types": [
                "http://data.test.io/test/Product"
            ],
            "columnMappings": [
                {
                    "fieldName": "Id",
                    "isIdColumn": true
                }
            ]
        }
    ]
}
```

Above is a complete example of a config with 2 tables from 2 different databases. It show all the available configuration options, and how to use them. This includes reading both and writing to the Customers-table

### Server config

```json
{
    "id": "test-import",
    "databaseServer": "test.testserver.io",
    "database": "my_test",
    "port": "1433",
    "schema": "dbo",
    "user": "<user>",
    "password": "<password>",
    "baseUri": "http://data.test.io/le-data/",
    "baseNameSpace": "http://data.test.io/le-data/",
    "postMappings": [],
    "tableMappings": []
}
```

The server config is used to set up the connection to the database server.

`id` field is technically not needed, but is used in the Datahub to differentiate configurations.

`databaseServer` is the mssql database server address, and must be available to access directly from the datalayer.

`database` is the mssql database to connect to.

`port` is the mssql server port

`instance` in the event that the database has a named instance you set the name here. It then ignores the port and uses instance to connect.

`schema` is the mssql database schema, most likely "dbo".

`user` is a user with access to the database. You should use the MSSQL_DB_USER env variable instead.

`password` is the password for the same user. You should use the MSSQL_DB_PASSWORD env variable instead.

`baseUri` is used together with the entityIdConstructor in the tableMapping to create the full id.

`baseNameSpace` is used together with the TableName in tableMapping to create the namespace.

`postMappings` is a list of datasets that are mapped to tables.

`tableMappings` is a list of tables that are mapped to datasets.

### PostMapping config

PostMapping writes single datasets from the datahub to a table. This includes INSERT, UPDATE (via MERGE) and DELETE.

```json
[
    {
        "datasetName":"postCustomers",
        "tableName": "Customers",
        "query": "",
        "idColumn": "Primary_key_in_Table",
        "fieldMappings": []
    }
]
```

`datasetName` name of the dataset in the datahub

`tableName` name of the table in the database

`query` Can either be a query to insert to a column with or without the PK in the dataset. If the PK is auto-incrementing we cannot do deletes on that table.

`nullEmptyColumnValues` if true, the datalayer will set null values for fields not included in the payload. To determine the correct null type, the `datatype` for each column must be defined on the field config.

`idColumn` specifies which property that contains the primary key for the table, if the table has an auto-incrementing PK, this field should be left empty.

`fieldMappings` list of columns, in order, that will be written to the table

### FieldMapping config

The FieldMapping adds order to the data that will be written to the table. If not set then we cannot guarantee the quality of data. For each property to match the correct column, this should be set.

```json
{
    "fieldMappings": [
                {
                    "fieldName": "Id",
                    "order": 1,
                    "dataType": "VARCHAR(100)",
                    "resolveNamespace": true
                },
                {
                    "fieldName": "Name",
                    "order": 2,
                    "dataType": "VARCHAR(100)"
                },
                {
                    "fieldName": "secretData",
                    "order": 3,
                    "dataType": "VARCHAR(100)"
                }
            ]
}
```

`fieldName` is the name of the property in the dataset

`order` is in what order it should be written in to the table

`dataType` is the type defined for the matching table column. These should correspond to one of these: https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16

`resolveNamespace` if true, this will resolve any namespace ref prefix to a full uri

We use could use this to specify if we want to write the property to the database or not.

### TableMapping config

TableMappings exposes single tables as datasets. They are also capable of exposing custom queries as datasets.

```json
[
    {
        "tableName": "Customers",
        "nameSpace": "customer",
        "query": "",
        "cdcEnabled": true,
        "entityIdConstructor": "customer/%s",
        "types": [
            "http://data.test.io/test/Customer"
        ],
        "columnMappings": []
    }
]
```

`tableName` is the name of the table in the database.

`nameSpace` if this is set, then it will be used in the namespace instead of the tableName.

`query` if set, this is used to return result. This Query must have a %s variable that will be replaced with a limit. Can not be used with CDC.

`cdcEnabled` when set, the layer will look for a matching CDC table for changes. If a since token is not sent with the request, the full table is returned, once you send a since token, then only the changes will be returned. CDC must be enabled in the database for this table for it to work.

`sinceColumn` when set will tell the layer to use a specific column in the table specified to look for changes. If a since token is not sent with the request, the full table is returned. This will also send this value as the new continuation token to the datahub.

`entityIdConstructor` tells the layer how to create ids. Ids are Namespaces in the Datahub, and should be unique for this given type (in the datahub). This uses Golang formatting, so if you have trouble getting the correct format, you should look there first.

`types` is a list with URI types present on this table.

`columnMappings` is a list of mappings that maps database columns to the dataset.

### Config

A TableMapping can take an optional "config" confgiuration. This can be used to override server settings on a per table basis. This allows that Datalayer server to return data from different databases.

```json
{
    "config": {
        "databaseServer": "server2",
        "database": "test2",
        "port": "4242",
        "schema": "test",
        "user": {
            "type": "env",
            "key": "TEST_USER"
        },
        "password": {
            "type": "env",
            "key": "TEST_PASSWORD"
        }
    },
}
```

Note that you only need to add the fields you want to override, all others will be used from the server configuration instead.

The only supported type for user and password is "env", and it will allow to read any environment variable you have set. If the env variable is not present, then "" will be set and used.


### ColumnMapping config

The ColumnMapping adds meaning to the dataset. By default, all columns will be returned in the dataset, but columns often have meaning, for example its an id, or its a (foreign) id pointing to another dataset.

By using ColumnMappings we can tell the DataHub about these meanings.

```json
{
    "columnMappings": [
        {
            "fieldName": "Id",
            "isIdColumn": true,
            "propertyName": "id"
        },
        {
            "fieldName": "Name",
            "propertyName": "name",
            "isReference": true,
            "referenceTemplate": "http://data.test.io/test/name/%s"
        },
        {
            "fieldName": "secretData",
            "ignoreColumn": true
        }
    ]
}
```

Again, remember that if it is not configured, the default is for the field to be included.

`fieldName` the name of the field in the database. This is case sensistive.

`propertyName` if set overrides the fieldName in the result. Use this for prettying your result.

`isIdColumn` is used together with the entityIdConstructor to create an id for the Entity.

`isReference` is used together with the referenceTemplate to create a link to a different Entity that may or may not exist yet. A column should never be an id and a reference column at the same time.

`referenceTemplate` is used to create the URI to the external Entity.

`ignoreColumn` sometimes you don't want to include the column as it is sensitive, or contains rubbish, and this ignores it.


## Running

### Set up the server

The datalayer has been configured to run in Docker, but you can also build it, and run the binary.

The server is configured trough environment variables.

### Docker

You first need to build the Docker image.

```
docker build . -t mssqldatalayer
```

```bash
docker run -d \
    -p 4343:4343 \
    -v $(pwd)/local.config.json:/root/config.json \
    -e PROFILE=dev \
    -e CONFIG_LOCATION=file://config.json \
    mssqldatalayer
```

### Binary

You need Golang installed on your machine, together with a build chain that includes make.

```
make build

```

or you can build directly with
```
go build -o bin/mssqldatalayer cmd/mssql/main.go
```

To get it correctly built on Windows you might have to try some options to get it to build right, but there is no reason why it should not work. However we do recommend you use the Docker option with a Linux toolchain (WSL2 on windows)


## Logging

The service will log to StdOut.

## Testing

You can run
```bash
make test
```
to run the unit tests locally.

## Run

Either do:
```bash
make run
```
or
```bash
make build && bin/mssqldatalayer
```

Ensure a config file exists in the location configured in the CONFIG_LOCATION
variable

With Docker

```bash
make docker
docker run -d -p 4343:4343 -v $(pwd)/local.config.json:/root/config.json -e PROFILE=dev -e CONFIG_LOCATION=file://config.json mssqldatalayer
```
## Env

Server will by default use the .env file, AND an extra file per environment,
for example .env-prod if PROFILE is set to "prod". This allows for pr environment
configuration of the environment in addition to the standard ones. All variables
declared in the .env file (but left empty) are available for reading from the ENV
in Docker. This will most likelly be changed in the future, and you should use ENV variables instead of a env based profile.

## Known issues

The driver does not handle the datetime format `2022-01-01T01:01:01 +01:00` it needs to get the date like this `2022-01-01T00:01:01Z` or `2022-01-01T00:01:01.555`
