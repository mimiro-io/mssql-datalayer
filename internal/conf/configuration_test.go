package conf

import (
	"encoding/json"
	"github.com/franela/goblin"
	"os"
	"testing"
)

const config = `
	{
    "id": "mssql-server",
    "databaseServer": "mssql-server",
    "baseUri": "https://data.test.io/mssql-server",
    "database": "test",
    "port": "1433",
    "schema": "dbo",
    "user": "user1",
    "password": "password1",
    "baseNameSpace": "https://data.test.io/mssql-server",
	"postMappings": [
	  {
        "tableName":"Customers",
		"idColumn": "Id",
		"fieldMappings": [
                {
                    "fieldName": "Id",
                    "order": 1
                },
                {
                    "fieldName": "Name",
                    "order": 2
                },
                {
                    "fieldName": "Secret_data",
                    "order": 3
                }
            ],
		"datasetName":"post.Customers",
		"query": "mssql"
	  }],
    "tableMappings": [
      {
        "tableName": "Customers",
        "nameSpace": "customers",
        "config": {
            "databaseServer": "server2",
            "database": "test2",
            "port": "4242",
            "schema": "dbo",
            "user": {
                "type": "env",
                "key": "TEST_USER"
            },
            "password": {
                "type": "env",
                "key": "TEST_PASSWORD"
            }
        },
        "entityIdConstructor": "customers/%s",
        "types": [
          "http://data.test.io/customer"
        ],
        "columnMappings": [
          {
            "fieldName": "Id",
            "isIdColumn": true
          }
        ]
      },
      {
          "tableName": "orders",
          "nameSpace": "order",
          "entityIdConstructor": "orders/%s",
          "types": [
              "http://data.test.io/order"
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

`

func TestParseConfig(t *testing.T) {
	g := goblin.Goblin(t)

	datalayer := &Datalayer{}

	g.Describe("when parsing a config", func() {
		g.Before(func() {
			err := json.Unmarshal([]byte(config), datalayer)
			if err != nil {
				g.Fail(err)
			}
			os.Setenv("TEST_USER", "test testesen")
			os.Setenv("TEST_PASSWORD", "password2")
		})
		g.It("should read config from the table", func() {
			table := datalayer.TableMappings[0]
			g.Assert(table.TableName).Equal("Customers")
			g.Assert(table.Config).IsNotNil()
		})
		g.It("should get user and password", func() {
			table := datalayer.TableMappings[0]
			g.Assert(table.Config).IsNotNil()
			g.Assert(table.Config.User.GetValue()).Equal("test testesen")
			g.Assert(table.Config.Password.GetValue()).Equal("password2")

		})
		g.It("should return a connection url", func() {
			table := datalayer.TableMappings[0]

			g.Assert(datalayer.GetUrl(table).String()).Equal("sqlserver://test%20testesen:password2@server2:4242?database=test2&packet+size=32767")
		})
		g.It("should return the table schema", func() {
			table := datalayer.TableMappings[0]
			g.Assert(datalayer.GetSchema(table)).Equal("dbo")
		})
		g.It("should get a datasetName and a tableName", func() {
			dataset := datalayer.PostMappings[0]
			g.Assert(dataset.TableName).Equal("Customers")
			g.Assert(dataset.DatasetName).Equal("post.Customers")
		})
		g.It("should have a query", func() {
			dataset := datalayer.PostMappings[0]
			g.Assert(dataset.Query).Equal("mssql")
			g.Assert(dataset.Query).IsNotNil()
		})
		g.It("should should specify an Id column", func() {
			dataset := datalayer.PostMappings[0]
			g.Assert(dataset.IdColumn).Equal("Id")
			g.Assert(dataset.IdColumn).IsNotNil()
		})

		g.After(func() {
			os.Unsetenv("TEST_USER")
			os.Unsetenv("TEST_PASSWORD")
		})
	})
}
