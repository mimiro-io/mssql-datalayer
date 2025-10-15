package layers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/franela/goblin"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
)

func TestUpsertBulk(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The postlayer", func() {
		g.It("Should validate postMappings", func() {
			postM, err := os.ReadFile("../../resources/test/test-upsertbulk.json")
			if err != nil {
				fmt.Print(err)
			}
			datalayer := conf.Datalayer{}
			if err := json.Unmarshal(postM, &datalayer); err != nil {
				fmt.Print(err)
			}
			pl := &PostLayer{PostRepo: &PostRepository{}}

			pl.PostRepo.PostTableDef = datalayer.PostMappings[0]
			// Do checks so that we read all properties from postmappings correctly
			g.Assert(pl.PostRepo.PostTableDef.DatasetName).IsNotNil()
			g.Assert(pl.PostRepo.PostTableDef.DatasetName).Eql("test.Sql")
			g.Assert(pl.PostRepo.PostTableDef.TableName).Eql("test")
			g.Assert(len(pl.PostRepo.PostTableDef.FieldMappings)).Equal(13)
			g.Assert(pl.PostRepo.PostTableDef.FieldMappings[0].DataType).Eql("VARCHAR(255)")
			g.Assert(pl.PostRepo.PostTableDef.FieldMappings[0].FieldName).Eql("Id")
			g.Assert(pl.PostRepo.PostTableDef.NullEmptyColumnValues).IsFalse()
			g.Assert(pl.PostRepo.PostTableDef.Query).Eql("upsertBulk")
		})
		g.It("Should build parameterised insert statements for upsertBulk", func() {
			postM, err := os.ReadFile("../../resources/test/test-upsertbulk.json")
			if err != nil {
				fmt.Print(err)
			}
			datalayer := conf.Datalayer{}
			if err := json.Unmarshal(postM, &datalayer); err != nil {
				fmt.Print(err)
			}
			file, err := os.ReadFile("../../resources/test/data/test1.json")
			if err != nil {
				fmt.Print(err)
			}
			var entities []*Entity
			if err := json.Unmarshal(file, &entities); err != nil {
				fmt.Println(err)
			}
			pl := &PostLayer{PostRepo: &PostRepository{}}
			pl.PostRepo.PostTableDef = datalayer.PostMappings[0]
			stmt, args, err := pl.buildInsertStatement(pl.PostRepo.PostTableDef.TableName, pl.PostRepo.PostTableDef.FieldMappings, entities[3].StripProps())
			g.Assert(err).IsNil()
			g.Assert(stmt).Eql("INSERT INTO test (Id, Column_Int, Column_Tinyint, Column_Smallint, Column_Bit, Column_Float, Column_Datetime, Column_Datetime2, Column_DatetimeOffset, Column_Varchar, Column_Decimal, Column_Numeric, Column_Date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			g.Assert(len(args)).Eql(13)
		})
		g.It("Should prepare named arguments for custom queries", func() {
			postM, err := os.ReadFile("../../resources/test/test-customquery.json")
			if err != nil {
				fmt.Print(err)
			}
			datalayer := conf.Datalayer{}
			if err := json.Unmarshal(postM, &datalayer); err != nil {
				fmt.Print(err)
			}
			file, err := os.ReadFile("../../resources/test/data/test2.json")
			if err != nil {
				fmt.Print(err)
			}
			var entities []*Entity
			if err := json.Unmarshal(file, &entities); err != nil {
				fmt.Println(err)
			}
			pl := &PostLayer{PostRepo: &PostRepository{}}
			pl.PostRepo.PostTableDef = datalayer.PostMappings[0]
			values, err := pl.columnValuesFromProps(entities[3].StripProps(), pl.PostRepo.PostTableDef.FieldMappings)
			g.Assert(err).IsNil()

			args, err := prepareArguments(pl.PostRepo.PostTableDef.Query, values)
			g.Assert(err).IsNil()
			expectedParams := parameterPattern.FindAllStringSubmatch(pl.PostRepo.PostTableDef.Query, -1)
			g.Assert(len(args)).Eql(len(expectedParams))

			named := args[0].(sql.NamedArg)
			g.Assert(named.Name).Eql("pId")
		})
	})
}
