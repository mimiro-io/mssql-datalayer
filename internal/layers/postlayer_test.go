package layers_test

import (
	"encoding/json"
	"fmt"
	"github.com/franela/goblin"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/mimiro-io/mssqldatalayer/internal/layers"
	"os"
	"strings"
	"testing"
)

// Query:         "INSERT INTO Test (Id,Column_Int,Column_Tinyint,Column_Smallint,Column_Bit,Column_Float,Column_Datetime,Column_Datetime2,Column_Varchar,Column_Decimal,Column_Numeric,Column_Date) VALUES (112341234,2,3,0,12.65,'2023-01-01T01:01:01.000','2023-01-01T02:02:02.222','Test',90.09, 211.11,'2023-01-01')",
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
			pl := &layers.PostLayer{
				PostRepo: &layers.PostRepository{},
			}
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
		g.It("Should create a sql-statement with upsertBulk", func() {
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
			var entities []*layers.Entity
			if err := json.Unmarshal(file, &entities); err != nil {
				fmt.Println(err)
			}
			pl := &layers.PostLayer{
				PostRepo: &layers.PostRepository{},
			}
			pl.PostRepo.PostTableDef = datalayer.PostMappings[0]
			query := (*layers.PostLayer).CreateUpsertBulk(pl, entities, pl.PostRepo.PostTableDef.FieldMappings, "DELETE FROM test WHERE Id = ")
			g.Assert(query).Eql("DELETE FROM test WHERE Id = 'a:1';DELETE FROM test WHERE Id = 'a:2';DELETE FROM test WHERE Id = 'a:3';INSERT INTO test (Id, Column_Int, Column_Tinyint, Column_Smallint, Column_Bit, Column_Float, Column_Datetime, Column_Datetime2, Column_DatetimeOffset, Column_Varchar, Column_Decimal, Column_Numeric, Column_Date ) VALUES ( 'a:3',12344556,13,41,0,7.990000,'2023-01-01T01:01:01','2023-01-01T00:01:01','2023-01-01T01:01:01+02:00','b:string',90.090000,211.110000,'2023-01-01' );DELETE FROM test WHERE Id = 'a:4';INSERT INTO test (Id, Column_Int, Column_Tinyint, Column_Smallint, Column_Bit, Column_Float, Column_Varchar, Column_Decimal, Column_Numeric ) VALUES ( 'a:4',12344556,13,41,0,7.990000,'b:string',90.090000,211.110000 );")
			resultSlice := strings.Split(query, ";")

			g.Assert(resultSlice).IsNotNil()
			g.Assert(len(resultSlice)).Eql(7)
			g.Assert(resultSlice[0]).Eql("DELETE FROM test WHERE Id = 'a:1'")
			g.Assert(resultSlice[1]).Eql("DELETE FROM test WHERE Id = 'a:2'")
			g.Assert(resultSlice[2]).Eql("DELETE FROM test WHERE Id = 'a:3'")
			g.Assert(resultSlice[3]).Eql("INSERT INTO test (Id, Column_Int, Column_Tinyint, Column_Smallint, Column_Bit, Column_Float, Column_Datetime, Column_Datetime2, Column_DatetimeOffset, Column_Varchar, Column_Decimal, Column_Numeric, Column_Date ) VALUES ( 'a:3',12344556,13,41,0,7.990000,'2023-01-01T01:01:01','2023-01-01T00:01:01','2023-01-01T01:01:01+02:00','b:string',90.090000,211.110000,'2023-01-01' )")
			g.Assert(resultSlice[4]).Eql("DELETE FROM test WHERE Id = 'a:4'")
			g.Assert(resultSlice[5]).Eql("INSERT INTO test (Id, Column_Int, Column_Tinyint, Column_Smallint, Column_Bit, Column_Float, Column_Varchar, Column_Decimal, Column_Numeric ) VALUES ( 'a:4',12344556,13,41,0,7.990000,'b:string',90.090000,211.110000 )")

		})
		g.It("Should create user defined statement", func() {
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
			var entities []*layers.Entity
			if err := json.Unmarshal(file, &entities); err != nil {
				fmt.Println(err)
			}
			pl := &layers.PostLayer{
				PostRepo: &layers.PostRepository{},
			}
			pl.PostRepo.PostTableDef = datalayer.PostMappings[0]
			s1 := entities[1].StripProps()
			s2 := entities[2].StripProps()
			s3 := entities[3].StripProps()

			delTest1 := (*layers.PostLayer).CustomDelete(pl, entities[1], pl.PostRepo.PostTableDef.FieldMappings, s1, "", "", "DELETE FROM test WHERE Id = ")
			delTest2 := (*layers.PostLayer).CustomDelete(pl, entities[2], pl.PostRepo.PostTableDef.FieldMappings, s2, "", "", "DELETE FROM test WHERE Id = ")
			delTest3 := (*layers.PostLayer).CustomDelete(pl, entities[3], pl.PostRepo.PostTableDef.FieldMappings, s3, "", "", "DELETE FROM test WHERE Id = ")
			g.Assert(delTest1).Eql("DELETE FROM test WHERE Id = 'a:1';")
			g.Assert(delTest2).Eql("DELETE FROM test WHERE Id = 'a:2';")
			g.Assert(delTest3).Eql("DELETE FROM test WHERE Id = 'a:3';")
			//DELETE FROM test WHERE Id = 'a:2';DELETE FROM test WHERE Id = 'a:3';INSERT INTO test (Id, Column_Int, Column_Tinyint, Column_Smallint, Column_Bit, Column_Float, Column_Datetime, Column_Datetime2, Column_DatetimeOffset, Column_Varchar, Column_Decimal, Column_Numeric, Column_Date ) VALUES ( 'a:3',12344556,13,41,0,7.990000,'2023-01-01T01:01:01','2023-01-01T00:01:01','2023-01-01T01:01:01+02:00','b:string',90.090000,211.110000,'2023-01-01' );")
		})
	})
}
