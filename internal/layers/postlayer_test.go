package layers_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/franela/goblin"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/mimiro-io/mssqldatalayer/internal/layers"
	"os"
	"strings"
	"testing"
)

// loadPostLayer builds a PostLayer around the first postMapping of a config fixture, plus its entities.
func loadPostLayer(cfgPath string, dataPath string) (*layers.PostLayer, []*layers.Entity) {
	postM, err := os.ReadFile(cfgPath)
	if err != nil {
		panic(err)
	}
	datalayer := conf.Datalayer{}
	if err := json.Unmarshal(postM, &datalayer); err != nil {
		panic(err)
	}
	file, err := os.ReadFile(dataPath)
	if err != nil {
		panic(err)
	}
	var entities []*layers.Entity
	if err := json.Unmarshal(file, &entities); err != nil {
		panic(err)
	}
	pl := &layers.PostLayer{PostRepo: &layers.PostRepository{}}
	pl.PostRepo.PostTableDef = datalayer.PostMappings[0]
	return pl, entities
}

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
			query, err := (*layers.PostLayer).CreateUpsertBulk(pl, entities, pl.PostRepo.PostTableDef.FieldMappings, "DELETE FROM test WHERE Id = ", "Id", "Europe/Oslo", "test")
			g.Assert(err).IsNil()
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
		g.It("Should emit NULL literals for missing values when nullEmptyColumnValues is set", func() {
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
			pl.PostRepo.PostTableDef.NullEmptyColumnValues = true

			// a:4 is missing the datetime and date columns
			query, err := (*layers.PostLayer).CreateUpsertBulk(pl, entities[4:5], pl.PostRepo.PostTableDef.FieldMappings, "DELETE FROM test WHERE Id = ", "Id", "Europe/Oslo", "test")
			g.Assert(err).IsNil()
			g.Assert(query).Eql("DELETE FROM test WHERE Id = 'a:4';INSERT INTO test (Id, Column_Int, Column_Tinyint, Column_Smallint, Column_Bit, Column_Float, Column_Datetime, Column_Datetime2, Column_DatetimeOffset, Column_Varchar, Column_Decimal, Column_Numeric, Column_Date ) VALUES ( 'a:4',12344556,13,41,0,7.990000,NULL,NULL,NULL,'b:string',90.090000,211.110000,NULL );")
		})
		g.It("Should emit NULL for datetimes outside the target column range", func() {
			postM, err := os.ReadFile("../../resources/test/test-upsertbulk.json")
			if err != nil {
				fmt.Print(err)
			}
			datalayer := conf.Datalayer{}
			if err := json.Unmarshal(postM, &datalayer); err != nil {
				fmt.Print(err)
			}
			file, err := os.ReadFile("../../resources/test/data/test-datetime-range.json")
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

			// Etc/GMT-1 is a fixed +01:00 zone with no LMT entry. A named zone such as Europe/Oslo would
			// resolve pre-1895 dates through local mean time, whose offset differs between tzdata releases.
			query, err := (*layers.PostLayer).CreateUpsertBulk(pl, entities, pl.PostRepo.PostTableDef.FieldMappings, "DELETE FROM test WHERE Id = ", "Id", "Etc/GMT-1", "test")
			g.Assert(err).IsNil()
			resultSlice := strings.Split(query, ";")

			// DATETIME starts at 1753-01-01, so the zero time is out of range; DATETIME2 starts at 0001-01-01 and keeps it
			g.Assert(resultSlice[1]).Eql("INSERT INTO test (Id, Column_Datetime, Column_Datetime2 ) VALUES ( 'a:5',NULL,'0001-01-01T01:00:00' )")
			// both types end at 9999-12-31, and the +01:00 offset pushes this past that
			g.Assert(resultSlice[3]).Eql("INSERT INTO test (Id, Column_Datetime, Column_Datetime2 ) VALUES ( 'a:6',NULL,NULL )")
		})
		g.It("Should null out-of-range datetimes in the parameterised payload", func() {
			postM, err := os.ReadFile("../../resources/test/test-upsertbulk.json")
			if err != nil {
				fmt.Print(err)
			}
			datalayer := conf.Datalayer{}
			if err := json.Unmarshal(postM, &datalayer); err != nil {
				fmt.Print(err)
			}
			file, err := os.ReadFile("../../resources/test/data/test-datetime-range.json")
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

			// a:5 carries the zero time in both a DATETIME and a DATETIME2 column
			payload, err := (*layers.PostLayer).CreatePayload(pl, entities[1], pl.PostRepo.PostTableDef.FieldMappings)
			g.Assert(err).IsNil()
			g.Assert(len(payload)).Eql(3)
			g.Assert(payload[0]).Eql("a:5")
			g.Assert(payload[1]).Eql(sql.NullTime{})
			g.Assert(payload[2]).IsNotNil()
		})
		g.It("Should return an error rather than exiting when a payload datetime cannot be parsed", func() {
			pl, entities := loadPostLayer("../../resources/test/test-upsertbulk.json", "../../resources/test/data/test-datetime-invalid.json")

			payload, err := (*layers.PostLayer).CreatePayload(pl, entities[1], pl.PostRepo.PostTableDef.FieldMappings)
			g.Assert(err == nil).IsFalse()
			g.Assert(payload).IsNil()
		})
		g.It("Should return an error rather than exiting when a delete id datetime cannot be parsed", func() {
			pl, entities := loadPostLayer("../../resources/test/test-datetime-idcolumn.json", "../../resources/test/data/test-datetime-invalid.json")
			s := entities[1].StripProps()

			delQueue, err := (*layers.PostLayer).CustomDelete(pl, entities[1], pl.PostRepo.PostTableDef.FieldMappings, s, "", "Europe/Oslo", "DELETE FROM test WHERE Column_Datetime = ")
			g.Assert(err == nil).IsFalse()
			g.Assert(delQueue).Eql("")
		})
		g.It("Should return an error when a bulk statement datetime cannot be parsed", func() {
			pl, entities := loadPostLayer("../../resources/test/test-upsertbulk.json", "../../resources/test/data/test-datetime-invalid.json")

			query, err := (*layers.PostLayer).CreateUpsertBulk(pl, entities, pl.PostRepo.PostTableDef.FieldMappings, "DELETE FROM test WHERE Id = ", "Id", "Europe/Oslo", "test")
			g.Assert(err == nil).IsFalse()
			g.Assert(strings.Contains(err.Error(), "Column_Datetime")).IsTrue()
			g.Assert(query).Eql("")
		})
		g.It("Should return an error naming the timezone when it is unknown", func() {
			pl, entities := loadPostLayer("../../resources/test/test-upsertbulk.json", "../../resources/test/data/test-datetime-range.json")

			query, err := (*layers.PostLayer).CreateUpsertBulk(pl, entities, pl.PostRepo.PostTableDef.FieldMappings, "DELETE FROM test WHERE Id = ", "Id", "Europe/Osloo", "test")
			g.Assert(err == nil).IsFalse()
			g.Assert(strings.Contains(err.Error(), "Europe/Osloo")).IsTrue()
			g.Assert(query).Eql("")
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

			delTest1, errDel1 := (*layers.PostLayer).CustomDelete(pl, entities[1], pl.PostRepo.PostTableDef.FieldMappings, s1, "", "", "DELETE FROM test WHERE Id = ")
			delTest2, errDel2 := (*layers.PostLayer).CustomDelete(pl, entities[2], pl.PostRepo.PostTableDef.FieldMappings, s2, "", "", "DELETE FROM test WHERE Id = ")
			delTest3, errDel3 := (*layers.PostLayer).CustomDelete(pl, entities[3], pl.PostRepo.PostTableDef.FieldMappings, s3, "", "", "DELETE FROM test WHERE Id = ")
			g.Assert(errDel1).IsNil()
			g.Assert(errDel2).IsNil()
			g.Assert(errDel3).IsNil()
			g.Assert(delTest1).Eql("DELETE FROM test WHERE Id = 'a:1';")
			g.Assert(delTest2).Eql("DELETE FROM test WHERE Id = 'a:2';")
			g.Assert(delTest3).Eql("DELETE FROM test WHERE Id = 'a:3';")
			//DELETE FROM test WHERE Id = 'a:2';DELETE FROM test WHERE Id = 'a:3';INSERT INTO test (Id, Column_Int, Column_Tinyint, Column_Smallint, Column_Bit, Column_Float, Column_Datetime, Column_Datetime2, Column_DatetimeOffset, Column_Varchar, Column_Decimal, Column_Numeric, Column_Date ) VALUES ( 'a:3',12344556,13,41,0,7.990000,'2023-01-01T01:01:01','2023-01-01T00:01:01','2023-01-01T01:01:01+02:00','b:string',90.090000,211.110000,'2023-01-01' );")
		})
	})
}
