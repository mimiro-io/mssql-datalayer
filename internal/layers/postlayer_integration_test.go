package layers

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mssql"
)

func TestPostLayerIntegration(t *testing.T) {
	ctx := context.Background()
	container, err := mssql.RunContainer(ctx,
		testcontainers.WithImage("mcr.microsoft.com/mssql/server:2022-latest"),
		mssql.WithPassword("P@ssword1234"),
	)
	if err != nil {
		t.Skipf("skipping integration tests, failed to start mssql container: %v", err)
		return
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	masterConnStr, err := container.ConnectionString(ctx, "master")
	if err != nil {
		t.Fatalf("failed to obtain master connection string: %v", err)
	}

	masterDB, err := sql.Open("sqlserver", masterConnStr)
	if err != nil {
		t.Fatalf("failed to connect to master db: %v", err)
	}
	defer func() { _ = masterDB.Close() }()

	statements := []string{
		"CREATE DATABASE datalayer;",
		"USE datalayer;",
		"CREATE SCHEMA dl;",
		"CREATE TABLE dl.bulk_items (Id VARCHAR(50) PRIMARY KEY, Name VARCHAR(255) NOT NULL, Amount INT NOT NULL);",
		"CREATE TABLE dl.custom_items (Id VARCHAR(50) PRIMARY KEY, Name VARCHAR(255) NOT NULL, Amount INT NOT NULL);",
	}

	for _, stmt := range statements {
		if _, err := masterDB.Exec(stmt); err != nil {
			t.Fatalf("bootstrap failed for %s: %v", stmt, err)
		}
	}

	connStr, err := container.ConnectionString(ctx, "datalayer")
	if err != nil {
		t.Fatalf("failed to obtain datalayer connection string: %v", err)
	}

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		t.Fatalf("failed to open datalayer connection: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := waitForPing(ctx, db); err != nil {
		t.Fatalf("database not ready: %v", err)
	}

	t.Run("UpsertBulk", func(t *testing.T) {
		mapping := &conf.PostMapping{
			TableName:             "dl.bulk_items",
			IdColumn:              "Id",
			Query:                 "upsertBulk",
			NullEmptyColumnValues: true,
			TimeZone:              "UTC",
			FieldMappings: []*conf.FieldMapping{
				{FieldName: "Id", DataType: "VARCHAR(50)", SortOrder: 1},
				{FieldName: "Name", DataType: "VARCHAR(255)", SortOrder: 2},
				{FieldName: "Amount", DataType: "INT", SortOrder: 3},
			},
		}

		layer := &PostLayer{PostRepo: &PostRepository{
			DB:           db,
			ctx:          context.Background(),
			PostTableDef: mapping,
		}}

		entity := &Entity{
			ID: "bulk/1",
			Properties: map[string]interface{}{
				"ns:Id":     "1",
				"ns:Name":   "Widget'); DROP TABLE dl.bulk_items;--",
				"ns:Amount": 12,
			},
		}

		deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", mapping.TableName, mapping.IdColumn)
		if err := layer.UpsertBulk([]*Entity{entity}, mapping.FieldMappings, deleteStmt, mapping.IdColumn, mapping.TableName); err != nil {
			t.Fatalf("upsert bulk failed: %v", err)
		}

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM dl.bulk_items WHERE Id = '1'").Scan(&count); err != nil {
			t.Fatalf("failed counting rows: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 row, got %d", count)
		}

		entity.IsDeleted = true
		if err := layer.UpsertBulk([]*Entity{entity}, mapping.FieldMappings, deleteStmt, mapping.IdColumn, mapping.TableName); err != nil {
			t.Fatalf("delete via upsert bulk failed: %v", err)
		}

		if err := db.QueryRow("SELECT COUNT(*) FROM dl.bulk_items WHERE Id = '1'").Scan(&count); err != nil {
			t.Fatalf("failed counting rows after delete: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 rows, got %d", count)
		}
	})

	t.Run("CustomQuery", func(t *testing.T) {
		mapping := &conf.PostMapping{
			TableName:             "dl.custom_items",
			IdColumn:              "Id",
			Query:                 "INSERT INTO dl.custom_items (Id, Name, Amount) VALUES (@pId, @pName, @pAmount)",
			NullEmptyColumnValues: true,
			TimeZone:              "UTC",
			FieldMappings: []*conf.FieldMapping{
				{FieldName: "Id", DataType: "VARCHAR(50)", SortOrder: 1},
				{FieldName: "Name", DataType: "VARCHAR(255)", SortOrder: 2},
				{FieldName: "Amount", DataType: "INT", SortOrder: 3},
			},
		}

		layer := &PostLayer{PostRepo: &PostRepository{
			DB:           db,
			ctx:          context.Background(),
			PostTableDef: mapping,
		}}

		entity := &Entity{
			ID: "custom/1",
			Properties: map[string]interface{}{
				"ns:Id":     "1",
				"ns:Name":   "Safe Value'); DROP TABLE dl.custom_items;--",
				"ns:Amount": 99,
			},
		}

		deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", mapping.TableName, mapping.IdColumn)
		if err := layer.CustomQuery([]*Entity{entity}, mapping.Query, mapping.FieldMappings, deleteStmt); err != nil {
			t.Fatalf("custom query insert failed: %v", err)
		}

		var storedName string
		if err := db.QueryRow("SELECT Name FROM dl.custom_items WHERE Id = '1'").Scan(&storedName); err != nil {
			t.Fatalf("failed retrieving row: %v", err)
		}
		if storedName != "Safe Value'); DROP TABLE dl.custom_items;--" {
			t.Fatalf("unexpected stored value: %s", storedName)
		}

		entity.IsDeleted = true
		if err := layer.CustomQuery([]*Entity{entity}, mapping.Query, mapping.FieldMappings, deleteStmt); err != nil {
			t.Fatalf("custom query delete failed: %v", err)
		}

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM dl.custom_items WHERE Id = '1'").Scan(&count); err != nil {
			t.Fatalf("failed counting rows after delete: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 rows after delete, got %d", count)
		}
	})
}

func waitForPing(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := db.PingContext(ctx); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}
