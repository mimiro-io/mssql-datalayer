package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type DockerDB struct {
	MsSQL      *dockertest.Resource
	Pool       *dockertest.Pool
	port       string
	connectStr string
}

func (s DockerDB) Close() error {
	return s.MsSQL.Close()
}

func New() (*DockerDB, error) {
	// Initialize docker pool
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Printf("Could not construct pool: %s", err)
		return nil, err
	}

	// Ping the docker daemon
	if err = pool.Client.Ping(); err != nil {
		log.Printf(`could not connect to docker: %s`, err)
		return nil, err
	}

	db, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/mssql/server", Tag: "2022-latest",
		Env: []string{"ACCEPT_EULA=yes", "MSSQL_SA_PASSWORD=Foobar123", "MSSQL_AGENT_ENABLED=true"},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Printf(`could not start mssql: %s`, err)
		return nil, err
	}
	port := db.GetPort("1433/tcp")
	connectStr := "sqlserver://sa:Foobar123@localhost:" + port + "?database=master"

	if err = pool.Retry(func() error {
		conn, err2 := sql.Open("sqlserver", connectStr)
		if err2 != nil {
			return err2
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		return conn.PingContext(ctx)
	}); err != nil {
		log.Printf("Could not connect to mssql container within 10s: %s", err)
		return nil, err
	}

	err = bootstrap(connectStr)
	if err != nil {
		return nil, err
	}

	return &DockerDB{MsSQL: db, Pool: pool, port: port, connectStr: connectStr}, nil
}

func bootstrap(connectStr string) error {
	db, err := sql.Open("sqlserver", connectStr)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	exec := func(query string) func() (sql.Result, error) {
		return func() (sql.Result, error) {
			return conn.ExecContext(ctx, query)
		}
	}

	return seq(
		exec(`CREATE DATABASE test;`),
		exec(`USE test;`),
		exec(`EXECUTE sys.sp_cdc_enable_db;`),
		exec(`CREATE SCHEMA test;`),
		exec(`CREATE TABLE test.test (Id VARCHAR(255) NOT NULL, Name VARCHAR(255) NOT NULL, PRIMARY KEY (Id));`),
		exec(
			`EXECUTE sys.sp_cdc_enable_table @source_schema = N'test', @source_name = N'test', @role_name = NULL, @supports_net_changes = 0;`,
		),
		exec(`INSERT INTO test.test (Id, Name) VALUES ('1', 'test');`),
	)
}

func seq(f ...func() (sql.Result, error)) error {
	for _, fun := range f {
		_, err := fun()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s DockerDB) TmpConfig() (string, error) {
	conf := fmt.Sprintf(
		`{  "databaseServer" : "%v",
				"baseUri" : "http://data.test.io/test/",
				"database" : "test",
				"port" : "%v",
				"schema" : "test",
				"password" : "Foobar123",
				"user" : "sa",
				"tableMappings" : [ { 	
						"tableName" : "test",
						"cdcEnabled": true,
						"entityIdConstructor" : "foo/%v",
						"config": {"schema": "test"},
						"columnMappings" : [ 	{ "fieldName": "Id", "isIdColumn" : true },
												{ "fieldName": "Name", "propertyName" : "ns0:Name" } ]
				} ]
			}`, "localhost", s.port, "%v")
	f, err := os.CreateTemp("", "config.json")
	if err != nil {
		return "", err
	}
	_, err = io.WriteString(f, conf)
	if err != nil {
		return "", err
	}
	err = f.Close()
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

func (s DockerDB) Insert(id int, name string) error {
	db, err := sql.Open("sqlserver", strings.ReplaceAll(s.connectStr, "master", "test"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	//
	//ctx := context.Background()
	//conn, err := db.Conn(ctx)
	//if err != nil {
	//	return err
	//}
	//defer func() { _ = conn.Close() }()

	stmt := fmt.Sprintf(`INSERT INTO test.test (Id, Name) VALUES ('%v', '%v');`, id, name)
	_, err = db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}