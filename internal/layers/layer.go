package layers

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/DataDog/datadog-go/v5/statsd"
	mssql "github.com/microsoft/go-mssqldb"
	"go.uber.org/fx"

	"github.com/mimiro-io/mssqldatalayer/internal/db"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/mimiro-io/mssqldatalayer/internal/conf"
)

type Layer struct {
	cmgr   *conf.ConfigurationManager
	logger *zap.SugaredLogger
	Repo   *Repository //exported because it needs to deferred from main
	statsd statsd.ClientInterface
	env    *conf.Env
}

type Repository struct {
	DB       *sql.DB
	ctx      context.Context
	tableDef *conf.TableMapping
	digest   [16]byte
}

type DatasetRequest struct {
	DatasetName string
	Since       string
	Limit       int64
}

type DatasetName struct {
	Name string   `json:"name"`
	Type []string `json:"type"`
}

const jsonNull = "null"

func NewLayer(lc fx.Lifecycle, cmgr *conf.ConfigurationManager, env *conf.Env, statsd statsd.ClientInterface) *Layer {
	layer := &Layer{
		cmgr:   cmgr,
		logger: env.Logger.Named("layer"),
		statsd: statsd,
		env:    env,
	}
	layer.Repo = &Repository{
		ctx: context.Background(),
	}
	_ = layer.EnsureConnection(nil) // ok with error here

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if layer.Repo.DB != nil {
				_ = layer.Repo.DB.Close()
			}
			return nil
		},
	})

	return layer
}

func (l *Layer) GetDatasetNames() []string {
	names := make([]string, 0)
	for _, table := range l.cmgr.Datalayer.TableMappings {
		names = append(names, table.TableName)
	}
	return names
}

func (l *Layer) GetDatasetEndpoints() []DatasetName {
	names := make([]DatasetName, 0)
	for _, table := range l.cmgr.Datalayer.TableMappings {
		names = append(names, DatasetName{Name: table.TableName, Type: []string{"GET"}})
	}
	for _, table := range l.cmgr.Datalayer.PostMappings {
		found := false
		for i, dataset := range names {
			if dataset.Name == table.TableName {
				names[i].Type = append(names[i].Type, "POST")
				found = true
			}
		}
		if !found {
			names = append(names, DatasetName{Name: table.TableName, Type: []string{"POST"}})
		}
	}
	// TODO: support listing dataset that only handle post
	return names
}

func (l *Layer) GetTableDefinition(datasetName string) *conf.TableMapping {
	for _, table := range l.cmgr.Datalayer.TableMappings {
		if table.TableName == datasetName {
			return table
		}
	}
	return nil
}

func (l *Layer) GetContext(datasetName string) map[string]interface{} {
	tableDef := l.GetTableDefinition(datasetName)
	ctx := make(map[string]interface{})
	namespaces := make(map[string]string)

	namespace := tableDef.TableName
	if tableDef.NameSpace != "" {
		namespace = tableDef.NameSpace
	}

	namespaces["ns0"] = l.cmgr.Datalayer.BaseNameSpace + namespace + "/"
	namespaces["rdf"] = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	ctx["namespaces"] = namespaces
	ctx["id"] = "@context"
	return ctx
}

func (l *Layer) DoesDatasetExist(datasetName string) bool {
	names := l.GetDatasetNames()
	for _, name := range names {
		if name == datasetName {
			return true
		}
	}
	return false
}

func (l *Layer) ChangeSet(request db.DatasetRequest, callBack func(*Entity)) error {
	tags := []string{
		fmt.Sprintf("application:%s", l.env.ServiceName),
		fmt.Sprintf("dataset:%s", request.DatasetName),
	}

	tableDef := l.GetTableDefinition(request.DatasetName)
	if tableDef == nil {
		l.er(fmt.Errorf("could not find defined dataset: %s", request.DatasetName))
		return nil
	}

	if tableDef.CustomQuery != "" {
		if !strings.Contains(tableDef.CustomQuery, "%s") {
			l.logger.Error("custom query defined, but missing required '%s' for limit injection")
			return nil
		}
	}

	err := l.EnsureConnection(tableDef)
	if err != nil {
		return err
	}

	query := db.NewQuery(request, tableDef, l.cmgr.Datalayer)

	var rows *sql.Rows
	since, _ := getSince(l.Repo.DB, tableDef)
	rows, err = l.Repo.DB.QueryContext(l.Repo.ctx, query.BuildQuery())

	if err != nil {
		l.er(err)
		return err
	}
	defer func() {
		_ = rows.Close()
	}()

	cols, err := rows.Columns()
	colTypes, _ := rows.ColumnTypes()

	// set up the row interface from the returned types
	nullableRowData := buildRowType(cols, colTypes)

	for rows.Next() {
		err = rows.Scan(nullableRowData...)

		if err != nil {
			l.er(err)
		} else {
			// map it
			_ = l.statsd.Incr("mssql.read", tags, 1)
			entity, err := l.toEntity(nullableRowData, cols, colTypes, tableDef)
			if err != nil {
				return err
			}
			if entity != nil {
				// add types to entity
				if len(tableDef.Types) == 1 {
					entity.References["rdf:type"] = tableDef.Types[0]
				} else if len(tableDef.Types) > 1 {
					// multiple types...
					// fix me
				}

				// call back function
				callBack(entity)
			}
		}
	}

	// this error can occur if there is an issue during the call to rows.Next()
	// we want to fail as not all data has been read
	if err := rows.Err(); err != nil {
		skipErr := false
		if driverErr, ok := err.(mssql.Error); ok { // Now the error number is accessible directly
			if driverErr.Number == 313 {
				// 313 is the error number for "lsn range exceeded. in our case that means the provided start lsn is at the end of the change set"
				// in this case, we just want to build an empty result with unaltered continuation token
				// https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-increment-lsn-transact-sql?view=sql-server-ver16#examples
				err = fmt.Errorf("token still at end of change set, (%w)", err)
				skipErr = true
			}
		}
		if !skipErr {
			l.er(err)
			return err
		} else {
			l.logger.Info("ignoring error and not updating token: ", err.Error())
			since = request.Since
		}
	}

	// only add continuation token if enabled or sinceColumn is set
	if tableDef.CDCEnabled || tableDef.SinceColumn != "" {
		entity := NewEntity()
		entity.ID = "@continuation"
		entity.Properties["token"] = since

		callBack(entity)
	}

	// clean it up
	return nil
}

func buildRowType(cols []string, colTypes []*sql.ColumnType) []interface{} {
	nullableRowData := make([]interface{}, len(cols))
	for i := range cols {
		colDef := colTypes[i]
		ctType := colDef.DatabaseTypeName()

		switch ctType {
		case "INT", "SMALLINT", "TINYINT":
			nullableRowData[i] = new(sql.NullInt64)
		case "VARCHAR", "NVARCHAR", "TEXT", "NTEXT", "CHAR":
			nullableRowData[i] = new(sql.NullString)
		case "DATETIME", "DATE", "DATETIME2":
			nullableRowData[i] = new(sql.NullTime)
		case "MONEY", "DECIMAL", "FLOAT":
			nullableRowData[i] = new(sql.NullFloat64)
		case "BIT":
			nullableRowData[i] = new(sql.NullBool)
		default:
			nullableRowData[i] = new(sql.RawBytes)
		}
	}
	return nullableRowData
}

func (l *Layer) er(err error) {
	l.logger.Warnf("Got error %s", err)
}

func (l *Layer) EnsureConnection(table *conf.TableMapping) error {
	l.logger.Debug("Ensuring connection")
	if l.cmgr.State.Digest != l.Repo.digest {
		l.logger.Debug("Configuration has changed need to reset connection")
		if l.Repo.DB != nil {
			_ = l.Repo.DB.Close() // don't really care about the error, as long as it is closed
		}
		db, err := l.connect(table) // errors are already logged
		if err != nil {
			return err
		}
		l.Repo.DB = db
		l.Repo.digest = l.cmgr.State.Digest
	}
	return nil
}

func (l *Layer) connect(table *conf.TableMapping) (*sql.DB, error) {

	u := l.cmgr.Datalayer.GetUrl(table)

	db, err := sql.Open("sqlserver", u.String())

	if err != nil {
		l.logger.Warn("Error creating connection pool: ", err.Error())
		return nil, err
	}
	err = db.PingContext(l.Repo.ctx)
	if err != nil {
		l.logger.Warn(err.Error())
		return nil, err
	}
	return db, nil
}

func (l *Layer) toEntity(rowType []interface{}, cols []string, colTypes []*sql.ColumnType, tableDef *conf.TableMapping) (*Entity, error) {
	entity := NewEntity()
	log := l.logger.With("table", tableDef.TableName)
	for i, raw := range rowType {
		if raw != nil {
			ct := colTypes[i]
			ctName := ct.DatabaseTypeName()
			colName := cols[i]
			colMapping := tableDef.Columns[colName]
			colName = "ns0:" + colName

			var val interface{} = nil
			var strVal = ""

			if colName == "ns0:__$operation" {
				ptrToNullInt := raw.(*sql.NullInt64)
				if (*ptrToNullInt).Valid {
					operation := (*ptrToNullInt).Int64
					if operation == 1 {
						entity.IsDeleted = true
					}
				}
			}

			if colMapping != nil {
				if colMapping.IgnoreColumn {
					continue
				}

				if colMapping.PropertyName != "" {
					colName = colMapping.PropertyName
				}
			} else {
				// filter out cdc columns
				if ignoreColumn(cols[i], tableDef) {
					continue
				}
			}

			entity.Properties[colName] = nil

			switch ctName {
			case "VARCHAR", "NVARCHAR", "TEXT", "NTEXT", "CHAR":
				ptrToNullString := raw.(*sql.NullString)
				if (*ptrToNullString).Valid {
					val = (*ptrToNullString).String
					strVal = val.(string)
					entity.Properties[colName] = val
				}
			case "UNIQUEIDENTIFIER":
				ptrToString := raw.(*sql.RawBytes)
				if (*ptrToString) != nil {
					uid, _ := uuid.FromBytes(*ptrToString)
					strVal = uid.String()
					entity.Properties[colName] = strVal
				}
			case "DATETIME", "DATE", "DATETIME2":
				ptrToNullDatetime := raw.(*sql.NullTime)
				if (*ptrToNullDatetime).Valid {
					val = (*ptrToNullDatetime).Time
					var location *time.Location
					var err error
					if tableDef.TimeZone != "" {
						location, err = time.LoadLocation(tableDef.TimeZone)
						if err != nil {
							log.Errorf("Error parsing TimeZone from table definition: %s", err)
							return nil, err
						}
					} else if l.cmgr.Datalayer.TimeZone != "" {
						location, err = time.LoadLocation(l.cmgr.Datalayer.TimeZone)
						if err != nil {
							log.Errorf("Error parsing TimeZone from db definition: %s", err)
							return nil, err
						}
					} else {
						location, _ = time.LoadLocation("UTC")
					}
					val, err = time.ParseInLocation("2006-01-02T15:04:05Z", val.(time.Time).Format(time.RFC3339), location)
					if err != nil {
						log.Errorf("Error parsing timestamp: %s", val)
					}
					entity.Properties[colName] = val.(time.Time).Format(time.RFC3339Nano)
				}
			case "INT", "SMALLINT", "TINYINT":
				ptrToNullInt := raw.(*sql.NullInt64)
				if (*ptrToNullInt).Valid {
					val = (*ptrToNullInt).Int64
					strVal = strconv.FormatInt((*ptrToNullInt).Int64, 10)
					entity.Properties[colName] = val
				}
			case "BIGINT":
				ptrToSomething := raw.(*sql.RawBytes)
				if *ptrToSomething != nil {
					val, err := toInt64(*ptrToSomething)
					if err != nil {
						log.Warnf("Error converting to int64: %v", err)
					} else {
						strVal = strconv.FormatInt(val, 10)
						entity.Properties[colName] = val
					}
				}
			case "MONEY", "DECIMAL", "FLOAT":
				ptrToNullFloat := raw.(*sql.NullFloat64)
				if (*ptrToNullFloat).Valid {
					entity.Properties[colName] = (*ptrToNullFloat).Float64
				}
			case "BIT":
				ptrToNullBool := raw.(*sql.NullBool)
				if (*ptrToNullBool).Valid {
					entity.Properties[colName] = (*ptrToNullBool).Bool
				} else {
					entity.Properties[colName] = false // default to false
				}
			default:
				log.Errorf("Got: %s for %s", ctName, colName)
			}

			if colMapping != nil {
				// is this the id column
				if colMapping.IsIdColumn && strVal != "" {
					entity.ID = l.cmgr.Datalayer.BaseUri + fmt.Sprintf(tableDef.EntityIdConstructor, strVal)
				}

				if colMapping.IsReference && strVal != "" {
					entity.References[colName] = fmt.Sprintf(colMapping.ReferenceTemplate, strVal)
				}
			}
		}
	}

	if entity.ID == "" { // this is invalid
		log.Errorf("empty id value from the database, this is probably pretty wrong. CDC access? entity: %+v", entity)
		return nil, fmt.Errorf("empty id value from the database. CDC access?")
	}

	return entity, nil
}

// serverSince queries the server for its time, this will be used as the source of the since to return
// when using cdc. The return value is Base64 encoded

func getSince(db *sql.DB, tableDef *conf.TableMapping) (string, error) {
	s := ""
	if tableDef.SinceColumn != "" {
		var dt time.Time
		row := db.QueryRow(fmt.Sprintf("SELECT MAX(%s) from %s", tableDef.SinceColumn, tableDef.TableName))
		err := row.Scan(&dt)
		if err != nil {
			return "", err
		}
		s = fmt.Sprintf("%s", dt.Format("2006-01-02T15:04:05.000Z"))
	} else if tableDef.CDCEnabled {
		query := "select sys.fn_cdc_get_max_lsn();"
		row := db.QueryRow(query)
		var bytes []byte
		err := row.Scan(&bytes)
		if err != nil {
			return "", err
		}
		hexString := fmt.Sprintf("0x%x", bytes)
		res := base64.RawURLEncoding.EncodeToString([]byte(hexString))
		return res, nil
	} else {
		var dt time.Time
		row := db.QueryRow("SELECT GETDATE()")
		err := row.Scan(&dt)
		if err != nil {
			return "", err
		}
		s = fmt.Sprintf("%s", dt.Format(time.RFC3339))
	}
	return base64.StdEncoding.EncodeToString([]byte(s)), nil
}
func toInt64(payload sql.RawBytes) (int64, error) {
	content := reflect.ValueOf(payload).Interface().(sql.RawBytes)
	data := string(content)                  //convert to string
	i, err := strconv.ParseInt(data, 10, 64) // convert to int or your preferred data type
	if err != nil {
		return 0, err
	}

	return i, nil
}

func ignoreColumn(column string, tableDef *conf.TableMapping) bool {
	if tableDef.CDCEnabled && strings.HasPrefix(column, "__$") {
		return true
	}
	return false
}
