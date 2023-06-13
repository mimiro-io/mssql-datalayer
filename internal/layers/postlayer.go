package layers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/microsoft/go-mssqldb"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/spf13/cast"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

type PostLayer struct {
	cmgr     *conf.ConfigurationManager //
	logger   *zap.SugaredLogger
	PostRepo *PostRepository
}
type PostRepository struct {
	DB            *sql.DB
	ctx           context.Context
	PostTableDef  *conf.PostMapping
	digest        [16]byte
	EntityContext *uda.Context
}

func NewPostLayer(lc fx.Lifecycle, cmgr *conf.ConfigurationManager, logger *zap.SugaredLogger) *PostLayer {
	postLayer := &PostLayer{logger: logger.Named("layer")}
	postLayer.cmgr = cmgr
	postLayer.PostRepo = &PostRepository{
		ctx: context.Background(),
	}

	_ = postLayer.ensureConnection()

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if postLayer.PostRepo.DB != nil {
				postLayer.PostRepo.DB.Close()
			}
			return nil
		},
	})

	return postLayer
}

func (postLayer *PostLayer) connect() (*sql.DB, error) {

	u := postLayer.cmgr.Datalayer.GetPostUrl(postLayer.PostRepo.PostTableDef)
	db, err := sql.Open("sqlserver", u.String())
	if err != nil {
		postLayer.logger.Warn("Error creating connection pool: ", err.Error())
		return nil, err
	}
	err = db.PingContext(postLayer.PostRepo.ctx)
	if err != nil {
		postLayer.logger.Warn(err.Error())
		return nil, err
	}
	return db, nil
}

func (postLayer *PostLayer) PostEntities(datasetName string, entities []*Entity, entityContext *uda.Context) error {

	postLayer.PostRepo.PostTableDef = postLayer.GetTableDefinition(datasetName)
	if postLayer.PostRepo.PostTableDef == nil {
		return errors.New(fmt.Sprintf("No configuration found for dataset: %s", datasetName))
	}

	postLayer.PostRepo.EntityContext = entityContext

	if postLayer.PostRepo.DB == nil {
		db, err := postLayer.connect() // errors are already logged
		if err != nil {
			return err
		}
		postLayer.PostRepo.DB = db
	}

	query := postLayer.PostRepo.PostTableDef.Query

	if query == "" {
		postLayer.logger.Errorf("Please add query in config for %s in ", datasetName)
		return errors.New(fmt.Sprintf("no query found in config for dataset: %s", datasetName))
	}
	queryDel := fmt.Sprintf(`DELETE FROM %s WHERE %s =`, postLayer.PostRepo.PostTableDef.TableName, postLayer.PostRepo.PostTableDef.IdColumn)

	fields := postLayer.PostRepo.PostTableDef.FieldMappings

	if len(fields) == 0 {
		postLayer.logger.Errorf("Please define all fields in config that is involved in dataset %s and query: %s", datasetName, query)
		return errors.New("fields needs to be defined in the configuration")
	}

	//Only Sort Fields if SortOrder is set
	count := 0
	for _, field := range fields {
		if field.SortOrder == 0 {
			count++
		}
	}
	if count >= 2 {
		postLayer.logger.Warn("No sort order is defined for fields in config, this might corrupt the query")
	} else {
		sort.SliceStable(fields, func(i, j int) bool {
			return fields[i].SortOrder < fields[j].SortOrder
		})
	}
	if query == "upsertBulk" {
		g := new(errgroup.Group)
		// create x parallel inserts with len(entities)/x rows in each default x = 20 and len(entities) = 10000
		groupCount := postLayer.PostRepo.PostTableDef.Workers
		if groupCount < 0 {
			groupCount = 20
		}
		if len(entities) < groupCount {
			err := postLayer.UpsertBulk(entities, fields, queryDel)
			if err != nil {
				return err
			}
		} else {
			for i := 0; i < groupCount; i++ {
				entslice := entities[(len(entities)/groupCount)*i : (((len(entities) / groupCount) * i) + len(entities)/groupCount)]
				g.Go(func() error {
					err := postLayer.UpsertBulk(entslice, fields, queryDel)
					if err != nil {
						return err
					}
					return err
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
		}
	} else {
		return postLayer.CustomQuery(entities, query, fields, queryDel)
	}
	return nil
}

func (postLayer *PostLayer) CustomQuery(entities []*Entity, query string, fields []*conf.FieldMapping, queryDel string) error {
	// TODO: Re-write to use mssql.CopyIn since query is defined from user.
	delQueue := ""
	for _, post := range entities {
		rowId := ""

		s := post.StripProps()
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}
		timeZone := postLayer.PostRepo.PostTableDef.TimeZone
		// put deleted in to own queue that fires at the end of batch.
		if post.IsDeleted {
			delQueue += postLayer.CustomDelete(post, fields, s, rowId, timeZone, queryDel)
		} else {
			payloadValues := postLayer.CreatePayload(post, fields)
			postLayer.logger.Debug(payloadValues)
			_, err := postLayer.PostRepo.DB.Exec(query, payloadValues...)
			if err != nil {
				postLayer.logger.Error(err)
				return err
			}

		}
	}
	_, err := postLayer.PostRepo.DB.Exec(delQueue)
	if err != nil {
		postLayer.logger.Error(err)
	}
	return nil
}

func (postLayer *PostLayer) CustomDelete(post *Entity, fields []*conf.FieldMapping, s map[string]interface{}, rowId string, timeZone string, queryDel string) string {
	delQueue := ""
	if postLayer.PostRepo.PostTableDef.IdColumn == "" {
		postLayer.logger.Warn(fmt.Sprintf("Cannot delete entitywhere Id-column is not specified:\t %s", post.ID))
	} else {
		for _, field := range fields {
			var value interface{}

			propValue := s[field.FieldName]
			if field.ResolveNamespace && propValue != nil {
				value = uda.ToURI(postLayer.PostRepo.EntityContext, s[field.FieldName].(string))
			} else {
				value = propValue
			}
			datatype := strings.Split(field.DataType, "(")[0]

			if field.FieldName == postLayer.PostRepo.PostTableDef.IdColumn {
				switch datatype {
				case "BIT":
					bit := false
					if value.(bool) {
						bit = true
					}
					rowId += strconv.FormatBool(bit)
				case "INT", "SMALLINT", "TINYINT", "INTEGER":
					rowId += strconv.FormatInt(cast.ToInt64(value.(float64)), 10)
				case "FLOAT", "DECIMAL", "NUMERIC":
					rowId += fmt.Sprintf("%f", value)
				case "DATETIME", "DATETIME2":
					t, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", value))
					var location *time.Location
					location, _ = time.LoadLocation(timeZone)
					if err != nil {
						log.Fatal("Couldn't parse datetime")
					}
					rowId += fmt.Sprintf("%s", t.In(location))
				case "DATETIMEOFFSET":
					t, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", value))
					if err != nil {
						log.Fatal("Couldn't parse datetime")
					}
					rowId += fmt.Sprintf("%s", t)
				default: // all other types can be sent as string
					rowId += fmt.Sprintf("'%s'", value)
				}
			}
		}
		delQueue += queryDel + rowId + ";"
	}
	return delQueue
}

func (postLayer *PostLayer) UpsertBulk(entities []*Entity, fields []*conf.FieldMapping, queryDel string) error {
	buildQuery := postLayer.CreateUpsertBulk(entities, fields, queryDel)
	if buildQuery == "" {
		return fmt.Errorf("could not resolve datetime, error in creating stmt")
	}
	conn, err := postLayer.PostRepo.DB.Conn(postLayer.PostRepo.ctx)
	conn.PingContext(postLayer.PostRepo.ctx)
	if err != nil {
		return err
	}
	conn.ExecContext(postLayer.PostRepo.ctx, buildQuery)
	conn.Close()
	return nil
}

// TODO: Implement prepared statement for nullEmptyColumnValues = true

func (postLayer *PostLayer) CreatePayload(post *Entity, fields []*conf.FieldMapping) []interface{} {
	s := post.StripProps()
	timeZone := postLayer.PostRepo.PostTableDef.TimeZone
	args := make([]interface{}, len(fields))
	columnValues := make([]any, 0)
	for i, field := range fields {
		var value interface{}

		propValue := s[field.FieldName]
		if field.ResolveNamespace && propValue != nil {
			value = uda.ToURI(postLayer.PostRepo.EntityContext, s[field.FieldName].(string))
		} else {
			value = propValue
		}
		args[i] = value
		datatype := strings.Split(field.DataType, "(")[0]
		if value == nil {
			if !postLayer.PostRepo.PostTableDef.NullEmptyColumnValues {
				continue // TODO:Need to fail properly when this happens
			}
			columnValues = append(columnValues, getSqlNull(datatype))
		} else {
			switch datatype {
			case "BIT":
				bit := 0
				if value.(bool) {
					bit = 1
				}
				columnValues = append(columnValues, bit)
			case "INT", "SMALLINT", "TINYINT", "INTEGER":
				columnValues = append(columnValues, int64(value.(float64)))
			case "FLOAT", "DECIMAL", "NUMERIC":
				columnValues = append(columnValues, fmt.Sprintf("%f", value))
			case "DATETIME", "DATETIME2":
				t, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", value))
				var location *time.Location
				location, _ = time.LoadLocation(timeZone)
				if err != nil {
					log.Fatal(err)
				}
				columnValues = append(columnValues, t.In(location))
			case "DATETIMEOFFSET":
				t, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", value))
				r := mssql.DateTimeOffset(t)
				if err != nil {
					log.Fatal(err)
				}
				columnValues = append(columnValues, r)
			default: // all other types can be sent as string
				columnValues = append(columnValues, fmt.Sprintf("%s", value))
			}
		}
	}
	return columnValues
}

func getSqlNull(datatype string) any {
	switch datatype {
	case "VARCHAR":
		return sql.NullString{}
	case "BIT":
		return sql.NullBool{}
	case "INT", "SMALLINT", "TINYINT", "INTEGER":
		return sql.NullInt64{}
	case "DATETIME", "DATETIME2", "DATETIMEOFFSET":
		return sql.NullTime{}
	case "FLOAT", "DECIMAL", "NUMERIC":
		return sql.NullBool{}
	default:
		return sql.RawBytes{}
	}
}
func (postLayer *PostLayer) CreateUpsertBulk(entities []*Entity, fields []*conf.FieldMapping, queryDel string) string {
	buildQuery := ""
	for _, post := range entities {
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}
		s := post.StripProps()
		args := make([]interface{}, len(fields))
		//columnNames := ""
		columnValues := ""
		rowId := ""
		InsertColumnNamesValues := ""
		timeZone := postLayer.PostRepo.PostTableDef.TimeZone
		if !post.IsDeleted { //If is deleted True just create the delete statement
			idColumn := postLayer.PostRepo.PostTableDef.IdColumn
			buildQuery += postLayer.createDelete(s, idColumn, fields)
			buildQuery += fmt.Sprintf("INSERT INTO %s (", strings.ToLower(postLayer.PostRepo.PostTableDef.TableName))
			for i, field := range fields {
				var value interface{}

				propValue := s[field.FieldName]
				if field.ResolveNamespace && propValue != nil {
					value = uda.ToURI(postLayer.PostRepo.EntityContext, s[field.FieldName].(string))
				} else {
					value = propValue
				}
				args[i] = value
				datatype := strings.Split(field.DataType, "(")[0]
				if value == nil {
					if !postLayer.PostRepo.PostTableDef.NullEmptyColumnValues {
						continue // TODO:Need to fail properly when this happens
					}
					columnValues += cast.ToString(getSqlNull(datatype)) + ","
				} else {
					switch datatype {
					case "BIT":
						bit := "0"
						if value.(bool) {
							bit = "1"
						}
						columnValues += bit + ","
					case "INT", "SMALLINT", "TINYINT", "INTEGER":
						columnValues += strconv.FormatInt(cast.ToInt64(value.(float64)), 10) + ","
					case "FLOAT", "DECIMAL", "NUMERIC":
						columnValues += fmt.Sprintf("%f", value) + ","
					case "DATETIME", "DATETIME2":
						t, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", value))
						var location *time.Location
						location, _ = time.LoadLocation(timeZone)
						if err != nil {
							return ""
						}
						time := fmt.Sprintf("%s", t.In(location).Format("2006-01-02T15:04:05"))
						columnValues += "'" + time + "',"
					case "DATETIMEOFFSET":
						columnValues += "'" + fmt.Sprintf("%s", value) + "',"
					default: // all other types can be sent as string
						columnValues += fmt.Sprintf("'%s',", value)
					}
				}
				if field.FieldName == postLayer.PostRepo.PostTableDef.IdColumn {
					rowId = strings.TrimRight(columnValues, ",")
				}
				//columnNames += fmt.Sprintf("%s,", field.FieldName)
				InsertColumnNamesValues += fmt.Sprintf("%s, ", field.FieldName)

			}
			columnValues = columnValues[:len(columnValues)-1]
			InsertColumnNamesValues = strings.TrimRight(InsertColumnNamesValues, ", ")
			buildQuery += fmt.Sprintf("%s ) VALUES ( %s );", InsertColumnNamesValues, columnValues)
		} else {
			for _, field := range fields {
				var value interface{}

				propValue := s[field.FieldName]
				if field.ResolveNamespace && propValue != nil {
					value = uda.ToURI(postLayer.PostRepo.EntityContext, s[field.FieldName].(string))
				} else {
					value = propValue
				}
				datatype := strings.Split(field.DataType, "(")[0]

				if field.FieldName == postLayer.PostRepo.PostTableDef.IdColumn {
					switch datatype {
					case "BIT":
						bit := "0"
						if value.(bool) {
							bit = "1"
						}
						rowId += bit
					case "INT", "BIGINT", "SMALLINT", "TINYINT", "INTEGER":
						rowId += strconv.FormatInt(cast.ToInt64(value.(float64)), 10)
					case "FLOAT", "DECIMAL", "NUMERIC":
						rowId += fmt.Sprintf("%f", value)
					case "DATETIME", "DATETIME2":
						t, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", value))
						var location *time.Location
						location, _ = time.LoadLocation(timeZone)
						if err != nil {
							return ""
						}
						time := fmt.Sprintf("%s", t.In(location).Format("2006-01-02T15:04:05"))
						rowId += "'" + time + "',"
					case "DATETIMEOFFSET":
						rowId += "'" + fmt.Sprintf("%s", value) + "',"
					default: // all other types can be sent as string
						rowId += fmt.Sprintf("'%s'", value)
					}
				}
			}
			buildQuery += queryDel + rowId + ";"
		}
	}
	return buildQuery
}

func (postLayer *PostLayer) GetTableDefinition(datasetName string) *conf.PostMapping {
	for _, table := range postLayer.cmgr.Datalayer.PostMappings {
		if table.DatasetName == datasetName {
			return table
		} else if table.TableName == datasetName { // fallback
			return table
		}
	}
	return nil
}

func (postLayer *PostLayer) createDelete(s map[string]interface{}, idColumn string, fields []*conf.FieldMapping) string {
	var value interface{}
	for _, field := range fields {
		if field.FieldName == idColumn {
			propValue := s[field.FieldName]

			if field.ResolveNamespace && propValue != nil {
				value = uda.ToURI(postLayer.PostRepo.EntityContext, s[field.FieldName].(string))
			} else {
				value = propValue
			}

			datatype := strings.Split(field.DataType, "(")[0]
			if value == nil {
				if !postLayer.PostRepo.PostTableDef.NullEmptyColumnValues {
					continue // TODO:Need to fail properly when this happens
				}
				value = cast.ToString(getSqlNull(datatype))
			} else {
				switch datatype {
				case "BIT":
					bit := "0"
					if value.(bool) {
						bit = "1"
					}
					value = bit
				case "INT", "SMALLINT", "TINYINT", "INTEGER":
					value = strconv.FormatInt(cast.ToInt64(value.(float64)), 10)
				case "FLOAT", "DECIMAL", "NUMERIC":
					value = fmt.Sprintf("%f", value)
				default: // all other types can be sent as string
					value = fmt.Sprintf("'%s'", value)
				}
			}
		}
	}
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s = %s"+";", postLayer.PostRepo.PostTableDef.TableName, postLayer.PostRepo.PostTableDef.IdColumn, value)

	return deleteStmt

}
func (postLayer *PostLayer) ensureConnection() error {
	postLayer.logger.Debug("Ensuring connection")
	if postLayer.cmgr.State.Digest != postLayer.PostRepo.digest {
		postLayer.logger.Debug("Configuration has changed need to reset connection")
		if postLayer.PostRepo.DB != nil {
			postLayer.PostRepo.DB.Close() // don't really care about the error, as long as it is closed
		}
		db, err := postLayer.connect() // errors are already logged
		if err != nil {
			return err
		}
		postLayer.PostRepo.DB = db
		postLayer.PostRepo.digest = postLayer.cmgr.State.Digest
	}
	return nil
}
