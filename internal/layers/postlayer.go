package layers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/spf13/cast"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"sort"
	"strconv"
	"strings"
)

type PostLayer struct {
	cmgr     *conf.ConfigurationManager //
	logger   *zap.SugaredLogger
	PostRepo *PostRepository
}
type PostRepository struct {
	DB            *sql.DB
	ctx           context.Context
	postTableDef  *conf.PostMapping
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

	u := postLayer.cmgr.Datalayer.GetPostUrl(postLayer.PostRepo.postTableDef)
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

	postLayer.PostRepo.postTableDef = postLayer.GetTableDefinition(datasetName)
	if postLayer.PostRepo.postTableDef == nil {
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

	query := postLayer.PostRepo.postTableDef.Query

	if query == "" {
		postLayer.logger.Errorf("Please add query in config for %s in ", datasetName)
		return errors.New(fmt.Sprintf("no query found in config for dataset: %s", datasetName))
	}
	queryDel := fmt.Sprintf(`DELETE FROM %s WHERE %s =`, postLayer.PostRepo.postTableDef.TableName, postLayer.PostRepo.postTableDef.IdColumn)

	fields := postLayer.PostRepo.postTableDef.FieldMappings

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
		// create 20 parallel inserts 500 entities each
		groupCount := 20
		for i := 0; i < groupCount; i++ {

			entslice := entities[(len(entities)/groupCount)*i : (((len(entities) / groupCount) * i) + len(entities)/groupCount)]
			g.Go(func() error {
				err := postLayer.upsertBulk(entslice, fields, queryDel)
				if err != nil {
					return err
				}
				return err
			})

		}

		if err := g.Wait(); err != nil {
			return err
		}
		return nil
	} else {
		return postLayer.execQuery(entities, query, fields, queryDel)
	}
}
func (postLayer *PostLayer) execQuery(entities []*Entity, query string, fields []*conf.FieldMapping, queryDel string) error {
	rowId := ""
	for _, post := range entities {
		s := post.StripProps()
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}
		if !post.IsDeleted { //If is deleted True --> Delete from table
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
					if !postLayer.PostRepo.postTableDef.NullEmptyColumnValues {
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
					case "FLOAT":
						columnValues = append(columnValues, fmt.Sprintf("%f", value))
					default: // all other types can be sent as string
						columnValues = append(columnValues, fmt.Sprintf("%s", value))
					}
				}
			}
			_, err := postLayer.PostRepo.DB.Exec(query, columnValues...)
			if err != nil {
				postLayer.logger.Error(err)
				return err
			}
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

				if field.FieldName == postLayer.PostRepo.postTableDef.IdColumn {
					switch datatype {
					case "BIT":
						bit := false
						if value.(bool) {
							bit = true
						}
						rowId += strconv.FormatBool(bit)
					case "INT", "SMALLINT", "TINYINT", "INTEGER":
						rowId += strconv.FormatInt(cast.ToInt64(value.(float64)), 10)
					case "FLOAT":
						rowId += fmt.Sprintf("%f", value)
					default: // all other types can be sent as string
						rowId += fmt.Sprintf("%s", value)
					}
				}
			}

			queryDel += queryDel + rowId + ";"
			_, err := postLayer.PostRepo.DB.Exec(queryDel)
			if err != nil {
				postLayer.logger.Error(err)
			}
		}
	}
	return nil
}

func (postLayer *PostLayer) upsertBulk(entities []*Entity, fields []*conf.FieldMapping, queryDel string) error {
	tx, err := postLayer.PostRepo.DB.Begin()
	if err != nil {
		return err
	}
	txOK := false
	defer func() {
		if !txOK {
			tx.Rollback()
		}
	}()
	buildQuery := ""
	for _, post := range entities {
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}

		s := post.StripProps()
		args := make([]interface{}, len(fields))
		columnNames := ""
		columnValues := ""
		rowId := ""
		InsertColumnNamesValues := ""
		if !post.IsDeleted { //If is deleted True --> Do not store
			buildQuery += fmt.Sprintf("DELETE FROM %s WHERE %s= %s", postLayer.PostRepo.postTableDef.TableName, postLayer.PostRepo.postTableDef.IdColumn, strings.Split(post.ID, ":")[1])
			buildQuery += ";"
			buildQuery += fmt.Sprintf("INSERT INTO %s values (", strings.ToLower(postLayer.PostRepo.postTableDef.TableName))
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
					if !postLayer.PostRepo.postTableDef.NullEmptyColumnValues {
						continue // TODO:Need to fail properly when this happens
					}
					columnValues += cast.ToString(getSqlNull(datatype))
				} else {
					switch datatype {
					case "BIT":
						bit := false
						if value.(bool) {
							bit = true
						}
						columnValues += strconv.FormatBool(bit) + ","
					case "INT", "SMALLINT", "TINYINT", "INTEGER":
						columnValues += strconv.FormatInt(cast.ToInt64(value.(float64)), 10) + ","
					case "FLOAT":
						columnValues += fmt.Sprintf("%f,", value)
					default: // all other types can be sent as string
						columnValues += fmt.Sprintf("'%s',", value)
					}
				}
				if field.FieldName == postLayer.PostRepo.postTableDef.IdColumn {
					rowId = strings.TrimRight(columnValues, ",")
				}
				columnNames += fmt.Sprintf("%s,", field.FieldName)
				InsertColumnNamesValues += fmt.Sprintf("%s = source.%s, ", field.FieldName, field.FieldName)

			}
			columnNames = columnNames[:len(columnNames)-1]
			columnValues = columnValues[:len(columnValues)-1]
			InsertColumnNamesValues = strings.TrimRight(InsertColumnNamesValues, ", ")
			buildQuery += columnValues + ");"
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

				if field.FieldName == postLayer.PostRepo.postTableDef.IdColumn {
					switch datatype {
					case "BIT":
						bit := false
						if value.(bool) {
							bit = true
						}
						rowId += strconv.FormatBool(bit)
					case "INT", "SMALLINT", "TINYINT", "INTEGER":
						rowId += strconv.FormatInt(cast.ToInt64(value.(float64)), 10)
					case "FLOAT":
						rowId += fmt.Sprintf("%f", value)
					default: // all other types can be sent as string
						rowId += fmt.Sprintf("%s", value)
					}
				}
			}
			buildQuery += queryDel + rowId + ";"
		}
	}
	_, err = tx.Exec(buildQuery)

	if err != nil {
		return err
	}
	tx.Commit()
	return nil
}

// TODO: Implement prepared statement for nullEmptyColumnValues = true
/*func (postLayer *PostLayer) upsertBulk2(entities []*Entity, query string, fields []*conf.FieldMapping, queryDel string) error {
	tx, err := postLayer.PostRepo.DB.Begin()
	if err != nil {
		return err
	}
	txOK := false
	defer func() {
		if !txOK {
			tx.Rollback()
		}
	}()
	//buildQuery := ""
	columnNames := make([]string, 0)
	columnValues := make([]interface{}, 0)
	insertColumnNames := ""
	for _, post := range entities {
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}

		s := post.StripProps()

		//rowId := ""
		//InsertColumnNamesValues := ""
		if !post.IsDeleted { //If is deleted True --> Do not store
			//buildQuery += fmt.Sprintf("DELETE FROM %s WHERE %s= %s", postLayer.PostRepo.postTableDef.TableName, postLayer.PostRepo.postTableDef.IdColumn, strings.Split(post.ID, ":")[1])
			//buildQuery += ";"
			//buildQuery += fmt.Sprintf("INSERT INTO %s values (", strings.ToLower(postLayer.PostRepo.postTableDef.TableName))
			for _, field := range fields {
				var value interface{}

				propValue := s[field.FieldName]
				if field.ResolveNamespace && propValue != nil {
					value = uda.ToURI(postLayer.PostRepo.EntityContext, s[field.FieldName].(string))
				} else {
					value = propValue
				}
				datatype := strings.Split(field.DataType, "(")[0]
				if value == nil {
					if !postLayer.PostRepo.postTableDef.NullEmptyColumnValues {
						continue // TODO:Need to fail properly when this happens
					}
					columnValues = append(columnValues, cast.ToString(getSqlNull(datatype)))
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
					case "FLOAT":
						columnValues = append(columnValues, fmt.Sprintf("%f,", value))
					default: // all other types can be sent as string
						columnValues = append(columnValues, fmt.Sprintf("'%s',", value))
					}
				}
				columnNames = append(columnNames, field.FieldName)
				insertColumnNames += "("
				insertColumnNames += "@p" + field.FieldName + ","

			}
			insertColumnNames += strings.TrimRight(insertColumnNames, ",")
			insertColumnNames += ")"
			//buildQuery += columnValues + ");"

		} else {
			for _, field := range fields {
				if field.FieldName == postLayer.PostRepo.postTableDef.IdColumn {
					//rowId = fmt.Sprintf("%s", s[field.FieldName])
				}
			}
			//buildQuery += queryDel + rowId + ";"
		}
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", postLayer.PostRepo.postTableDef.TableName,
		strings.Join(columnNames, ","), insertColumnNames)
	_, err = tx.Exec(stmt, columnValues...)

	if err != nil {
		return err
	}
	tx.Commit()
	return nil
}*/

func getSqlNull(datatype string) any {
	switch datatype {
	case "VARCHAR":
		return sql.NullString{}
	case "BIT":
		return sql.NullBool{}
	case "INT":
		return sql.NullInt64{}
	case "DATETIME2":
		return sql.NullTime{}
	case "FLOAT":
		return sql.NullBool{}
	default:
		return sql.RawBytes{}
	}
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
