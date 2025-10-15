package layers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	_ "github.com/microsoft/go-mssqldb"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/spf13/cast"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var parameterPattern = regexp.MustCompile(`@([A-Za-z0-9_]+)`)

type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type columnValue struct {
	field *conf.FieldMapping
	value interface{}
}

type PostLayer struct {
	Cmgr     *conf.ConfigurationManager //
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
	postLayer.Cmgr = cmgr
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

func (postLayer *PostLayer) Connect() (*sql.DB, error) {

	u := postLayer.Cmgr.Datalayer.GetPostUrl(postLayer.PostRepo.PostTableDef)
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

	if postLayer.PostRepo.PostTableDef == nil {
		return errors.New(fmt.Sprintf("No configuration found for dataset: %s", datasetName))
	}
	postLayer.PostRepo.PostTableDef = postLayer.GetTableDefinition(datasetName)
	idColumn, _, tableName, query, fields := postLayer.setVars()
	postLayer.PostRepo.EntityContext = entityContext

	if postLayer.PostRepo.DB == nil {
		db, err := postLayer.Connect() // errors are already logged
		if err != nil {
			return err
		}
		postLayer.PostRepo.DB = db
	}

	if query == "" {
		postLayer.logger.Errorf("Please add query in config for %s in ", datasetName)
		return errors.New(fmt.Sprintf("no query found in config for dataset: %s", datasetName))
	}
	queryDel := fmt.Sprintf(`DELETE FROM %s WHERE %s = ?`, tableName, idColumn)

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
		return postLayer.UpsertBulk(entities, fields, queryDel, idColumn, tableName)
	} else {
		return postLayer.CustomQuery(entities, query, fields, queryDel)
	}
}

func (postLayer *PostLayer) CustomQuery(entities []*Entity, query string, fields []*conf.FieldMapping, deleteStmt string) error {
	idField := postLayer.lookupIDField(fields)
	if idField == nil {
		return fmt.Errorf("id column %s not present in field mappings", postLayer.PostRepo.PostTableDef.IdColumn)
	}

	for _, post := range entities {
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}

		props := post.StripProps()

		if post.IsDeleted {
			if err := postLayer.execDelete(postLayer.PostRepo.DB, deleteStmt, idField, props); err != nil {
				return err
			}
			continue
		}

		payload, err := postLayer.columnValuesFromProps(props, fields)
		if err != nil {
			return err
		}

		args, err := prepareArguments(query, payload)
		if err != nil {
			return err
		}

		_, err = postLayer.PostRepo.DB.ExecContext(postLayer.PostRepo.ctx, query, args...)
		if err != nil {
			postLayer.logger.Error(err)
			return err
		}
	}

	return nil
}

func (postLayer *PostLayer) UpsertBulk(entities []*Entity, fields []*conf.FieldMapping, deleteStmt string, idColumn string, tableName string) error {
	idField := postLayer.lookupIDField(fields)
	if idField == nil {
		return fmt.Errorf("id column %s not present in field mappings", idColumn)
	}

	tx, err := postLayer.PostRepo.DB.BeginTx(postLayer.PostRepo.ctx, nil)
	if err != nil {
		return err
	}

	rollback := func(err error) error {
		if rbErr := tx.Rollback(); rbErr != nil {
			postLayer.logger.Warnf("rollback failed: %v", rbErr)
		}
		return err
	}

	for _, post := range entities {
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}

		props := post.StripProps()

		if post.IsDeleted {
			if err := postLayer.execDelete(tx, deleteStmt, idField, props); err != nil {
				return rollback(err)
			}
			continue
		}

		if err := postLayer.execDelete(tx, deleteStmt, idField, props); err != nil {
			return rollback(err)
		}

		insertStmt, args, err := postLayer.buildInsertStatement(tableName, fields, props)
		if err != nil {
			return rollback(err)
		}

		if insertStmt == "" {
			continue
		}

		if _, err = tx.ExecContext(postLayer.PostRepo.ctx, insertStmt, args...); err != nil {
			postLayer.logger.Error(err)
			return rollback(err)
		}
	}

	return tx.Commit()
}

func (postLayer *PostLayer) CreatePayload(post *Entity, fields []*conf.FieldMapping) ([]interface{}, error) {
	columnValues, err := postLayer.columnValuesFromProps(post.StripProps(), fields)
	if err != nil {
		return nil, err
	}
	positional := make([]interface{}, len(columnValues))
	for i, cv := range columnValues {
		positional[i] = cv.value
	}
	return positional, nil
}

func (postLayer *PostLayer) columnValuesFromProps(props map[string]interface{}, fields []*conf.FieldMapping) ([]*columnValue, error) {
	columnValues := make([]*columnValue, 0, len(fields))
	for _, field := range fields {
		cv, include, err := postLayer.buildColumnValue(field, props)
		if err != nil {
			return nil, err
		}
		if include {
			columnValues = append(columnValues, cv)
		}
	}
	return columnValues, nil
}

func (postLayer *PostLayer) buildColumnValue(field *conf.FieldMapping, props map[string]interface{}) (*columnValue, bool, error) {
	var value interface{}
	if propValue, ok := props[field.FieldName]; ok {
		value = propValue
	}
	if field.ResolveNamespace && value != nil {
		if strValue, ok := value.(string); ok {
			value = uda.ToURI(postLayer.PostRepo.EntityContext, strValue)
		}
	}

	converted, include, err := postLayer.convertFieldValue(field, value)
	if err != nil {
		return nil, false, err
	}
	if !include {
		return nil, false, nil
	}

	return &columnValue{field: field, value: converted}, true, nil
}

func (postLayer *PostLayer) convertFieldValue(field *conf.FieldMapping, value interface{}) (interface{}, bool, error) {
	datatype := strings.ToUpper(strings.Split(field.DataType, "(")[0])

	if value == nil {
		if !postLayer.PostRepo.PostTableDef.NullEmptyColumnValues {
			return nil, false, nil
		}
		return nil, true, nil
	}

	switch datatype {
	case "BIT":
		boolValue, err := cast.ToBoolE(value)
		if err != nil {
			return nil, false, err
		}
		return boolValue, true, nil
	case "INT", "SMALLINT", "TINYINT", "INTEGER", "BIGINT":
		intValue, err := cast.ToInt64E(value)
		if err != nil {
			return nil, false, err
		}
		return intValue, true, nil
	case "FLOAT", "DECIMAL", "NUMERIC", "REAL":
		floatValue, err := cast.ToFloat64E(value)
		if err != nil {
			return nil, false, err
		}
		return floatValue, true, nil
	case "DATETIME", "DATETIME2":
		strValue := fmt.Sprintf("%v", value)
		t, err := time.Parse(time.RFC3339, strValue)
		if err != nil {
			return nil, false, err
		}
		if location := postLayer.loadLocation(); location != nil {
			t = t.In(location)
		}
		return t, true, nil
	case "DATETIMEOFFSET":
		strValue := fmt.Sprintf("%v", value)
		t, err := time.Parse(time.RFC3339, strValue)
		if err != nil {
			return nil, false, err
		}
		return mssql.DateTimeOffset(t), true, nil
	default:
		return value, true, nil
	}
}

func (postLayer *PostLayer) loadLocation() *time.Location {
	if postLayer.PostRepo.PostTableDef == nil {
		return nil
	}
	if postLayer.PostRepo.PostTableDef.TimeZone == "" {
		return nil
	}
	location, err := time.LoadLocation(postLayer.PostRepo.PostTableDef.TimeZone)
	if err != nil {
		postLayer.logger.Warnf("failed loading location %s: %v", postLayer.PostRepo.PostTableDef.TimeZone, err)
		return nil
	}
	return location
}

func (postLayer *PostLayer) lookupIDField(fields []*conf.FieldMapping) *conf.FieldMapping {
	for _, field := range fields {
		if strings.EqualFold(field.FieldName, postLayer.PostRepo.PostTableDef.IdColumn) {
			return field
		}
	}
	return nil
}

func (postLayer *PostLayer) execDelete(execer sqlExecer, stmt string, idField *conf.FieldMapping, props map[string]interface{}) error {
	cv, include, err := postLayer.buildColumnValue(idField, props)
	if err != nil {
		return err
	}
	if !include {
		return nil
	}

	_, err = execer.ExecContext(postLayer.PostRepo.ctx, stmt, cv.value)
	if err != nil {
		postLayer.logger.Error(err)
	}
	return err
}

func (postLayer *PostLayer) buildInsertStatement(tableName string, fields []*conf.FieldMapping, props map[string]interface{}) (string, []interface{}, error) {
	columnNames := make([]string, 0, len(fields))
	args := make([]interface{}, 0, len(fields))

	for _, field := range fields {
		cv, include, err := postLayer.buildColumnValue(field, props)
		if err != nil {
			return "", nil, err
		}
		if !include {
			continue
		}
		columnNames = append(columnNames, field.FieldName)
		args = append(args, cv.value)
	}

	if len(columnNames) == 0 {
		return "", nil, nil
	}

	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columnNames, ", "), strings.Join(placeholders, ", "))
	return stmt, args, nil
}

func prepareArguments(query string, values []*columnValue) ([]interface{}, error) {
	matches := parameterPattern.FindAllStringSubmatch(query, -1)
	if len(matches) == 0 {
		args := make([]interface{}, len(values))
		for i, value := range values {
			args[i] = value.value
		}
		return args, nil
	}

	columnMap := make(map[string]interface{}, len(values))
	for _, value := range values {
		columnMap[strings.ToLower(value.field.FieldName)] = value.value
	}

	args := make([]interface{}, 0, len(matches))
	for _, match := range matches {
		name := match[1]
		lookup := strings.ToLower(name)
		fieldKey := lookup

		if strings.HasPrefix(lookup, "p") {
			candidate := lookup[1:]
			if idx, err := strconv.Atoi(candidate); err == nil {
				if idx >= 1 && idx <= len(values) {
					args = append(args, sql.Named(name, values[idx-1].value))
					continue
				}
			}

			if _, ok := columnMap[candidate]; ok {
				fieldKey = candidate
			}
		}

		if val, ok := columnMap[fieldKey]; ok {
			args = append(args, sql.Named(name, val))
			continue
		}

		return nil, fmt.Errorf("no value provided for parameter %s", name)
	}

	return args, nil
}
func (postLayer *PostLayer) GetTableDefinition(datasetName string) *conf.PostMapping {
	for _, table := range postLayer.Cmgr.Datalayer.PostMappings {
		if table.DatasetName == datasetName {
			return table
		} else if table.TableName == datasetName { // fallback
			return table
		}
	}
	return nil
}

func (postLayer *PostLayer) setVars() (string, string, string, string, []*conf.FieldMapping) {
	// set props to pass on
	idColumn := postLayer.PostRepo.PostTableDef.IdColumn
	timeZone := postLayer.PostRepo.PostTableDef.TimeZone
	tableName := postLayer.PostRepo.PostTableDef.TableName
	query := postLayer.PostRepo.PostTableDef.Query
	fields := postLayer.PostRepo.PostTableDef.FieldMappings
	return idColumn, timeZone, tableName, query, fields
}
func (postLayer *PostLayer) ensureConnection() error {
	postLayer.logger.Debug("Ensuring connection")
	if postLayer.Cmgr.State.Digest != postLayer.PostRepo.digest {
		postLayer.logger.Debug("Configuration has changed need to reset connection")
		if postLayer.PostRepo.DB != nil {
			postLayer.PostRepo.DB.Close() // don't really care about the error, as long as it is closed
		}
		db, err := postLayer.Connect() // errors are already logged
		if err != nil {
			return err
		}
		postLayer.PostRepo.DB = db
		postLayer.PostRepo.digest = postLayer.Cmgr.State.Digest
	}
	return nil
}
