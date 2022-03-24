package layers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"sort"
	"strings"
)

type PostLayer struct {
	cmgr     *conf.ConfigurationManager //
	logger   *zap.SugaredLogger
	PostRepo *PostRepository //exported because it needs to deferred from main??
}
type PostRepository struct {
	DB           *sql.DB
	ctx          context.Context
	postTableDef *conf.PostMapping
	digest       [16]byte
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
	postLayer.logger.Debug(u.String())
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

func (postLayer *PostLayer) PostEntities(datasetName string, entities []*Entity) error {

	postLayer.PostRepo.postTableDef = postLayer.GetTableDefinition(datasetName)
	if postLayer.PostRepo.postTableDef == nil {
		return errors.New(fmt.Sprintf("No configuration found for dataset: %s", datasetName))
	}

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
	postLayer.logger.Debug(query)

	queryDel := fmt.Sprintf(`DELETE FROM %s WHERE id =`, strings.ToLower(postLayer.PostRepo.postTableDef.TableName))

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
	buildQuery := ""
	for _, post := range entities {
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}
		rowId := strings.SplitAfter(post.ID, ":")[1]
		if !post.IsDeleted { //If is deleted True --> Delete from table
			//using holdlock to make sure nothing is changed during upsert the need for this can be discussed. Performance?
			buildQuery += fmt.Sprintf("MERGE %s WITH(HOLDLOCK) as target using (values(", strings.ToLower(postLayer.PostRepo.postTableDef.TableName))
			s := post.StripProps()
			args := make([]interface{}, len(fields)+1)
			args[0] = strings.SplitAfter(post.ID, ":")[1]
			columnNames := ""
			columnValues := ""
			InsertColumnNamesValues := ""
			for i, field := range fields {

				args[i+1] = s[field.FieldName]
				if s[field.FieldName] == nil {
					continue
				}
				switch s[field.FieldName].(type) {
				case float64:
					columnValues += fmt.Sprintf("%f,", s[field.FieldName])
				case int:
					columnValues += fmt.Sprintf("%s,", s[field.FieldName])
				case bool:
					if s[field.FieldName] == true {
						createBit := fmt.Sprintf("%t", s[field.FieldName])
						columnValues += strings.Replace(createBit, "true", "1", 1)
					} else {
						columnValues += fmt.Sprintf("%s", 0)
					}

				default:
					columnValues += fmt.Sprintf("'%s',", s[field.FieldName])
				}

				columnNames += fmt.Sprintf("%s,", field.FieldName)
				InsertColumnNamesValues += fmt.Sprintf("%s = source.%s, ", field.FieldName, field.FieldName)

			}
			//remove trailing comma, remnant from looping through values.
			columnNames = strings.TrimRight(columnNames, ", ")
			//columnValues = columnValues[:len(columnValues)-1]
			columnValues = strings.TrimRight(columnValues, ", ")
			InsertColumnNamesValues = strings.TrimRight(InsertColumnNamesValues, ", ")
			// build full upsert query using merge

			buildQuery += columnValues + ")) as source (" + columnNames + ") on target." + postLayer.PostRepo.postTableDef.IdColumn + "= '" + rowId + "' when matched then update set " + InsertColumnNamesValues + " when not matched then insert (" + columnNames + ") values (" + columnValues + ");"
			//debug logger
			postLayer.logger.Debug("1")
		} else {
			buildQuery += queryDel + "'" + rowId + "';"
		}
	}
	//debug logger
	//postLayer.logger.Debug("sending...")
	_, err := postLayer.PostRepo.DB.Exec(buildQuery)

	if err != nil {
		err2 := postLayer.PostRepo.DB.Close()
		if err2 != nil {
			postLayer.logger.Error(err)
			return err2
		}
		return err
	}

	return nil
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
