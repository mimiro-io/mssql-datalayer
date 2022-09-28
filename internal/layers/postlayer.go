package layers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
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

	return postLayer.execQuery(entities, query, fields, queryDel)
}
func (postLayer *PostLayer) execQuery(entities []*Entity, query string, fields []*conf.FieldMapping, queryDel string) error {
	for _, post := range entities {
		if !strings.ContainsAny(post.ID, ":") {
			continue
		}
		if !post.IsDeleted { //If is deleted True --> Delete from table
			s := post.StripProps()
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

				switch value.(type) {
				case nil:
					if !postLayer.PostRepo.postTableDef.NullEmptyColumnValues {
						continue // TODO:Need to fail properly when this happens
					}
					switch strings.Split(field.Datatype, "(")[0] {
					case "VARCHAR":
						columnValues = append(columnValues, sql.NullString{})
					case "BIT":
						columnValues = append(columnValues, sql.NullBool{})
					case "INT":
						columnValues = append(columnValues, sql.NullInt64{})
					case "DATETIME2":
						columnValues = append(columnValues, sql.NullTime{})
					case "FLOAT":
						columnValues = append(columnValues, sql.NullBool{})
					}
				case float64:
					columnValues = append(columnValues, fmt.Sprintf("%f", value))
				case int:
					columnValues = append(columnValues, fmt.Sprintf("%s", value))
				case bool:
					if value == true {
						createBit := fmt.Sprintf("%t", value)
						columnValues = append(columnValues, strings.Replace(createBit, "true", "1", 1))
					} else {
						columnValues = append(columnValues, "0")
					}

				default:
					columnValues = append(columnValues, fmt.Sprintf("%s", value))
				}
			}
			_, err := postLayer.PostRepo.DB.Exec(query, columnValues...)
			if err != nil {
				postLayer.logger.Error(err)
				return err
			}
		} else if postLayer.PostRepo.postTableDef.IdColumn != "" {
			_, err := postLayer.PostRepo.DB.Exec(queryDel)
			if err != nil {
				postLayer.logger.Error(err)
			}
		}
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
