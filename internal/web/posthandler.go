package web

import (
	"context"
	"database/sql"
	"errors"
	"github.com/bcicen/jstream"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/mssqldatalayer/internal/layers"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"net/url"
)

type postHandler struct {
	logger    *zap.SugaredLogger
	postLayer *layers.PostLayer
}

func NewPostHandler(lc fx.Lifecycle, e *echo.Echo, mw *Middleware, logger *zap.SugaredLogger, layer *layers.PostLayer) {
	log := logger.Named("web")

	handler := &postHandler{
		logger:    log,
		postLayer: layer,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.POST("/datasets/:dataset/changes", handler.postHandler, mw.authorizer(log, "datahub:w"))
			e.POST("/datasets/:dataset/entities", handler.postHandler, mw.authorizer(log, "datahub:w"))
			return nil
		},
	})

}
func (handler *postHandler) postHandler(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	handler.logger.Debugf("Working on dataset %s", datasetName)
	postLayer := handler.postLayer
	postLayer.PostRepo.PostTableDef = postLayer.GetTableDefinition(datasetName)
	// get database handle
	if handler.postLayer.PostRepo.DB == nil {
		u := handler.postLayer.Cmgr.Datalayer.GetPostUrl(postLayer.PostRepo.PostTableDef)
		db, err := sql.Open("sqlserver", u.String())
		if err != nil {
			handler.logger.Warn("Error creating connection pool: ", err.Error())
			return err
		}
		err = db.Ping()
		if err != nil {
			handler.logger.Warn(err.Error())
			return err
		}
		handler.postLayer.PostRepo.DB = db
	}

	groupCount := handler.postLayer.PostRepo.PostTableDef.Workers
	batchSize := postLayer.PostRepo.PostTableDef.BatchSize
	if batchSize < 0 || batchSize == 0 {
		batchSize = 10000
	}
	read := 0
	entities := make([]*layers.Entity, 0) //why 0?
	var entityContext *uda.Context

	isFirst := true

	err := parseStream(c.Request().Body, func(value *jstream.MetaValue) error {
		if isFirst {
			ec := uda.AsContext(value)
			entityContext = ec
			isFirst = false
		} else {
			entities = append(entities, asEntity(value))
			read++
			if read == batchSize {
				read = 0
				g := new(errgroup.Group)
				// create x parallel inserts with len(entities)/x rows in each default x = 20 and len(entities) = 10000

				if groupCount < 0 {
					groupCount = 20
				}
				if len(entities) < groupCount {
					err := postLayer.PostEntities(datasetName, entities, entityContext)
					if err != nil {
						return err
					}
				} else {
					for i := 0; i < groupCount; i++ {
						entslice := entities[(len(entities)/groupCount)*i : (((len(entities) / groupCount) * i) + len(entities)/groupCount)]
						g.Go(func() error {
							err := postLayer.PostEntities(datasetName, entslice, entityContext)
							if err != nil {
								handler.logger.Error(err)
								handler.logger.Info("should close handle?")
								return err
							}
							entities = make([]*layers.Entity, 0)
							return err
						})
					}
					if err := g.Wait(); err != nil {
						return err
					}
				}

			}
		}
		return nil
	})

	if err != nil {
		handler.logger.Warn(err)
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("could not parse the json payload").Error())
	}
	if read > 0 {
		err := postLayer.PostEntities(datasetName, entities, entityContext)
		if err != nil {
			handler.logger.Error(err)
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	}

	return c.NoContent(200)
}

func parseStream(reader io.Reader, emitEntity func(value *jstream.MetaValue) error) error {
	decoder := jstream.NewDecoder(reader, 1) //Reads json

	for mv := range decoder.Stream() {
		err := emitEntity(mv)
		if err != nil {
			return err
		}
	}

	return nil
}

func asEntity(value *jstream.MetaValue) *layers.Entity {
	entity := layers.NewEntity()
	raw := value.Value.(map[string]interface{})

	entity.ID = raw["id"].(string)

	deleted, ok := raw["deleted"]
	if ok {
		entity.IsDeleted = deleted.(bool)
	}

	props, ok := raw["props"]
	if ok {
		entity.Properties = props.(map[string]interface{})
	}
	return entity
}
