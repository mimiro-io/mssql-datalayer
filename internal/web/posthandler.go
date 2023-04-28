package web

import (
	"context"
	"errors"
	"github.com/bcicen/jstream"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/mimiro-io/mssqldatalayer/internal/layers"
	"go.uber.org/fx"
	"go.uber.org/zap"
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
	//set batch size to default batch size of data hub
	//TODO: Get this as a configurable variable and document usage.
	batchSize := 10000
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

				err := postLayer.PostEntities(datasetName, entities, entityContext)
				if err != nil {
					handler.logger.Error(err)
					return err
				}
				entities = make([]*layers.Entity, 0)
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

	if postLayer.PostRepo.DB != nil {
		postLayer.PostRepo.DB.Close()
		postLayer.PostRepo.DB = nil
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
