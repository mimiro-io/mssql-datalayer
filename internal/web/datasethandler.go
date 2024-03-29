package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/mssqldatalayer/internal/db"
	"github.com/mimiro-io/mssqldatalayer/internal/layers"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type ServiceInfo struct {
	Name     string
	Location string
}

type datasetHandler struct {
	logger *zap.SugaredLogger
	layer  *layers.Layer
}

func NewDatasetHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, layer *layers.Layer) {
	log := logger.Named("web")

	dh := &datasetHandler{
		logger: log,
		layer:  layer,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.GET("/datasets", dh.listDatasetsHandler, mw.authorizer(log, "datahub:r"))
			e.GET("/datasets/:dataset/changes", dh.getChangesHandler, mw.authorizer(log, "datahub:r"))
			e.GET("/datasets/:dataset/entities", dh.getChangesHandler, mw.authorizer(log, "datahub:r"))
			return nil
		},
	})

}

// Handlers

// listDatasetsHandler
func (handler *datasetHandler) listDatasetsHandler(c echo.Context) error {
	datasets := handler.layer.GetDatasetEndpoints()
	//sort.Slice(datasets, func(i, j int) bool {
	//	return datasets[i].Name < datasets[j].Name
	//})

	return c.JSON(http.StatusOK, datasets)
}

// getEntitiesHandler
// path param dataset
// query param continuationToken
func (handler *datasetHandler) getEntitiesHandler(c echo.Context) error {
	datasetName, err := url.QueryUnescape(c.Param("dataset"))
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}
	limit := c.QueryParam("limit")
	var l int64
	if limit != "" {
		f, _ := strconv.ParseInt(limit, 10, 64)
		l = f
	}

	// check dataset exists
	if !handler.layer.DoesDatasetExist(datasetName) {
		return c.NoContent(http.StatusNotFound)
	}

	c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	c.Response().WriteHeader(http.StatusOK)
	enc := json.NewEncoder(c.Response())
	c.Response().Write([]byte("["))

	// make and send context as the first object
	context := handler.layer.GetContext(datasetName)

	_ = enc.Encode(context)

	request := db.DatasetRequest{
		DatasetName: datasetName,
		Limit:       l,
	}
	handler.layer.ChangeSet(request, func(entity *layers.Entity) {
		c.Response().Write([]byte(","))
		_ = enc.Encode(entity)
		c.Response().Flush()
	})

	c.Response().Write([]byte("]"))
	c.Response().Flush()
	return nil
}

func (handler *datasetHandler) getChangesHandler(c echo.Context) error {
	datasetName, err := url.QueryUnescape(c.Param("dataset"))
	if err != nil {
		handler.logger.Warn(err)
		return c.NoContent(http.StatusBadRequest)
	}

	since := c.QueryParam("since")
	if since != "" {
		s, _ := url.QueryUnescape(since)
		since = s
	}

	limit := c.QueryParam("limit")
	var l int64
	if limit != "" {
		f, _ := strconv.ParseInt(limit, 10, 64)
		l = f
	}

	// check dataset exists
	if !handler.layer.DoesDatasetExist(datasetName) {
		return c.NoContent(http.StatusNotFound)
	}

	// ensure db connection before starting json stream
	tableDef := handler.layer.GetTableDefinition(datasetName)

	err = handler.layer.EnsureConnection(tableDef)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	c.Response().WriteHeader(http.StatusOK)
	enc := json.NewEncoder(c.Response())

	c.Response().Write([]byte("["))

	// make and send context as the first object
	context := handler.layer.GetContext(datasetName)

	_ = enc.Encode(context)

	request := db.DatasetRequest{
		DatasetName: datasetName,
		Since:       since,
		Limit:       l,
	}

	err = handler.layer.ChangeSet(request, func(entity *layers.Entity) {
		if entity.ID == "@continuation" { // it is returned as a normal entity, and we need to flatten it to the token format
			cont := map[string]interface{}{
				"id":    "@continuation",
				"token": entity.Properties["token"],
			}
			c.Response().Write([]byte(","))
			_ = enc.Encode(cont)
			c.Response().Flush()
		} else {
			c.Response().Write([]byte(","))
			_ = enc.Encode(entity)
			c.Response().Flush()
		}
	})

	if err != nil {
		// dont write the closing bracket and imply to the client through this that the stream is broken
		handler.logger.Warn(err)
	} else {
		c.Response().Write([]byte("]"))
		c.Response().Flush()
	}

	return nil
}
