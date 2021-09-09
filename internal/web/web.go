package web

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"net/http"
)

type Handler struct {
	Logger       *zap.SugaredLogger
	Port         string
	StatsDClient statsd.ClientInterface
	Profile      string
}

func NewWebServer(lc fx.Lifecycle, env *conf.Env, logger *zap.SugaredLogger, statsd statsd.ClientInterface) (*Handler, *echo.Echo) {
	e := echo.New()
	e.HideBanner = true

	l := logger.Named("web")

	handler := &Handler{
		Logger:       l,
		Port:         env.Port,
		StatsDClient: statsd,
		Profile:      env.Env,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {

			l.Infof("Starting Http server on :%s", env.Port)
			go func() {
				_ = e.Start(":" + env.Port)
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			l.Infof("Shutting down Http server")
			return e.Shutdown(ctx)

		},
	})

	return handler, e
}

func Register(e *echo.Echo, env *conf.Env, handler *Handler, mw *Middleware) {
	// this sets up the main chain
	env.Logger.Infof("Registering endpoints")
	e.GET("/health", health)
	e.GET("/", handler.serviceInfoHandler, mw.authorizer(handler.Logger, "datahub:r"))

}

func health(c echo.Context) error {
	return c.String(http.StatusOK, "UP")
}

// serviceInfoHandler
func (handler *Handler) serviceInfoHandler(c echo.Context) error {
	serviceInfo := &ServiceInfo{"MSSQL DataLayer", "server:" + handler.Port}
	return c.JSON(http.StatusOK, serviceInfo)
}
