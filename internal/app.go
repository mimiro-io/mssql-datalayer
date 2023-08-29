package internal

import (
	"go.uber.org/fx"

	"github.com/mimiro-io/mssqldatalayer"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/mimiro-io/mssqldatalayer/internal/layers"
	"github.com/mimiro-io/mssqldatalayer/internal/security"
	"github.com/mimiro-io/mssqldatalayer/internal/web"
)

func CreateLayer() *fx.App {
	return fx.New(
		fx.Provide(
			mssqldatalayer.NewEnv,
			conf.NewStatsd,
			conf.NewLogger,
			security.NewTokenProviders,
			conf.NewConfigurationManager,
			layers.NewLayer,
			layers.NewPostLayer,
			web.NewWebServer,
			web.NewMiddleware,
		),
		fx.Invoke(
			web.Register,
			web.NewDatasetHandler,
			web.NewPostHandler,
		),
	)
}