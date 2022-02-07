package main

import (
	"github.com/mimiro-io/mssqldatalayer"
	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/mimiro-io/mssqldatalayer/internal/layers"
	"github.com/mimiro-io/mssqldatalayer/internal/security"
	"github.com/mimiro-io/mssqldatalayer/internal/web"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func main() {

	app := fx.New(
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

	app.Run()
}

func startup(log *zap.SugaredLogger) {
	log.Infof("Starting up")
}
