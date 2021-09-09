package conf

import (
	"fmt"
	"github.com/franela/goblin"
	"github.com/mimiro-io/mssqldatalayer/internal/security"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoadFile(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("when loading a config file", func() {
		g.It("should not fail", func() {
			cmgr := ConfigurationManager{
				logger: zap.NewNop().Sugar(),
			}
			conf, err := cmgr.loadFile("file://../../resources/test/test-config.json")
			if err != nil {
				g.Fail(err)
			}
			g.Assert(conf).IsNotNil()
		})
		g.It("should return error when not found", func() {
			cmgr := ConfigurationManager{
				logger: zap.NewNop().Sugar(),
			}
			_, err := cmgr.loadFile("file://../../resources/rnadom-file.json")
			g.Assert(err).IsNotNil()
		})
	})

}

func TestLoadUrl(t *testing.T) {
	srv := serverMock()
	defer srv.Close()

	g := goblin.Goblin(t)
	g.Describe("when loading from http", func() {
		g.It("should not fail", func() {
			cmgr := ConfigurationManager{
				logger:         zap.NewNop().Sugar(),
				TokenProviders: security.NoOpTokenProviders(),
			}
			conf, err := cmgr.loadUrl(fmt.Sprintf("%s/test/config.json", srv.URL))
			if err != nil {
				g.Fail(err)
			}
			g.Assert(conf).IsNotNil()
		})
		g.It("should return error when not found", func() {
			cmgr := ConfigurationManager{
				logger:         zap.NewNop().Sugar(),
				TokenProviders: security.NoOpTokenProviders(),
			}
			_, err := cmgr.loadUrl(fmt.Sprintf("%s/test/configx.json", srv.URL))
			g.Assert(err).IsNotNil()
		})
	})

}

func TestParse(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("when parsing a confgi from a file", func() {
		g.It("should succeed", func() {
			cmgr := ConfigurationManager{
				logger: zap.NewNop().Sugar(),
			}

			res, err := cmgr.loadFile("file://../../resources/test/test-config.json")
			if err != nil {
				g.Fail(err)
			}
			config, err := cmgr.parse(res)
			if err != nil {
				g.Fail(err)
			}

			g.Assert(config).IsNotNil()
			g.Assert(config.Schema).Equal("SalesLT")

		})
	})

}

func serverMock() *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/test/config.json", configMock)

	srv := httptest.NewServer(handler)

	return srv
}

func configMock(w http.ResponseWriter, r *http.Request) {
	cmgr := ConfigurationManager{
		logger: zap.NewNop().Sugar(),
	}
	res, _ := cmgr.loadFile("file://../../resources/test/test-config.json")
	_, _ = w.Write(res)
}
