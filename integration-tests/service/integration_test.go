package service

import (
	"context"
	"net/http"
	"os"
	"testing"

	egdm "github.com/mimiro-io/entity-graph-data-model"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/mimiro-io/mssqldatalayer/integration-tests"
	"github.com/mimiro-io/mssqldatalayer/internal"
)

func TestLayer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = Describe("IntegrationTests", Ordered, func() {
	var (
		db    *integration.DockerDB
		layer *fx.App
	)
	BeforeAll(func() {
		var err error
		db, err = integration.New()
		Expect(err).To(BeNil())
		Expect(db).NotTo(BeNil())
		DeferCleanup(db.Close)

		confLocation, err := db.TmpConfig()
		Expect(err).To(BeNil())
		DeferCleanup(os.Remove, confLocation)
		_ = os.Setenv("SERVER_PORT", "12412")
		_ = os.Setenv("CONFIG_LOCATION", "file://"+confLocation)
		//_ = os.Setenv("CONFIG_REFRESH_INTERVAL", "@every 60s")
		_ = os.Setenv("SERVICE_NAME", "datahub-mssql-datalayer")
		_ = os.Setenv("MSSQL_DB_USER", "sa")
		_ = os.Setenv("MSSQL_DB_PASSWORD", "Foobar123")
		_ = os.Setenv("AUTHORIZATION_MIDDLEWARE", "noop")
		layer = internal.CreateLayer(fx.NopLogger)
		Expect(layer.Start(context.Background())).To(Succeed())
		DeferCleanup(layer.Stop, context.Background())
	})

	Describe("Get /changes", func() {
		It("initial should be a 200 OK response", func() {
			response, err := http.Get("http://localhost:12412/datasets/test/changes")
			Expect(err).To(BeNil())
			//b, _ := io.ReadAll(response.Body)
			//println(string(b))
			batch, err := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs().LoadEntityCollection(response.Body)
			Expect(err).To(BeNil())
			Expect(batch.Continuation).NotTo(BeNil())
			token := batch.Continuation.Token
			Expect(token).NotTo(BeEmpty())
			Expect(batch.GetEntities()).To(HaveLen(1))

			Expect(db.Insert(2, "Name2")).To(Succeed())

			response, err = http.Get("http://localhost:12412/datasets/test/changes?since=" + token)
			Expect(err).To(BeNil())
			batch, err = egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs().LoadEntityCollection(response.Body)
			Expect(err).To(BeNil())
			Expect(batch.Continuation).NotTo(BeNil())
			Expect(batch.Continuation.Token).NotTo(BeEmpty())
			Expect(batch.Continuation.Token).NotTo(Equal(token))
			Expect(batch.GetEntities()).To(HaveLen(1))
		})
	})
})