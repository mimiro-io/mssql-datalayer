package service

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mimiro-io/mssqldatalayer/integration-tests"
)

func TestLayer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = Describe("IntegrationTests", Ordered, func() {
	var (
		srv     *integration_tests.TestService
		err     error
		counter float64
	)
	// Initialize our docker containers
	// Pass all health checks
	// Expect no errors
	BeforeAll(func() {
		srv, err = integration_tests.New()
		counter = float64(15)
		Expect(err).To(BeNil())
		Expect(srv).NotTo(BeNil())
	})

	// Cleanup our resoursec after all steps
	AfterAll(func() {
		err = srv.Close()
		Expect(err).To(BeNil())
	})

	Describe("put item to redis", func() {
		Context("send POST request", func() {
			It("should be a 200 OK response", func() {
				body := []byte(fmt.Sprintf(`{ "counter": %v }`, counter))
				// Use `localhost:8080` since we're outside of docker network
				response, err := service.PostRequst("http://localhost:8080/item", body)
				Expect(err).To(BeNil())
				Expect(response).To(Equal(map[string]interface{}{"success": true}))
			})
		})
	})

	Describe("get item from redis", func() {
		Context("send GET request", func() {
			It("should be a 200 OK response", func() {
				response, err := service.GetRequst("http://localhost:8080/item")
				Expect(err).To(BeNil())
				Expect(response).To(Equal(map[string]interface{}{"counter": counter}))
			})
		})
	})

	Describe("get item from redis", func() {
		Context("wait 15 seconds and send GET request", func() {
			It("should be a 500 Internal Error response", func() {
				time.Sleep(15 * time.Second)
				_, err := service.GetRequst("http://localhost:8080/item")
				Expect(err).NotTo(BeNil())
			})
		})
	})
})