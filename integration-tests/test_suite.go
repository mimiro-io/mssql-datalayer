package integration_tests

import (
	"log"

	"github.com/ory/dockertest/v3"
)

type TestService struct {
	MsSql *dockertest.Resource
	Pool  *dockertest.Pool
}

func New() (*TestService, error) {
	// Initialize docker pool
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Printf("Could not construct pool: %s", err)
		return nil, err
	}

	// Ping the docker daemon
	// check if everything is good and
	// there is the connection with docker
	if err = pool.Client.Ping(); err != nil {
		log.Printf(`could not connect to docker: %s`, err)
		return nil, err
	}

	// Build and run the redis server
	redisContainer, err := pool.Run("bitnami/redis", "latest", []string{"ALLOW_EMPTY_PASSWORD=yes"})
	if err != nil {
		log.Printf(`could not start redis: %s`, err)
		return nil, err
	}

	return &TestService{redisContainer, pool}, nil
}