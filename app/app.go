package app

import "github.com/AlphaMinZ/myredis_go/server"

type Application struct {
	server *server.Server
	conf   *Config
}

func NewApplication(server *server.Server, conf *Config) *Application {
	return &Application{
		server: server,
		conf:   conf,
	}
}

func (a *Application) Run() error {
	return a.server.ListenAndServer(a.conf.Address)
}
