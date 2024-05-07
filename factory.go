package myredisgo

import (
	"github.com/AlphaMinZ/myredis_go/database"
	"github.com/AlphaMinZ/myredis_go/datastore"
	"github.com/AlphaMinZ/myredis_go/handler"
	"github.com/AlphaMinZ/myredis_go/log"
	"github.com/AlphaMinZ/myredis_go/protocol"
	"github.com/AlphaMinZ/myredis_go/server"
	"go.uber.org/dig"
)

var container = dig.New()

func init() {
	// logger
	_ = container.Provide(log.GetDefaultLogger)

	// parser
	_ = container.Provide(protocol.NewParser)

	// datastore
	_ = container.Provide(datastore.NewKVStore)

	// database
	_ = container.Provide(database.NewDBExecutor)
	_ = container.Provide(database.NewDBTrigger)

	// handler
	_ = container.Provide(handler.NewHandler)

	// server
	_ = container.Provide(server.NewServer)
}

func constructServer() (*server.Server, error) {
	var s *server.Server
	if err := container.Invoke(func(_s *server.Server) {
		s = _s
	}); err != nil {
		return nil, err
	}
	return s, nil
}
