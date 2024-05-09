package main

import "github.com/AlphaMinZ/myredis_go/app"

func main() {
	server, err := app.ConstructServer()
	if err != nil {
		panic(err)
	}

	app := app.NewApplication(server, &app.Config{
		Address: ":6378",
	})
	if err := app.Run(); err != nil {
		panic(err)
	}
}
