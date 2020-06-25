package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

var (
	configFlag = flag.String("config", "k8s-workload-registrar.conf", "configuration file")
)

func main() {
	flag.Parse()
	if err := run(context.Background(), *configFlag); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, configPath string) error {
	config, err := LoadConfig(configPath)
	if err != nil {
		return err
	}

	return config.Run(ctx)
}
