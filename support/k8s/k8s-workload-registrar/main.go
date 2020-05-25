package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/spiffe/spire/pkg/common/log"
	"github.com/spiffe/spire/proto/spire/api/registration"
	"github.com/spiffe/spire/support/k8s/k8s-workload-registrar/controllers"
	"github.com/zeebo/errs"
	"google.golang.org/grpc"

	ctrl "sigs.k8s.io/controller-runtime"
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

	log, err := log.NewLogger(log.WithLevel(config.LogLevel), log.WithFormat(config.LogFormat), log.WithOutputFile(config.LogPath))
	if err != nil {
		return err
	}
	defer log.Close()

	log.WithField("socket_path", config.ServerSocketPath).Info("Dialing server")
	serverConn, err := grpc.DialContext(ctx, "unix://"+config.ServerSocketPath, grpc.WithInsecure())
	if err != nil {
		return errs.New("failed to dial server: %v", err)
	}
	defer serverConn.Close()

	switch config.Mode {
	case modeWebhook:
		controller := NewController(ControllerConfig{
			Log:           log,
			R:             registration.NewRegistrationClient(serverConn),
			TrustDomain:   config.TrustDomain,
			Cluster:       config.Cluster,
			PodLabel:      config.PodLabel,
			PodAnnotation: config.PodAnnotation,
		})

		log.Info("Initializing registrar")
		if err := controller.Initialize(ctx); err != nil {
			return err
		}

		server, err := NewServer(ServerConfig{
			Log:                            log,
			Addr:                           config.Addr,
			Handler:                        NewWebhookHandler(controller),
			CertPath:                       config.CertPath,
			KeyPath:                        config.KeyPath,
			CaCertPath:                     config.CaCertPath,
			InsecureSkipClientVerification: config.InsecureSkipClientVerification,
		})
		if err != nil {
			return err
		}
		return server.Run(ctx)
	case modeCRD:
		mgr, err := controllers.NewManager()
		if err != nil {
			return err
		}

		spiffeIDReconciler, err := controllers.NewSpiffeIDReconciler(controllers.SpiffeIDReconcilerConfig{
			Log:         log,
			Mgr:         mgr,
			R:           registration.NewRegistrationClient(serverConn),
			Cluster:     config.Cluster,
			TrustDomain: config.TrustDomain,
		})
		if err != nil {
			return err
		}

		log.Info("Initializing SPIFFE ID Reconciler")
		if err := spiffeIDReconciler.Initialize(ctx); err != nil {
			return err
		}

		if config.PodController {
			_, err := controllers.NewPodReconciler(controllers.PodReconcilerConfig{
				Log:                log,
				Mgr:                mgr,
				TrustDomain:        config.TrustDomain,
				PodLabel:           config.PodLabel,
				PodAnnotation:      config.PodAnnotation,
				DisabledNamespaces: config.DisabledNamespaces,
			})
			if err != nil {
				return err
			}
		}

		if config.AddSvcDNSName {
			_, err := controllers.NewEndpointReconciler(controllers.EndpointReconcilerConfig{
				Log:                log,
				Mgr:                mgr,
				DisabledNamespaces: config.DisabledNamespaces,
			})
			if err != nil {
				return err
			}
		}

		if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
			return err
		}
	}

	return nil
}
