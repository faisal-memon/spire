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
	"k8s.io/apimachinery/pkg/runtime"
	spiffeidv1beta1 "github.com/spiffe/spire/api/spiffecrd/v1beta1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/api/core/v1"
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
		// Setup all Controllers
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = spiffeidv1beta1.AddToScheme(scheme)
		_ = corev1.AddToScheme(scheme)

		mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
			Scheme:             scheme,
			// MetricsBindAddress: config.MetricsAddr,
			//LeaderElection:     config.LeaderElection,
			//Port:               9443,
		})
		if err != nil {
			return err
		}

		if err = (&controllers.SpiffeIDReconciler{
			Client:      mgr.GetClient(),
			Log:         ctrl.Log.WithName("controllers").WithName("SpiffeID"),
			Scheme:      mgr.GetScheme(),
			SpireClient: registration.NewRegistrationClient(serverConn),
			Cluster:     config.Cluster,
			TrustDomain: config.TrustDomain,
		}).SetupWithManager(mgr); err != nil {
			return err
		}
		if config.PodController {
			mode := controllers.PodReconcilerModeServiceAccount
			value := ""
			if len(config.PodLabel) > 0 {
				mode = controllers.PodReconcilerModeLabel
				value = config.PodLabel
			}
			if len(config.PodAnnotation) > 0 {
				mode = controllers.PodReconcilerModeAnnotation
				value = config.PodAnnotation
			}
			ctlr := &controllers.PodController{
				Client:             mgr.GetClient(),
				Mgr:                mgr,
				Log:                ctrl.Log.WithName("controllers"),
				Scheme:             mgr.GetScheme(),
				TrustDomain:        config.TrustDomain,
				Mode:               mode,
				Value:              value,
				DisabledNamespaces: config.DisabledNamespaces,
				AddSvcDNSName:      config.AddSvcDNSName,
			}
			if err := controllers.BuildPodControllers(ctlr); err != nil {
				return err
			}
		}

		if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
			return err
		}

	}

	return nil
}
