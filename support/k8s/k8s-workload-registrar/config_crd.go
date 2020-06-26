package main

import (
	"context"

	"github.com/hashicorp/hcl"
	"github.com/spiffe/spire/pkg/common/log"
	"github.com/spiffe/spire/proto/spire/api/registration"
	"github.com/spiffe/spire/support/k8s/k8s-workload-registrar/controllers"
	"github.com/zeebo/errs"
	"google.golang.org/grpc"

	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	defaultPodController = true
	defaultAddSvcDNSName = true
)

type CRDConfig struct {
	CommonConfig
	AddSvcDNSName      bool     `hcl:"add_svc_dns_name"`
	DisabledNamespaces []string `hcl:"disabled_namespaces"`
	PodController      bool     `hcl:"pod_controller"`
}

func (c *CRDConfig) ParseConfig(hclConfig string) error {
	c.PodController = defaultPodController
	c.AddSvcDNSName = defaultAddSvcDNSName
	if err := hcl.Decode(c, hclConfig); err != nil {
		return errs.New("unable to decode configuration: %v", err)
	}

	if c.DisabledNamespaces == nil {
		c.DisabledNamespaces = defaultDisabledNamespaces()
	}

	return nil
}

func (c *CRDConfig) Run(ctx context.Context) error {
	log, err := log.NewLogger(log.WithLevel(c.LogLevel), log.WithFormat(c.LogFormat), log.WithOutputFile(c.LogPath))
	if err != nil {
		return err
	}
	defer log.Close()

	log.WithField("socket_path", c.ServerSocketPath).Info("Dialing server")
	serverConn, err := grpc.DialContext(ctx, "unix://"+c.ServerSocketPath, grpc.WithInsecure())
	if err != nil {
		return errs.New("failed to dial server: %v", err)
	}
	defer serverConn.Close()

	mgr, err := controllers.NewManager()
	if err != nil {
		return err
	}

	log.Info("Initializing SPIFFE ID CRD Mode")
	_, err = controllers.NewSpiffeIDReconciler(controllers.SpiffeIDReconcilerConfig{
		Cluster:     c.Cluster,
		Ctx:         ctx,
		Log:         log,
		Mgr:         mgr,
		R:           registration.NewRegistrationClient(serverConn),
		TrustDomain: c.TrustDomain,
	})
	if err != nil {
		return err
	}

	if c.PodController {
		_, err = controllers.NewNodeReconciler(controllers.NodeReconcilerConfig{
			Cluster:     c.Cluster,
			Ctx:         ctx,
			Log:         log,
			Mgr:         mgr,
			R:           registration.NewRegistrationClient(serverConn),
			TrustDomain: c.TrustDomain,
		})
		if err != nil {
			return err
		}
		_, err := controllers.NewPodReconciler(controllers.PodReconcilerConfig{
			Cluster:            c.Cluster,
			Ctx:                ctx,
			DisabledNamespaces: c.DisabledNamespaces,
			Log:                log,
			Mgr:                mgr,
			PodLabel:           c.PodLabel,
			PodAnnotation:      c.PodAnnotation,
			TrustDomain:        c.TrustDomain,
		})
		if err != nil {
			return err
		}
	}

	if c.AddSvcDNSName {
		_, err := controllers.NewEndpointReconciler(controllers.EndpointReconcilerConfig{
			Ctx:                ctx,
			DisabledNamespaces: c.DisabledNamespaces,
			Log:                log,
			Mgr:                mgr,
		})
		if err != nil {
			return err
		}
	}

	return mgr.Start(ctrl.SetupSignalHandler())
}

func defaultDisabledNamespaces() []string {
	return []string{"kube-system"}
}
