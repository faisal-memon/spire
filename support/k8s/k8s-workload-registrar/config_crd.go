package main

import (
	"context"

	"github.com/hashicorp/hcl"
	"github.com/spiffe/spire/proto/spire/api/registration"
	"github.com/spiffe/spire/support/k8s/k8s-workload-registrar/controllers"
	"github.com/zeebo/errs"

	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	defaultAddSvcDNSName = true
	defaultPodController = true
	defaultMetricsBindAddr = ":8080"
)

type CRDMode struct {
	CommonMode
	AddSvcDNSName      bool     `hcl:"add_svc_dns_name"`
	DisabledNamespaces []string `hcl:"disabled_namespaces"`
	LeaderElection     bool     `hcl:"leader_election"`
	MetricsBindAddr    string   `hcl:"metrics_bind_addr"`
	PodController      bool     `hcl:"pod_controller"`
}

func (c *CRDMode) ParseConfig(hclConfig string) error {
	c.PodController = defaultPodController
	c.AddSvcDNSName = defaultAddSvcDNSName
	if err := hcl.Decode(c, hclConfig); err != nil {
		return errs.New("unable to decode configuration: %v", err)
	}

	if c.MetricsBindAddr == "" {
		c.MetricsBindAddr = defaultMetricsBindAddr
	}

	if c.DisabledNamespaces == nil {
		c.DisabledNamespaces = defaultDisabledNamespaces()
	}

	return nil
}

func (c *CRDMode) Run(ctx context.Context) error {
	mgr, err := controllers.NewManager(c.LeaderElection, c.MetricsBindAddr)
	if err != nil {
		return err
	}

	c.log.Info("Initializing SPIFFE ID CRD Mode")
	err = controllers.AddSpiffeIDReconciler(controllers.SpiffeIDReconcilerConfig{
		Cluster:     c.Cluster,
		Ctx:         ctx,
		Log:         c.log,
		Mgr:         mgr,
		R:           registration.NewRegistrationClient(c.serverConn),
		TrustDomain: c.TrustDomain,
	})
	if err != nil {
		return err
	}

	if c.PodController {
		err = controllers.AddNodeReconciler(controllers.NodeReconcilerConfig{
			Cluster:     c.Cluster,
			Ctx:         ctx,
			Log:         c.log,
			Mgr:         mgr,
			R:           registration.NewRegistrationClient(c.serverConn),
			TrustDomain: c.TrustDomain,
		})
		if err != nil {
			return err
		}
		err := controllers.AddPodReconciler(controllers.PodReconcilerConfig{
			Cluster:            c.Cluster,
			Ctx:                ctx,
			DisabledNamespaces: c.DisabledNamespaces,
			Log:                c.log,
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
		err := controllers.AddEndpointReconciler(controllers.EndpointReconcilerConfig{
			Ctx:                ctx,
			DisabledNamespaces: c.DisabledNamespaces,
			Log:                c.log,
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
