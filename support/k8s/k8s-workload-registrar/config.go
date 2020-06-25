package main

import (
	"context"
	"io/ioutil"

	"github.com/hashicorp/hcl"
	"github.com/spiffe/spire/pkg/common/log"
	"github.com/spiffe/spire/proto/spire/api/registration"
	"github.com/spiffe/spire/support/k8s/k8s-workload-registrar/controllers"
	"github.com/zeebo/errs"
	"google.golang.org/grpc"

	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	defaultLogLevel      = "info"
	defaultAddr          = ":8443"
	defaultCertPath      = "cert.pem"
	defaultKeyPath       = "key.pem"
	defaultCaCertPath    = "cacert.pem"
	defaultPodController = true
	defaultAddSvcDNSName = true

	modeCRD     = "crd"
	modeWebhook = "webhook"
	defaultMode = modeWebhook
)

func LoadConfig(path string) (Config, error) {
	hclBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errs.New("unable to load configuration: %v", err)
	}

	c := &CommonConfig{}
	c.ParseConfig(string(hclBytes))

	var config Config
	switch c.Mode {
	case modeCRD:
		config = &CRDConfig{
			CommonConfig: *c,
		}
	default:
		config = &WebhookConfig{
			CommonConfig: *c,
		}
	}

	err = config.ParseConfig(string(hclBytes))
	return config, err
}

type Config interface {
	ParseConfig(hclConfig string) error
	Run(ctx context.Context) error
}

type CommonConfig struct {
	LogFormat                      string   `hcl:"log_format"`
	LogLevel                       string   `hcl:"log_level"`
	LogPath                        string   `hcl:"log_path"`
	TrustDomain                    string   `hcl:"trust_domain"`
	ServerSocketPath               string   `hcl:"server_socket_path"`
	Cluster                        string   `hcl:"cluster"`
	PodLabel                       string   `hcl:"pod_label"`
	PodAnnotation                  string   `hcl:"pod_annotation"`
	Mode                           string   `hcl:"mode"`
}

func (c *CommonConfig) ParseConfig (hclConfig string) error {
	c.Mode = defaultMode
	if err := hcl.Decode(c, hclConfig); err != nil {
		return errs.New("unable to decode configuration: %v", err)
	}
	if c.LogLevel == "" {
		c.LogLevel = defaultLogLevel
	}
	if c.ServerSocketPath == "" {
		return errs.New("server_socket_path must be specified")
	}
	if c.TrustDomain == "" {
		return errs.New("trust_domain must be specified")
	}
	if c.Cluster == "" {
		return errs.New("cluster must be specified")
	}
	if c.PodLabel != "" && c.PodAnnotation != "" {
		return errs.New("workload registration mode specification is incorrect, can't specify both pod_label and pod_annotation")
	}
	if c.Mode != modeCRD && c.Mode != modeWebhook {
		return errs.New("invalid mode \"%s\", valid values are %s and %s", c.Mode, modeCRD, modeWebhook)
	}
	return nil
}

func (c *CommonConfig) Run(ctx context.Context) error {
	return nil
}

type CRDConfig struct {
	CommonConfig
	AddSvcDNSName       bool `hcl:"add_svc_dns_name"`
	DisabledNamespaces []string `hcl:"disabled_namespaces"`
	PodController       bool `hcl:"pod_controller"`
}

func (c *CRDConfig) ParseConfig (hclConfig string) error {
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

type WebhookConfig struct {
	CommonConfig
	Addr                           string   `hcl:"addr"`
	CaCertPath                     string   `hcl:"cacert_path"`
	CertPath                       string   `hcl:"cert_path"`
	InsecureSkipClientVerification bool     `hcl:"insecure_skip_client_verification"`
	KeyPath                        string   `hcl:"key_path"`
}

func (c *WebhookConfig) ParseConfig(hclConfig string) error {
	if err := hcl.Decode(c, hclConfig); err != nil {
		return errs.New("unable to decode configuration: %v", err)
	}
	if c.Addr == "" {
		c.Addr = defaultAddr
	}
	if c.CertPath == "" {
		c.CertPath = defaultCertPath
	}
	if c.CaCertPath == "" {
		c.CaCertPath = defaultCaCertPath
	}
	if c.KeyPath == "" {
		c.KeyPath = defaultKeyPath
	}
	return nil
}

func (c *WebhookConfig) Run(ctx context.Context) error {
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

	controller := NewController(ControllerConfig{
		Log:           log,
		R:             registration.NewRegistrationClient(serverConn),
		TrustDomain:   c.TrustDomain,
		Cluster:       c.Cluster,
		PodLabel:      c.PodLabel,
		PodAnnotation: c.PodAnnotation,
	})

	log.Info("Initializing registrar")
	if err := controller.Initialize(ctx); err != nil {
		return err
	}

	server, err := NewServer(ServerConfig{
		Log:                            log,
		Addr:                           c.Addr,
		Handler:                        NewWebhookHandler(controller),
		CertPath:                       c.CertPath,
		KeyPath:                        c.KeyPath,
		CaCertPath:                     c.CaCertPath,
		InsecureSkipClientVerification: c.InsecureSkipClientVerification,
	})
	if err != nil {
		return err
	}
	return server.Run(ctx)
}
