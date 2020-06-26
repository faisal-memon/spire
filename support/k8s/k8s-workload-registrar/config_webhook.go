package main

import (
	"context"

	"github.com/hashicorp/hcl"
	"github.com/spiffe/spire/pkg/common/log"
	"github.com/spiffe/spire/proto/spire/api/registration"
	"github.com/zeebo/errs"
	"google.golang.org/grpc"
)

const (
	defaultAddr       = ":8443"
	defaultCertPath   = "cert.pem"
	defaultKeyPath    = "key.pem"
	defaultCaCertPath = "cacert.pem"
)

type WebhookConfig struct {
	CommonConfig
	Addr                           string `hcl:"addr"`
	CaCertPath                     string `hcl:"cacert_path"`
	CertPath                       string `hcl:"cert_path"`
	InsecureSkipClientVerification bool   `hcl:"insecure_skip_client_verification"`
	KeyPath                        string `hcl:"key_path"`
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
