package main

import (
	"context"
	"io/ioutil"

	"github.com/hashicorp/hcl"
	"github.com/zeebo/errs"
)

const (
	defaultLogLevel = "info"

	modeCRD     = "crd"
	modeWebhook = "webhook"
	defaultMode = modeWebhook
)

type Config interface {
	ParseConfig(hclConfig string) error
	Run(ctx context.Context) error
}

type CommonConfig struct {
	LogFormat        string `hcl:"log_format"`
	LogLevel         string `hcl:"log_level"`
	LogPath          string `hcl:"log_path"`
	TrustDomain      string `hcl:"trust_domain"`
	ServerSocketPath string `hcl:"server_socket_path"`
	Cluster          string `hcl:"cluster"`
	PodLabel         string `hcl:"pod_label"`
	PodAnnotation    string `hcl:"pod_annotation"`
	Mode             string `hcl:"mode"`
}

func (c *CommonConfig) ParseConfig(hclConfig string) error {
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
