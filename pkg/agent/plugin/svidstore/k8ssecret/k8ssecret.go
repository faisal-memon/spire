package k8ssecret

import (
	"context"

	"github.com/spiffe/spire/pkg/common/catalog"
	"github.com/hashicorp/go-hclog"
	svidstorev1 "github.com/spiffe/spire-plugin-sdk/proto/spire/plugin/agent/svidstore/v1"
	configv1 "github.com/spiffe/spire-plugin-sdk/proto/spire/service/common/config/v1"
)


const (
	pluginName = "k8s_secret"
)

func BuiltIn() catalog.BuiltIn {
	return builtin(New())
}

func builtin(p *K8sSecretPlugin) catalog.BuiltIn {
	return catalog.MakeBuiltIn(pluginName,
		svidstorev1.SVIDStorePluginServer(p),
		configv1.ConfigServiceServer(p),
	)
}

func New() *K8sSecretPlugin {
	return newPlugin()
}

func newPlugin() *K8sSecretPlugin {
	p := &K8sSecretPlugin{}

	return p
}

type K8sSecretPlugin struct {
	svidstorev1.UnsafeSVIDStoreServer
	configv1.UnsafeConfigServer

	log      hclog.Logger
}

func (p *K8sSecretPlugin) SetLogger(log hclog.Logger) {
	p.log = log
}

func (p *K8sSecretPlugin) Configure(ctx context.Context, req *configv1.ConfigureRequest) (*configv1.ConfigureResponse, error) {
	p.log.Debug("Configure")
	return &configv1.ConfigureResponse{}, nil
}

func (p *K8sSecretPlugin) PutX509SVID(ctx context.Context, req *svidstorev1.PutX509SVIDRequest) (*svidstorev1.PutX509SVIDResponse, error) {
	p.log.Debug("PutX509SVID")
	return &svidstorev1.PutX509SVIDResponse{}, nil
}

func (p *K8sSecretPlugin) DeleteX509SVID(ctx context.Context, req *svidstorev1.DeleteX509SVIDRequest) (*svidstorev1.DeleteX509SVIDResponse, error) {
	p.log.Debug("DeleteX509SVID")
	return &svidstorev1.DeleteX509SVIDResponse{}, nil
}
