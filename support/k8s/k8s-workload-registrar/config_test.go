package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testMinimalConfig = `
		trust_domain = "TRUSTDOMAIN"
		cluster = "CLUSTER"
		server_socket_path = "SOCKETPATH"
`
)

func TestLoadMode(t *testing.T) {
	require := require.New(t)

	dir, err := ioutil.TempDir("", "spire-adm-webhook-config-")
	require.NoError(err)
	defer os.RemoveAll(dir)

	confPath := filepath.Join(dir, "test.conf")

	_, err = LoadMode(context.TODO(), confPath)
	require.Error(err)
	require.Contains(err.Error(), "unable to load configuration:")

	err = ioutil.WriteFile(confPath, []byte(testMinimalConfig), 0600)
	require.NoError(err)

	config, err := LoadMode(context.TODO(), confPath)
	require.NoError(err)

	require.Equal(&WebhookMode{
		CommonMode: CommonMode{
			ServerSocketPath: "SOCKETPATH",
			TrustDomain:      "TRUSTDOMAIN",
			Cluster:          "CLUSTER",
			LogLevel:         defaultLogLevel,
			Mode:             "webhook",
		},
		Addr:             ":8443",
		CertPath:         defaultCertPath,
		KeyPath:          defaultKeyPath,
		CaCertPath:       defaultCaCertPath,
	}, config)

	testCases := []struct {
		name string
		in   string
		out  *WebhookMode
		err  string
	}{
		{
			name: "defaults",
			in:   testMinimalConfig,
			out: &WebhookMode{
				CommonMode: CommonMode{
					LogLevel:                       defaultLogLevel,
					ServerSocketPath:               "SOCKETPATH",
					TrustDomain:                    "TRUSTDOMAIN",
					Cluster:                        "CLUSTER",
					Mode:                           "webhook",
				},
				Addr:                           ":8443",
				CertPath:                       defaultCertPath,
				KeyPath:                        defaultKeyPath,
				CaCertPath:                     defaultCaCertPath,
				InsecureSkipClientVerification: false,
			},
		},
		{
			name: "overrides",
			in: `
				log_level = "LEVELOVERRIDE"
				log_path = "PATHOVERRIDE"
				addr = ":1234"
				cert_path = "CERTOVERRIDE"
				key_path = "KEYOVERRIDE"
				cacert_path = "CACERTOVERRIDE"
				insecure_skip_client_verification = true
				server_socket_path = "SOCKETPATHOVERRIDE"
				trust_domain = "TRUSTDOMAINOVERRIDE"
				cluster = "CLUSTEROVERRIDE"
				pod_label = "PODLABEL"
			`,
			out: &WebhookMode{
				CommonMode: CommonMode{
					LogLevel:                       "LEVELOVERRIDE",
					LogPath:                        "PATHOVERRIDE",
					ServerSocketPath:               "SOCKETPATHOVERRIDE",
					TrustDomain:                    "TRUSTDOMAINOVERRIDE",
					Cluster:                        "CLUSTEROVERRIDE",
					PodLabel:                       "PODLABEL",
					Mode:                           "webhook",
				},
				Addr:                           ":1234",
				CertPath:                       "CERTOVERRIDE",
				KeyPath:                        "KEYOVERRIDE",
				CaCertPath:                     "CACERTOVERRIDE",
				InsecureSkipClientVerification: true,
			},
		},
		{
			name: "bad HCL",
			in:   `INVALID`,
			err:  "unable to decode configuration",
		},
		{
			name: "missing server_socket_path",
			in: `
				trust_domain = "TRUSTDOMAIN"
				cluster = "CLUSTER"
			`,
			err: "server_socket_path must be specified",
		},
		{
			name: "missing trust domain",
			in: `
				cluster = "CLUSTER"
				server_socket_path = "SOCKETPATH"
			`,
			err: "trust_domain must be specified",
		},
		{
			name: "missing cluster",
			in: `
				trust_domain = "TRUSTDOMAIN"
				server_socket_path = "SOCKETPATH"
			`,
			err: "cluster must be specified",
		},
		{
			name: "workload registration mode specification is incorrect",
			in: testMinimalConfig + `
				pod_label = "PODLABEL"
				pod_annotation = "PODANNOTATION"
			`,
			err: "workload registration mode specification is incorrect, can't specify both pod_label and pod_annotation",
		},
	}

	for _, testCase := range testCases {
		testCase := testCase // alias loop variable as it is used in the closure
		t.Run(testCase.name, func(t *testing.T) {
			err := ioutil.WriteFile(confPath, []byte(testCase.in), 0600)
			require.NoError(err)

			actual, err := LoadMode(context.TODO(), confPath)
			if testCase.err != "" {
				require.Error(err)
				require.Contains(err.Error(), testCase.err)
				return
			}
			require.NoError(err)
			require.Equal(testCase.out, actual)
		})
	}
}
