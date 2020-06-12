/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"net/url"
	"path"

	"github.com/sirupsen/logrus"

	"github.com/spiffe/spire/pkg/common/idutil"
	"github.com/spiffe/spire/proto/spire/api/registration"
	"github.com/spiffe/spire/proto/spire/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeReconcilerConfig holds the config passed in when creating the reconciler
type NodeReconcilerConfig struct {
	Log         logrus.FieldLogger
	R           registration.RegistrationClient
	Mgr         ctrl.Manager
	TrustDomain string
	Cluster     string
}

// NodeReconciler holds the runtime configuration and state of this controller
type NodeReconciler struct {
	client.Client
	c NodeReconcilerConfig
}

// NewNodeReconciler creates a new NodeReconciler object
func NewNodeReconciler(config NodeReconcilerConfig) (*NodeReconciler, error) {
	r := &NodeReconciler{
		Client: config.Mgr.GetClient(),
		c:      config,
	}

	err := ctrl.NewControllerManagedBy(config.Mgr).
		For(&corev1.Node{}).
		Complete(r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Reconcile steps through the endpoints for each service and adds the name of the service as
// a DNS name to the SPIFFE ID CRD
func (n *NodeReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	var node corev1.Node
	ctx := context.Background()

	if err := n.Get(ctx, req.NamespacedName, &node); err != nil {
		if errors.IsNotFound(err) {
			// Delete event
			if err := n.deleteExternalResources(ctx, req.NamespacedName); err != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, client.IgnoreNotFound(err)
		}

		n.c.Log.WithError(err).Error("Unable to fetch Node")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	n.c.Log.Debug(node.ObjectMeta.Name)

	entry := &common.RegistrationEntry{
		ParentId: idutil.ServerID(n.c.TrustDomain),
		SpiffeId: n.nodeID(node.ObjectMeta.Name),
		Selectors: []*common.Selector{
			{Type: "k8s_psat", Value: fmt.Sprintf("cluster:%s", n.c.Cluster)},
		},
	}

	response, err := n.c.R.CreateEntryIfNotExists(ctx, entry)
	if err != nil {
		n.c.Log.WithError(err).Error("Failed to create registration entry")
		return ctrl.Result{}, err
	}
	if !response.Preexisting {
		n.c.Log.WithFields(logrus.Fields{
			"entryID":  response.Entry.EntryId,
			"spiffeID": n.nodeID(node.ObjectMeta.Name),
		}).Info("Created entry")
	}

	return ctrl.Result{}, nil
}

func (n *NodeReconciler) makeID(pathFmt string, pathArgs ...interface{}) string {
	id := url.URL{
		Scheme: "spiffe",
		Host:   n.c.TrustDomain,
		Path:   path.Clean(fmt.Sprintf(pathFmt, pathArgs...)),
	}
	return id.String()
}

func (n *NodeReconciler) nodeID(nodeName string) string {
	return n.makeID("k8s-workload-registrar/%s/node/%s", n.c.Cluster, nodeName)
}

// deleteExternalResources remove the service name from the list of DNS Names when the service is removed
func (n *NodeReconciler) deleteExternalResources(ctx context.Context, namespacedName types.NamespacedName) error {
	return nil
}
