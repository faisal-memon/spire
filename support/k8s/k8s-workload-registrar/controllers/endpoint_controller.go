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

	"github.com/sirupsen/logrus"
	spiffeidv1beta1 "github.com/spiffe/spire/api/spiffecrd/v1beta1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EndpointReconcilerConfig holds the config passed in when creating the reconciler
type EndpointReconcilerConfig struct {
	Log                logrus.FieldLogger
	Mgr                ctrl.Manager
	DisabledNamespaces []string
}

// EndpointReconciler holds the runtime configuration and state of this controller
type EndpointReconciler struct {
	client.Client
	c EndpointReconcilerConfig
}

// NewEndpointReconciler creates a new EndpointReconciler object
func NewEndpointReconciler(config EndpointReconcilerConfig) (*EndpointReconciler, error) {
	r := &EndpointReconciler{
		Client: config.Mgr.GetClient(),
		c:      config,
	}

	err := ctrl.NewControllerManagedBy(config.Mgr).
		For(&corev1.Endpoints{}).
		Complete(r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Reconcile steps through the endpoints for each service and adds the name of the service as
// a DNS name to the SPIFFE ID CRD
func (e *EndpointReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	if containsString(e.c.DisabledNamespaces, req.NamespacedName.Namespace) {
		return ctrl.Result{}, nil
	}

	var endpoints corev1.Endpoints
	ctx := context.Background()

	if err := e.Get(ctx, req.NamespacedName, &endpoints); err != nil {
		if errors.IsNotFound(err) {
			// Delete event
			if err := e.deleteExternalResources(ctx, req.NamespacedName); err != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, client.IgnoreNotFound(err)
		}

		e.c.Log.WithError(err).Error("Unable to fetch Endpoints")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	svcName := getServiceDNSName(req.NamespacedName)
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			if address.TargetRef == nil {
				continue
			}

			spiffeIDList := spiffeidv1beta1.SpiffeIDList{}
			labelSelector := labels.Set(map[string]string{
				"podUid": string(address.TargetRef.UID),
			})
			err := e.List(ctx, &spiffeIDList, &client.ListOptions{
				LabelSelector: labelSelector.AsSelector(),
			})
			if err != nil {
				e.c.Log.WithError(err).Error("Error getting spiffeid list")
				return ctrl.Result{}, err
			}

			for _, spiffeID := range spiffeIDList.Items {
				if !containsString(spiffeID.Spec.DnsNames[1:], svcName) {
					spiffeID.Spec.DnsNames = append(spiffeID.Spec.DnsNames, svcName)
					err := e.Update(ctx, &spiffeID)
					if err != nil {
						return ctrl.Result{}, err
					}
				}
			}
		}
	}

	return ctrl.Result{}, nil
}

// deleteExternalResources remove the service name from the list of DNS Names when the service is removed
func (e *EndpointReconciler) deleteExternalResources(ctx context.Context, namespacedName types.NamespacedName) error {
	svcName := getServiceDNSName(namespacedName)
	spiffeIDList := spiffeidv1beta1.SpiffeIDList{}

	err := e.List(ctx, &spiffeIDList, &client.ListOptions{
		Namespace: namespacedName.Namespace,
	})
	if err != nil {
		if !errors.IsNotFound(err) {
			e.c.Log.WithFields(logrus.Fields{
				"service": svcName,
			}).WithError(err).Error("Failed to get list of SpiffeID CRDs")
			return err
		}

		return nil
	}

	for _, spiffeId := range spiffeIDList.Items {
		e.c.Log.WithFields(logrus.Fields{
			"spiffeId": spiffeId.ObjectMeta.Name,
		}).Info("Removing DNS names")

		spiffeId.Spec.DnsNames = removeStringIf(spiffeId.Spec.DnsNames, svcName)

		if err := e.Update(ctx, &spiffeId); err != nil {
			e.c.Log.WithFields(logrus.Fields{
				"spiffeId": spiffeId.ObjectMeta.Name,
			}).WithError(err).Error("Unable to delete DNS names in SpiffeID CRD")
			return err
		}
	}

	return nil
}

func getServiceDNSName(namespacedName types.NamespacedName) string {
	return namespacedName.Name + "." + namespacedName.Namespace + ".svc"
}
