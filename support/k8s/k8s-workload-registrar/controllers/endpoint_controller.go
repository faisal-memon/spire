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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
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
	c                 EndpointReconcilerConfig
	svcNametoSpiffeID map[string][]string
}

// NewEndpointReconciler creates a new EndpointReconciler object
func NewEndpointReconciler(config EndpointReconcilerConfig) (*EndpointReconciler, error) {
	r := &EndpointReconciler{
		Client:            config.Mgr.GetClient(),
		c:                 config,
		svcNametoSpiffeID: make(map[string][]string),
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

			// Retrieve pod object for this endpoint
			pod := corev1.Pod{}
			podNamespacedName := types.NamespacedName{
				Name:      address.TargetRef.Name,
				Namespace: address.TargetRef.Namespace,
			}
			if err := e.Get(ctx, podNamespacedName, &pod); err != nil {
				e.c.Log.WithError(err).Error("Error retrieving pod")
				return ctrl.Result{}, err
			}

			// Retrieve SPIFFE ID CRD for the pod
			existing := &spiffeidv1beta1.SpiffeID{}
			spiffeIdName := pod.ObjectMeta.Labels["spiffe.io/spiffeid"]
			spiffeIdNamespacedName := types.NamespacedName{
				Name:      spiffeIdName,
				Namespace: address.TargetRef.Namespace,
			}
			if err := e.Get(ctx, spiffeIdNamespacedName, existing); err != nil {
				if !errors.IsNotFound(err) {
					e.c.Log.WithFields(logrus.Fields{
						"name": spiffeIdName,
					}).WithError(err).Error("Failed to get SpiffeID CRD")
					return ctrl.Result{}, err
				}
				continue
			}

			// Add service as DNS name if it doesn't already exist
			dnsNameAdded := false
			retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				if err := e.Get(ctx, spiffeIdNamespacedName, existing); err != nil {
					e.c.Log.WithError(err).Error("Failed to get latest version of SpiffeID CRD")
					return err
				}

				if existing != nil && !containsString(existing.Spec.DnsNames[1:], svcName) {
					existing.Spec.DnsNames = append(existing.Spec.DnsNames, svcName)
					err := e.Update(ctx, existing)
					if err == nil {
						dnsNameAdded = true
					}
					return err
				}

				return nil
			})
			if retryErr != nil {
				e.c.Log.WithFields(logrus.Fields{
					"name": spiffeIdName,
				}).Error(retryErr, "Unable to add DNS names in SpiffeID CRD")
				return ctrl.Result{}, retryErr
			}

			if dnsNameAdded {
				e.c.Log.WithFields(logrus.Fields{
					"service": svcName,
					"pod":     pod.ObjectMeta.Name,
				}).Info("Added DNS names to CRD")

				if e.svcNametoSpiffeID[svcName] == nil {
					e.svcNametoSpiffeID[svcName] = make([]string, 0)
				}
				e.svcNametoSpiffeID[svcName] = append(e.svcNametoSpiffeID[svcName], spiffeIdName)
			}
		}
	}

	return ctrl.Result{}, nil
}

// deleteExternalResources remove the service name from the list of DNS Names when the service is removed
func (e *EndpointReconciler) deleteExternalResources(ctx context.Context, namespacedName types.NamespacedName) error {
	svcName := getServiceDNSName(namespacedName)

	for _, spiffeidname := range e.svcNametoSpiffeID[svcName] {
		existing := &spiffeidv1beta1.SpiffeID{}
		namespacedName := types.NamespacedName{
			Name: spiffeidname,
			Namespace: namespacedName.Namespace,
		}
		if err := e.Get(ctx, namespacedName, existing); err != nil {
			if !errors.IsNotFound(err) {
				e.c.Log.WithFields(logrus.Fields{
					"name": spiffeidname,
				}).WithError(err).Error("Failed to get SpiffeID CRD")
				return err
			}

			return nil
		}
		if existing != nil {
			e.c.Log.WithFields(logrus.Fields{
				"service": svcName,
			}).Info("Deleting DNS names on SpiffeID CRD")

			existing.Spec.DnsNames = removeStringIf(existing.Spec.DnsNames, svcName)
			if err := e.Update(ctx, existing); err != nil {
				e.c.Log.WithFields(logrus.Fields{
					"name": spiffeidname,
				}).WithError(err).Error("Unable to delete DNS names in SpiffeID CRD")
				return err
			}

			delete(e.svcNametoSpiffeID, svcName)
		}
	}
	return nil
}

func getServiceDNSName(namespacedName types.NamespacedName) string {
	return namespacedName.Name + "." + namespacedName.Namespace + ".svc"
}
