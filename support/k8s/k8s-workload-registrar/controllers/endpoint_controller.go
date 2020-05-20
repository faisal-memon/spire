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

type EndpointReconcilerConfig struct {
	Log           logrus.FieldLogger
	Mgr           ctrl.Manager
	DisabledNamespaces []string
}

type EndpointReconciler struct {
	client.Client
	c                  EndpointReconcilerConfig
	svcNametoSpiffeID  map[string][]string
}

func NewEndpointReconciler(config EndpointReconcilerConfig) (*EndpointReconciler, error) {
	r := &EndpointReconciler {
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

		e.c.Log.WithError(err).Error("unable to fetch Endpoints")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	svcName := req.NamespacedName.Name + "." + req.NamespacedName.Namespace + "." + "svc"
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			if address.TargetRef == nil {
				continue
			}
			pod := corev1.Pod{}
			if err := e.Get(ctx, types.NamespacedName{Name: address.TargetRef.Name, Namespace: address.TargetRef.Namespace}, &pod); err != nil {
				e.c.Log.WithError(err).Error("Error retreiving pod")
				return ctrl.Result{}, err
			}
			spiffeidname := pod.ObjectMeta.Labels["spiffe.io/spiffeid"]
			existing := &spiffeidv1beta1.SpiffeID{}
			if err := e.Get(ctx, types.NamespacedName{Name: spiffeidname, Namespace: address.TargetRef.Namespace}, existing); err != nil {
				if !errors.IsNotFound(err) {
					e.c.Log.WithFields(logrus.Fields{
						"name": spiffeidname,
					}).WithError(err).Error("failed to get SpiffeID")
					return ctrl.Result{}, err
				}
				continue
			}
			dnsNameAdded := false
			retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				if err := e.Get(ctx, types.NamespacedName{Name: spiffeidname, Namespace: address.TargetRef.Namespace}, existing); err != nil {
					e.c.Log.WithError(err).Error("Failed to get latest version of SPIFFE ID")
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
					"name": spiffeidname,
				}).Error(retryErr, "unable to add DNS names in SPIFFE ID CRD")
				return ctrl.Result{}, retryErr
			}

			if dnsNameAdded {
				e.c.Log.WithFields(logrus.Fields{
					"service": svcName,
					"pod": pod.ObjectMeta.Name,
				}).Info("added DNS names for")

				if e.svcNametoSpiffeID[svcName] == nil {
					e.svcNametoSpiffeID[svcName] = make([]string, 0)
				}
				e.svcNametoSpiffeID[svcName] = append(e.svcNametoSpiffeID[svcName], spiffeidname)
			}
		}
	}

	return ctrl.Result{}, nil
}
func (e *EndpointReconciler) deleteExternalResources(ctx context.Context, namespacedName types.NamespacedName) error {
	svcName := namespacedName.Name + "." + namespacedName.Namespace
	for _, spiffeidname := range e.svcNametoSpiffeID[svcName] {

		existing := &spiffeidv1beta1.SpiffeID{}
		if err := e.Get(ctx, types.NamespacedName{Name: spiffeidname, Namespace: namespacedName.Namespace}, existing); err != nil {
			if !errors.IsNotFound(err) {
				e.c.Log.WithFields(logrus.Fields{
					"name": spiffeidname,
				}).WithError(err).Error("failed to get SpiffeID")
				return err
			}

			return nil
		}
		if existing != nil {
			e.c.Log.WithFields(logrus.Fields{
				"service": svcName,
			}).Info("deleting DNS names for")
			i := 0 // output index
			for _, dnsName := range existing.Spec.DnsNames {
				if dnsName != svcName {
					// copy and increment index
					existing.Spec.DnsNames[i] = dnsName
					i++
				}
			}

			existing.Spec.DnsNames = existing.Spec.DnsNames[:i]
			if err := e.Update(ctx, existing); err != nil {
				e.c.Log.WithFields(logrus.Fields{
					"name": spiffeidname,
				}).WithError(err).Error("unable to delete DNS names in SPIFFE ID CRD")
				return err
			}

			delete(e.svcNametoSpiffeID, svcName)
		}
	}
	return nil
}
