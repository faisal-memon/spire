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
	"reflect"

	"github.com/sirupsen/logrus"
	spiffeidv1beta1 "github.com/spiffe/spire/api/spiffecrd/v1beta1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type PodReconcilerMode int32

const (
	PodReconcilerModeServiceAccount PodReconcilerMode = iota
	PodReconcilerModeLabel
	PodReconcilerModeAnnotation
)

type PodControllerConfig struct {
	Log           logrus.FieldLogger
	Mgr           ctrl.Manager
	TrustDomain   string
	PodLabel      string
	PodAnnotation string
	AddSvcDNSName bool
	DisabledNamespaces []string
}

type PodController struct {
	client.Client
	c                  PodControllerConfig
	Scheme             *runtime.Scheme
	Mode               PodReconcilerMode
	Value              string
	AddSvcDNSName      bool
	svcNametoSpiffeID  map[string][]string
}

func NewPodController(config PodControllerConfig) (*PodController, error) {
	mode := PodReconcilerModeServiceAccount
	value := ""
	if len(config.PodLabel) > 0 {
		mode = PodReconcilerModeLabel
		value = config.PodLabel
	}
	if len(config.PodAnnotation) > 0 {
		mode = PodReconcilerModeAnnotation
		value = config.PodAnnotation
	}

	r := &PodController {
		Client: config.Mgr.GetClient(),
		c:      config,
		Mode:   mode,
		Value:  value,
		Scheme: config.Mgr.GetScheme(),
	}

	err := ctrl.NewControllerManagedBy(config.Mgr).
		For(&corev1.Pod{}).
		Complete(&PodReconciler{ctlr: r})
	if err != nil {
		return nil, err
	}

	if config.AddSvcDNSName {
		r.svcNametoSpiffeID = make(map[string][]string)
		err = ctrl.NewControllerManagedBy(config.Mgr).
			For(&corev1.Endpoints{}).
			Complete(&EndpointReconciler{ctlr: r})
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

// PodReconciler reconciles a Pod object
type PodReconciler struct {
	ctlr *PodController
}

// Reconcile creates a new SPIFFE ID when pods are created
func (r *PodReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	ctx := context.Background()

	r.ctlr.c.Log.WithFields(logrus.Fields{
		"namespace": req.NamespacedName.Namespace,
		"name":      req.NamespacedName.Name,
	}).Debug("Pod Reconcile called")

	if containsString(r.ctlr.c.DisabledNamespaces, req.NamespacedName.Namespace) {
		return ctrl.Result{}, nil
	}

	if err := r.ctlr.Get(ctx, req.NamespacedName, &pod); err != nil {
		if !errors.IsNotFound(err) {
			r.ctlr.c.Log.WithError(err).Error("unable to fetch Pod")
			return ctrl.Result{}, err
		}

		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	spiffeIdUri := ""
	switch r.ctlr.Mode {
	case PodReconcilerModeServiceAccount:
		spiffeIdUri = r.makeID("ns/%s/sa/%s", req.Namespace, pod.Spec.ServiceAccountName)
	case PodReconcilerModeLabel:
		if val, ok := pod.GetLabels()[r.ctlr.Value]; ok {
			spiffeIdUri = r.makeID("%s", val)
		} else {
			// No relevant label
			return ctrl.Result{}, nil
		}
	case PodReconcilerModeAnnotation:
		if val, ok := pod.GetAnnotations()[r.ctlr.Value]; ok {
			spiffeIdUri = r.makeID("%s", val)
		} else {
			// No relevant annotation
			return ctrl.Result{}, nil
		}
	}

	spiffeId := &spiffeidv1beta1.SpiffeID{
		ObjectMeta: v1.ObjectMeta{
			Namespace:   pod.Namespace,
			Annotations: make(map[string]string),
		},
		Spec: spiffeidv1beta1.SpiffeIDSpec{
			SpiffeId: spiffeIdUri,
			DnsNames: make([]string, 0),
			Selector: spiffeidv1beta1.Selector{
				PodUid:    pod.GetUID(),
				Namespace: pod.Namespace,
			},
		},
	}

	// Set pod as owner of new SPIFFE ID
	err := controllerutil.SetControllerReference(&pod, spiffeId, r.ctlr.Scheme)
	if err != nil {
		r.ctlr.c.Log.WithFields(logrus.Fields{
			"SpiffeID.Name": spiffeId.Name,
		}).WithError(err).Error("Failed to set pod as owner of new SpiffeID")
		return ctrl.Result{}, err
	}

	// Set pod name as first DNS name
	spiffeId.Spec.DnsNames = append(spiffeId.Spec.DnsNames, pod.Name)

	err = r.createSpiffeId(ctx, pod.ObjectMeta.Name, spiffeId)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Add label to pod with name of SPIFFE ID
	if pod.ObjectMeta.Labels["spiffe.io/spiffeid"] != spiffeId.ObjectMeta.Name {
		retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Retrieve the latest version of Pod before attempting update
			// RetryOnConflict uses exponential backoff to avoid exhausting the apiserver
			if err := r.ctlr.Get(ctx, req.NamespacedName, &pod); err != nil {
				r.ctlr.c.Log.WithError(err).Error("Failed to get latest version of Pod")
				return err
			}
			pod.ObjectMeta.Labels["spiffe.io/spiffeid"] = spiffeId.ObjectMeta.Name

			err = r.ctlr.Update(ctx, &pod)

			return err
		})
		if retryErr != nil {
			r.ctlr.c.Log.WithError(retryErr).Error("Update failed")
			return ctrl.Result{}, retryErr
		}
		r.ctlr.c.Log.Info("Added label to pod")
	}

	return ctrl.Result{}, nil
}

func (r *PodReconciler) createSpiffeId(ctx context.Context, podName string, spiffeId *spiffeidv1beta1.SpiffeID) error {
	var collisionCount int32
	var existing spiffeidv1beta1.SpiffeID
	for {
		spiffeId.ObjectMeta.Name = podName + "-" + computeHash(&spiffeId.Spec, nil)
		r.ctlr.c.Log.Infof("in for loop: %v", spiffeId.ObjectMeta.Name)
		err := r.ctlr.Create(ctx, spiffeId)
		if errors.IsAlreadyExists(err) {
			r.ctlr.Get(ctx, types.NamespacedName{Name: spiffeId.ObjectMeta.Name, Namespace: spiffeId.ObjectMeta.Namespace}, &existing)
			if !reflect.DeepEqual(spiffeId.Spec, existing.Spec) {
				collisionCount++
				continue
			}
		}
		return nil
	}

}

func (r *PodReconciler) makeID(pathFmt string, pathArgs ...interface{}) string {
	id := url.URL{
		Scheme: "spiffe",
		Host:   r.ctlr.c.TrustDomain,
		Path:   path.Clean(fmt.Sprintf(pathFmt, pathArgs...)),
	}
	return id.String()
}

// EndPointReconciler reconciles a EndPoint object
type EndpointReconciler struct {
	ctlr *PodController
}

// +kubebuilder:rbac:groups=core,resources=endpoints,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=endpoints/status,verbs=get;update;patch

func (e *EndpointReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	var endpoints corev1.Endpoints

	ctx := context.Background()
	e.ctlr.c.Log.WithFields(logrus.Fields{
		"namespace": req.NamespacedName.Namespace,
		"name":      req.NamespacedName.Name,
	}).Debug("Endpoint Reconcile called")

	if containsString(e.ctlr.c.DisabledNamespaces, req.NamespacedName.Namespace) {
		return ctrl.Result{}, nil
	}

	if err := e.ctlr.Get(ctx, req.NamespacedName, &endpoints); err != nil {
		if errors.IsNotFound(err) {
			// Delete event
			if err := e.deleteExternalResources(ctx, req.NamespacedName); err != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, client.IgnoreNotFound(err)
		}

		e.ctlr.c.Log.WithError(err).Error("unable to fetch Endpoints")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	svcName := req.NamespacedName.Name + "." + req.NamespacedName.Namespace + "." + "svc"
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			if address.TargetRef != nil {
				pod := corev1.Pod{}
				if err := e.ctlr.Get(ctx, types.NamespacedName{Name: address.TargetRef.Name, Namespace: address.TargetRef.Namespace}, &pod); err != nil {
					e.ctlr.c.Log.WithError(err).Error("Error retreiving pod")
					return ctrl.Result{}, err
				}
				spiffeidname := pod.ObjectMeta.Labels["spiffe.io/spiffeid"]
				existing := &spiffeidv1beta1.SpiffeID{}
				if err := e.ctlr.Get(ctx, types.NamespacedName{Name: spiffeidname, Namespace: address.TargetRef.Namespace}, existing); err != nil {
					if !errors.IsNotFound(err) {
						e.ctlr.c.Log.WithFields(logrus.Fields{
							"name": spiffeidname,
						}).WithError(err).Error("failed to get SpiffeID")
						return ctrl.Result{}, err
					}
					continue
				}
				if existing != nil {
					e.ctlr.c.Log.WithFields(logrus.Fields{
						"service": svcName,
					}).Info("adding DNS names for")
					if !containsString(existing.Spec.DnsNames[1:], svcName) {
						retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
							if err := e.ctlr.Get(ctx, types.NamespacedName{Name: spiffeidname, Namespace: address.TargetRef.Namespace}, existing); err != nil {
								e.ctlr.c.Log.WithError(err).Error("Failed to get latest version of SPIFFE ID")
								return err
							}

							existing.Spec.DnsNames = append(existing.Spec.DnsNames, svcName)
							err := e.ctlr.Update(ctx, existing)
							if err != nil {
								e.ctlr.c.Log.WithFields(logrus.Fields{
									"name": spiffeidname,
								}).WithError(err).Error("unable to add DNS names in SPIFFE ID CRD")
							}
							return err
						})
						if retryErr != nil {
							e.ctlr.c.Log.WithFields(logrus.Fields{
								"name": spiffeidname,
							}).Error(retryErr, "unable to add DNS names in SPIFFE ID CRD")
							return ctrl.Result{}, retryErr
						}

						if e.ctlr.svcNametoSpiffeID[svcName] == nil {
							e.ctlr.svcNametoSpiffeID[svcName] = make([]string, 0)
						}
						e.ctlr.svcNametoSpiffeID[svcName] = append(e.ctlr.svcNametoSpiffeID[svcName], spiffeidname)
					}
				}
			}
		}
	}

	return ctrl.Result{}, nil
}
func (e *EndpointReconciler) deleteExternalResources(ctx context.Context, namespacedName types.NamespacedName) error {
	svcName := namespacedName.Name + "." + namespacedName.Namespace
	for _, spiffeidname := range e.ctlr.svcNametoSpiffeID[svcName] {

		existing := &spiffeidv1beta1.SpiffeID{}
		if err := e.ctlr.Get(ctx, types.NamespacedName{Name: spiffeidname, Namespace: namespacedName.Namespace}, existing); err != nil {
			if !errors.IsNotFound(err) {
				e.ctlr.c.Log.WithFields(logrus.Fields{
					"name": spiffeidname,
				}).WithError(err).Error("failed to get SpiffeID")
				return err
			}

			return nil
		}
		if existing != nil {
			e.ctlr.c.Log.WithFields(logrus.Fields{
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
			if err := e.ctlr.Update(ctx, existing); err != nil {
				e.ctlr.c.Log.WithFields(logrus.Fields{
					"name": spiffeidname,
				}).WithError(err).Error("unable to delete DNS names in SPIFFE ID CRD")
				return err
			}

			delete(e.ctlr.svcNametoSpiffeID, svcName)
		}
	}
	return nil
}
