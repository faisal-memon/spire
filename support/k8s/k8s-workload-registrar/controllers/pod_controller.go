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
	spiffeidv1beta1 "github.com/spiffe/spire/api/spiffecrd/v1beta1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/pointer"
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

// PodReconcilerConfig holds the config passed in when creating the reconciler
type PodReconcilerConfig struct {
	Log                logrus.FieldLogger
	Mgr                ctrl.Manager
	Cluster            string
	TrustDomain        string
	PodLabel           string
	PodAnnotation      string
	DisabledNamespaces []string
}

// PodReconciler holds the runtime configuration and state of this controller
type PodReconciler struct {
	client.Client
	c      PodReconcilerConfig
	scheme *runtime.Scheme
	mode   PodReconcilerMode
	value  string
}

// NewPodReconciler creates a new PodReconciler object
func NewPodReconciler(config PodReconcilerConfig) (*PodReconciler, error) {
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

	r := &PodReconciler{
		Client: config.Mgr.GetClient(),
		c:      config,
		mode:   mode,
		value:  value,
		scheme: config.Mgr.GetScheme(),
	}

	err := ctrl.NewControllerManagedBy(config.Mgr).
		For(&corev1.Pod{}).
		Complete(r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Reconcile creates a new SPIFFE ID when pods are created
func (r *PodReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	ctx := context.Background()

	if containsString(r.c.DisabledNamespaces, req.NamespacedName.Namespace) {
		return ctrl.Result{}, nil
	}

	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		if !errors.IsNotFound(err) {
			r.c.Log.WithError(err).Error("Unable to fetch Pod")
			return ctrl.Result{}, err
		}

		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Pod neeeds to be assigned a node before it can get a SPIFFE ID
	if pod.Spec.NodeName == "" {
		return ctrl.Result{}, nil
	}

	parentIdUri := r.getParentId(pod.Spec.NodeName)
	spiffeIdUri := ""
	switch r.mode {
	case PodReconcilerModeServiceAccount:
		spiffeIdUri = r.makeID("ns/%s/sa/%s", req.Namespace, pod.Spec.ServiceAccountName)
	case PodReconcilerModeLabel:
		if val, ok := pod.GetLabels()[r.value]; ok {
			spiffeIdUri = r.makeID("%s", val)
		} else {
			// No relevant label
			return ctrl.Result{}, nil
		}
	case PodReconcilerModeAnnotation:
		if val, ok := pod.GetAnnotations()[r.value]; ok {
			spiffeIdUri = r.makeID("%s", val)
		} else {
			// No relevant annotation
			return ctrl.Result{}, nil
		}
	}

	spiffeId := &spiffeidv1beta1.SpiffeID{
		ObjectMeta: v1.ObjectMeta{
			Namespace: pod.Namespace,
		},
		Spec: spiffeidv1beta1.SpiffeIDSpec{
			ParentId: parentIdUri,
			SpiffeId: spiffeIdUri,
			DnsNames: []string{pod.Name}, // Set pod name as first DNS name
			Selector: spiffeidv1beta1.Selector{
				PodUid:    pod.GetUID(),
				Namespace: pod.Namespace,
				NodeName:  pod.Spec.NodeName,
			},
		},
	}

	// Set pod as owner of new SPIFFE ID
	err := controllerutil.SetControllerReference(&pod, spiffeId, r.scheme)
	if err != nil {
		r.c.Log.WithFields(logrus.Fields{
			"Name":      spiffeId.Name,
			"Namespace": spiffeId.Namespace,
		}).WithError(err).Error("Failed to set pod as owner of new SpiffeID CRD")
		return ctrl.Result{}, err
	}

	// Make owner reference non-blocking, so pod can be deleted if registrar is down
	ownerRef := v1.GetControllerOfNoCopy(spiffeId)
	if ownerRef == nil {
		r.c.Log.Error("Error updating owner reference")
		return ctrl.Result{}, err
	}
	ownerRef.BlockOwnerDeletion = pointer.BoolPtr(false)

	err = r.createSpiffeId(ctx, pod.ObjectMeta.Name, spiffeId)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Add label to pod with name of SPIFFE ID
	if pod.ObjectMeta.Labels["spiffe.io/spiffeid"] != spiffeId.ObjectMeta.Name {
		retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Retrieve the latest version of Pod before attempting update
			// RetryOnConflict uses exponential backoff to avoid exhausting the apiserver
			if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
				r.c.Log.WithError(err).Error("Failed to get latest version of Pod")
				return err
			}
			pod.ObjectMeta.Labels["spiffe.io/spiffeid"] = spiffeId.ObjectMeta.Name

			return r.Update(ctx, &pod)
		})
		if retryErr != nil {
			r.c.Log.WithError(retryErr).Error("Update failed")
			return ctrl.Result{}, retryErr
		}
		r.c.Log.Info("Added label to pod")
	}

	return ctrl.Result{}, nil
}

// createSpiffeId attempts to create the SPIFFE ID object. It uses the pod name appeneded with hash of the SPIFFE ID Spec
// to create a unique name.
func (r *PodReconciler) createSpiffeId(ctx context.Context, podName string, spiffeId *spiffeidv1beta1.SpiffeID) error {
	var collisionCount int32
	var existing spiffeidv1beta1.SpiffeID
	for {
		spiffeId.ObjectMeta.Name = podName + "-" + computeHash(&spiffeId.Spec, &collisionCount)
		err := r.Create(ctx, spiffeId)
		if errors.IsAlreadyExists(err) {
			spiffeIdNamespacedName := types.NamespacedName{
				Name:      spiffeId.ObjectMeta.Name,
				Namespace: spiffeId.ObjectMeta.Namespace,
			}
			r.Get(ctx, spiffeIdNamespacedName, &existing)
			if spiffeId.Spec.Selector.PodUid != existing.Spec.Selector.PodUid {
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
		Host:   r.c.TrustDomain,
		Path:   path.Clean(fmt.Sprintf(pathFmt, pathArgs...)),
	}
	return id.String()
}
func (r *PodReconciler) getParentId(nodeName string) string {
	return r.makeID("k8s-workload-registrar/%s/node/%s", r.c.Cluster, nodeName)
}
