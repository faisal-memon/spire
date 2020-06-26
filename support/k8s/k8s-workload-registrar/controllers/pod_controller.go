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
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// PodReconcilerConfig holds the config passed in when creating the reconciler
type PodReconcilerConfig struct {
	Cluster            string
	Ctx                context.Context
	DisabledNamespaces []string
	Log                logrus.FieldLogger
	Mgr                ctrl.Manager
	PodLabel           string
	PodAnnotation      string
	TrustDomain        string
}

// PodReconciler holds the runtime configuration and state of this controller
type PodReconciler struct {
	client.Client
	c      PodReconcilerConfig
	scheme *runtime.Scheme
}

// NewPodReconciler creates a new PodReconciler object
func NewPodReconciler(config PodReconcilerConfig) (*PodReconciler, error) {
	r := &PodReconciler{
		Client: config.Mgr.GetClient(),
		c:      config,
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
	if containsString(r.c.DisabledNamespaces, req.NamespacedName.Namespace) {
		return ctrl.Result{}, nil
	}

	pod := corev1.Pod{}
	ctx := r.c.Ctx

	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		if !errors.IsNotFound(err) {
			r.c.Log.WithError(err).Error("Unable to get Pod")
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Pod neeeds to be assigned a node before it can get a SPIFFE ID
	if pod.Spec.NodeName == "" {
		return ctrl.Result{}, nil
	}

	_, err := r.createPodEntry(ctx, &pod)
	if err != nil && !errors.IsAlreadyExists(err) {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// getOrCreatePodEntry attempts to create a new SpiffeID resource. if the entry already exists, it returns it.
func (r *PodReconciler) createPodEntry(ctx context.Context, pod *corev1.Pod) (*spiffeidv1beta1.SpiffeID, error) {
	spiffeIDURI := r.podSpiffeID(pod)
	// If we have no spiffe ID for the pod, do nothing
	if spiffeIDURI == "" {
		return nil, nil
	}

	spiffeID := &spiffeidv1beta1.SpiffeID{
		ObjectMeta: v1.ObjectMeta{
			Namespace: pod.Namespace,
			Labels: map[string]string{
				"podUid": string(pod.ObjectMeta.UID),
			},
		},
		Spec: spiffeidv1beta1.SpiffeIDSpec{
			SpiffeId: spiffeIDURI,
			ParentId: r.podParentID(pod.Spec.NodeName),
			DnsNames: []string{pod.Name}, // Set pod name as first DNS name
			Selector: spiffeidv1beta1.Selector{
				PodUid:    pod.GetUID(),
				Namespace: pod.Namespace,
				NodeName:  pod.Spec.NodeName,
			},
		},
	}
	err := r.setOwnerRef(pod, spiffeID)
	if err != nil {
		return nil, err
	}

	err = r.createSpiffeId(ctx, pod.ObjectMeta.Name, spiffeID)
	if err != nil {
		return nil, err
	}

	return spiffeID, nil
}

// podSpiffeID returns the desired spiffe ID for the pod, or nil if it should be ignored
func (r *PodReconciler) podSpiffeID(pod *corev1.Pod) string {
	if r.c.PodLabel != "" {
		// the controller has been configured with a pod label. if the pod
		// has that label, use the value to construct the pod entry. otherwise
		// ignore the pod altogether.
		if labelValue, ok := pod.Labels[r.c.PodLabel]; ok {
			return r.makeID("%s", labelValue)
		}
		return ""
	}

	if r.c.PodAnnotation != "" {
		// the controller has been configured with a pod annotation. if the pod
		// has that annotation, use the value to construct the pod entry. otherwise
		// ignore the pod altogether.
		if annotationValue, ok := pod.Annotations[r.c.PodAnnotation]; ok {
			return r.makeID("%s", annotationValue)
		}
		return ""
	}

	// the controller has not been configured with a pod label or a pod annotation.
	// create an entry based on the service account.
	return r.makeID("ns/%s/sa/%s", pod.Namespace, pod.Spec.ServiceAccountName)
}

// setOwnerRef sets the pod as owner of new SPIFFE ID
func (r *PodReconciler) setOwnerRef(pod *corev1.Pod, spiffeID *spiffeidv1beta1.SpiffeID) error {
	err := controllerutil.SetControllerReference(pod, spiffeID, r.scheme)
	if err != nil {
		return err
	}

	// Make owner reference non-blocking, so pod can be deleted if registrar is down
	ownerRef := v1.GetControllerOfNoCopy(spiffeID)
	if ownerRef == nil {
		return err
	}
	ownerRef.BlockOwnerDeletion = pointer.BoolPtr(false)

	return nil
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

func (r *PodReconciler) podParentID(nodeName string) string {
	return r.makeID("k8s-workload-registrar/%s/node/%s", r.c.Cluster, nodeName)
}

func (r *PodReconciler) makeID(pathFmt string, pathArgs ...interface{}) string {
	id := url.URL{
		Scheme: "spiffe",
		Host:   r.c.TrustDomain,
		Path:   path.Clean(fmt.Sprintf(pathFmt, pathArgs...)),
	}
	return id.String()
}
