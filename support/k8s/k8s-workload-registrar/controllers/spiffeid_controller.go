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

	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/proto/spire/api/registration"
	"github.com/spiffe/spire/proto/spire/common"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	spiffeidv1beta1 "github.com/spiffe/spire/api/spiffecrd/v1beta1"
)

// SpiffeIDReconcilerConfig holds the config passed in when creating the reconciler
type SpiffeIDReconcilerConfig struct {
	Log         logrus.FieldLogger
	R           registration.RegistrationClient
	TrustDomain string
	Cluster     string
	Mgr         ctrl.Manager
}

// SpiffeIDReconciler holds the runtime configuration and state of this controller
type SpiffeIDReconciler struct {
	client.Client
	c    SpiffeIDReconcilerConfig
	myId string
}

// NewSpiffeIDReconciler creates a new SpiffeIDReconciler object
func NewSpiffeIDReconciler(config SpiffeIDReconcilerConfig) (*SpiffeIDReconciler, error) {
	r := &SpiffeIDReconciler{
		Client: config.Mgr.GetClient(),
		c:      config,
	}

	err := ctrl.NewControllerManagedBy(r.c.Mgr).
		For(&spiffeidv1beta1.SpiffeID{}).
		Complete(r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Reconcile ensures the SPIRE Server entry matches the corresponding CRD
func (r *SpiffeIDReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	spiffeID := spiffeidv1beta1.SpiffeID{}
	ctx := context.Background()

	if err := r.Get(ctx, req.NamespacedName, &spiffeID); err != nil {
		if !k8serrors.IsNotFound(err) {
			r.c.Log.WithError(err).Error("Unable to fetch SpiffeID CRD")
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	myFinalizerName := "finalizers.spiffeid.spiffe.io"
	if spiffeID.ObjectMeta.DeletionTimestamp.IsZero() {
		// Add our finalizer if it doesn't already exist
		if !containsString(spiffeID.GetFinalizers(), myFinalizerName) {
			spiffeID.SetFinalizers(append(spiffeID.GetFinalizers(), myFinalizerName))
			if err := r.Update(ctx, &spiffeID); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		// Delete event
		if containsString(spiffeID.GetFinalizers(), myFinalizerName) {
			if err := r.deleteSpiffeID(ctx, &spiffeID); err != nil {
				r.c.Log.WithFields(logrus.Fields{
					"name":      spiffeID.Name,
					"namespace": spiffeID.Namespace,
					"entryID":   *spiffeID.Status.EntryId,
				}).WithError(err).Error("Unable to delete registration entry")
				return ctrl.Result{}, err
			}

			// Remove our finalizer from the list and update it.
			spiffeID.SetFinalizers(removeStringIf(spiffeID.GetFinalizers(), myFinalizerName))
			if err := r.Update(ctx, &spiffeID); err != nil {
				return ctrl.Result{}, err
			}
			r.c.Log.WithFields(logrus.Fields{
				"name":      spiffeID.Name,
				"namespace": spiffeID.Namespace,
			}).Info("Finalized SPIFFE ID Resource")
		}
		return ctrl.Result{}, nil
	}

	entryID, preexisting, err := r.updateOrCreateSpiffeID(ctx, &spiffeID)
	if err != nil {
		r.c.Log.WithFields(logrus.Fields{
			"name":      spiffeID.Name,
			"namespace": spiffeID.Namespace,
		}).WithError(err).Error(err, "Unable to update or create registration entry")
		return ctrl.Result{}, err
	}

	if !preexisting || spiffeID.Status.EntryId == nil {
		spiffeID.Status.EntryId = &entryID
		if err := r.Status().Update(ctx, &spiffeID); err != nil {
			r.c.Log.WithFields(logrus.Fields{
				"name":      spiffeID.Name,
				"namespace": spiffeID.Namespace,
			}).WithError(err).Error("Unable to update SPIFFE ID status")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// updateOrCreateSpiffeID attempts to create a new entry. if the entry already exists, it updates it.
func (r *SpiffeIDReconciler) updateOrCreateSpiffeID(ctx context.Context, spiffeID *spiffeidv1beta1.SpiffeID) (string, bool, error) {
	entry := &common.RegistrationEntry{
		ParentId:  spiffeID.Spec.ParentId,
		SpiffeId:  spiffeID.Spec.SpiffeId,
		Selectors: toCommonSelector(spiffeID.Spec.Selector),
		DnsNames:  spiffeID.Spec.DnsNames,
	}

	response, err := r.c.R.CreateEntryIfNotExists(ctx, entry)
	if err != nil {
		return "", false, err
	}

	entryId := response.Entry.EntryId
	if response.Preexisting {
		existing := response.Entry
		if !equalStringSlice(existing.DnsNames, spiffeID.Spec.DnsNames) {
			r.c.Log.WithFields(logrus.Fields{
				"entryID":  entryId,
				"spiffeID": spiffeID.Spec.SpiffeId,
			}).Info("Updated entry DNS names")

			entry.EntryId = entryId
			_, err := r.c.R.UpdateEntry(ctx, &registration.UpdateEntryRequest{
				Entry: entry,
			})
			if err != nil {
				return "", false, err
			}
		}
	} else {
		r.c.Log.WithFields(logrus.Fields{
			"entryID":  entryId,
			"spiffeID": spiffeID.Spec.SpiffeId,
		}).Info("Created entry")
	}

	return entryId, response.Preexisting, nil
}

// deleteSpiffeID deletes the specified entry on the SPIRE Server
func (r *SpiffeIDReconciler) deleteSpiffeID(ctx context.Context, spiffeID *spiffeidv1beta1.SpiffeID) error {
	err := deleteRegistrationEntry(ctx, r.c.R, *spiffeID.Status.EntryId)
	if err != nil {
		return err
	}

	r.c.Log.WithFields(logrus.Fields{
		"entryID":  spiffeID.Status.EntryId,
		"spiffeID": spiffeID.Spec.SpiffeId,
	}).Info("Deleted entry")

	return nil
}

// toCommonSelector converts the selectors from the CRD to the common.Selector format
// needed to create the entry on the SPIRE server
func toCommonSelector(selector spiffeidv1beta1.Selector) []*common.Selector {
	commonSelector := make([]*common.Selector, 0, len(selector.PodLabel))
	for k, v := range selector.PodLabel {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-label:%s:%s", k, v),
		})
	}
	if len(selector.PodName) > 0 {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-name:%s", selector.PodName),
		})
	}
	if len(selector.PodUid) > 0 {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-uid:%s", selector.PodUid),
		})
	}
	if len(selector.Namespace) > 0 {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("ns:%s", selector.Namespace),
		})
	}
	if len(selector.ServiceAccount) > 0 {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("sa:%s", selector.ServiceAccount),
		})
	}
	if len(selector.ContainerName) > 0 {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("container-name:%s", selector.ContainerName),
		})
	}
	if len(selector.ContainerImage) > 0 {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("container-image:%s", selector.ContainerImage),
		})
	}
	if len(selector.NodeName) > 0 {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("node-name:%s", selector.NodeName),
		})
	}
	for _, v := range selector.Arbitrary {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: v,
		})
	}

	return commonSelector
}
