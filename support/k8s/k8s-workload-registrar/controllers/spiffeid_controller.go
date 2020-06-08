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
	"github.com/zeebo/errs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	spiffeidv1beta1 "github.com/spiffe/spire/api/spiffecrd/v1beta1"
)

// SpiffeIDReconcilerConfig holds the config passed in when creating the reconciler
type SpiffeIDReconcilerConfig struct {
	Log          logrus.FieldLogger
	R            registration.RegistrationClient
	TrustDomain  string
	Cluster      string
	Mgr          ctrl.Manager
}

// SpiffeIDReconciler holds the runtime configuration and state of this controller
type SpiffeIDReconciler struct {
	client.Client
	c                  SpiffeIDReconcilerConfig
	myId               *string
	spiffeIDCollection map[string]string
}

// NewSpiffeIDReconciler creates a new SpiffeIDReconciler object
func NewSpiffeIDReconciler(config SpiffeIDReconcilerConfig) (*SpiffeIDReconciler, error) {
	r := &SpiffeIDReconciler {
		Client: config.Mgr.GetClient(),
		c: config,
		spiffeIDCollection:  make(map[string]string),
	}

	err := ctrl.NewControllerManagedBy(r.c.Mgr).
		For(&spiffeidv1beta1.SpiffeID{}).
		Complete(r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Initialize ensures there is a node registration entry for PSAT nodes in the cluster.
func (r *SpiffeIDReconciler) Initialize(ctx context.Context) error {
	nodeID := r.nodeID()
	r.myId = &nodeID
	return r.createEntry(ctx, &common.RegistrationEntry{
		ParentId: idutil.ServerID(r.c.TrustDomain),
		SpiffeId: nodeID,
		Selectors: []*common.Selector{
			{Type: "k8s_psat", Value: fmt.Sprintf("cluster:%s", r.c.Cluster)},
		},
	})
}

// Reconcile ensures the SPIRE Server entry matches the corresponding CRD
func (r *SpiffeIDReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	spiffeID := spiffeidv1beta1.SpiffeID{}

	if err := r.Get(ctx, req.NamespacedName, &spiffeID); err != nil {
		if !k8serrors.IsNotFound(err) {
			r.c.Log.WithError(err).Error("Unable to fetch SpiffeID CRD")
			return ctrl.Result{}, err
		}

		// Delete event
		if err := r.ensureDeleted(ctx, r.spiffeIDCollection[req.NamespacedName.String()]); err != nil {
			r.c.Log.WithFields(logrus.Fields{
				"entryid": r.spiffeIDCollection[req.NamespacedName.String()],
			}).WithError(err).Error("Unable to delete registration entry")
			return ctrl.Result{}, err
		}
		delete(r.spiffeIDCollection, req.NamespacedName.String())
		r.c.Log.WithFields(logrus.Fields{
			"entry": req.NamespacedName.String(),
		}).Info("Finalized entry")

		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	entryId, err := r.updateOrCreateSpiffeID(ctx, &spiffeID)
	if err != nil {
		r.c.Log.WithFields(logrus.Fields{
			"request": req,
		}).WithError(err).Error(err, "Unable to get or create registration entry")
		return ctrl.Result{}, err
	}

	oldEntryId := spiffeID.Status.EntryId
	if oldEntryId == nil || *oldEntryId != entryId {
		// We need to update the Status field
		if oldEntryId != nil {
			// entry resource must have been modified, delete the hanging one
			if err := r.ensureDeleted(ctx, *spiffeID.Status.EntryId); err != nil {
				r.c.Log.WithFields(logrus.Fields{
					"entryid": *spiffeID.Status.EntryId,
				}).WithError(err).Error("Unable to delete old registration entry")
				return ctrl.Result{}, err
			}
		}
		spiffeID.Status.EntryId = &entryId
		if err := r.Status().Update(ctx, &spiffeID); err != nil {
			r.c.Log.WithError(err).Error("unable to update SpiffeID status")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// ensureDeleted deletes the specified entry on the SPIRE Server
func (r *SpiffeIDReconciler) ensureDeleted(ctx context.Context, entryId string) error {
	if _, err := r.c.R.DeleteEntry(ctx, &registration.RegistrationEntryID{Id: entryId}); err != nil {
		if status.Code(err) != codes.NotFound {
			if status.Code(err) == codes.Internal {
				// Spire server currently returns a 500 if delete fails due to the entry not existing.
				// We work around it by attempting to fetch the entry, and if it's not found then all is good.
				if _, err := r.c.R.FetchEntry(ctx, &registration.RegistrationEntryID{Id: entryId}); err != nil {
					if status.Code(err) == codes.NotFound {
						r.c.Log.WithFields(logrus.Fields{
							"entry": entryId,
						}).Info("Entry already deleted")
						return nil
					}
				}
			}
			return err
		}
	}
	r.c.Log.WithFields(logrus.Fields{
		"entry": entryId,
	}).Info("Deleted entry")
	return nil
}

// updateOrCreateSpiffeID attempts to create a new entry. if the entry already exists, it updates it.
func (r *SpiffeIDReconciler) updateOrCreateSpiffeID(ctx context.Context, instance *spiffeidv1beta1.SpiffeID) (string, error) {
	spiffeId := instance.Spec.SpiffeId
	selectors := toCommonSelector(instance.Spec.Selector)
	entry :=  &common.RegistrationEntry{
		Selectors: selectors,
		ParentId:  *r.myId,
		SpiffeId:  spiffeId,
		DnsNames:  instance.Spec.DnsNames,
	}

	response, err := r.c.R.CreateEntryIfNotExists(ctx, entry)
	if err != nil {
		r.c.Log.WithError(err).Error("Failed to create registration entry")
		return "", err
	}

	entryId := response.Entry.EntryId
	if response.Preexisting {
		existing := response.Entry
		if !equalStringSlice(existing.DnsNames, instance.Spec.DnsNames) {
			r.c.Log.Info("Updating Spire Entry")

			entry.EntryId = entryId
			_, err := r.c.R.UpdateEntry(ctx, &registration.UpdateEntryRequest{
				Entry: entry,
			})
			if err != nil {
				r.c.Log.WithError(err).Error("Unable to update SpiffeID CRD with new DNS names")
				return "", err
			}
		}
	} else {
		r.c.Log.WithFields(logrus.Fields{
			"entryID": entryId,
			"spiffeID": spiffeId,
		}).Info("Created entry")
		r.spiffeIDCollection[instance.ObjectMeta.Namespace+"/"+instance.ObjectMeta.Name] = entryId
	}

	return entryId, nil
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
	for _, v := range selector.Arbitrary {
		commonSelector = append(commonSelector, &common.Selector{
			Type:  "k8s",
			Value: v,
		})
	}

	return commonSelector
}

func (r *SpiffeIDReconciler) makeID(pathFmt string, pathArgs ...interface{}) string {
	id := url.URL{
		Scheme: "spiffe",
		Host:   r.c.TrustDomain,
		Path:   path.Clean(fmt.Sprintf(pathFmt, pathArgs...)),
	}
	return id.String()
}

func (r *SpiffeIDReconciler) nodeID() string {
	return r.makeID("k8s-workload-registrar/%s/node", r.c.Cluster)
}

func (r *SpiffeIDReconciler) createEntry(ctx context.Context, entry *common.RegistrationEntry) error {
	// ensure there is a node registration entry for PSAT nodes in the cluster.
	log := r.c.Log.WithFields(logrus.Fields{
		"parent_id": entry.ParentId,
		"spiffe_id": entry.SpiffeId,
		"selectors": selectorsField(entry.Selectors),
	})
	_, err := r.c.R.CreateEntry(ctx, entry)
	switch status.Code(err) {
	case codes.OK, codes.AlreadyExists:
		log.Info("Created pod entry")
		return nil
	default:
		log.WithError(err).Error("Failed to create pod entry")
		return errs.Wrap(err)
	}
}
