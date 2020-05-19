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
	"errors"
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

type SpiffeIDReconcilerConfig struct {
	Log          logrus.FieldLogger
	R            registration.RegistrationClient
	TrustDomain  string
	Cluster      string
	Mgr          ctrl.Manager
}

// SpiffeIDReconciler reconciles a SpiffeID object
type SpiffeIDReconciler struct {
	client.Client
	c                  SpiffeIDReconcilerConfig
	myId               *string
	spiffeIDCollection map[string]string
}

// NewSpiffeIDReconciler create a new SpiffeIDReconciler object
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

func (r *SpiffeIDReconciler) Initialize(ctx context.Context) error {
	// ensure there is a node registration entry for PSAT nodes in the cluster.
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
	r.c.Log.WithFields(logrus.Fields{
		"namespace": req.NamespacedName.Namespace,
		"name":      req.NamespacedName.Name,
	}).Debug("Spiffe ID Reconcile called")

	var spiffeID spiffeidv1beta1.SpiffeID
	if err := r.Get(ctx, req.NamespacedName, &spiffeID); err != nil {
		if !k8serrors.IsNotFound(err) {
			r.c.Log.WithError(err).Error("unable to fetch SpiffeID")
			return ctrl.Result{}, err
		}

		// Delete event
		if err := r.ensureDeleted(ctx, r.spiffeIDCollection[req.NamespacedName.String()]); err != nil {
			r.c.Log.WithFields(logrus.Fields{
				"entryid": r.spiffeIDCollection[req.NamespacedName.String()],
			}).WithError(err).Error("unable to delete spire entry", )
			return ctrl.Result{}, err
		}
		delete(r.spiffeIDCollection, req.NamespacedName.String())
		r.c.Log.WithFields(logrus.Fields{
			"entry": req.NamespacedName.String(),
		}).Info("Finalized entry")

		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	entryId, err := r.getOrCreateSpiffeID(ctx, &spiffeID)
	if err != nil {
		r.c.Log.WithFields(logrus.Fields{
			"request": req,
		}).WithError(err).Error(err, "unable to get or create spire entry")
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
				}).WithError(err).Error("unable to delete old spire entry")
				return ctrl.Result{}, err
			}
		}
		spiffeID.Status.EntryId = &entryId
		if err := r.Status().Update(ctx, &spiffeID); err != nil {
			r.c.Log.WithError(err).Error("unable to update SpiffeID status")
			return ctrl.Result{}, err
		}
	}

	currentEntry, err := r.c.R.FetchEntry(ctx, &registration.RegistrationEntryID{Id: entryId})
	if err != nil {
		r.c.Log.WithError(err).Error("unable to fetch current SpiffeID entry")
		return ctrl.Result{}, err
	}

	if !equalStringSlice(currentEntry.DnsNames, spiffeID.Spec.DnsNames) {
		r.c.Log.Info("Updating Spire Entry")

		_, err := r.c.R.UpdateEntry(ctx, &registration.UpdateEntryRequest{
			Entry: r.createCommonRegistrationEntry(spiffeID),
		})
		if err != nil {
			r.c.Log.WithError(err).Error("unable to update SpiffeID with new DNS names")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}


func (r *SpiffeIDReconciler) ensureDeleted(ctx context.Context, entryId string) error {
	if _, err := r.c.R.DeleteEntry(ctx, &registration.RegistrationEntryID{Id: entryId}); err != nil {
		if status.Code(err) != codes.NotFound {
			if status.Code(err) == codes.Internal {
				// Spire server currently returns a 500 if delete fails due to the entry not existing. This is probably a bug.
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

// ServerURI creates a server SPIFFE URI given a trustDomain.
func ServerURI(trustDomain string) *url.URL {
	return &url.URL{
		Scheme: "spiffe",
		Host:   trustDomain,
		Path:   path.Join("spire", "server"),
	}
}

// ServerID creates a server SPIFFE ID string given a trustDomain.
func ServerID(trustDomain string) string {
	return ServerURI(trustDomain).String()
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

func (r *SpiffeIDReconciler) getExistingEntry(ctx context.Context, id string, selectors []*common.Selector) (string, error) {
	entries, err := r.c.R.ListByParentID(ctx, &registration.ParentID{
		Id: *r.myId,
	})
	if err != nil {
		r.c.Log.WithError(err).Error("Failed to retrieve existing spire entry")
		return "", err
	}

	selectorMap := map[string]bool{}
	for _, sel := range selectors {
		key := sel.Type + ":" + sel.Value
		selectorMap[key] = true
	}
	for _, entry := range entries.Entries {
		if entry.GetSpiffeId() == id {
			if len(entry.GetSelectors()) != len(selectors) {
				continue
			}
			found := true
			for _, sel := range entry.GetSelectors() {
				key := sel.Type + ":" + sel.Value
				if _, ok := selectorMap[key]; !ok {
					found = false
					break
				}
			}
			if found {
				return entry.GetEntryId(), nil
			}
		}
	}
	return "", errors.New("no existing matching entry found")
}

func (r *SpiffeIDReconciler) createCommonRegistrationEntry(instance spiffeidv1beta1.SpiffeID) *common.RegistrationEntry {
	selectors := make([]*common.Selector, 0, len(instance.Spec.Selector.PodLabel))
	for k, v := range instance.Spec.Selector.PodLabel {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-label:%s:%s", k, v),
		})
	}
	if len(instance.Spec.Selector.PodName) > 0 {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-name:%s", instance.Spec.Selector.PodName),
		})
	}
	if len(instance.Spec.Selector.PodUid) > 0 {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-uid:%s", instance.Spec.Selector.PodUid),
		})
	}
	if len(instance.Spec.Selector.Namespace) > 0 {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("ns:%s", instance.Spec.Selector.Namespace),
		})
	}
	if len(instance.Spec.Selector.ServiceAccount) > 0 {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("sa:%s", instance.Spec.Selector.ServiceAccount),
		})
	}
	for _, v := range instance.Spec.Selector.Arbitrary {
		selectors = append(selectors, &common.Selector{Value: v})
	}

	return &common.RegistrationEntry{
		Selectors: selectors,
		ParentId:  *r.myId,
		SpiffeId:  instance.Spec.SpiffeId,
		DnsNames:  instance.Spec.DnsNames,
		EntryId:   *instance.Status.EntryId,
	}
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


func (r *SpiffeIDReconciler) getOrCreateSpiffeID(ctx context.Context, instance *spiffeidv1beta1.SpiffeID) (string, error) {

	// TODO: sanitize!
	selectors := make([]*common.Selector, 0, len(instance.Spec.Selector.PodLabel))
	for k, v := range instance.Spec.Selector.PodLabel {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-label:%s:%s", k, v),
		})
	}
	if len(instance.Spec.Selector.PodName) > 0 {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-name:%s", instance.Spec.Selector.PodName),
		})
	}
	if len(instance.Spec.Selector.PodUid) > 0 {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("pod-uid:%s", instance.Spec.Selector.PodUid),
		})
	}
	if len(instance.Spec.Selector.Namespace) > 0 {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("ns:%s", instance.Spec.Selector.Namespace),
		})
	}
	if len(instance.Spec.Selector.ServiceAccount) > 0 {
		selectors = append(selectors, &common.Selector{
			Type:  "k8s",
			Value: fmt.Sprintf("sa:%s", instance.Spec.Selector.ServiceAccount),
		})
	}
	for _, v := range instance.Spec.Selector.Arbitrary {
		selectors = append(selectors, &common.Selector{Value: v})
	}

	spiffeId := instance.Spec.SpiffeId

	regEntryId, err := r.c.R.CreateEntry(ctx, &common.RegistrationEntry{
		Selectors: selectors,
		ParentId:  *r.myId,
		SpiffeId:  spiffeId,
		DnsNames:  instance.Spec.DnsNames,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			entryId, err := r.getExistingEntry(ctx, spiffeId, selectors)
			if err != nil {
				r.c.Log.WithError(err).Error("Failed to reuse existing spire entry")
				return "", err
			}
			r.c.Log.WithFields(logrus.Fields{
				"entryID": entryId,
				"spiffeID": spiffeId,
			}).Info("Found existing entry")
			return entryId, err
		}
		r.c.Log.WithError(err).Error("Failed to create spire entry")
		return "", err
	}
	r.spiffeIDCollection[instance.ObjectMeta.Namespace+"/"+instance.ObjectMeta.Name] = regEntryId.Id
	r.c.Log.WithFields(logrus.Fields{
		"entryID": regEntryId.Id,
		"spiffeID": spiffeId,
	}).Info("Created entry")

	return regEntryId.Id, nil

}
