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
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"

	"github.com/davecgh/go-spew/spew"
	spiffeidv1beta1 "github.com/spiffe/spire/support/k8s/k8s-workload-registrar/mode-crd/api/spiffeid/v1beta1"
	"github.com/spiffe/spire/proto/spire/api/registration"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
)

func NewManager(leaderElection bool, metricsBindAddr string) (ctrl.Manager, error) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = spiffeidv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		LeaderElection:     leaderElection,
		MetricsBindAddress: metricsBindAddr,
		Scheme:             scheme,
	})
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func deepHashObject(hasher hash.Hash, objectToWrite interface{}) {
	hasher.Reset()
	printer := spew.ConfigState{
		Indent:         " ",
		SortKeys:       true,
		DisableMethods: true,
		SpewKeys:       true,
	}
	printer.Fprintf(hasher, "%#v", objectToWrite)
}

// ComputeHash returns a hash value calculated from a SPIFFE ID Spec and
// a collisionCount to avoid hash collision. The hash will be safe encoded to
// avoid bad words.
func computeHash(template *spiffeidv1beta1.SpiffeIDSpec, collisionCount *int32) string {
	podTemplateSpecHasher := fnv.New32a()
	deepHashObject(podTemplateSpecHasher, *template)

	// Add collisionCount in the hash if it exists.
	if collisionCount != nil {
		collisionCountBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(collisionCountBytes, uint32(*collisionCount))
		podTemplateSpecHasher.Write(collisionCountBytes)
	}

	return rand.SafeEncodeString(fmt.Sprint(podTemplateSpecHasher.Sum32()))
}

// deleteRegistrationEntry deletes an entry on the SPIRE Server
func deleteRegistrationEntry(ctx context.Context, R registration.RegistrationClient, entryID string) error {
	_, err := R.DeleteEntry(ctx, &registration.RegistrationEntryID{Id: entryID})
	switch status.Code(err) {
	case codes.OK, codes.NotFound:
		return nil
	case codes.Internal:
		// Spire server currently returns a 500 if delete fails due to the entry not existing.
		// We work around it by attempting to fetch the entry, and if it's not found then all is good.
		_, err := R.FetchEntry(ctx, &registration.RegistrationEntryID{Id: entryID})
		if status.Code(err) == codes.NotFound {
			return nil
		}

		return err
	}

	return nil
}

// Helper functions for string operations.
func equalStringSlice(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}

	for i, v := range x {
		if v != y[i] {
			return false
		}
	}

	return true
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeStringIf(slice []string, s string) []string {
	i := 0 // output index
	for _, item := range slice {
		if item != s {
			// copy and increment index
			slice[i] = item
			i++
		}
	}

	return slice[:i]
}
