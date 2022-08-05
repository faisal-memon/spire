package core

import (
	"context"
	"errors"

	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/record"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/protobuf/proto"
)

func (ds *DataStore) CountAttestedNodes(context.Context) (int32, error) {
	return int32(ds.agents.Count()), nil
}

func (ds *DataStore) CreateAttestedNode(ctx context.Context, in *common.AttestedNode) (*common.AttestedNode, error) {
	if err := ds.agents.Create(ctx, agentObject{AttestedNode: in}); err != nil {
		return nil, dsErr(err, "failed to create agent")
	}
	return in, nil
}

func (ds *DataStore) DeleteAttestedNode(ctx context.Context, agentID string) error {
	if err := ds.agents.Delete(ctx, agentID); err != nil {
		return dsErr(err, "failed to delete agent")
	}
	return nil
}

func (ds *DataStore) FetchAttestedNode(ctx context.Context, agentID string) (*common.AttestedNode, error) {
	r, err := ds.agents.Get(agentID)
	switch {
	case err == nil:
		return r.Object.AttestedNode, nil
	case errors.Is(err, record.ErrNotFound):
		return nil, nil
	default:
		return nil, dsErr(err, "failed to agent bundle")
	}
}

func (ds *DataStore) ListAttestedNodes(ctx context.Context, req *datastore.ListAttestedNodesRequest) (*datastore.ListAttestedNodesResponse, error) {
	records, cursor, err := ds.agents.List(req)
	if err != nil {
		return nil, err
	}
	resp := &datastore.ListAttestedNodesResponse{
		Pagination: newPagination(req.Pagination, cursor),
	}
	resp.Nodes = make([]*common.AttestedNode, 0, len(records))
	for _, record := range records {
		resp.Nodes = append(resp.Nodes, record.Object.AttestedNode)
	}
	return resp, nil
}

func (ds *DataStore) UpdateAttestedNode(ctx context.Context, newAgent *common.AttestedNode, mask *common.AttestedNodeMask) (*common.AttestedNode, error) {
	record, err := ds.agents.Get(newAgent.SpiffeId)
	if err != nil {
		return nil, dsErr(err, "failed to update agent")
	}
	existing := record.Object

	if mask == nil {
		mask = protoutil.AllTrueCommonAgentMask
	}

	if mask.AttestationDataType {
		existing.AttestationDataType = newAgent.AttestationDataType
	}
	if mask.CertSerialNumber {
		existing.CertSerialNumber = newAgent.CertSerialNumber
	}
	if mask.CertNotAfter {
		existing.CertNotAfter = newAgent.CertNotAfter
	}
	if mask.NewCertSerialNumber {
		existing.NewCertSerialNumber = newAgent.NewCertSerialNumber
	}
	if mask.NewCertNotAfter {
		existing.NewCertNotAfter = newAgent.NewCertNotAfter
	}
	if mask.Selectors {
		existing.Selectors = newAgent.Selectors
	}

	if err := ds.agents.Update(ctx, existing, record.Metadata.Revision); err != nil {
		return nil, dsErr(err, "failed to update agent")
	}
	return existing.AttestedNode, nil
}

func (ds *DataStore) GetNodeSelectors(ctx context.Context, spiffeID string, dataConsistency datastore.DataConsistency) ([]*common.Selector, error) {
	record, err := ds.agents.Get(spiffeID)
	if err != nil {
		return nil, dsErr(err, "failed to get agent selectors")
	}
	return record.Object.Selectors, nil
}

func (ds *DataStore) ListNodeSelectors(ctx context.Context, req *datastore.ListNodeSelectorsRequest) (*datastore.ListNodeSelectorsResponse, error) {
	records, _, err := ds.agents.List(&datastore.ListAttestedNodesRequest{
		ByExpiresAfter: req.ValidAt,
	})
	if err != nil {
		return nil, err
	}
	resp := &datastore.ListNodeSelectorsResponse{
		Selectors: map[string][]*common.Selector{},
	}
	for _, record := range records {
		resp.Selectors[record.Object.SpiffeId] = record.Object.Selectors
	}
	return resp, nil
}

func (ds *DataStore) SetNodeSelectors(ctx context.Context, spiffeID string, selectors []*common.Selector) error {
	agent, err := ds.FetchAttestedNode(ctx, spiffeID)
	switch {
	case err != nil:
		return err
	case agent == nil:
		_, err = ds.CreateAttestedNode(ctx, &common.AttestedNode{SpiffeId: spiffeID, Selectors: selectors})
		return err
	default:
		_, err = ds.UpdateAttestedNode(ctx, &common.AttestedNode{SpiffeId: spiffeID, Selectors: selectors}, &common.AttestedNodeMask{Selectors: true})
		return err
	}
}

type agentCodec struct{}

func (agentCodec) Marshal(in *agentObject) (string, []byte, error) {
	out, err := proto.Marshal(in.AttestedNode)
	if err != nil {
		return "", nil, err
	}
	return in.AttestedNode.SpiffeId, out, nil
}

func (agentCodec) Unmarshal(in []byte, out *agentObject) error {
	attestedNode := new(common.AttestedNode)
	if err := proto.Unmarshal(in, attestedNode); err != nil {
		return err
	}
	out.AttestedNode = attestedNode
	return nil
}

type agentObject struct {
	*common.AttestedNode
}

func (r agentObject) Key() string { return r.AttestedNode.SpiffeId }

type agentIndex struct {
	all             record.Set[agentObject]
	attestationType record.UnaryIndex[agentObject, string]
	banned          record.UnaryIndexCmp[agentObject, bool, boolCmp]
	expiresAt       record.UnaryIndex[agentObject, int64]
	selectors       record.MultiIndexCmp[agentObject, *common.Selector, selectorCmp]
}

func (idx *agentIndex) Count() int {
	return idx.all.Count()
}

func (idx *agentIndex) Get(key string) (*record.Record[agentObject], bool) {
	return idx.all.Get(key)
}

func (idx *agentIndex) Put(r *record.Record[agentObject]) error {
	idx.all.Set(r)
	idx.attestationType.Set(r, r.Object.AttestationDataType)
	idx.banned.Set(r, r.Object.CertSerialNumber == "" && r.Object.NewCertSerialNumber == "")
	idx.expiresAt.Set(r, r.Object.CertNotAfter)
	idx.selectors.Set(r, r.Object.Selectors)
	return nil
}

func (idx *agentIndex) Delete(key string) {
	idx.all.Delete(key)
	idx.attestationType.Delete(key)
	idx.banned.Delete(key)
	idx.expiresAt.Delete(key)
	idx.selectors.Delete(key)
}

func (idx *agentIndex) List(req *datastore.ListAttestedNodesRequest) (record.Iterator[agentObject], error) {
	cursor, limit, err := getPaginationParams(req.Pagination)
	if err != nil {
		return nil, err
	}

	var filters []record.Iterator[agentObject]
	if req.ByAttestationType != "" {
		filters = append(filters, idx.attestationType.EqualTo(cursor, req.ByAttestationType))
	}
	if req.ByBanned != nil {
		filters = append(filters, idx.banned.EqualTo(cursor, *req.ByBanned))
	}
	if !req.ByExpiresBefore.IsZero() {
		filters = append(filters, idx.expiresAt.LessThan(cursor, req.ByExpiresBefore.Unix()))
	}
	if !req.ByExpiresBefore.IsZero() {
		filters = append(filters, idx.expiresAt.GreaterThan(cursor, req.ByExpiresAfter.Unix()))
	}
	if req.BySelectorMatch != nil {
		filters = append(filters, idx.selectors.Matching(cursor, req.BySelectorMatch.Selectors, matchBehavior(req.BySelectorMatch.Match)))
	}

	var iter record.Iterator[agentObject]
	if len(filters) > 0 {
		iter = record.And(filters)
	} else {
		iter = idx.all.Iterate(cursor)
	}

	if limit > 0 {
		iter = record.Limit(iter, limit)
	}
	return iter, nil
}
