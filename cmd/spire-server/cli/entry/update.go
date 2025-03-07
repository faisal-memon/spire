package entry

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/mitchellh/cli"
	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	serverutil "github.com/spiffe/spire/cmd/spire-server/util"
	commoncli "github.com/spiffe/spire/pkg/common/cli"
	"github.com/spiffe/spire/pkg/common/cliprinter"
	"github.com/spiffe/spire/pkg/common/util"
	"google.golang.org/grpc/codes"
)

// NewUpdateCommand creates a new "update" subcommand for "entry" command.
func NewUpdateCommand() cli.Command {
	return newUpdateCommand(commoncli.DefaultEnv)
}

func newUpdateCommand(env *commoncli.Env) cli.Command {
	return serverutil.AdaptCommand(env, &updateCommand{env: env})
}

type updateCommand struct {
	// Path to an optional data file. If set, other
	// opts will be ignored.
	path string

	// Registration entry id to update
	entryID string

	// Type and value are delimited by a colon (:)
	// ex. "unix:uid:1000" or "spiffe_id:spiffe://example.org/foo"
	selectors StringsFlag

	// Workload parent spiffeID
	parentID string

	// Workload spiffeID
	spiffeID string

	// whether the entry is for a downstream SPIRE server
	downstream bool

	// TTL for x509 SVIDs issued to this workload
	x509SvidTTL int

	// TTL for JWT SVIDs issued to this workload
	jwtSvidTTL int

	// List of SPIFFE IDs of trust domains the registration entry is federated with
	federatesWith StringsFlag

	// whether the registration entry is for an "admin" workload
	admin bool

	// Expiry of entry
	entryExpiry int64

	// DNSNames entries for SVIDs based on this entry
	dnsNames StringsFlag

	// storeSVID determines if the issued SVID must be stored through an SVIDStore plugin
	storeSVID bool

	// Entry hint, used to disambiguate entries with the same SPIFFE ID
	hint string

	printer cliprinter.Printer

	env *commoncli.Env
}

func (*updateCommand) Name() string {
	return "entry update"
}

func (*updateCommand) Synopsis() string {
	return "Updates registration entries"
}

func (c *updateCommand) AppendFlags(f *flag.FlagSet) {
	f.StringVar(&c.entryID, "entryID", "", "The Registration Entry ID of the record to update")
	f.StringVar(&c.parentID, "parentID", "", "The SPIFFE ID of this record's parent")
	f.StringVar(&c.spiffeID, "spiffeID", "", "The SPIFFE ID that this record represents")
	f.IntVar(&c.x509SvidTTL, "x509SVIDTTL", 0, "The lifetime, in seconds, for x509-SVIDs issued based on this registration entry.")
	f.IntVar(&c.jwtSvidTTL, "jwtSVIDTTL", 0, "The lifetime, in seconds, for JWT-SVIDs issued based on this registration entry.")
	f.StringVar(&c.path, "data", "", "Path to a file containing registration JSON (optional). If set to '-', read the JSON from stdin.")
	f.Var(&c.selectors, "selector", "A colon-delimited type:value selector. Can be used more than once")
	f.Var(&c.federatesWith, "federatesWith", "SPIFFE ID of a trust domain to federate with. Can be used more than once")
	f.BoolVar(&c.admin, "admin", false, "If set, the SPIFFE ID in this entry will be granted access to the SPIRE Server's management APIs")
	f.BoolVar(&c.downstream, "downstream", false, "A boolean value that, when set, indicates that the entry describes a downstream SPIRE server")
	f.BoolVar(&c.storeSVID, "storeSVID", false, "A boolean value that, when set, indicates that the resulting issued SVID from this entry must be stored through an SVIDStore plugin")
	f.Int64Var(&c.entryExpiry, "entryExpiry", 0, "An expiry, from epoch in seconds, for the resulting registration entry to be pruned")
	f.Var(&c.dnsNames, "dns", "A DNS name that will be included in SVIDs issued based on this entry, where appropriate. Can be used more than once")
	f.StringVar(&c.hint, "hint", "", "The entry hint, used to disambiguate entries with the same SPIFFE ID")
	cliprinter.AppendFlagWithCustomPretty(&c.printer, f, c.env, prettyPrintUpdate)
}

func (c *updateCommand) Run(ctx context.Context, _ *commoncli.Env, serverClient serverutil.ServerClient) error {
	if err := c.validate(); err != nil {
		return err
	}

	var entries []*types.Entry
	var err error
	if c.path != "" {
		entries, err = parseFile(c.path)
	} else {
		entries, err = c.parseConfig()
	}
	if err != nil {
		return err
	}

	resp, err := updateEntries(ctx, serverClient.NewEntryClient(), entries)
	if err != nil {
		return err
	}

	return c.printer.PrintProto(resp)
}

// validate performs basic validation, even on fields that we
// have defaults defined for
func (c *updateCommand) validate() (err error) {
	// If a path is set, we have all we need
	if c.path != "" {
		return nil
	}

	if c.entryID == "" {
		return errors.New("entry ID is required")
	}

	if len(c.selectors) < 1 {
		return errors.New("at least one selector is required")
	}

	if c.parentID == "" {
		return errors.New("a parent ID is required")
	}

	if c.spiffeID == "" {
		return errors.New("a SPIFFE ID is required")
	}

	if c.x509SvidTTL < 0 {
		return errors.New("a positive x509-SVID TTL is required")
	}

	if c.jwtSvidTTL < 0 {
		return errors.New("a positive JWT-SVID TTL is required")
	}

	return nil
}

// parseConfig builds a registration entry from the given config
func (c *updateCommand) parseConfig() ([]*types.Entry, error) {
	parentID, err := idStringToProto(c.parentID)
	if err != nil {
		return nil, err
	}
	spiffeID, err := idStringToProto(c.spiffeID)
	if err != nil {
		return nil, err
	}

	x509SvidTTL, err := util.CheckedCast[int32](c.x509SvidTTL)
	if err != nil {
		return nil, fmt.Errorf("invalid value for X509 SVID TTL: %w", err)
	}

	jwtSvidTTL, err := util.CheckedCast[int32](c.jwtSvidTTL)
	if err != nil {
		return nil, fmt.Errorf("invalid value for JWT SVID TTL: %w", err)
	}

	e := &types.Entry{
		Id:          c.entryID,
		ParentId:    parentID,
		SpiffeId:    spiffeID,
		Downstream:  c.downstream,
		ExpiresAt:   c.entryExpiry,
		DnsNames:    c.dnsNames,
		X509SvidTtl: x509SvidTTL,
		JwtSvidTtl:  jwtSvidTTL,
		Hint:        c.hint,
	}

	selectors := []*types.Selector{}
	for _, s := range c.selectors {
		cs, err := serverutil.ParseSelector(s)
		if err != nil {
			return nil, err
		}

		selectors = append(selectors, cs)
	}

	e.Selectors = selectors
	e.FederatesWith = c.federatesWith
	e.Admin = c.admin
	e.StoreSvid = c.storeSVID
	return []*types.Entry{e}, nil
}

func updateEntries(ctx context.Context, c entryv1.EntryClient, entries []*types.Entry) (resp *entryv1.BatchUpdateEntryResponse, err error) {
	resp, err = c.BatchUpdateEntry(ctx, &entryv1.BatchUpdateEntryRequest{
		Entries: entries,
	})
	if err != nil {
		return
	}

	for i, r := range resp.Results {
		if r.Status.Code != int32(codes.OK) {
			// The Entry API does not include in the results the entries that
			// failed to be updated, so we populate them from the request data.
			r.Entry = entries[i]
		}
	}

	return
}

func prettyPrintUpdate(env *commoncli.Env, results ...any) error {
	var succeeded, failed []*entryv1.BatchUpdateEntryResponse_Result
	updateResp, ok := results[0].(*entryv1.BatchUpdateEntryResponse)
	if !ok {
		return cliprinter.ErrInternalCustomPrettyFunc
	}

	for _, r := range updateResp.Results {
		switch r.Status.Code {
		case int32(codes.OK):
			succeeded = append(succeeded, r)
		default:
			failed = append(failed, r)
		}
	}
	// Print entries that succeeded to be updated
	for _, e := range succeeded {
		printEntry(e.Entry, env.Printf)
	}

	// Print entries that failed to be updated
	for _, r := range failed {
		env.ErrPrintf("Failed to update the following entry (code: %s, msg: %q):\n",
			util.MustCast[codes.Code](r.Status.Code),
			r.Status.Message)
		printEntry(r.Entry, env.ErrPrintf)
	}

	if len(failed) > 0 {
		return errors.New("failed to update one or more entries")
	}

	return nil
}
