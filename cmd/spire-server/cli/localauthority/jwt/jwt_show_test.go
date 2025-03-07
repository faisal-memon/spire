package jwt_test

import (
	"fmt"
	"testing"

	"github.com/gogo/status"
	localauthorityv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/localauthority/v1"
	authoritycommon_test "github.com/spiffe/spire/cmd/spire-server/cli/authoritycommon/test"
	"github.com/spiffe/spire/cmd/spire-server/cli/localauthority/jwt"
	"github.com/spiffe/spire/test/clitest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestJWTShowHelp(t *testing.T) {
	test := authoritycommon_test.SetupTest(t, jwt.NewJWTShowCommandWithEnv)

	test.Client.Help()
	require.Equal(t, jwtShowUsage, test.Stderr.String())
}

func TestJWTShowSynopsys(t *testing.T) {
	test := authoritycommon_test.SetupTest(t, jwt.NewJWTShowCommandWithEnv)
	require.Equal(t, "Shows the local JWT authorities", test.Client.Synopsis())
}

func TestJWTShow(t *testing.T) {
	for _, tt := range []struct {
		name               string
		args               []string
		expectReturnCode   int
		expectStdoutPretty string
		expectStdoutJSON   string
		expectStderr       string
		serverErr          error

		active,
		prepared,
		old *localauthorityv1.AuthorityState
	}{
		{
			name:             "success",
			expectReturnCode: 0,
			active: &localauthorityv1.AuthorityState{
				AuthorityId: "active-id",
				ExpiresAt:   1001,
			},
			prepared: &localauthorityv1.AuthorityState{
				AuthorityId: "prepared-id",
				ExpiresAt:   1002,
			},
			old: &localauthorityv1.AuthorityState{
				AuthorityId: "old-id",
				ExpiresAt:   1003,
			},
			expectStdoutPretty: "Active JWT authority:\n  Authority ID: active-id\n  Expires at: 1970-01-01 00:16:41 +0000 UTC\n\nPrepared JWT authority:\n  Authority ID: prepared-id\n  Expires at: 1970-01-01 00:16:42 +0000 UTC\n\nOld JWT authority:\n  Authority ID: old-id\n  Expires at: 1970-01-01 00:16:43 +0000 UTC\n",
			expectStdoutJSON:   `{"active":{"authority_id":"active-id","expires_at":"1001","upstream_authority_subject_key_id":""},"prepared":{"authority_id":"prepared-id","expires_at":"1002","upstream_authority_subject_key_id":""},"old":{"authority_id":"old-id","expires_at":"1003","upstream_authority_subject_key_id":""}}`,
		},
		{
			name:             "success - no active",
			expectReturnCode: 0,
			prepared: &localauthorityv1.AuthorityState{
				AuthorityId: "prepared-id",
				ExpiresAt:   1002,
			},
			old: &localauthorityv1.AuthorityState{
				AuthorityId: "old-id",
				ExpiresAt:   1003,
			},
			expectStdoutPretty: "Active JWT authority:\n  No active JWT authority found\n\nPrepared JWT authority:\n  Authority ID: prepared-id\n  Expires at: 1970-01-01 00:16:42 +0000 UTC\n\nOld JWT authority:\n  Authority ID: old-id\n  Expires at: 1970-01-01 00:16:43 +0000 UTC\n",
			expectStdoutJSON:   `{"prepared":{"authority_id":"prepared-id","expires_at":"1002","upstream_authority_subject_key_id":""},"old":{"authority_id":"old-id","expires_at":"1003","upstream_authority_subject_key_id":""}}`,
		},
		{
			name:             "success - no prepared",
			expectReturnCode: 0,
			active: &localauthorityv1.AuthorityState{
				AuthorityId: "active-id",
				ExpiresAt:   1001,
			},
			old: &localauthorityv1.AuthorityState{
				AuthorityId: "old-id",
				ExpiresAt:   1003,
			},
			expectStdoutPretty: "Active JWT authority:\n  Authority ID: active-id\n  Expires at: 1970-01-01 00:16:41 +0000 UTC\n\nPrepared JWT authority:\n  No prepared JWT authority found\n\nOld JWT authority:\n  Authority ID: old-id\n  Expires at: 1970-01-01 00:16:43 +0000 UTC\n",
			expectStdoutJSON:   `{"active":{"authority_id":"active-id","expires_at":"1001","upstream_authority_subject_key_id":""},"old":{"authority_id":"old-id","expires_at":"1003","upstream_authority_subject_key_id":""}}`,
		},
		{
			name:             "success - no old",
			expectReturnCode: 0,
			active: &localauthorityv1.AuthorityState{
				AuthorityId: "active-id",
				ExpiresAt:   1001,
			},
			prepared: &localauthorityv1.AuthorityState{
				AuthorityId: "prepared-id",
				ExpiresAt:   1002,
			},
			expectStdoutPretty: "Active JWT authority:\n  Authority ID: active-id\n  Expires at: 1970-01-01 00:16:41 +0000 UTC\n\nPrepared JWT authority:\n  Authority ID: prepared-id\n  Expires at: 1970-01-01 00:16:42 +0000 UTC\n\nOld JWT authority:\n  No old JWT authority found\n",
			expectStdoutJSON:   `{"active":{"authority_id":"active-id","expires_at":"1001","upstream_authority_subject_key_id":""},"prepared":{"authority_id":"prepared-id","expires_at":"1002","upstream_authority_subject_key_id":""}}`,
		},
		{
			name:             "wrong UDS path",
			args:             []string{clitest.AddrArg, clitest.AddrValue},
			expectReturnCode: 1,
			expectStderr:     "Error: " + clitest.AddrError,
		},
		{
			name:             "server error",
			serverErr:        status.Error(codes.Internal, "internal server error"),
			expectReturnCode: 1,
			expectStderr:     "Error: rpc error: code = Internal desc = internal server error\n",
		},
	} {
		for _, format := range authoritycommon_test.AvailableFormats {
			t.Run(fmt.Sprintf("%s using %s format", tt.name, format), func(t *testing.T) {
				test := authoritycommon_test.SetupTest(t, jwt.NewJWTShowCommandWithEnv)
				test.Server.ActiveJWT = tt.active
				test.Server.PreparedJWT = tt.prepared
				test.Server.OldJWT = tt.old
				test.Server.Err = tt.serverErr
				args := tt.args
				args = append(args, "-output", format)

				returnCode := test.Client.Run(append(test.Args, args...))

				authoritycommon_test.RequireOutputBasedOnFormat(t, format, test.Stdout.String(), tt.expectStdoutPretty, tt.expectStdoutJSON)
				require.Equal(t, tt.expectStderr, test.Stderr.String())
				require.Equal(t, tt.expectReturnCode, returnCode)
			})
		}
	}
}
