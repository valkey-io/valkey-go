package valkeygcp

import (
	"time"

	"github.com/valkey-io/valkey-go"
	"golang.org/x/oauth2"
)

// tokenSourceFunc adapts a function to oauth2.TokenSource.
type tokenSourceFunc func() (*oauth2.Token, error)

func (f tokenSourceFunc) Token() (*oauth2.Token, error) { return f() }

// staticIAM is an IAM option set that never touches ADC.
func staticIAM() IAMAuthOptions {
	return IAMAuthOptions{CustomTokenSource: oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: "iam-token",
		Expiry:      time.Now().Add(time.Hour),
	})}
}

// initOption is a ClientOption with only InitAddress set.
func initOption(addrs ...string) valkey.ClientOption {
	return valkey.ClientOption{InitAddress: addrs}
}
