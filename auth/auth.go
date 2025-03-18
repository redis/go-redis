package auth

type StreamingCredentialsProvider interface {
	// Subscribe subscribes to the credentials provider and returns a channel that will receive updates.
	// The first response is blocking, then data will be pushed to the channel.
	Subscribe(listener CredentialsListener) (Credentials, CancelProviderFunc, error)
}

type CancelProviderFunc func() error

type CredentialsListener interface {
	OnNext(credentials Credentials)
	OnError(err error)
}

type Credentials interface {
	BasicAuth() (username string, password string)
	RawCredentials() string
}

type basicAuth struct {
	username string
	password string
}

func (b *basicAuth) RawCredentials() string {
	return b.username + ":" + b.password
}

func (b *basicAuth) BasicAuth() (username string, password string) {
	return b.username, b.password
}

func NewCredentials(username, password string) Credentials {
	return &basicAuth{
		username: username,
		password: password,
	}
}
