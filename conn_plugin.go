package redis

import "context"

type (
	// PreInitConnPlugin plugin executed before connection initialization. At this point,
	// the network connection has been established, but Redis authentication has not yet
	// taken place. You can perform specific operations before the Redis authentication,
	// such as third-party Redis proxy authentication or executing any necessary commands.
	// Please note that the `HELLO` command has not been executed yet. If you invoke any Redis
	// commands, the default RESP version of the Redis server will be used.
	PreInitConnPlugin func(ctx context.Context, conn *Conn) error

	// InitConnPlugin redis connection authentication plugin. go-redis sets a default
	// authentication plugin, but if you need to implement a special authentication
	// mechanism for your Redis server, you can use this plugin instead of the default one.
	// This plugin can only be set once, and if set multiple times,
	// only the last set plugin will be executed.
	InitConnPlugin func(ctx context.Context, conn *Conn) error

	// PostInitConnPlugin Plugin executed after connection initialization. At this point,
	// Redis authentication has been completed, and you can execute commands related to
	// the connection status, such as `SELECT DB`, `CLIENT SETNAME`.
	PostInitConnPlugin func(ctx context.Context, conn *Conn) error
)

// ---------------------------------------------------------------------------------------

type plugin struct {
	preInitConnPlugins []PreInitConnPlugin
	initConnPlugin     InitConnPlugin
	postInitConnPlugin []PostInitConnPlugin
}

// RegistryPreInitConnPlugin register a PreInitConnPlugin plugin, which can be registered
// multiple times. It will be executed in the order of registration.
func (p *plugin) RegistryPreInitConnPlugin(pre PreInitConnPlugin) {
	p.preInitConnPlugins = append(p.preInitConnPlugins, pre)
}

// RegistryInitConnPlugin register an InitConnPlugin plugin, which will override the default
// authentication mechanism of go-redis. If registered multiple times, only the plugin
// registered last will be executed.
func (p *plugin) RegistryInitConnPlugin(init InitConnPlugin) {
	p.initConnPlugin = init
}

// RegistryPostInitConnPlugin register a PostInitConnPlugin plugin, which can be registered
// multiple times. It will be executed in the order of registration.
func (p *plugin) RegistryPostInitConnPlugin(post PostInitConnPlugin) {
	p.postInitConnPlugin = append(p.postInitConnPlugin, post)
}
