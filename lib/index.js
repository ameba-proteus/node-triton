
var clients = {};
var TritonClient = require('./client');

/**
 * Return TritonClient and connecting to the server.
 * {
 *   host: "127.0.0.1",
 *   port: 4848,
 *   pool: 1
 * }
 */
exports.open = function(option) {
	return new TritonClient(option);
};

/**
 * Create a client.
 * Named clients are singleton for the app.
 */
exports.client = function(name, option) {
	option = option || {};
	if (name) {
		var client = clients[name];
		if (client) {
			return client;
		} else {
			client = new TritonClient(option);
			clients[name] = client;
			return client;
		}
	} else {
		return new TritonClient(option);
	}
};

// error codes
exports.errors = require('./errors');

// cassandra
exports.cassandra = require('./service/cassandra');

// memcached

// lock

