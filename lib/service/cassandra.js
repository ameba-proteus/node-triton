/**
 * Cassandra wrapper for node-triton client
 */

// Cluster wrapper
function Cluster(client, name) {
	this.client = client;
	this.name = name;
	this.keyspaces = {};
}

Cluster.prototype = {
	constructor: Cluster,
	/**
	 * Get keyspace instance of the cluster.
	 * @param name String cluster name
	 */
	keyspace: function(name) {
		var keyspace = this.keyspaces[name];
		if (keyspace) {
			return keyspace;
		}
		// create new keyspace if not exists
		keyspace = new Keyspace(this.client, this, name);
		keyspaces[name] = keyspace;
		return keyspace;
	},
	/**
	 * List keyspaces
	 * @param callback Function keyspace list
	 */
	list: function(callback) {
		this.client.send('cassandra.keyspace.list', callback);
	}
};

// Keyspace wrapper
function Keyspace(client, cluster, name) {
	this.client = client;
	this.cluster = cluster;
	this.name = name;
	this.families = {};
}

Keyspace.prototype = {
	constructor: Keyspace,
	prepare: function(data) {
		data.cluster = this.cluster.name;
		data.keyspace = this.name;
		return data;
	},
	// get column family
	family: function(name) {
		var family = this.families[name];
		if (family) {
			return family;
		}
		family = new ColumnFamily(this.client, this.cluster, this, name);
		this.families[name] = family;
		return family;
	},
	create: function(data, callback) {
		this.client.send(
			'cassandra.keyspace.create',
			this.prepare(data),
			callback
		);
	},
	drop: function(data, callback) {
		this.client.send(
			'cassandra.keyspace.drop',
			this.parepare(data),
			callback
		);
	},
	/**
	 * List column families
	 */
	list: function(callback) {
		this.client.send(
			'cassandra.columnfamily.list',
			callback
		);
	}
};

// Column Family wrapper
function ColumnFamily(client, cluster, keyspace, name) {
	this.client = client;
	this.cluster = cluster;
	this.keyspace = keyspace;
	this.name = name;
}

ColumnFamily.prototype = {
	constructor: ColumnFamily,
	/**
	 * prepare for column family request
	 */
	prepare: function(data) {
		var self = this;
		data = data || {};
		data.cluster = this.cluster.name;
		data.keyspace = this.keyspace.name;
		data.column_family = self.name;
		return data;
	},
	get: function(data, callback) {
		this.client.send(
			'cassandra.column.get',
			this.prepare(data),
			callback);
	},
	set: function(data, callback) {
		this.client.send(
			'cassandra.column.set',
		   	this.prepare(data),
		   	callback);
	},
	remove: function(data, callback) {
		this.client.send(
			'cassandra.column.remove',
		   	this.prepare(data),
		   	callback);
	}
};

exports.cluster = function(client, name) {
	return new Cluster(client, name);
};

exports.keyspace = function(client, cluster, keyspace) {
	return new Cluster(client, cluster).keyspace(keyspace);
};

