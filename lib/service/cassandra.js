
/**
 * pickup object from arguments
 */
function pickup(args, type) {
	for (var i = 0; i < args.length; i++) {
		var arg = args[i];
		if (typeof arg === type) {
			return arg;
		}
	}
	return null;
}

/**
 * Cassandra wrapper for node-triton client
 */

// root wrapper
function Cassandra(client) {
	this.client = client;
	this.clusters = {};
}

/**
 * Get root instance
 */
Cassandra.prototype = {
	constructor: Cassandra,
	/**
	 * Get a cluster
	 */
	cluster: function(name) {
		var cluster = this.clusters[name];
		if (cluster) {
			return cluster;
		}
		// create new cluster
		cluster = new Cluster(this.client, name);
		this.clusters[name] = cluster;
		return cluster;
	},
	/**
	 * List clusters
	 */
	list: function(callback) {
		this.client.send('cassandra.cluster.list', callback);
	}
};


// Cluster wrapper
function Cluster(client, name) {
	this.client = client;
	this.name = name;
	this.keyspaces = {};
}

Cluster.prototype = {
	constructor: Cluster,
	prepare: function(data) {
		data = data || {};
		data.cluster = this.name;
		return data;
	},
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
		this.keyspaces[name] = keyspace;
		return keyspace;
	},
	/**
	 * List keyspaces
	 * @param callback Function keyspace list
	 */
	list: function(callback) {
		var data = pickup(arguments, 'object');
		this.client.send('cassandra.keyspace.list', this.prepare(data), callback);
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
		data = data || {};
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
	create: function() {
		var callback = pickup(arguments, 'function');
		var data = pickup(arguments, 'object');
		this.client.send(
			'cassandra.keyspace.create',
			this.prepare(data),
			callback
		);
	},
	drop: function() {
		var callback = pickup(arguments, 'function');
		var data = pickup(arguments, 'object');
		this.client.send(
			'cassandra.keyspace.drop',
			this.prepare(data),
			callback
		);
	},
	detail: function() {
		var callback = pickup(arguments, 'function');
		var data = pickup(arguments, 'object');
		this.client.send(
			'cassandra.keyspace.detail',
			this.prepare(data),
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
		data.cluster = self.cluster.name;
		data.keyspace = self.keyspace.name;
		data.column_family = self.name;
		return data;
	},
	/**
	 * create the column family
	 */
	create: function() {
		var data = pickup(arguments, 'object');
		var callback = pickup(arguments, 'function');
		this.client.send(
			'cassandra.columnfamily.create',
			this.prepare(data),
			callback);
	},
	/**
	 * drop the column family
	 */
	drop: function(data, callback) {
		var data = pickup(arguments, 'object');
		var callback = pickup(arguments, 'function');
		this.client.send(
			'cassandra.columnfamily.drop',
			this.prepare(data),
			callback);
	},
	/**
	 * get data from the column family
	 */
	get: function(data, callback) {
		this.client.send(
			'cassandra.column.get',
			this.prepare(data),
			callback);
	},
	/**
	 * set data from the column family.
	 * optional consistency can be 'one','two',three','quorum','local_quorum','each_quorum','all','any'
	 * @param rows Object row map
	 * @param option Object option { ttl: Number, consistency: 'quorum' }
	 * @param callback Functino callback
	 */
	set: function(rows, option, callback) {
		if (typeof option === 'function') {
			callback = option;
			option = {};
		}
		var data = this.prepare();
		data.rows = rows;
		if ('ttl' in option) {
			data.ttl = option.ttl;
		}
		if ('consitency' in option) {
			data.consistency = option.consistency;
		}
		this.client.send(
			'cassandra.column.set',
		   	this.prepare(data),
		   	callback);
	},
	/**
	 * remove data from the column family
	 */
	remove: function(data, callback) {
		this.client.send(
			'cassandra.column.remove',
		   	this.prepare(data),
		   	callback);
	}
};

module.exports = Cassandra;
