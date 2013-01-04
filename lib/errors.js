
module.exports = {

	client: {
		error: 400,
		not_found: 404,
		timeout: 408,
		json_format: 415,
		body_format: 416,
		not_connected: 420,
	},

	server: {
		error: 500
	},

	cassandra: {
		error: 600,
		connection_fail: 601,
		no_cluster: 610,
		no_keyspace: 611,
		no_column_family: 612,
		invalid_consistency: 680,
		invalid_comparator: 681,
		invalid_token_type: 682
	},
	
	memcached: {
		not_configured: 700,
		error: 710,
		timeout: 711
	}

};
