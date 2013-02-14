# Triton client for node.js

node-triton is the client to communicate with triton data access gateway.

- pure javascript implementation works on node.js.
- no 3rd party dependencies.
- load balance and fail over connections.

## Index

* [Usage](#usage)
* [Cassandra](#cassandra)
* [Lock](#lock)

## Usage

	var triton = require('triton');
	var client = triton.open({host:'127.0.0.1',port:4848});
	client.on('open', function() {
		client.send('triton.echo', { prop1: 'value1' }, function(err, data) {
			console.log(data);
		});
	});
	client.on('error', function(err) {
		// unexpected error
		console.error(err);
	});
	client.on('close', function() {
		console.log('connection closed');
	});

## Load balanced connections

	var triton = require('triton');
	var client = triton.open({ hosts: ['127.0.0.1:4848','127.0.0.2:4848','127.0.0.3:4848'] });
	...

## Cassandra

### configure cassandra interface

configure virtual keyspaces with cluster and actual keyspace name.

	triton.open({
		hosts: ['127.0.0.1'],
		cassandra: {
			mappingname: {
				cluster: 'cluster name defined in triton-server',
				keyspace: 'name of the keyspace'
			}
		}
	});

### keyspace interface

	var keyspace = client.keyspaces.mappingname;
	
#### create the keyspace

	keyspace.create({
		strategy_class: 'SimpleStrategy',
		strategy_options: {replication_factor: 3}
	}, function(err, result) {
	});

#### drop the keyspace

	keyspace.drop(function(err, result) {});

#### describe the keyspace

	keyspace.detail(function(err, detail) {
		detail.name;
		detail.strategy_class;
		detail.strategy_options;
		detail.column_families;
	});

### column family interface

	var family = keyspace.family('column family name');

#### create the column family

	family.create({
		comparator: 'UTF8Type',
		key_validation_class: 'UTF8Type',
		default_validation_class: 'UTF8Type'
	}, function(err, result) {});

#### drop the column family

	family.drop({}, function(err, result) {});

#### get rows/columns

##### get single row

Row will be the object which is mapped with column and value. It will be null if row cannot be found.

	family.get({ key: 'rowkey1'}, function(err, row) {
		/*
		row = {
			column1: value1,
			column2: value2
		}
		*/
	});

##### get rows

Rows will be object which is mapped with keys.

	family.get({ keys: ['rowkey1', 'rowkey2'] }, function(err, rows) {
		/*
		rows = {
			rowkey1: {
				column1: value1,
				column2: value2
			},
			rowkey2: {
				column2: value2,
				column3: value3
			}
		..
		*/
	});

##### get row range

Rows will be array if keys are range query.

	family.get({ keys: {start:'rowkey1', limit: 10} }, function(err, rows) {
		/*
		rows = [{
			key: 'rowkey1',
			columns: { test1: value1, test2: value 2 }
		}, {
			key: 'rowkey2',
			columns: { test2: value2, test3: value3 }
		}]
		*/
	});

The key range will be sorted by hashed value. start will not be alphabetical order since cassandra uses hashed partitioner as default.

##### get columns

Columns are mapped as object. They have only non-null properties.

	family.get({ keys: ['row1', 'row2'], columns: ['column1', 'column2'] }, function(err, rows) {
		/*
		rows = {
			key1: {
				column1: 'test value1',
				column2: 'test value2',
			},
			key2: {
				column1: 'test value3'
			}
		};
		*/
	});

##### get column range

Columns are array sorted by column values.

	family.get({ keys: ['row1', 'row2'], columns: { start: 'startkey' }, function(err, rows) {
		/*
		rows = {
			row1: [{
				column: 'startkey1',
				timestamp: 123456789, // nano sec of unixtime
				value: 'column value1'
			}, {
				column: 'startkey2',
				timestamp: 123456789,
				value: 'column value2'
			}],
			row2
			...
		}
		*/
	});

#### set rows/columns

#### remove rows/columns

#### batch update of rows/columns

#### retrieving rows/columns with cursor

## Lock server interface
