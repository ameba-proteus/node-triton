# Triton client for node.js

node-triton is the client to communicate with triton data access gateway.

- pure javascript implementation works on node.js.
- no 3rd party dependencies.
- load balance and fail over connections.

## Usage

	var triton = require('triton');
	var client = triton.open({host:'127.0.0.1',port:4848});
	client.on('open', function() {

		client.send('triton.echo', { prop1: 'value1' }, function(data) {
			console.log(data);
		});

	});
	client.on('error', function(err) {
		console.error(err);
	});
	client.on('close', function() {
		console.log('connection closed');
	});

## Load balanced connections

	var triton = require('triton');
	var client = triton.open({ hosts: ['127.0.0.1:4848','127.0.0.2:4848','127.0.0.3:4848'] });
	...

