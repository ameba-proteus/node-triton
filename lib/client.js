
var net = require('net');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var murmur3 = require('./murmur3');
var Cassandra = require('./service/cassandra');
var Lock = require('./service/lock');

/**
 * Create TritonClient with host and port.
 * @param option Object connection info
 * --- single host ---
 * { host: '127.0.0.1', port: 4848 }
 *  --- multiple hosts ---
 * { hosts: ['127.0.0.1:4848','127.0.0.2.4848] }
 *
 *  Port parameter can be ommited.
 */
function TritonClient(option) {
	EventEmitter.call(this);
	this.rrcount= 0;
	this.timeout = option.timeout || 5000;
	this.hosts = [];
	this.queue = [];
	this.calls = {};
	// connected property should return socket status
	this.__defineGetter__('connected', function() {
		return this.active.length > 0;
	});
	// mapping cassandra keyspaces
	if (option.cassandra) {
		this.cassandra = new Cassandra(this);
		this.keyspaces = {};
		for (var name in option.cassandra) {
			var ksdef = option.cassandra[name];
			this.keyspaces[name] = this.cassandra
				.cluster(ksdef.cluster)
				.keyspace(ksdef.keyspace);
		}
	}
	// connect to the host if address defined
	if (option.host || option.hosts) {
		this.open(option.host || option.hosts);
	}

	// close socket when disconnected
	process.on('disconnect', this.close.bind(this));
}

util.inherits(TritonClient, EventEmitter);

/**
 * open the connection to the hosts
 * @param hosts String/Array host ex) 127.0.0.1:4848. array to balance connections.
 */
TritonClient.prototype.open = function(hosts) {
	var self = this;
	self.active = [];
	self.deactive = [];
	delete this.closed;
	if (typeof hosts === 'string') {
		hosts = [hosts];
	}
	// reset hosts
	this.hosts = [];

	var host;
	// parse host array
	for (var i = 0; i < hosts.length; i++) {
		host = hosts[i];
		var port = 4848;
		if (host.indexOf(':') >= 0) {
			port = Number(host.substring(host.indexOf(':')+1));
			host = host.substring(host, host.indexOf(':'));
		}
		this.hosts.push({ id: i+1, host: host, port: port });
	}
	// connect to the gateways
	for (i = 0; i < this.hosts.length; i++) {
		host = this.hosts[i];
		self.socket(host);
	}

	if (this.intervalId) {
		clearInterval(this.intervalId);
	}
	this.intervalId = this.interval();

	return self;
};

/**
 * Create a socket and connect to the host.
 * @param host Object {host:host, port:port}
 */
TritonClient.prototype.socket = function(host, attempt) {

	var self = this;

	attempt = attempt || 0;

	var socket = new net.Socket();
	socket.id = host.id;
	socket.setNoDelay(true);
	socket.setTimeout(self.timeout);

	// active / deactive
	var active = self.active;
	var deactive = self.deactive;

	// buffers for each client
	var buff = null;
	
	socket.connect(host.port, host.host, function() {
		// remove from deactive list
		if (deactive.indexOf(socket) >= 0) {
			deactive.splice(deactive.indexOf(socket), 1);
		}
		// add to active list
		if (active.indexOf(socket) < 0) {
			active.push(socket);
			active.sort(function(a,b) {
				return a.id - b.id;
			});
			// emit open if first connection are establiesh
			if (active.length === 1) {
				self.emit('open');
				var queue = self.queue;
				if (queue.length > 0) {
					// process queue if queue is not empty
					self.processQueue();
				}
			}
		}
		// reset retrying wait time
		attempt = 0;
	});
	socket.on('data', function(data) {
		if (buff === null) {
			// set buff as  buffer
			buff = data;
		} else {
			// concatenate buffer
			buff = Buffer.concat([buff, data]);
		}
		readData();
	});

	/**
	 * Read buffer and execute callbacks
	 */
	function readData() {
		if (buff === null || buff.length < 16) {
			// return with no header
			return;
		}
		// get data length
		var calls = self.calls;
		var datalen = buff.readInt32BE(2);
		var totallen = datalen + 16;
		if (buff.length < totallen) {
			// return if buffer is not sufficient.
			return;
		}
		try {
			// get call type
			var type = buff.readInt16BE(0);
			// get callId
			var callId = buff.readInt32BE(6);
			// get body
			var body = buff.toString('utf8', 16, totallen);
			// parse body as json
			body = JSON.parse(body);

			var call = calls[callId];
			if (call) {
				delete calls[callId];
				var callback = call.callback;
				if (type === 0x10) {
					// reply
					callback(null, body);
				} else if (type === 0x11) {
					// error
					var err = new Error(body.message || 'unknown error');
					err.code = body.code;
					callback(err);
				} else {
					// TODO handle invalid response type
					callback(new Error('invalid resposne type'));
				}
			}

		} catch (e) {
			// error while parsing JSON and executing callback
			self.emit('error', e);
		}

		try {
			// if buffer has more data,
			// create new buffer to execute next call
			if (buff.length > totallen) {
				// create a new buffer which has exceeded size.
				var newBuffer = new Buffer(buff.length - totallen);
				buff.copy(newBuffer, 0, totallen);
				// swap to new buffer
				buff = newBuffer;
				// re-execute buffer in next tick
				process.nextTick(readData);
			} else {
				buff = null;
			}
		} catch (e) {
			self.emit('error', e);
			buff = null;
		}
	}
	socket.on('timeout', function() {
		self.emit('timeout');
		socket.destroy();
	});
	socket.on('error', function(err) {
		self.emit('error', err);
		socket.destroy();
	});
	socket.on('close', function() {
		// add to deactive list
		if (deactive.indexOf(socket) < 0) {
			deactive.push(socket);
		}
		// remove from active list
		if (active.indexOf(socket) >= 0) {
			active.splice(active.indexOf(socket), 1);
			// retry after x sec
			// emit close if no more active connections
			if (active.length === 0) {
				self.emit('close');
			}
		}
		// start retry timeout if client still active
		// wait time will be expanded while failure.
		// max 60 sec
		if (!self.closed) {
			setTimeout(function() {
				self.log('retrying to connect to', host.host, host.port);
				self.socket(host, attempt+1);
			}, Math.min(60,attempt)*1000);
		}
	});
	return socket;

};

/**
 * Send the command to the server.
 * @param name String method name.
 * @param data Object data to send to the server.
 * @param hash String hash (optional) - hash key to resolve which connection will be used.
 * @param callback Function callback which called after receiving reply.
 */
TritonClient.prototype.send = function() {
	// get arguments
	var args = Array.prototype.slice.apply(arguments);
	var callback, data, name;
	// parse arguments
	var obj = args.pop();
	var hash = -1;
	while (obj) {
		if (typeof obj === 'function') {
			callback = obj;
		} else if (typeof obj === 'string') {
			// make 2nd string as hash and
			// 1st string as name
			if (name) {
				hash = murmur3(name);
			}
			name = obj;
		} else if (typeof obj === 'object') {
			data = obj;
		}
		obj = args.pop();
	}
	var timeout = this.timeout;
	// pickup active socket
	var socket = this.pickup(hash);
	// get connection
	var call = this.createCall(name, data, timeout, callback);
	if (socket) {
		// send call
		socket.write(call);
	} else {
		var queue = this.queue;
		// queueing until 1000 calls
		if (queue.length < 1000) {
			queue.push({
				hash: hash,
				call: call
			});
		} else {
			// throw error if queued command has more than 1000 errors
			queue.length = 0;
			throw new Error('Client could not connect to the server');
		}
	}
};

/**
 * Send heartbeat to the server to maintian connection.
 */
TritonClient.prototype.heartbeat = function() {
	var self = this;
	var call = self.createCall('triton.heartbeat', null, self.timeout);
	var active = self.active;
	for (var i = 0; i < active.length; i++) {
		try {
			active[i].write(call);
		} catch (e) {
			// ignore error to send hearbeat.
		}
	}
};

/**
 * Process queue.
 */
TritonClient.prototype.processQueue = function() {
	var self = this;
	var queue = self.queue;
	var data = queue.shift();
	if (data) {
		process.nextTick(function() {
			var socket = self.pickup(data.hash);
			if (socket) {
				socket.write(data.call);
			}
			self.processQueue();
		});
	}
};

var CALLID_COUNT = 0;

/**
 * Create command buffer
 */
TritonClient.prototype.createCall = function(name, body, timeout, callback) {
	var data = {
		name: name,
		body: body
	};
	var callId = 0;
	if (callback) {
		// assign callID
		callId = ++CALLID_COUNT;
		if (CALLID_COUNT >= 0x7fffffff) {
			// reset global count
			CALLID_COUNT = 0;
		}
		// set callback to LRU call cache.
		this.calls[callId] = {
			callId: callId,
			expire: Date.now() + timeout,
			callback: callback
		};
	}
	var datatext = JSON.stringify(data);
	var datalen = Buffer.byteLength(datatext);
	// create a buffer with header and body length.
	var buffer = new Buffer(datalen + 16);
	// write header
	buffer.writeInt16BE(0x01, 0);
	buffer.writeInt32BE(datalen, 2);
	buffer.writeInt32BE(callId, 6);
	// write body
	buffer.write(datatext, 16);
	return buffer;
};

/**
 * Pickup connection with hash.
 * @param hash Number -1 to round robin.
 */
TritonClient.prototype.pickup = function(hash) {
	// pickup active connection
	var active = this.active;
	if (!active || active.length === 0) {
		return null;
		// throw error if there are no active connections
		// throw new Error('no active connections on TritonClient');
	}
	// get round robin count
	if (hash === -1) {
		var rrcount = this.rrcount;
		if (++rrcount >= active.length) {
			// reset count when exceeds size.
			rrcount = 0;
		}
		// set round robin count
		this.rrcount = rrcount;
		// return next active socket
		return active[rrcount];
	} else {
		// return active connection with hash value
		return active[hash % active.length];
	}
};

/**
 * Close all active connections.
 */
TritonClient.prototype.close = function() {
	// close all active connections
	this.closed = true;
	var active = this.active;
	for (var i = 0; i < active.length; i++) {
		var socket = active[i];
		socket.end();
	}
	if (this.intervalId) {
		clearInterval(this.intervalId);
	}
};

/**
 * Get lock instance.
 * @param {String} name lock name
 * @param {Function} callback callback
 */
TritonClient.prototype.lock = function(name, callback) {
	var lock = new Lock(this, name);
	if (callback) {
		// automatically acquire lock if callback specified
		lock.acquire(callback);
	}
	return lock;
};

/**
 * Emit log event
 */
TritonClient.prototype.log = function() {
	var args = Array.prototype.slice.apply(arguments);
	this.emit('log', args.join(' '));
};

/**
 * check expired call each 1 sec
 */
TritonClient.prototype.interval = function() {
	var self = this;
	return setInterval(function() {
		var calls = self.calls;
		var now = Date.now();
		for (var callId in calls) {
			var call = calls[callId];
			if (now > call.expire) {
			// callback with timeout error
				if (call.callback) {
					call.callback(new Error('timed out'));
				}
				// delete expired call
				delete calls[callId];
			}
		}
		// send heartbeat
		self.heartbeat();
	}, 1000);
};

module.exports = TritonClient;
