
var net = require('net');
var EventEmitter = require('events').EventEmitter;

var calls = {};

/**
 * Create TritonClient with host and port.
 * --- single host ---
 *  {
 *  	host: '127.0.0.1', port: 4848
 *  }
 *  --- multiple hosts ---
 *  {
 *  	hosts: ['127.0.0.1:4848','127.0.0.2.4848]
 *  }
 *
 *  Port parameter can be ommited.
 */
function TritonClient(option) {
	EventEmitter.call(this);
	this.rrcount= 0;
	this.timeout = option.timeout || 5000;
	var hosts = this.hosts = [];
	// parse host
	if (option.host) {
		hosts.push({
			host: option.host,
			port: option.port || 4848
		});
	}
	// parse host array
	if (option.hosts) {
		for (var i = 0; i < option.hosts.length; i++) {
			var host = option.hosts[i];
			if (host.indexOf(':') >= 0) {
				host = host.substring(host, host.indexOf(':'));
				port = host.substring(host.indexOf(':')+1);
			} else {
				port = 4848;
			}
			hosts.push({ host: host, port: port });
		}
	}
	// connected property should return socket status
	this.__defineGetter__('connected', function() {
		return this.active.length > 0;
	});
}
TritonClient.prototype = new EventEmitter;

TritonClient.prototype.open = function() {
	var self = this;
	var active = self.active = [];
	var deactive = self.deactive = [];
	var left = this.pool;
	delete this.closed;
	for (var i = 0; i < this.hosts.length; i++) {
		var host = this.hosts[i];
		self.socket(host);
	}
	return self;
};

TritonClient.prototype.socket = function(host, attempt) {

	var self = this;

	attempt = attempt || 0;

	var socket = new net.Socket();
	socket.setNoDelay(true);

	// active / deactive
	var active = self.active;
	var deactive = self.deactive;

	// buffers for each client
	var buff = null;
	
	socket.connect(host.port, host.host, function(err) {
		// remove from deactive list
		if (deactive.indexOf(socket) >= 0) {
			deactive.splice(deactive.indexOf(socket), 1);
		}
		// add to active list
		if (active.indexOf(socket) < 0) {
			active.push(socket);
			// emit open if first connection are establiesh
			if (active.length === 1) {
				self.emit('open');
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
			buff = Buffer.concat(buff, data);
		}
		readData();
	});

	/**
	 * Read buffer and execute callbacks
	 */
	function readData() {
		if (buff.length < 16) {
			// return with no header
			return;
		}
		// get data length
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
		socket.end();
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

TritonClient.prototype.send = function() {
	// get arguments
	var args = Array.prototype.slice.apply(arguments);
	var callback, data, name;
	// parse arguments
	var obj = args.pop();
	while (obj) {
		if (typeof obj === 'function') {
			callback = obj;
		} else if (typeof obj === 'string') {
			name = obj;
		} else if (typeof obj === 'object') {
			data = obj;
		}
		obj = args.pop();
	}
	var timeout = this.timeout;
	// get connection
	var call = createCall(name, data, timeout, callback);
	// pickup active socket
	var socket = this.pickup();
	// send call
	socket.write(call);
};


var CALLID_COUNT = 0;

/**
 * Create command buffer
 */
function createCall(name, body, timeout, callback) {
	var data = {
		name: name,
		body: body
	};
	var callId = 0;
	if (callback) {
		// assign callID
		callId = ++CALLID_COUNT;
		if (CALLID_COUNT > 10000000) {
			CALLID_COUNT = 0;
		}
		// set callback to LRU call cache.
		calls[callId] = {
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

TritonClient.prototype.pickup = function() {
	// pickup active connection
	var active = this.active;
	if (active.length === 0) {
		// throw error if there are no active connections
		throw new Error('no active connections on TritonClient');
	}
	// get round robin count
	var rrcount = this.rrcount;
	if (++rrcount >= active.length) {
		// reset count when exceeds size.
		rrcount = 0;
	}
	// set round robin count
	this.rrcount = rrcount;
	// return next active socket
	return active[rrcount];
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
setInterval(function() {
	var now = Date.now();
	for (var callId in calls) {
		var call = calls[callId];
		if (now > call.expired) {
			// callback with timeout error
			if (call.callback) {
				call.callback(new Error('timed out'));
			}
			// delete expired call
			delete calls[callId];
		}
	}
}, 1000);

/**
 * Return TritonClient and connecting to the server.
 * {
 *   host: "127.0.0.1",
 *   port: 4848,
 *   pool: 1
 * }
 */
exports.open = function(option) {
	return new TritonClient(option).open();
};
