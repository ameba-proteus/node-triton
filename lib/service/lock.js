
/**
 * @class Lock locker
 */
function Lock(client, name) {
	this.client = client;
	this.name = name;
}

Lock.prototype = {

	/**
	 * Acquire lock
	 * @param {Function} callback called when lock is available
	 */
	acquire: function(callback) {

		var self = this;
		var client = self.client;
		var name = self.name;

		client.send(
			'lock.acquire',
			{ key: name },
			name, // use name as hash source
			function(err, ownerId) {
				self.ownerId = ownerId;
				if (callback) {
					if (err) {
						callback.call(self, err);
					} else {
						callback.call(self, null, self);
					}
				}
			}
		);

	},

	/**
	 * Release lock
	 * @param {Function} callback called when lock is released.
	 */
	release: function(callback) {

		var self = this;

		if (!('ownerId' in self)) {
			return callback(new Error('lock has not been acquired yet.'));
		}

		var client = self.client;
		var name = self.name;
		var ownerId = self.ownerId;

		client.send(
			'lock.release',
			{ key: name, ownerId: ownerId },
			name, // use name as hash source
			function(err, result) {
				if (callback) {
					if (err) {
						callback.call(self, err);
					} else {
						callback.call(self, null, self, result);
					}
				}
			}
		);

	}

};

module.exports = Lock;
