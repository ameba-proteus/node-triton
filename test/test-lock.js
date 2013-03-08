/*global describe: true, before:true, it:true  */

var triton = require('../');

describe('TritonClient', function() {

	describe('#lock', function() {

		var client;
		
		before(function(done) {
			client = triton.open({ host: '127.0.0.1' });
			client.on('open', function(err) {
				done(err);
			});
		});

		it('can lock and release normally', function(done) {
			client.lock('test', function(err, lock) {
				if (err) {
					return done(err);
				}
				lock.release(function(err) {
					done(err);
				});
			});
		});

		it('wait locked object', function(done) {

			var count = 1;

			client.lock('test1', function(err, lock) {
				if (err) {
					return done(err);
				}
				setTimeout(function() {
					count.should.be.equal(1);
					count++;
					lock.release();
				}, 100);
			});

			client.lock('test1', function(err, lock) {
				count.should.be.equal(2);
				lock.release(done);
			});

		});

	});

});
