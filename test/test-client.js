
var triton = require('../');

describe('TritonClient', function() {
	describe('#open', function() {
		it('should connect to the server', function(done) {
			var client = triton.open({ host:'127.0.0.1' });
			client.on('open', function() {
				client.close();
				done();
			});
			client.on('error', function(err) {
				done(err);
			});
			client.on('close', function() {
			});
			client.on('log', function(msg) {
				console.log(msg);
			});
		});
	});
	describe('#close', function() {
		it('should close the client', function(done) {
			var client = triton.open({ host:'127.0.0.1' });
			client.on('open', function() {
				client.close();
			});
			client.on('close', function() {
				done();
			});
			client.on('error', function(err) {
				done(err);
			});
		});
	});
	describe('#send', function() {
		it('should get heartbeat', function(done) {
			var client = triton.open({host:'127.0.0.1'});
			client.on('open', function() {
				client.send('triton.heartbeat', function(err, body) {
					done(err);
				});
			});
			client.on('error', function(err) {
				done(err);
			});
		});
		it('should get echo', function(done) {
			var client = triton.open({host:'127.0.0.1'});
			client.on('open', function() {
				client.send('triton.echo', {name:'test'}, function(err, body) {
					body.should.have.property('name');
					body.name.should.equal('test');
					done();
				});
			});
			client.on('error', function(err) {
				done(err);
			});
		});
	});
});
