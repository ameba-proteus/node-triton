
var triton = require('../');
var should = require('should');
var async = require('async');

describe('TritonCassandraClient', function() {
	var client;
	before(function(done) {
		// create connection
//		client = triton.open({ host : '127.0.0.1' });
		// need cassandra settings to use method based API
		client = triton.open({
			host : '127.0.0.1',
				cassandra : {
					ks_triton1 : {
						cluster : 'test',
						keyspace : 'triton1'
					}
				}
		});
		client.on('log', function(msg) {
			console.log(msg);
		});
		done();
	});
	after(function(done) {
		// close connection
		client.close();
		client.on('close', function() {
			done();
		});
	});
	describe('#cassandra', function() {
		describe('Cluster API', function() {
			it('should show cluster list', function(done) {
				client.cassandra.list(function (err, result) {
					result.should.includeEql( { name : 'test' });
					done(err);
				});
			});
		});
		describe('keyspace API', function() {
			it('should create and drop', function(done) {
				async.waterfall([
					// create keyspace
					function(callback) {
						client.keyspaces['ks_triton1']
							.create({ strategy_class : 'SimpleStrategy',
									strategy_options : {replication_factor : 1},
									durable_writes : true
							}, function(err, result) {
								// may be already created
								if (!err) {
									result.should.be.true;
								}
								callback(null, result);
							});
					},
					// verify created keyspace
					function(result, callback) {
						client.cassandra.cluster('test').list(function (err, result) {
							for (var i = 0; i < result.length; i++) {
								if (result[i].name === 'triton1') {
									result[i].strategy_class.should.include('SimpleStrategy');
									result[i].strategy_options.replication_factor.should.equal('1');
									break;
								}
							}
							callback(null, result);
						});
					},
					// verify created keyspace
					function(result, callback) {
						client.keyspaces['ks_triton1'].detail(function (err, result) {
							result.strategy_class.should.include('SimpleStrategy');
							result.strategy_options.replication_factor.should.equal('1');
							callback(err, result);
						});
					},
					// drop keyspace
					function(result, callback) {
						client.keyspaces['ks_triton1'].drop(function (err, result) {
							callback(null, result);
						});
					}
				], function(err, result) {
					if (err) return done(err);
					done();
				});
			});
		});
		describe('column family API', function() {
			before(function(done) {
				// create keyspace
				client.keyspaces['ks_triton1']
				.create({ strategy_class : 'SimpleStrategy',
						strategy_options : {replication_factor : 1},
						durable_writes : true
				}, function(err, result) {
					done();
				});
			});
			after(function(done) {
				// drop keyspace
				client.keyspaces['ks_triton1']
				.drop(function(err, result) {
					done();
				});
			});
			describe('create', function() {
				it('should create', function(done) {
					client.keyspaces['ks_triton1']
					.family('test1').create({ key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err, result) {
						result.should.be.true;
						done();
					});
				});
			});
			describe('set and get', function() {
				it('should set and get result', function(done) {
					async.waterfall([
						// create column family
						function(callback) {
							client.keyspaces['ks_triton1']
							.family('test_setget').create({ key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err, result) {
								callback(err, result);
							});
						},
						// set value to column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_setget').set({ 'row1' : { column1 : 'value1', column2 : 'value2',  column3 : { name1 : 'valuechild', name2 : 1000 }, column4 : 100 } }, { consistency : 'one' }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_setget').get({ consistency : 'one', keys : 'row1' }, function(err, result){
								result.column1.should.equal('value1');
								result.column2.should.equal('value2');
								result.column3.name1.should.equal('valuechild');
								result.column3.name2.should.equal(1000);
								result.column4.should.equal(100);
								callback(err, result);
							});
						}
					], function(err, result) {
						if (err) return done(err);
						done();
					});
				});
			});
			describe('remove', function() {
				it('should remove result', function(done) {
					async.waterfall([
						// create column family
						function(callback) {
							client.keyspaces['ks_triton1']
							.family('test_remove').create({ key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err, result) {
								callback(err, result);
							});
						},
						// set value to column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_remove').set(
								{ 'row1' : { column1 : 'value11', column2 : 'value12',  column3 : 'value13' },
									'row2' : { column1 : 'value21', column2 : 'value22',  column3 : 'value23' },
									'row3' : { column1 : 'value31', column2 : 'value32',  column3 : 'value33' } },
								{ consistency : 'one' }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_remove').get({ consistency : 'one' }, function(err, result){
								result.row1.column1.should.equal('value11');
								result.row2.column1.should.equal('value21');
								result.row3.column1.should.equal('value31');
								callback(err, result);
							});
						},
						// remove some value from column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_remove').remove({ consistency : 'one', rows : { row1 : [ 'column1', 'column2' ] } }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_remove').get({ consistency : 'one' }, function(err, result){
								result.row1.should.not.have.property('column1');
								result.row1.should.not.have.property('column2');
								result.row1.should.have.property('column3');
								result.row2.should.have.property('column1');
								result.row2.should.have.property('column2');
								result.row2.should.have.property('column3');
								result.row3.should.have.property('column1');
								result.row3.should.have.property('column2');
								result.row3.should.have.property('column3');
								callback(err, result);
							});
						},
						// remove some value from column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_remove').remove({ consistency : 'one', key : 'row2' }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// remove some value from column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_remove').remove({ consistency : 'one', rows : { row1 : [ 'column3' ], row3 : [ 'column2', 'column3' ] } }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.keyspaces['ks_triton1']
							.family('test_remove').get({ consistency : 'one' }, function(err, result){
								result.should.not.have.property('row1');
								result.should.not.have.property('row2');
								result.row3.should.have.property('column1');
								result.row3.should.not.have.property('column2');
								result.row3.should.not.have.property('column3');
								callback(err, result);
							});
						}
					], function(err, result) {
						if (err) return done(err);
						done();
					});
				});
			});
		});
	});
	describe('#send', function() {
		describe('cluster commands', function() {
			it('should show cluster list', function(done) {
				client.send('cassandra.cluster.list', {}, function(err, result) {
					result.should.includeEql( { name : 'test' });
					done(err);
				});
			});
		});
		describe('keyspace commands', function() {
			it('should create and drop', function(done) {
				async.waterfall([
					// create keyspace
					function(callback) {
						client.send('cassandra.keyspace.create', { cluster : 'test', keyspace : 'triton1', strategy_class : 'SimpleStrategy', strategy_options : {replication_factor : 1}, durable_writes : true }, function(err, result) {
							// may be already created
							if (!err) {
								result.should.be.true;
							}
							callback(null, result);
						});
					},
					// verify created keyspace
					function(result, callback) {
						client.send('cassandra.keyspace.list', { cluster : 'test' }, function(err, result) {
							for (var i = 0; i < result.length; i++) {
								if (result[i].name === 'triton1') {
									result[i].strategy_class.should.include('SimpleStrategy');
									result[i].strategy_options.replication_factor.should.equal('1');
									break;
								}
							}
							callback(err, result);
						});
					},
					// verify created keyspace
					function(result, callback) {
						client.send('cassandra.keyspace.detail', { cluster : 'test', keyspace : 'triton1' }, function(err, result) {
							result.strategy_class.should.include('SimpleStrategy');
							result.strategy_options.replication_factor.should.equal('1');
							callback(err, result);
						});
					},
					// drop keyspace
					function(result, callback) {
						client.send('cassandra.keyspace.drop', { cluster : 'test', keyspace : 'triton1' }, function(err, result) {
							result.should.be.true;
							callback(err, result);
						});
					}
				], function(err, result) {
					if (err) return done(err);
					done();
				});
			});
		});
		describe('column family commands', function() {
			before(function(done) {
				// create keyspace
				client.send('cassandra.keyspace.create', { cluster : 'test', keyspace : 'triton1', strategy_class : 'SimpleStrategy', strategy_options : {replication_factor : 1}, durable_writes : true }, function(err, result) {
					done();
				});
			});
			after(function(done) {
				// drop keyspace
				client.send('cassandra.keyspace.drop', { cluster : 'test', keyspace : 'triton1' }, function(err, result){
					done();
				});
			});
			describe('create', function() {
				it('should create', function(done) {
					client.send('cassandra.columnfamily.create', { cluster : 'test', keyspace : 'triton1', column_family : 'test1', key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err, result) {
						result.should.be.true;
						done();
					});
				});
			});
			describe('set and get', function() {
				it('should set and get result', function(done) {
					async.waterfall([
						// create column family
						function(callback) {
							client.send('cassandra.columnfamily.create', { cluster : 'test', keyspace : 'triton1', column_family : 'test_setget', key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err, result) {
								callback(err, result);
							});
						},
						// set value to column family
						function(result, callback) {
							client.send('cassandra.column.set', { cluster : 'test', keyspace : 'triton1', column_family : 'test_setget', consistency : 'one', 
									rows : { 'row1' : { column1 : 'value1', column2 : 'value2',  column3 : { name1 : 'valuechild', name2 : 1000 }, column4 : 100 } } },
									function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.send('cassandra.column.get', { cluster : 'test', keyspace : 'triton1', column_family : 'test_setget', consistency : 'one', keys : 'row1' }, function(err, result){
								result.column1.should.equal('value1');
								result.column2.should.equal('value2');
								result.column3.name1.should.equal('valuechild');
								result.column3.name2.should.equal(1000);
								result.column4.should.equal(100);
								callback(err, result);
							});
						}
					], function(err, result) {
						if (err) return done(err);
						done();
					});
				});
			});
			describe('remove', function() {
				it('should remove result', function(done) {
					async.waterfall([
						// create column family
						function(callback) {
							client.send('cassandra.columnfamily.create', { cluster : 'test', keyspace : 'triton1', column_family : 'test_remove', key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err, result) {
								callback(err, result);
							});
						},
						// set value to column family
						function(result, callback) {
							client.send('cassandra.column.set', { cluster : 'test', keyspace : 'triton1', column_family : 'test_remove', consistency : 'one',
									rows : { 'row1' : { column1 : 'value11', column2 : 'value12',  column3 : 'value13' },
											'row2' : { column1 : 'value21', column2 : 'value22',  column3 : 'value23' },
											'row3' : { column1 : 'value31', column2 : 'value32',  column3 : 'value33' }
									} },
									function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.send('cassandra.column.get', { cluster : 'test', keyspace : 'triton1', column_family : 'test_remove', consistency : 'one' }, function(err, result){
								result.row1.column1.should.equal('value11');
								result.row2.column1.should.equal('value21');
								result.row3.column1.should.equal('value31');
								callback(err, result);
							});
						},
						// remove some value from column family
						function(result, callback) {
							client.send('cassandra.column.remove', { cluster : 'test', keyspace : 'triton1', column_family : 'test_remove', consistency : 'one', rows : { row1 : [ 'column1', 'column2' ] } }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.send('cassandra.column.get', { cluster : 'test', keyspace : 'triton1', column_family : 'test_remove', consistency : 'one' }, function(err, result){
								result.row1.should.not.have.property('column1');
								result.row1.should.not.have.property('column2');
								result.row1.should.have.property('column3');
								result.row2.should.have.property('column1');
								result.row2.should.have.property('column2');
								result.row2.should.have.property('column3');
								result.row3.should.have.property('column1');
								result.row3.should.have.property('column2');
								result.row3.should.have.property('column3');
								callback(err, result);
							});
						},
						// remove some value from column family
						function(result, callback) {
							client.send('cassandra.column.remove', { cluster : 'test', keyspace : 'triton1', column_family : 'test_remove', consistency : 'one', key : 'row2' }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// remove some value from column family
						function(result, callback) {
							client.send('cassandra.column.remove', { cluster : 'test', keyspace : 'triton1', column_family : 'test_remove', consistency : 'one', rows : { row1 : [ 'column3' ], row3 : [ 'column2', 'column3' ] } }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.send('cassandra.column.get', { cluster : 'test', keyspace : 'triton1', column_family : 'test_remove', consistency : 'one' }, function(err, result){
								result.should.not.have.property('row1');
								result.should.not.have.property('row2');
								result.row3.should.have.property('column1');
								result.row3.should.not.have.property('column2');
								result.row3.should.not.have.property('column3');
								callback(err, result);
							});
						}
					], function(err, result) {
						if (err) return done(err);
						done();
					});
				});
			});
		});
	});
});
