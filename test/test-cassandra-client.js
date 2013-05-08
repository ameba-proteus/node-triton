/*global describe:true before:true after:true it:true */
/*jshint expr:true */

var triton = require('../');
var async = require('async');
var should = require('should');

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
						client.keyspaces.ks_triton1
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
						client.keyspaces.ks_triton1.detail(function (err, result) {
							result.strategy_class.should.include('SimpleStrategy');
							result.strategy_options.replication_factor.should.equal('1');
							callback(err, result);
						});
					},
					// drop keyspace
					function(result, callback) {
						client.keyspaces.ks_triton1.drop(function (err, result) {
							callback(null, result);
						});
					}
				], function(err) {
					if (err) return done(err);
					done();
				});
			});
		});
		describe('column family API', function() {
			before(function(done) {
				// create keyspace
				client.keyspaces.ks_triton1
				.create({ strategy_class : 'SimpleStrategy',
						strategy_options : {replication_factor : 1},
						durable_writes : true
				}, function(err) {
					done(err);
				});
			});
			after(function(done) {
				// drop keyspace
				client.keyspaces.ks_triton1
				.drop(function(err) {
					done(err);
				});
			});
			describe('create', function() {
				it('should create', function(done) {
					client.keyspaces.ks_triton1
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
							client.keyspaces.ks_triton1
							.family('test_setget').create({ key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err, result) {
								callback(err, result);
							});
						},
						// set value to column family
						function(result, callback) {
							client.keyspaces.ks_triton1
							.family('test_setget').set({ 'row1' : { column1 : 'value1', column2 : 'value2',  column3 : { name1 : 'valuechild', name2 : 1000 }, column4 : 100 } }, { consistency : 'one' }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.keyspaces.ks_triton1
							.family('test_setget').get({ consistency : 'one', keys : 'row1' }, function(err, result){
								result.column1.should.equal('value1');
								result.column2.should.equal('value2');
								result.column3.name1.should.equal('valuechild');
								result.column3.name2.should.equal(1000);
								result.column4.should.equal(100);
								callback(err, result);
							});
						}
					], function(err) {
						done(err);
					});
				});
			});
			describe('remove', function() {
				it('should remove result', function(done) {
					async.waterfall([
						// create column family
						function(callback) {
							client.keyspaces.ks_triton1
							.family('test_remove').create({ key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err, result) {
								callback(err, result);
							});
						},
						// set value to column family
						function(result, callback) {
							client.keyspaces.ks_triton1
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
							client.keyspaces.ks_triton1
							.family('test_remove').get({ consistency : 'one' }, function(err, result){
								result.row1.column1.should.equal('value11');
								result.row2.column1.should.equal('value21');
								result.row3.column1.should.equal('value31');
								callback(err, result);
							});
						},
						// remove some value from column family
						function(result, callback) {
							client.keyspaces.ks_triton1
							.family('test_remove').remove({ consistency : 'one', rows : { row1 : [ 'column1', 'column2' ] } }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.keyspaces.ks_triton1
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
							client.keyspaces.ks_triton1
							.family('test_remove').remove({ consistency : 'one', key : 'row2' }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// remove some value from column family
						function(result, callback) {
							client.keyspaces.ks_triton1
							.family('test_remove').remove({ consistency : 'one', rows : { row1 : [ 'column3' ], row3 : [ 'column2', 'column3' ] } }, function(err, result){
								result.should.be.true;
								callback(err, result);
							});
						},
						// get value from column family
						function(result, callback) {
							client.keyspaces.ks_triton1
							.family('test_remove').get({ consistency : 'one' }, function(err, result){
								result.should.not.have.property('row1');
								result.should.not.have.property('row2');
								result.row3.should.have.property('column1');
								result.row3.should.not.have.property('column2');
								result.row3.should.not.have.property('column3');
								callback(err, result);
							});
						}
					], function(err) {
						done(err);
					});
				});
			});
			describe('row cursors', function() {
				it('create test family', function(done) {
					client.keyspaces.ks_triton1
					.family('test_row_cursor').create({ key_validation_class : 'UTF8Type', comparator : 'UTF8Type', default_validation_class : 'UTF8Type' }, function(err) {
						done(err);
					});
				});
				it('should insert data', function(done) {
					var ks = client.keyspaces.ks_triton1;
					var sets = {};
					for (var i = 0; i < 500; i++) {
						sets['key'+i] = { count: i };
					}
					ks.family('test_row_cursor').set(sets, function(err) {
						done(err);
					});
				});
				it('can iterate cursor', function(done) {
					var ks = client.keyspaces.ks_triton1;
					var family = ks.family('test_row_cursor');
					var cursor = family.rowCursor();
					var count = 0;
					var existKeys = {};
					cursor
					.on('row', function(row) {
						row.should.have.property('key');
						row.should.have.property('columns');
						// judge unique
						existKeys.should.not.have.property(row.key);
						existKeys[row.key] = row.columns;
						row.columns.should.have.property('count');
						count++;
					})
					.on('end', function() {
						count.should.equal(500);
						done();
					})
					.on('error', function(err) {
						done(err);
					});
				});
				it('can iterate limited cursor', function(done) {
					var ks = client.keyspaces.ks_triton1;
					var family = ks.family('test_row_cursor');
					var cursor = family.rowCursor({keys:{start:null,limit:250}});
					var count = 0;
					var existKeys = {};
					cursor
					.on('row', function(row) {
						row.should.have.property('key');
						row.should.have.property('columns');
						// judge unique
						existKeys.should.not.have.property(row.key);
						existKeys[row.key] = row.columns;
						row.columns.should.have.property('count');
						count++;
					})
					.on('end', function() {
						count.should.equal(250);
						done();
					})
					.on('error', function(err) {
						done(err);
					});
				});
			});
			describe('column cursors', function() {
				it('create test family', function(done) {
					client.keyspaces.ks_triton1
					.family('test_column_cursor').create({
						key_validation_class : 'UTF8Type',
						comparator : 'UTF8Type',
						default_validation_class : 'UTF8Type' }, function(err) {
						done(err);
					});
				});
				it('should insert data', function(done) {
					var ks = client.keyspaces.ks_triton1;
					var sets = {key:{}};
					for (var i = 0; i < 500; i++) {
						sets.key['column'+i] = "value " + i;
					}
					ks.family('test_column_cursor').set(sets, function(err) {
						done(err);
					});
				});
				it('should iterate column cursor', function(done) {
					var ks = client.keyspaces.ks_triton1;
					var family = ks.family('test_column_cursor');
					var count = 0;
					family.columnCursor('key')
					.on('column', function() {
						count++;
					})
					.on('error', function(err) {
						done(err);
					})
					.on('end', function() {
						count.should.equal(500);
						done();
					});
				});
				it('can iterate limited column cursor', function(done) {
					var ks = client.keyspaces.ks_triton1;
					var family = ks.family('test_column_cursor');
					var count = 0;
					family.columnCursor('key', { columns: { limit: 150 }})
					.on('column', function() {
						count++;
					})
					.on('error', function(err) {
						done(err);
					})
					.on('end', function() {
						count.should.equal(150);
						done();
					});
				});
			});
			describe('column reversed', function() {
				it('create test family', function(done) {
					client.keyspaces.ks_triton1
					.family('test_column_reversed').create({
						key_validation_class : 'UTF8Type',
						comparator : 'UTF8Type',
						default_validation_class : 'UTF8Type' }, function(err) {
						done(err);
					});
				});
				it('should insert data', function(done) {
					client.keyspaces.ks_triton1
					.family('test_column_reversed').set({
						'row1' : { 
							column1 : 'value1', 
							column2 : 'value2',  
							column3 : 'value3', 
							column4 : 'value4'
						} 
					}, { consistency : 'one' }, function(err, result){
						result.should.be.true;
						done(err, result);
					});
				});
				it('can get reverse order data', function(done) {
					var ks = client.keyspaces.ks_triton1;
					var family = ks.family('test_column_reversed');
					family.get({
						keys: 'row1',
						columns: { 
							limit: 1,
							reversed: true
						}
					}, function(err, data) {
						data.length.should.eql(1);
						data[0].column.should.eql("column4");
						data[0].value.should.eql("value4");
						done();
					});
					
				});
			});
			describe('batch', function() {

				it('create test family', function(done) {
					var keyspace = client.keyspaces.ks_triton1;
					keyspace.family('test_batch1').create({
						key_validation_class : 'UTF8Type',
						comparator : 'UTF8Type',
						default_validation_class : 'UTF8Type'
					}, function(err) {
						if (err) {
							return done(err);
						}
						keyspace.family('test_batch2').create({
							key_validation_class : 'UTF8Type',
							comparator : 'UTF8Type',
							default_validation_class : 'UTF8Type'
						}, function(err) {
							return done(err);
						});
					});
				});
				it('should mutate rows', function(done) {
					var keyspace = client.keyspaces.ks_triton1;
					var batch = keyspace.batch();
					should.exist(batch);

					batch.update('test_batch1', {
						row1: {
							column1: 'value11',
							column2: 'value12'
						},
						row2: {
							column1: 'value21',
							column2: 'value22'
						}
					});
					batch.update('test_batch2', {
						row1: {
							column4: 'value14',
							column5: 'value15'
						},
						row3: {
							column6: 'value36'
						}
					});

					batch.execute(function(err, result) {
						if (err) {
							return done(err);
						}
						result.should.be.true;

						keyspace.family('test_batch1').get({}, function(err, rows) {
							if (err) {
								return done(err);
							}
							should.exist(rows);
							rows.should.have.property('row1');
							rows.should.have.property('row2');
							rows.row1.column1.should.equal('value11');
							rows.row1.column2.should.equal('value12');

							keyspace.family('test_batch2').get({}, function(err, rows) {
								if (err) {
									return done(err);
								}
								should.exist(rows);
								rows.should.have.property('row1');
								rows.should.have.property('row3');
								rows.row1.column4.should.equal('value14');
								rows.row1.column5.should.equal('value15');
								rows.row3.column6.should.equal('value36');
								done();
							});
						});
					});
				});
				it('should remove rows', function(done) {
					var keyspace = client.keyspaces.ks_triton1;
					var batch = keyspace.batch();
					should.exist(batch);

					batch.remove('test_batch1', {
						row1: true,
						row2: ['column1']
					});

					batch.remove('test_batch2', {
						row1: ['column4','column5']
					});

					batch.execute(function(err) {
						if (err) {
							return done(err);
						}

						keyspace.family('test_batch1').get({}, function(err, rows) {
							if (err) {
								return done(err);
							}
							rows.should.not.have.property('row1');
							rows.should.have.property('row2');
							rows.row2.should.not.have.property('column1');
							rows.row2.column2.should.equal('value22');

							keyspace.family('test_batch2').get({}, function(err, rows) {
								if (err) {
									return done(err);
								}
								rows.should.not.have.property('row1');
								rows.should.have.property('row3');
								rows.row3.should.have.property('column6');
								rows.row3.column6.should.equal('value36');
								done();
							});

						});

					});
				});
				it('combine update/remove', function(done) {

					var keyspace = client.keyspaces.ks_triton1;
					var batch = keyspace.batch();
					should.exist(batch);

					batch.update('test_batch1', {
						row3: {
							column1: 'value31',
							column2: 'value32'
						},
						row4: {
							column1: 'value41',
							column2: 'value42'
						}
					});
					batch.update('test_batch2', {
						row5: {
							column1: 'value51',
							column2: 'value52'
						},
						row6: {
							column1: 'value61',
							column2: 'value62'
						}
					});
					batch.remove('test_batch1', {
						row3: true
					});
					batch.remove('test_batch2', {
						row5: ['column1']
					});

					batch.execute(function(err) {
						if (err) {
							return done(err);
						}

						keyspace.family('test_batch1').get({}, function(err, rows) {

							if (err) {
								return done(err);
							}

							rows.should.have.property('row2');
							rows.should.not.have.property('row3');
							rows.should.have.property('row4');
							rows.row4.column1.should.equal('value41');
							rows.row4.column2.should.equal('value42');

							keyspace.family('test_batch2').get({}, function(err, rows) {

								rows.should.have.property('row5');
								rows.should.have.property('row6');
								rows.row5.should.not.have.property('column1');
								rows.row5.column2.should.equal('value52');
								rows.row6.column1.should.equal('value61');
								rows.row6.column2.should.equal('value62');
								done();
							});
						});
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
				], function(err) {
					done(err);
				});
			});
		});
		describe('column family commands', function() {
			before(function(done) {
				// create keyspace
				client.send('cassandra.keyspace.create', { cluster : 'test', keyspace : 'triton1', strategy_class : 'SimpleStrategy', strategy_options : {replication_factor : 1}, durable_writes : true }, function(err) {
					done(err);
				});
			});
			after(function(done) {
				// drop keyspace
				client.send('cassandra.keyspace.drop', { cluster : 'test', keyspace : 'triton1' }, function(err) {
					done(err);
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
					], function(err) {
						done(err);
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
					], function(err) {
						done(err);
					});
				});
			});
		});
	});
});
