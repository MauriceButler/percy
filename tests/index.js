var test = require('grape'),
    Percy = require('../');

function createMockConnector(){
    var db = {};
    return function(callback){
        callback(null, {
            get: function(key, callback){
                if(!(key in db)){
                    return callback(key + ' not in db');
                }
                callback(null, db[key]);
            },
            set: function(key, model, callback){
                db[key] = {cas:{0:0,1:1}, value: model};
                callback(null, {cas:{0:0,1:1}});
            },
            add: function(key, model, callback){
                if(key in db){
                    return callback(key + ' already in db');
                }
                db[key] = {value: model};
                callback(null, {cas:{0:0,1:1}});
            },
            remove: function(key, callback){
                if(!(key in db)){
                    return callback(key + ' not in db');
                }
                delete db[key];
                callback(null, {cas:{0:0,1:1}});
            },
            replace: function(key, model, callback){
                if(!(key in db)){
                    return callback(key + ' not in db');
                }
                db[key] = {value: model};
                callback(null, {cas:{0:0,1:1}});
            }
        });
    };
}

function createMockValidator(){
    return {
        validate: function(data, callback){
            callback(null, data);
        }
    };
}

function createTestPercy(){
    var itemIndex = 0,
        percy = new Percy('thing', createMockConnector(), createMockValidator());

    percy.createId = function(callback){
        callback(null, (itemIndex++).toString());
    };

    return percy;
}

test('create percy', function(t){

    t.plan(1);

    var percy = createTestPercy();

    t.ok(percy, 'Percy was created');
});

test('set model', function(t){

    t.plan(2);

    var percy = createTestPercy(),
        testData = {};

    percy.set('abc', testData, function(error, model){
        t.notOk(error, 'no error');
        t.equal(model, testData, 'model returned');
    });
});

test('can double set model', function(t){

    t.plan(4);

    var percy = createTestPercy(),
        testData = {};

    percy.set('abc', testData, function(error, model){
        t.notOk(error, 'no error');
        t.equal(model, testData, 'model returned');

        percy.set('abc', testData, function(error, model){
            t.notOk(error, 'no error');
            t.equal(model, testData, 'model returned');
        });
    });
});

test('get model', function(t){

    t.plan(4);

    var percy = createTestPercy(),
        testData = {};

    percy.set('abc', testData, function(error, model){
        t.notOk(error, 'no error');
        t.equal(model, testData, 'model returned');

        percy.get('abc', function(error, model){
            t.notOk(error, 'no error');
            t.equal(model, testData, 'model returned');
        });
    });
});

test('cannot get nonexistant model', function(t){

    t.plan(1);

    var percy = createTestPercy();

    percy.get('abc', function(error){
        t.equal(error, 'thing:abc not in db', 'got correct error');
    });
});

test('add model', function(t){

    t.plan(2);

    var percy = createTestPercy(),
        testData = {};

    percy.add(testData, function(error, model){
        t.notOk(error, 'no error');
        t.equal(model, testData, 'model returned');
    });
});

test('cannot double-add model', function(t){

    t.plan(3);

    var percy = createTestPercy(),
        testData = {};

    percy.add(testData, function(error, model){
        t.notOk(error, 'no error');
        t.equal(model, testData, 'model returned');

        percy.add(model, function(error){
            t.equal(error, 'object already has an id', 'correct error');
        });
    });
});

test('remove model', function(t){

    t.plan(3);

    var percy = createTestPercy(),
        testData = {};

    percy.add(testData, function(error, model){
        t.notOk(error, 'no error');
        t.equal(model, testData, 'model returned');

        percy.remove(model.id, function(error){
            t.notOk(error, 'no error');
        });
    });
});

test('cannot remove nonexistant', function(t){

    t.plan(1);

    var percy = createTestPercy();

    percy.remove('abc', function(error){
        t.equal(error, 'thing:abc not in db', 'error thrown as expected');
    });
});

test('replace model', function(t){

    t.plan(2);

    var percy = createTestPercy();

    percy.add({a:1}, function(error, model){
        t.pass('model added');

        percy.replace(model.id, {b:2}, function(error, model){
            t.deepEqual(model, {b:2}, 'replace succeded');
        });
    });
});

test('cannot replace nonexistant', function(t){

    t.plan(1);

    var percy = createTestPercy();

    percy.replace('abc', {b:2}, function(error){
        t.equal(error, 'thing:abc not in db', 'error thrown as expected');
    });
});

test('update model', function(t){

    t.plan(2);

    var percy = createTestPercy();

    percy.add({a:1}, function(error, model){
        t.pass('model added');

        percy.update(model.id, {b:2}, function(error, model){
            // remove the id for comparison
            delete model.id;

            t.deepEqual(model, {a:1, b:2}, 'update succeded');
        });
    });
});

test('cannot update nonexistant', function(t){

    t.plan(1);

    var percy = createTestPercy();

    percy.update('abc', {b:2}, function(error){
        t.equal(error, 'thing:abc not in db', 'error thrown as expected');
    });
});

test('handels error from createKey', function(t){

    t.plan(2);

    var percy = createTestPercy(),
        testError = new Error('BOOM!!');

    percy.createId = function(callback){
        callback(testError);
    };

    percy.add({foo: 'bar'}, function(error){
        t.ok(error, 'error passed as expected');
        t.equal(error, testError, 'correct error passed');
    });
});

test('handels valid key from createKey', function(t){
    t.plan(3);

    var percy = createTestPercy(),
        expectedResult = {
            foo: 'bar',
            id: 1
        },
        testkeys = 0;

    percy.createId = function(callback){
        callback(null, ++testkeys);
    };

    percy.add({foo: 'bar'}, function(error, result){
        t.notOk(error, 'no error as expected');
        t.ok(result, 'result passed as expected');
        t.deepEqual(result, expectedResult, 'correct error passed');
    });
});

test('createKey callsback only once with correct id', function(t){
    t.plan(3);

    var percy = createTestPercy(),
        testId = 1234567890;

    percy.createId = function(callback){
        callback(null, testId);
    };

    percy.createKey(null, {foo: 'bar'}, function(error, result){
        t.notOk(error, 'no error as expected');
        t.ok(result, 'result passed as expected');
        t.equal(result, 'thing:' + testId, 'result is correct id');
    });
});

test('createKey sets id on data', function(t){
    t.plan(4);

    var percy = createTestPercy(),
        testData = {foo: 'bar'},
        testId = 1234567890;

    percy.createId = function(callback){
        callback(null, testId);
    };

    percy.createKey(null, testData, function(error, result){
        t.notOk(error, 'no error as expected');
        t.ok(result, 'result passed as expected');
        t.equal(result, 'thing:' + testId, 'result is correct id');
        t.equal(testData.id, testId, 'data.id is correct id');
    });
});

test('createKey dosent go bang if data is null', function(t){
    t.plan(3);

    var percy = createTestPercy(),
        testData = null,
        testId = 1234567890;

    percy.createId = function(callback){
        callback(null, testId);
    };

    percy.createKey(null, testData, function(error, result){
        t.notOk(error, 'no error as expected');
        t.ok(result, 'result passed as expected');
        t.equal(result, 'thing:' + testId, 'result is correct id');
    });
});