var test = require('grape'),
    Percy = require('../');

function createMockConnector(){
    var db = {};
    return function(callback){
        callback(null, {
            get: function(key, callback){
                if(!(key in db)){
                    return callback(true);
                }
                callback(null, db[key]);
            },
            set: function(key, model, callback){
                db[key] = model;
                callback(null, model);
            },
            add: function(key, model, callback){
                if(key in db){
                    return callback(true);
                }
                db[key] = model;
                callback(null, model);
            },
            remove: function(key, callback){
                if(!(key in db)){
                    return callback(true);
                }
                delete db[key];
                callback(null);
            },
            replace: function(key, model, callback){
                if(!(key in db)){
                    return callback(true);
                }
                db[key] = model;
                callback(null, model);
            }
        });
    };
}

function createMockValidator(){
    return {
        validate: function(data, callback){
            callback(null, true);
        }
    };
}

test('create percy', function(t){

    t.plan(1);

    var percy = new Percy(createMockConnector(), createMockValidator());

    t.pass('Percy was created');
});

test('set model', function(t){

    t.plan(1);

    var percy = new Percy(createMockConnector(), createMockValidator());

    percy.set('abc', {}, function(error, model){
        t.pass('model added');
    });
});

test('can double set model', function(t){

    t.plan(2);

    var percy = new Percy(createMockConnector(), createMockValidator());

    percy.set('abc', {}, function(error, model){
        t.pass('model added');

        percy.set('abc', {}, function(error, model){
            t.pass('model added again');
        });
    });
});

test('get model', function(t){

    t.plan(2);

    var percy = new Percy(createMockConnector(), createMockValidator());

    percy.set('abc', {}, function(error, model){
        t.pass('model added');

        percy.get('abc', function(error, model){
            t.ok(model, 'got model');
        });
    });
});

test('cannot get nonexistant model', function(t){

    t.plan(1);

    var percy = new Percy(createMockConnector(), createMockValidator());

    percy.get('abc', function(error, model){
        t.ok(error, 'error thrown as expected');
    });
});

test('add model', function(t){

    t.plan(1);

    var percy = new Percy(createMockConnector(), createMockValidator());

    percy.add('abc', {}, function(error, model){
        t.pass('model added');
    });
});

test('cannot double-add model', function(t){

    t.plan(2);

    var percy = new Percy(createMockConnector(), createMockValidator());

    percy.add('abc', {}, function(error, model){
        t.pass('model added');
    });

    percy.add('abc', {}, function(error, model){
        t.ok(error, 'error thrown as expected');
    });
});

test('remove model', function(t){

    t.plan(2);

    var percy = new Percy(createMockConnector(), createMockValidator());

    percy.add('abc', {}, function(error, model){
        t.pass('model added');

        percy.remove('abc', function(error, model){
            t.equal(error, null, 'model removed');
        });
    });
});

test('cannot remove nonexistant', function(t){

    t.plan(1);

    var percy = new Percy(createMockConnector(), createMockValidator());

    percy.remove('abc', function(error, model){
        t.ok(error, 'error thrown as expected');
    });
});