var test = require('grape'),
    Percy = require('../');

function mockConnector(callback){
    callback(null, {
        get: function(key, callback){
            callback(null, {});
        },
        set: function(key, model, callback){
            callback(null, model);
        },
    });
};

mockValidator = {
    validate: function(callback){
        callback(null, true);
    }
};

test('create Model', function(t){

    t.plan(2);

    var percy = new Percy(mockConnector, mockValidator);

    percy.get('abc', function(error, model){
        t.pass('Got from db');
    });

    percy.set('abc', {}, function(error, model){
        t.pass('set to db');
    });
});