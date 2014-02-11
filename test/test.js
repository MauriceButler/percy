var test = require('grape'),
    Cborm = require('../');

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

test('create Model', function(t){

    t.plan(2);

    var cborm = new Cborm(mockConnector, {});

    cborm.get('abc', function(error, model){
        t.pass('Got from db');
    });

    cborm.set('abc', {}, function(error, model){
        t.pass('set to db');
    });
});

test('validation works', function(t){

    t.plan(2);

    var cborm = new Cborm(mockConnector, {type:'number'});

    cborm.set('abc', 123, function(error, model){
        if(error){
            t.fail('Should have validated');
        }else{
            t.pass();
        }
    });

    cborm.set('abc', {}, function(error, model){
        if(error){
            t.pass(error);
        }
    });
});