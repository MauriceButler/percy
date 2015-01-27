var kgo = require('kgo'),
    map = require('map');

function createKey(id, data, callback){
    var percy = this;

    if(id == null){
        if(data && 'id' in data){
            return callback('object already has an id');
        }

        percy.createId(function(error, id){
            if(error){
                return callback(error);
            }

            if(data){
                data.id = id;
            }

            callback(null, percy.entityType + ':' + id);
        });

        return;
    }

    callback(null, percy.entityType + ':' + id);
}

function createKeys(ids, callback){
    var percy = this;

    kgo
    ({
        ids: ids
    })
    ('keys', ['ids'], map.bind(null, function(id, done){
        percy.createKey(id, true, done);
    }))
    (['keys'], callback.bind(null, null))
    .on('error', function(error){
        callback(error);
    });
}

function createId(){
    throw new Error('Not Implemented');
}

function get(id, callback) {
    var percy = this;

    kgo
    ('entityKey', percy.createKey.bind(percy, id, null))
    (['entityKey'], function(entityKey){
        percy.bucket.get(entityKey, function(error, result){
            // Error code 13 is No such keys
            if(error && error.code !== 13){
                return callback(error);
            }

            callback(null, result && result.value);
        });
    })
    .on('error', callback);
}

function getMulti(ids, options, callback) {
    var percy = this;

    if(!callback && typeof options === 'function'){
        callback = options;
        options = {};
    }

    if(!ids || !ids.length || !Array.isArray(ids)){
        return callback(null, []);
    }

    kgo
    ({
        ids: ids,
        options: options
    })
    ('keys', ['ids'], createKeys.bind(percy))
    ('results', ['keys', 'options'], function(keys, options, done){
        if(!keys || !keys.length){
            return done();
        }

        percy.bucket.getMulti(keys, done);
    })
    (['results'], function(results){
        var actualResults = [];

        for(var key in results){
            actualResults.push(results[key].value);
        }

        callback(null, actualResults);
    })
    .on('error', function(error){
        callback(error);
    });
}

function set(id, data, callback) {
    var percy = this;

    kgo
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    ('entityKey', ['model'], percy.createKey.bind(percy, id))
    (['entityKey', 'model'], function(entityKey, model){
        percy.bucket.upsert(entityKey, model, function(error){
            if(error){
                return callback(error);
            }

            callback(null, model);
        });
    })
    .on('error', callback);
}

function add(data, callback){
    var percy = this;

    kgo
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    ('entityKey', ['model'], percy.createKey.bind(percy, null))
    (['entityKey', 'model'], function(entityKey, model){
        percy.bucket.insert(entityKey, model, function(error){
            if(error){
                return callback(error);
            }

            callback(null, model);
        });
    })
    .on('error', callback);
}

function replace(id, data, callback){
    var percy = this;

    kgo
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    ('entityKey', ['model'], percy.createKey.bind(percy, id))
    (['entityKey', 'model'], function(entityKey, model){
        percy.bucket.replace(entityKey, model, function(error){
            callback(error, error ? null : model);
        });
    })
    .on('error', callback);
}

function update(id, data, callback){
    var percy = this;

    percy.get(id, function(error, model){
        if(error){
            return callback(error);
        }
        if(!model){
            return callback(new Error('No such key'));
        }

        for(var property in data){
            model[property] = data[property];
        }
        percy.replace(id, model, callback);
    });
}

function remove(id, callback){
    var percy = this;

    kgo
    ('entityKey', percy.createKey.bind(percy, id, null))
    (['entityKey'], function(entityKey){
        percy.bucket.remove(entityKey, function(error){
            callback(error);
        });
    })
    .on('error', callback);
}

function touch(id, expiry, options, callback){
    var percy = this;

    kgo
    ('entityKey', percy.createKey.bind(percy, id, null))
    (['entityKey'], function(entityKey){
        percy.bucket.touch(entityKey, expiry, options, callback);
    })
    .on('error', callback);
}

function Percy(entityType, bucket, validator){
    this.entityType = entityType;
    this.bucket = bucket;
    this.validator = validator;
}

Percy.prototype.get = get;
Percy.prototype.set = set;
Percy.prototype.add = add;
Percy.prototype.replace = replace;
Percy.prototype.update = update;
Percy.prototype.remove = remove;
Percy.prototype.touch = touch;
Percy.prototype.createKey = createKey;
Percy.prototype.createId = createId;
Percy.prototype.getMulti = getMulti;

module.exports = Percy;
