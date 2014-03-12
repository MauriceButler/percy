var kgo = require('kgo'),
    arrayProto = [];

function get(key, callback) {
    var entityKey = this.createKey(key);

    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.get(entityKey, function(error, result){
            // Error code 13 is No such keys
            if(error && error.code !== 13){
                return callback(error);
            }

            callback(null, result && result.value);
        });
    });
}

function set(key, data, callback) {
    var percy = this,
        entityKey = this.createKey(key);

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    (['bucket', 'model'], function(bucket, model){
        bucket.set(entityKey, model, function(error, result){
            if(error){
                return callback(error);
            }
            callback(null, model);
        });
    }).errors({
        bucket: callback,
        model: callback
    });
}

function add(key, data, callback){
    var percy = this,
        entityKey = this.createKey(key);

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    (['bucket', 'model'], function(bucket, model){
        bucket.add(entityKey, model, function(error, result){
            if(error){
                return callback(error);
            }
            callback(null, model);
        });
    }).errors({
        bucket: callback,
        model: callback
    });
}

function replace(key, data, callback){
    var percy = this,
        entityKey = this.createKey(key);

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    (['bucket', 'model'], function(bucket, model){
        bucket.replace(entityKey, model, function(error, result){
            if(error){
                return callback(error);
            }
            callback(null, model);
        });
    }).errors({
        bucket: callback,
        model: callback
    });
}

function update(key, data, callback){
    var percy = this;

    this.get(key, function(error, model){
        if(error){
            return callback(error);
        }
        for(var property in data){
            model[property] = data[property];
        }
        percy.replace(key, model, callback);
    });
}

function remove(key, callback){
    var entityKey = this.createKey(key);

    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.remove(entityKey, function(error, result){
            if(error){
                return callback(error);
            }
            callback(null);
        });
    });
}

function touch(key, options, callback){
    var entityKey = this.createKey(key);

    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.touch(entityKey, options, callback);
    });
}

function exists(key, callback){
    this.touch(key, null, function(error){
        var exists = true;
        if(error){
            if(error.message === 'No such key'){
                exists = false;
            }else{
                callback(error);
            }
        }
        callback(null, exists);
    });
}

function createKey(){
    var keyParts = arrayProto.slice.call(arguments);
    keyParts.unshift(this.entityType);
    return keyParts.join(':');
}

function Percy(entityType, connector, validator){
    this.entityType = entityType
    this.connector = connector;
    this.validator = validator;
}
Percy.prototype.get = get;
Percy.prototype.set = set;
Percy.prototype.add = add;
Percy.prototype.replace = replace;
Percy.prototype.update = update;
Percy.prototype.remove = remove;
Percy.prototype.touch = touch;
Percy.prototype.exists = exists;
Percy.prototype.createKey = createKey;

module.exports = Percy;
