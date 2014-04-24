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

function getMulti(keys, options, callback) {

    if(!callback && typeof options === 'function'){
        callback = options;
        options = {};
    }

    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.getMulti(keys, options, function(errors, results){
            if(errors){
                return callback(error);
            }

            var actualResults = [];
            for(var key in results){
                actualResults.push(results[key].value);
            }

            callback(null, actualResults);
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
        if(!model){
            return callback(new Error('No such key'));
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

function createKey(key){
    if(!Array.isArray(key)){
        key = [key];
    }
    key.unshift(this.entityType);
    return key.join(':');
}

function getView(viewName, callback){
    var percy = this;

    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        callback(null, bucket.view(percy.designName, viewName));
    });
}

function increment(key, options, callback){
    if(typeof options === 'function'){
        callback = options;
        options = {initial: 0, offset: 1};
    }

    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }

        bucket.incr(key, options, callback);
    });
}

function Percy(entityType, connector, validator){
    this.entityType = entityType;
    this.designName = entityType;
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
Percy.prototype.getView = getView;
Percy.prototype.getMulti = getMulti;
Percy.prototype.increment = increment;

module.exports = Percy;
