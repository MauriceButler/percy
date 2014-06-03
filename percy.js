var kgo = require('kgo'),
    arrayProto = [];

function get(id, callback) {
    var percy = this;

    kgo
    ('bucket', this.connector)
    ('entityKey', this.createKey.bind(this, id, null))
    (['entityKey', 'bucket'], function(entityKey, bucket){
        bucket.get(entityKey, function(error, result){
            // Error code 13 is No such keys
            if(error && error.code !== 13){
                return callback(error);
            }

            callback(null, result && result.value);
        });
    })
    .on('error', callback);
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

function set(id, data, callback) {
    var percy = this;

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    ('entityKey', ['model'], this.createKey.bind(this, id))
    (['entityKey', 'bucket', 'model'], function(entityKey, bucket, model){
        bucket.set(entityKey, model, function(error, result){
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
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    ('entityKey', ['model'], this.createKey.bind(this, null))
    (['entityKey', 'bucket', 'model'], function(entityKey, bucket, model){
        bucket.add(entityKey, model, function(error, result){
            callback(error, error ? null : model);
        });
    })
    .on('error', callback);
}

function replace(id, data, callback){
    var percy = this;

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    ('entityKey', ['model'], this.createKey.bind(this, id))
    (['entityKey', 'bucket', 'model'], function(entityKey, bucket, model){
        bucket.replace(entityKey, model, function(error, result){
            callback(error, error ? null : model);
        });
    })
    .on('error', callback);
}

function update(id, data, callback){
    var percy = this;

    this.get(id, function(error, model){
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
    kgo
    ('bucket', this.connector)
    ('entityKey', this.createKey.bind(this, id, null))
    (['entityKey', 'bucket'], function(entityKey, bucket){
        bucket.remove(entityKey, function(error, result){
            callback(error);
        });
    })
    .on('error', callback);
}

function touch(id, options, callback){
    kgo
    ('bucket', this.connector)
    ('entityKey', this.createKey.bind(this, id, null))
    (['entityKey', 'bucket'], function(entityKey, bucket){
        if(error){
            return callback(error);
        }
        bucket.touch(entityKey, options, callback);
    })
    .on('error', callback);
}

function exists(id, callback){
    this.touch(id, null, function(error){
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

function createKey(id, data, callback){
    var percy = this;

    if(id == null){
        if(data && 'id' in data){
            return callback("object already has an ID");
        }
        percy.createId(function(error, id){
            if(error){
                return callback(error);
            }

            data.id = id;
            callback(null, percy.entityType + ':' + id);
        });
    }

    callback(null, percy.entityType + ':' + id);
}

function createId(callback){
    throw new Error("Not Implemented");
}

function getView(viewName, callback){
    var percy = this;

    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        callback(null, bucket.view(percy.entityType, viewName));
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
Percy.prototype.createId = createId;
Percy.prototype.getView = getView;
Percy.prototype.getMulti = getMulti;
Percy.prototype.increment = increment;

module.exports = Percy;
