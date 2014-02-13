var kgo = require('kgo');

function get(key, callback) {
    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.get(key, function(error, result){
            if(error){
                return callback(error);
            }
            callback(null, result.value);
        });
    });
}

function set(key, data, callback) {
    var percy = this;

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    (['bucket', 'model'], function(bucket, model){
        bucket.set(key, model, function(error, result){
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
    var percy = this;

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    (['bucket', 'model'], function(bucket, model){
        bucket.add(key, model, function(error, result){
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
    var percy = this;

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        percy.validator.validate(data, done);
    })
    (['bucket', 'model'], function(bucket, model){
        bucket.replace(key, model, function(error, result){
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

function remove(key, callback){
    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.remove(key, function(error, result){
            if(error){
                return callback(error);
            }
            callback(null);
        });
    });
}

function touch(key, options, callback){
    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.touch(key, options, callback);
    });
}

function exists(key, callback){
    this.touch(key, null, function(error, result){
        if(error){
            return callback(error);
        }
        callback(null, !!result);
    });
}

function Percy(connector, validator){
    this.connector = connector;
    this.validator = validator;
}
Percy.prototype.get = get;
Percy.prototype.set = set;
Percy.prototype.add = add;
Percy.prototype.replace = replace;
Percy.prototype.remove = remove;
Percy.prototype.touch = touch;
Percy.prototype.exists = exists;

module.exports = Percy;