var kgo = require('kgo');

function get(key, callback) {
    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.get(key, callback);
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
        bucket.set(key, model, callback);
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
        bucket.add(key, model, callback);
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
        bucket.replace(key, model, callback);
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
        bucket.remove(key, callback);
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

module.exports = Percy;