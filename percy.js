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
    var model = this;

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        model.validator.validate(data, done);
    })
    (['bucket', 'model'], function(bucket, model){
        bucket.set(key, model, callback);
    }).errors({
        bucket: callback,
        model: callback
    });
}

function Model(connector, validator){
    this.connector = connector;
    this.validator = validator;
}
Model.prototype.get = get;
Model.prototype.set = set;

module.exports = Model;