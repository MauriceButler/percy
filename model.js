var kgo = require('kgo'),
    validate = require('jsonschema').validate;

function validateModel(model, callback) {
    var validation = this.validator(model);

    if(validation.errors && validation.errors.length){
        return callback(validation.errors);
    }

    callback(null, model);
};

function get(key, callback) {
    this.connector(function(error, bucket){
        if(error){
            return callback(error);
        }
        bucket.get(key, callback);
    });
};

function set(key, data, callback) {
    var model = this;

    kgo
    ('bucket', this.connector)
    ('model', function(done){
        model.validate(data, done);
    })
    (['bucket', 'model'], function(bucket, model){
        bucket.set(key, model, callback);
    }).errors({
        bucket: callback,
        model: callback
    });
};

function Model(connector, schema){
    this.connector = connector;
    this.validator = function(data){
        return validate(data, schema);
    }
};
Model.prototype.get = get;
Model.prototype.set = set;
Model.prototype.validate = validateModel;

module.exports = Model;