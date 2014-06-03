#percy

A persistance layer that plays nice with Couchbase via [Couchector](https://www.npmjs.org/package/couchector)

##Usage
    var Percy = require('percy'),
        connection, // connection object such as a Couchector (https://www.npmjs.org/package/couchector)
        validator, // object that has a validate function with a signature of function(model, callback)
        myCoolIdGenerator; // returns a unique id for this entity

    var persistance = new Percy('user', connection, validator);

    // createId must be set so Percy knows how to generate unique ids
    persistance.createId = function(callback){
        callback(null, myCoolIdGenerator());
    };

    persistance.add({ userName: 'bob' }, function(error, user){
        if(error){
            console.log(error);
            return;
        }

        console.log(user); // { id: 'myCoolId', userName: 'bob'}
    });


    // Results in the following db record

    KEY                 VALUE
    user:myCoolId       {
                            id: 'myCoolId'
                            userName: 'bob'
                        }