#percy

A persistance layer that plays nice with Couchbase via [Couchector](https://www.npmjs.org/package/couchector)

##Usage
    var Percy = require('percy'),
        connection, // connection object such as a Couchector (https://www.npmjs.org/package/couchector)
        validator; // object that has a validate function with a signature of function(model, callback)


    var persistance = new Percy('user', connection, validator);

    persistance.add('1234', { userName: 'bob' }, function(error, user){
        if(error){
            console.log(error);
            return;
        }

        console.log(user);
    });