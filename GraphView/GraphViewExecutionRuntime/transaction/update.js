/*
  Insert Edge Transaction
*/

function updateSproc(id, update, update2) {
    var collection = getContext().getCollection();
    var collectionLink = collection.getSelfLink();
    var response = getContext().getResponse();
    //response.setBody(id);
    // Validate input.
    if (!id) throw new Error("The id is undefined or null.");
    if (!update) throw new Error("The update is undefined or null.");
    //throw new Error("Document not found.");
    tryQueryAndUpdate();

    // Recursively queries for a document by id w/ support for continuation tokens.
    // Calls tryUpdate(document) as soon as the query returns a document.
    function tryQueryAndUpdate(continuation) {
        var query = {query: "select * from root r where r.id = @id", parameters: [{name: "@id", value: id}]};
        var requestOptions = {continuation: continuation};

        var isAccepted = collection.queryDocuments(collectionLink, query, requestOptions, function (err, documents, responseOptions) {
            if (err) throw err;

            if (documents.length > 0) {
                // If the document is found, update it.
                // There is no need to check for a continuation token since we are querying for a single document.
                tryUpdate(documents[0]);
            } else if (responseOptions.continuation) {
                // Else if the query came back empty, but with a continuation token; repeat the query w/ the token.
                // It is highly unlikely for this to happen when performing a query by id; but is included to serve as an example for larger queries.

                tryQueryAndUpdate(responseOptions.continuation);
            } else {
                // Else a document with the given id does not exist..
                throw new Error("Document not found.");
            }
        });

        // If we hit execution bounds - throw an exception.
        // This is highly unlikely given that this is a query by id; but is included to serve as an example for larger queries.
        if (!isAccepted) {
            throw new Error("The stored procedure timed out.");
        }
    }

    // Updates the supplied document according to the update object passed in to the sproc.
    function tryUpdate(document) {
        // DocumentDB supports optimistic concurrency control via HTTP ETag.
        var requestOptions = {etag: document._etag};
        console.log("execute the stored procedure");
        // Update operators.
        // (1) inc the next offset
        //inc(document, update);
        incOne(document, update);
        //mul(document, update);
        //rename(document, update);
        //set(document, update);
        //unset(document, update);
        //min(document, update);
        //max(document, update);
        //currentDate(document, update);
        // (2) update the edge object next offset
        //if (update.$inc._nextEdgeOffset != null) update2.$addToSet._edge._ID = update.$inc._nextEdgeOffset - 1;
        //if (update.$inc._nextReverseEdgeOffset != null) update2.$addToSet._reverse_edge._reverse_ID = update.$inc._nextReverseEdgeOffset - 1;

        if (update.$inc["_nextEdgeOffset"] != null) {
            update2.$addToSet["_edge"]["_ID"] = document["_nextEdgeOffset"] - 1;
            update2.$addToSet["_edge"]["_reverse_ID"] = 0;
        }
        if (update.$inc["_nextReverseEdgeOffset"] != null) {
            update2.$addToSet["_reverse_edge"]["_reverse_ID"] = update.$inc["_nextReverseEdgeOffset"];
            update2.$addToSet["_reverse_edge"]["_ID"] = 0;
        }
        // (3) update the vertex edge property
        addToSet(document, update2);
        // pop(document, update);
        //push(document, update);
        // Update the document.
        //throw new Error(document);
        var isAccepted = collection.replaceDocument(document._self, document, requestOptions, function (err, updatedDocument, responseOptions) {
            if (err) throw err;

            // If we have successfully updated the document - return it in the response body.
            response.setBody(update2);
            //response.setBody(id);

        });

        // If we hit execution bounds - throw an exception.
        if (!isAccepted) {
            throw new Error("The stored procedure timed out.");
        }
    }

    function incOne(document, update) {
        var fields, i;
        //throw new Error(update.m_StringValue.$inc)

        if (update.$inc) {
            //throw new Error("Enter")

            fields = Object.keys(update.$inc);
            for (i = 0; i < fields.length; i++) {
                if (isNaN(update.$inc[fields[i]])) {
                    // Validate the field; throw an exception if it is not a number (can't increment by NaN).
                    throw new Error("Bad $inc parameter - 1value must be a number")
                } else if (document[fields[i]]) {
                    // If the field exists, increment it by the given amount.
                    document[fields[i]] += 1;
                    //throw new Error("Bad Flag")

                    //throw new Error("Bad $inc parameter - 2value must be a number")
                } else {
                    // Otherwise set the field to the given amount.
                    document[fields[i]] = 1;

                    //throw new Error("Bad $inc parameter - 3value must be a number")
                }
            }
        }
    }

    // Operator implementations.
    // The $inc operator increments the value of a field by a specified amount.
    function inc(document, update) {
        var fields, i;
        //throw new Error(update.m_StringValue.$inc)

        if (update.$inc) {
            //throw new Error("Enter")

            fields = Object.keys(update.$inc);
            for (i = 0; i < fields.length; i++) {
                if (isNaN(update.$inc[fields[i]])) {
                    // Validate the field; throw an exception if it is not a number (can't increment by NaN).
                    throw new Error("Bad $inc parameter - 1value must be a number")
                } else if (document[fields[i]]) {
                    // If the field exists, increment it by the given amount.
                    document[fields[i]] += update.$inc[fields[i]];
                    //throw new Error("Bad Flag")

                    //throw new Error("Bad $inc parameter - 2value must be a number")
                } else {
                    // Otherwise set the field to the given amount.
                    document[fields[i]] = update.$inc[fields[i]];

                    //throw new Error("Bad $inc parameter - 3value must be a number")
                }
            }
        }
    }

    // The $mul operator multiplies the value of the field by the specified amount.
    function mul(document, update) {
        var fields, i;

        if (update.$mul) {
            fields = Object.keys(update.$mul);
            for (i = 0; i < fields.length; i++) {
                if (isNaN(update.$mul[fields[i]])) {
                    // Validate the field; throw an exception if it is not a number (can't multiply by NaN).
                    throw new Error("Bad $mul parameter - value must be a number")
                } else if (document[fields[i]]) {
                    // If the field exists, multiply it by the given amount.
                    document[fields[i]] *= update.$mul[fields[i]];
                } else {
                    // Otherwise set the field to 0.
                    document[fields[i]] = 0;
                }
            }
        }
    }

    // The $rename operator renames a field.
    function rename(document, update) {
        var fields, i, existingFieldName, newFieldName;

        if (update.$rename) {
            fields = Object.keys(update.$rename);
            for (i = 0; i < fields.length; i++) {
                existingFieldName = fields[i];
                newFieldName = update.$rename[fields[i]];

                if (existingFieldName == newFieldName) {
                    throw new Error("Bad $rename parameter: The new field name must differ from the existing field name.")
                } else if (document[existingFieldName]) {
                    // If the field exists, set/overwrite the new field name and unset the existing field name.
                    document[newFieldName] = document[existingFieldName];
                    delete document[existingFieldName];
                } else {
                    // Otherwise this is a noop.
                }
            }
        }
    }

    // The $set operator sets the value of a field.
    function set(document, update) {
        var fields, i;

        if (update.$set) {
            fields = Object.keys(update.$set);
            for (i = 0; i < fields.length; i++) {
                document[fields[i]] = update.$set[fields[i]];
            }
        }
    }

    // The $unset operator removes the specified field.
    function unset(document, update) {
        var fields, i;

        if (update.$unset) {
            fields = Object.keys(update.$unset);
            for (i = 0; i < fields.length; i++) {
                delete document[fields[i]];
            }
        }
    }

    // The $min operator only updates the field if the specified value is less than the existing field value.
    function min(document, update) {
        var fields, i;

        if (update.$min) {
            fields = Object.keys(update.$min);
            for (i = 0; i < fields.length; i++) {
                if (update.$min[fields[i]] < document[fields[i]]) {
                    document[fields[i]] = update.$min[fields[i]];
                }
            }
        }
    }

    // The $max operator only updates the field if the specified value is greater than the existing field value.
    function max(document, update) {
        var fields, i;

        if (update.$max) {
            fields = Object.keys(update.$max);
            for (i = 0; i < fields.length; i++) {
                if (update.$max[fields[i]] > document[fields[i]]) {
                    document[fields[i]] = update.$max[fields[i]];
                }
            }
        }
    }

    // The $currentDate operator sets the value of a field to current date as a POSIX epoch.
    function currentDate(document, update) {
        var currentDate = new Date();
        var fields, i;

        if (update.$currentDate) {
            fields = Object.keys(update.$currentDate);
            for (i = 0; i < fields.length; i++) {
                // ECMAScript's Date.getTime() returns milliseconds, where as POSIX epoch are in seconds.
                document[fields[i]] = Math.round(currentDate.getTime() / 1000);
            }
        }
    }

    // The $addToSet operator adds elements to an array only if they do not already exist in the set.
    function addToSet(document, update) {
        var fields, i;

        if (update.$addToSet) {
            fields = Object.keys(update.$addToSet);

            for (i = 0; i < fields.length; i++) {
                if (!Array.isArray(document[fields[i]])) {
                    // Validate the document field; throw an exception if it is not an array.
                    throw new Error("Bad $addToSet parameter - field in document must be an array.")
                } else if (document[fields[i]].indexOf(update.$addToSet[fields[i]]) === -1) {
                    // Add the element if it doesn't already exist in the array.
                    document[fields[i]].push(update.$addToSet[fields[i]]);
                }
            }
        }
    }

    // The $pop operator removes the first or last item of an array.
    // Pass $pop a value of -1 to remove the first element of an array and 1 to remove the last element in an array.
    function pop(document, update) {
        var fields, i;

        if (update.$pop) {
            fields = Object.keys(update.$pop);

            for (i = 0; i < fields.length; i++) {
                if (!Array.isArray(document[fields[i]])) {
                    // Validate the document field; throw an exception if it is not an array.
                    throw new Error("Bad $pop parameter - field in document must be an array.")
                } else if (update.$pop[fields[i]] < 0) {
                    // Remove the first element from the array if it's less than 0 (be flexible).
                    document[fields[i]].shift();
                } else {
                    // Otherwise, remove the last element from the array (have 0 default to javascript's pop()).
                    document[fields[i]].pop();
                }
            }
        }
    }

    // The $push operator adds an item to an array.
    function push(document, update) {
        var fields, i;

        if (update.$push) {
            fields = Object.keys(update.$push);

            for (i = 0; i < fields.length; i++) {
                if (!Array.isArray(document[fields[i]])) {
                    // Validate the document field; throw an exception if it is not an array.
                    throw new Error("Bad $push parameter - field in document must be an array.")
                } else {
                    // Push the element in to the array.
                    document[fields[i]].push(update.$push[fields[i]]);
                }
            }
        }
    }
}
