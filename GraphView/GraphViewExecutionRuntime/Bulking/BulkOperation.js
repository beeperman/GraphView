
function BulkOperation(opArray, etagsObject) {

    "use strict";

    // Access all database operations - CRUD, query against documents in the current collection
    var collection = getContext().getCollection();
    var collectionLink = collection.getSelfLink();
    // Access HTTP request body and headers for the procedure
    var request = getContext().getRequest();
    // Access HTTP response body and headers from the procedure
    var response = getContext().getResponse();


    // Constants
    var DUMMY = null;


    /**
     * Description:
     *   This part provides basic functions
     *
     * Export variables:
     *   collection, request, response
     *
     * Export functions:
     *   ASSERT(condition, [message])
     *   ERROR(e, [message])
     *   SUCCESS([content])
     */
    
    var Status = { // REQUIRES: errCode < 0
        Success: 0,
        DBError: -1,
        NotAccepted: -2, // usually timeout
        AssertionFailed: -3,
        InternalError: -4
    };
    Object.freeze(Status);

    var __respObject = {
        Status: Status.Success,
        Message: "",
        Debug: "",
        DocDBErrorCode: null,
        DocDBErrorMessage: null,
        Content: new Array(opArray.length),
        Etags: { }
    };
    Object.preventExtensions(__respObject);


    function UPDATE_ETAG(documentId, documentEtag) {
        ASSERT(typeof documentId === "string" && documentId, documentId);
        ASSERT(documentEtag == null || (typeof documentEtag === "string" && documentEtag, documentEtag));
        //DEBUG("Update Etag: '" + documentId + "' = '" + documentEtag + "'");
        __respObject.Etags[documentId] = documentEtag;
    }

    function GET_ETAG(documentId) {
        ASSERT(typeof documentId === "string" && documentId);
        ASSERT(typeof __respObject.Etags[documentId] !== "undefined");

        if (__respObject.Etags[documentId] === null) {
            return null;
        } else {
            ASSERT(typeof __respObject.Etags[documentId] === "string", 
                documentId + "'s etag is: " + typeof (__respObject.Etags[documentId]));
            return __respObject.Etags[documentId];
        }
    }


    function DEBUG(message) {
        if (typeof message === "undefined" || message === null) {
            return;
        }
        else if (typeof message === "object") {
            __respObject.Debug = __respObject.Debug + JSON.stringify(message) + "\r\n";
        } else {
            __respObject.Debug = __respObject.Debug + message.toString() + "\r\n";
        }
    }

    /**
     * Abort the procedure (and throw Error) and report the error
     * 
     * @param {string|object} e - docDBErrorObject or `Status`
     * @param {string|object|undefined} message - anything, the message
     * @returns {} - will not return!
     * @example
     *      ERROR(docDBErrorObject);
     *      ERROR(docDBErrorObject, myMessage);
     *      ERROR(myErrorStatus, myMessage);
     */
    function ERROR(e, message) {
        // __respObject.Status, __respObject.DocDBErrorXxx
        if (typeof (e) === "number") {
            ASSERT(e !== Status.Success, "[ERROR] Should not pass Status.Success to ERROR() function.");
            __respObject.Status = e;
            __respObject.DocDBErrorCode = null;
            __respObject.DocDBErrorMessage = null;
        }
        else {
            __respObject.Status = Status.DBError;
            __respObject.DocDBErrorCode = e["number"];
            __respObject.DocDBErrorMessage = e["body"];
        }

        // __respObject.Message
        if (typeof (message) === "undefined") {
            __respObject.Message = "No message";
        }
        else if (typeof (message) === "object") {
            __respObject.Message = JSON.stringify(message);
        }
        else {
            __respObject.Message = message.toString();
        }

        // __respObject.Content, __respObject.Etags
        __respObject.Content = new Array(opArray.length);
        __respObject.Etags = {};


        response.setBody(JSON.stringify(__respObject));
        throw new Error(JSON.stringify(__respObject));
    }


    /**
     * Note one operation is successful and set the response content
     * @param {} index 
     * @param {} content 
     * @returns {} 
     */
    function SUCCESS(index, content) {
        ASSERT(__respObject.Content.length === opArray.length);
        ASSERT(index >= 0 && index < opArray.length);

        if (typeof (content) === "undefined" || content === null) {
            __respObject.Content[index] = new Object();
        } else {
            ASSERT(typeof content === "object");
            __respObject.Content[index] = content;
        }
    }

    function DONE() {
        __respObject.Status = Status.Success;
        __respObject.Message = "OK";
        __respObject.DocDBErrorCode = null;
        __respObject.DocDBErrorMessage = null;

        response.setBody(JSON.stringify(__respObject));
    }

    /**
     * 
     * @param {} condition 
     * @param {} messageOnFail 
     * @returns {}
     */
    function ASSERT(condition, messageOnFail) {
        if (!condition) {
            ERROR(Status.AssertionFailed, messageOnFail || (new Error()).stack.toString());
        }
    }


    //================================================================================================
    //================================================================================================
    //================================================================================================

    //
    // Here comes the main logic
    //

    // Prepare known etags
    ASSERT(typeof etagsObject === "object");
    for (var tmpDocId in etagsObject) {
        if (etagsObject.hasOwnProperty(tmpDocId)) {
            var tmpEtag = etagsObject[tmpDocId];
            ASSERT(tmpEtag === null || (typeof tmpEtag === "string" && tmpEtag));
            UPDATE_ETAG(tmpDocId, tmpEtag);
        }
    }
    //DEBUG("Prepare done");


    function DispatchOperation(index, operation) {
        if (operation["op"] === "AddVertex") {
            /* Parameter schema: 
                {
                    "op": "AddVertex",
                    "vertexObject": { ... }
                }
               Response content:
                {
                    "etag": ...
                }
            */
            return new Promise(function(resolve, reject) {
                AddVertex(
                    operation["vertexObject"],
                    function(vertexDocument) {
                        resolve(DUMMY);
                    });
            });
        }
        else if (operation["op"] === "AddEdge") {
            /* Parameter schema: 
                {
                    "op": "AddEdge",
                    "srcV": ...,
                    "sinkV": ...,
                    "isReverse", true/false
                    "spillThreshold", null/0/>0     // Can be null
                    "edgeObject", {
                        "id": ...,
                        "label": ...,
                        "_srcV"/"_sinkV": "...",
                        "_srcVLabel"/"_sinkVLabel": "...",
                        ... (Other properties)
                    }
                }
               Response content:
                {
                    "firstSpillEdgeDocId": "..."    // Not null when spilling the _edge/_reverse_edge the first time
                    "newEdgeDocId": "..."           // Which document is this edge added to? (Can be null)
                }
            */
            return new Promise(function(resolve, reject) {
                AddEdge(
                    operation["srcV"],
                    operation["sinkV"],
                    operation["isReverse"],
                    operation["spillThreshold"],
                    operation["edgeObject"],
                    function (firstEdgeDocId, latestEdgeDocId) {
                        var content = {
                            "firstSpillEdgeDocId": firstEdgeDocId,
                            "newEdgeDocId": latestEdgeDocId
                        };
                        SUCCESS(index, content);
                        resolve(DUMMY);
                    });
            });
        }
        else if (operation["op"] === "DropVertexProperty") {
            /* Parameter schema: 
                {
                    "op": "DropVertexProperty",
                    "vertexId": ...,
                    "propertyName": ...,
                }
               Response content:
                {
                    "found": true/false
                }
            */
            return new Promise(function (resolve, reject) {
                DropVertexProperty(
                    operation["vertexId"],
                    operation["propertyName"],
                    function (found) {
                        var content = {
                            "found": found
                        };
                        SUCCESS(index, content);
                        resolve(DUMMY);
                    });
            });
        }
        else if (operation["op"] === "DropVertexSingleProperty") {
            /* Parameter schema: 
                {
                    "op": "DropVertexProperty",
                    "vertexId": ...,
                    "propertyName": ...,
                    "singlePropertyId": ...,
                }
               Response content:
                {
                    "found": true/false
                }
            */
            return new Promise(function (resolve, reject) {
                DropVertexSingleProperty(
                    operation["vertexId"],
                    operation["propertyName"],
                    operation["singlePropertyId"],
                    function (found) {
                        var content = {
                            "found": found
                        };
                        SUCCESS(index, content);
                        resolve(DUMMY);
                    });
            });
        }
        else if (operation["op"] === "DropVertexSinglePropertyMetaProperty") {
            /* Parameter schema: 
                {
                    "op": "DropVertexProperty",
                    "vertexId": ...,
                    "propertyName": ...,
                    "singlePropertyId": ...,
                    "metaName": ...,
                }
               Response content:
                {
                    "found": true/false
                }
            */
            return new Promise(function (resolve, reject) {
                DropVertexSinglePropertyMetaProperty(
                    operation["vertexId"],
                    operation["propertyName"],
                    operation["singlePropertyId"],
                    operation["metaName"],
                    function (found) {
                        var content = {
                            "found": found
                        };
                        SUCCESS(index, content);
                        resolve(DUMMY);
                    });
            });
        }
        else {
            ERROR(Status.InternalError, "Unknown operation string: " + operation["op"]);
            return null;  // Will not reach here!
        }
    }


    function ProcessOperation(lastPromise, thisIndex) {
        lastPromise.then(function (dummyValue) {
            if (thisIndex === opArray.length) {
                DONE();
            } else {
                var operation = opArray[thisIndex];
                var thisPromise = DispatchOperation(thisIndex, operation);
                ProcessOperation(thisPromise, thisIndex + 1);
            }
            return DUMMY;  // Dummy value
        });
    }

    ProcessOperation(Promise.resolve(DUMMY), 0);
    return;





    function AddVertex(vertexObject, addVCallback) {
        ASSERT(vertexObject["id"]);
        ASSERT(vertexObject["_partition"]);
        ASSERT(vertexObject["id"] === vertexObject["_partition"]);

        CreateDocument(vertexObject, false, addVCallback);
    }


    function AddEdge(srcVId, sinkVId, isReverse, spillThreshold, edgeObject, addECallback) {
        ASSERT(typeof isReverse === "boolean");
        if (!spillThreshold) {
            spillThreshold = 0;
        } else {
            ASSERT(typeof spillThreshold === "number");
        }

        //
        // Check edge object
        //
        ASSERT(typeof edgeObject["id"] === "string");
        ASSERT(edgeObject["label"] === null || typeof edgeObject["label"] === "string");
        if (isReverse) {
            ASSERT(typeof edgeObject["_srcV"] === "string");
            ASSERT(edgeObject["_srcVLabel"] === null || typeof edgeObject["_srcVLabel"] === "string");
        } else {
            ASSERT(typeof edgeObject["_sinkV"] === "string");
            ASSERT(edgeObject["_sinkVLabel"] === null || typeof edgeObject["_sinkVLabel"] === "string");
        }


        //
        // Do the insertion
        //
        var modifyVertexId = isReverse ? sinkVId : srcVId;
        var modifyArrayName = isReverse ? "_reverse_edge" : "_edge";
        var edgeSpillName = isReverse ? "_revEdgeSpilled" : "_edgeSpilled";
        RetrieveDocumentById(
            modifyVertexId,
            function(vertexDocument) {
                var edgeContainer = vertexDocument[modifyArrayName];
                ASSERT(edgeContainer instanceof Array);

                ASSERT(typeof vertexDocument[edgeSpillName] === "boolean");
                if (vertexDocument[edgeSpillName]) {
                    // This edges are spilled
                    var tryEdgeDocId = edgeContainer[0]["id"];
                    ASSERT(typeof tryEdgeDocId === "string" && tryEdgeDocId);

                    AddEdgeToEdgeDocument(
                        edgeObject,
                        spillThreshold,
                        tryEdgeDocId,
                        function(addToEdgeDocId) {
                            if (addToEdgeDocId === tryEdgeDocId) {  // Add to the latest edge document
                                addECallback(null, tryEdgeDocId);
                            } else {  // Add to a new edge document
                                // Update the latest edge document id in vertex document
                                edgeContainer[0]["id"] = addToEdgeDocId;
                                TryReplaceDocument(
                                    vertexDocument,
                                    function(dummyTooLarge, newVertexDocument) {
                                        ASSERT(dummyTooLarge === false);
                                        addECallback(null, addToEdgeDocId);
                                    });
                            }
                        });
                } else {
                    // This edge array is not spilled
                    edgeContainer.push(edgeObject);

                    new Promise(function(resolve, reject) {
                        if (spillThreshold !== 0 && edgeContainer.length > spillThreshold) {
                            // Spilling threshold is reached: too large!
                            resolve(true);
                        } else {
                            TryReplaceDocument(
                                vertexDocument,
                                function(tooLarge, newVertexDocument) {
                                    resolve(tooLarge);
                                });
                        }
                    }).then(function(tooLarge) {
                        if (tooLarge) {
                            // This vertex document is too large, either because the spilling threshold is reached,
                            // or the document excceeds the size limit. Now spill this vertex document!
                            SpillVertex(
                                vertexDocument,
                                isReverse,
                                function (firstEdgeDocId, secondEdgeDocId) {
                                    addECallback(firstEdgeDocId, secondEdgeDocId);
                                });
                        } else {
                            addECallback(null, null);
                        }
                    });
                }
            }
        );
    }


    /**
     * 
     * @param {string} vertexId 
     * @param {string} propertyName 
     * @param {function(boolean)} callback - Returns whether this property is found?
     * @returns {} 
     */
    function DropVertexProperty(vertexId, propertyName, callback) {
        ASSERT(typeof vertexId === "string" && vertexId);
        ASSERT(typeof propertyName === "string" && propertyName);
        ASSERT(typeof callback === "function" && callback);

        RetrieveDocumentById(
            vertexId,
            function(vertexDocument) {
                var found = !(vertexDocument[propertyName] === undefined);
                if (found) {
                    delete vertexDocument[propertyName];
                    TryReplaceDocument(
                        vertexDocument,
                        function(dummyTooLarge, newVertexDocument) {
                            ASSERT(dummyTooLarge === false);
                            callback(true);
                        });
                } else {
                    callback(false);
                }
            });
    }


    function DropVertexSingleProperty(vertexId, propertyName, singlePropertyId, callback) {
        ASSERT(typeof vertexId === "string" && vertexId);
        ASSERT(typeof propertyName === "string" && propertyName);
        ASSERT(typeof singlePropertyId === "string" && singlePropertyId);
        ASSERT(typeof callback === "function" && callback);

        RetrieveDocumentById(
            vertexId,
            function (vertexDocument) {
                var found = !(vertexDocument[propertyName] === undefined);
                if (found) {
                    found = false;
                    var singlePropArray = vertexDocument[propertyName];
                    for (var index = 0; index < singlePropArray.length; ++index) {
                        if (singlePropArray[index]["id"] === singlePropertyId) {
                            if (singlePropArray.length === 1) {
                                // If this single property is not duplicated, delete the whole vertex property!
                                delete vertexDocument[propertyName];
                            } else {
                                // singlePropArray.length > 1, just delete this single-property
                                delete singlePropArray[index];
                            }
                            found = true;
                            break;
                        }
                    }

                    if (found) {
                        TryReplaceDocument(
                            vertexDocument,
                            function (dummyTooLarge, newVertexDocument) {
                                ASSERT(dummyTooLarge === false);
                                callback(true);
                            });
                    } else {
                        callback(false);
                    }
                } else {
                    callback(false);
                }
            });
    }


    function DropVertexSinglePropertyMetaProperty(vertexId, propertyName, singlePropertyId, metaName, callback) {

        ASSERT(typeof vertexId === "string" && vertexId);
        ASSERT(typeof propertyName === "string" && propertyName);
        ASSERT(typeof singlePropertyId === "string" && singlePropertyId);
        ASSERT(typeof metaName === "string" && metaName);
        ASSERT(typeof callback === "function" && callback);

        RetrieveDocumentById(
            vertexId,
            function(vertexDocument) {

                var found = !(vertexDocument[propertyName] === undefined);
                if (found) {
                    var singlePropArray = vertexDocument[propertyName];
                    var singleProp = null;
                    for (var index = 0; index < singlePropArray.length; ++index) {
                        if (singlePropArray[index]["id"] === singlePropertyId) {
                            singleProp = singlePropArray[index];
                            break;
                        }
                    }

                    if (singleProp !== null) {
                        ASSERT(singleProp["_meta"] !== undefined);
                        found = (singleProp["_meta"][metaName] !== undefined);

                        if (found) {
                            delete (singleProp["_meta"])[metaName];
                            TryReplaceDocument(
                                vertexDocument,
                                function(dummyTooLarge, newVertexDocument) {
                                    ASSERT(dummyTooLarge === false);
                                    callback(true);
                                });
                        } else {
                            callback(false);
                        }
                    } else {
                        callback(false);
                    }
                } else {
                    callback(false);
                }
            });
    }


    /**
     * Try to add an edge to the edge document.
     * If too large, create a new edge document to store the edge.
     * 
     * @param {Object} edgeObject 
     * @param {string} edgeDocId 
     * @param {function(string)} callback - The edge is added to which edge document?
     * @returns {} 
     */
    function AddEdgeToEdgeDocument(edgeObject, spillThreshold, tryEdgeDocId, callback) {
        ASSERT(typeof spillThreshold === "number" && spillThreshold >= 0);
        ASSERT(typeof tryEdgeDocId === "string" && tryEdgeDocId);
        ASSERT(typeof callback === "function" && callback);

        RetrieveDocumentById(
            tryEdgeDocId,
            function(tryEdgeDoc) {
                var vertexId = tryEdgeDoc["_vertex_id"];
                ASSERT(typeof vertexId === "string" && vertexId);

                var isReverse = tryEdgeDoc["_is_reverse"];
                ASSERT(typeof isReverse === "boolean");

                var edgeContainer = tryEdgeDoc["_edge"];

                new Promise(
                    function (resolve, reject) {

                        // If spill threshold is reached, tooLarge = true
                        if (spillThreshold > 0) {
                            ASSERT(edgeContainer.length <= spillThreshold);
                            if (edgeContainer.length === spillThreshold) {
                                resolve(true);
                                return;
                            }
                        }

                        // Try to update the edge document
                        edgeContainer.push(edgeObject);
                        TryReplaceDocument(
                            tryEdgeDoc,
                            function (tooLarge, newDocument) {
                                resolve(tooLarge);
                            });
                    })
                    .then(function(tooLarge) {
                        if (!tooLarge) {
                            callback(tryEdgeDocId);
                        } else {
                            // Now the `tryEdgeDoc` is too small for the new edge
                            // Spill this edge to a new edge document
                            // NOTE: `tryEdgeDpc` is not modified! (etag remains unchanged)
                            var newEdgeDocObject = {
                                "_vertex_id": vertexId,
                                "_partition": vertexId,
                                "_is_reverse": isReverse,
                                "_edge": new Array(1)
                            };
                            newEdgeDocObject["_edge"][0] = edgeObject;

                            CreateDocument(
                                newEdgeDocObject,
                                true,
                                function (newEdgeDoc) {
                                    ASSERT(typeof newEdgeDoc["id"] === "string");
                                    ASSERT(typeof newEdgeDoc["_etag"] === "string");

                                    callback(newEdgeDoc["id"]);
                                });
                        }
                    });
            });
    }

    /**
     * Spill a not-spilled vertex
     * 
     * @param {} vertexDocument 
     * @param {} isReverse 
     * @param {function(string, string)} spillCallback 
     * @returns {} 
     */
    function SpillVertex(vertexDocument, isReverse, spillCallback) {

        ASSERT(typeof isReverse === "boolean");

        var edgeSpillName = isReverse ? "_revEdgeSpilled" : "_edgeSpilled";
        ASSERT(vertexDocument[edgeSpillName] === false);

        var modifyArrayName = isReverse ? "_reverse_edge" : "_edge";
        var edgeContainer = vertexDocument[modifyArrayName];
        ASSERT(edgeContainer instanceof Array);
        ASSERT(edgeContainer.length > 0);

        // Prepare the first edge document, which contains all but the last edges of the original vertex
        var firstEdgeDocObject = {
            "_vertex_id": vertexDocument["id"],
            "_partition": vertexDocument["id"],
            "_is_reverse": isReverse,
            "_edge": new Array(edgeContainer.length - 1)
        };
        for (var i = 0; i < edgeContainer.length - 1; i++) {
            firstEdgeDocObject["_edge"][i] = edgeContainer[i];
        }

        // Prepare the second edge document, which contains the last edge of the original vertex
        var secondEdgeDocObject = {
            "_vertex_id": vertexDocument["id"],
            "_partition": vertexDocument["id"],
            "_is_reverse": isReverse,
            "_edge": new Array(1)
        };
        secondEdgeDocObject["_edge"][0] = edgeContainer[edgeContainer.length - 1];

        //
        // Now create the two edge documents and update the vertex document
        //
        var edgeDocIds = new Array(2); // [0]: firstEdgeDocId, [1]: secondEdgeDocId

        // Create the first edge document
        CreateDocument(
            firstEdgeDocObject,
            true,
            function(firstEdgeDoc) {
                edgeDocIds[0] = firstEdgeDoc["id"];

                // Create the second edge document
                CreateDocument(
                    secondEdgeDocObject,
                    true,
                    function(secondEdgeDoc) {
                        edgeDocIds[1] = secondEdgeDoc["id"];

                        // Update the vertex document
                        vertexDocument[edgeSpillName] = true;
                        edgeContainer.length = 0;
                        edgeContainer.push(new Object());
                        edgeContainer[0]["id"] = edgeDocIds[1];
                        TryReplaceDocument(
                            vertexDocument,
                            function(dummyTooLarge) {
                                ASSERT(dummyTooLarge === false);
                                spillCallback(edgeDocIds[0], edgeDocIds[1]);
                            });
                    });
            });
    }


    //================================================================================================
    //================================================================================================
    //================================================================================================
    //
    // Here comes the document-level operations
    //


    /**
     * Try to replace an existing document with a new one.
     * 
     * @param {Object} newDocument - The new document object
     * @param {function(boolean, object)} replaceCallback - The callback indicating whether the document is too large! 
     *        The parameter is `true` if the replacement failed because the new document is too large
     * @returns {} 
     */
    function TryReplaceDocument(newDocument, replaceCallback) {
        ASSERT(newDocument);
        ASSERT(newDocument["id"] && newDocument["_partition"]);
        ASSERT(typeof newDocument["_self"] === "string", "documentObject must contain `_self` as its link");

        var replaceOptions = {
            // indexAction: "default" | "include" | "exclude",
            // etag: GET_ETAG()
        };
        var etag = GET_ETAG(newDocument["id"]);  // Can be null: means unknown
        if (etag !== null) {
            replaceOptions["etag"] = etag;
        }

        var isAccepted = collection.replaceDocument(
            newDocument["_self"], // documentLink: string
            newDocument, // document: Object
            replaceOptions, // options: ReplaceOptions
            function (error, resource, options) { // callback: RequestCallback
                if (error) {
                    if (error.number === ErrorCodes.RequestEntityTooLarge) {
                        // This document is too large!
                        ASSERT(resource === null);
                        replaceCallback(true, null);
                    } else {
                        // This operations failed due to other reasons
                        ERROR(error);
                    }
                } else {
                    // This document is successfully uploaded
                    UPDATE_ETAG(resource["id"], resource["_etag"]);
                    replaceCallback(false, resource);
                }
            }
        );
        if (!isAccepted) {
            ERROR(Status.NotAccepted, "[TryUpload] Not accepted");
        }
    }


    function CreateDocument(documentObject, generateId, createCallback) {

        ASSERT(typeof documentObject["_partition"] === "string");

        if (!generateId) {
            ASSERT(typeof documentObject["id"] === "string");
        }

        var createOptions = {
            // indexAction: "default" | "include" | "exclude",
            disableAutomaticIdGeneration: !generateId
        };

        var isAccepted = collection.createDocument(
            collectionLink, // collectionLink: string
            documentObject, // body: Object
            createOptions, // options: CreateOptions
            function (error, resource, options) { // callback: RequestCallback
                if (error) {
                    ERROR(error);
                } else {
                    UPDATE_ETAG(resource["id"], resource["_etag"]);
                    createCallback(resource);
                }
            }
        );
        if (!isAccepted) {
            ERROR(Status.NotAccepted, "[CreateDocument] Not accepted");
        }
    }


    function RetrieveDocumentById(id, retrieveCallback) {
        ASSERT(id && typeof id === "string");
        ASSERT(retrieveCallback && typeof (retrieveCallback) === "function");

        var queryOptions = {
            enableScan: false,
            enableLowPrecisionOrderBy: true
        };

        var isAccepted = collection.queryDocuments(
            collectionLink, // collectionLink: string
            "SELECT * FROM doc WHERE doc['id'] = '" + id + "'", // filterQuery: string|object
            queryOptions, // options: FeedOptions
            function (error, resources, options) { // callback: FeedCallback
                if (error) {
                    ERROR(error);
                }
                else {
                    ASSERT(resources instanceof Array, "Query result should be an array");
                    ASSERT(resources.length === 1, "The retrieve-by-id result should have exactly one document");
                    retrieveCallback(resources[0]);
                }
            }
        );
        if (!isAccepted) {
            ERROR(Status.NotAccepted, "[RetrieveDocumentById] Not accepted");
        }
    }
}
