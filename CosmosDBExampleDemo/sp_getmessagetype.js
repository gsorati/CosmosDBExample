// SAMPLE STORED PROCEDURE
function sample(tenantId, messageType) {
    var collection = getContext().getCollection();
    //var query = "select * from root c where c.tenantid ='tmd001.tm.test' and c.messagetype='tmd001.tm.test:MessageServer'";
    //console.log(query);
    // Query documents and take 1st item.
    var isAccepted = collection.queryDocuments(
        collection.getSelfLink(),
        'SELECT * FROM c WHERE c.TenantId = "TMD001.TM.Test" AND c.messageType ="'+messageType+'"',
        function (err, feed, options) {
            if (err) throw err;

            // Check the feed and if empty, set the body to 'no docs found', 
            // else take 1st element from feed
            if (!feed || !feed.length) {
                var response = getContext().getResponse();
                response.setBody('no docs found');
            }
            else {
                var response = getContext().getResponse();
                //var body = { feed };
                response.setBody(JSON.stringify(feed));
            }
        });
    if (!isAccepted) throw new Error('The query was not accepted by the server.');
}