
//loading and configuring aws-sdk api
const AWS = require('aws-sdk'); 

AWS.config.update({
    region: 'us-east-1' 
});

// starting kinesis
const kinesis = new AWS.Kinesis();

//function is needed to configure Kinesis to the right shard, then calls retrieval function with the correct kinesis data
function receiveKinesisData() {
    const streamName = 'test-stream';

    kinesis.describeStream({
        StreamName: streamName
    }, (err, data) => {
        if (err) {
            console.error("Error describing stream:", err);
            return;
        }

        const shardId = data.StreamDescription.Shards[0].ShardId; // only need one shard

        kinesis.getShardIterator({
            StreamName: streamName,
            ShardId: shardId,
            ShardIteratorType: 'LATEST' //LATEST reads all the data in the current data stream (doesnt look on past data)
        }, (err, iteratorData) => {
            if (err) {
                console.error("Error getting shard iterator:", err);
                return;
            }

            const shardIterator = iteratorData.ShardIterator;

            //starts the retrieval process
            retrieveRecords(shardIterator);
        });
    });
}

//function interacts with kinesis 
function retrieveRecords(shardIterator) {
    kinesis.getRecords({
        ShardIterator: shardIterator,
        Limit: 10 //max # of records to fetch per call
    }, (err, data) => {
        if (err) {
            console.error("Error fetching records:", err);
            return;
        }

        //recieve and print the records 
        if (data.Records.length > 0) {
            data.Records.forEach(record => {
                const payload = JSON.parse(Buffer.from(record.Data, 'base64').toString('utf8'));
                console.log(`Latitude: ${payload.latitude}, Longitude: ${payload.longitude}`);
            });
        } else {
            console.log("No new records available.");
        }

        //keep retrieval call every second so it can recieve new data
        if (data.NextShardIterator) {
            setTimeout(() => retrieveRecords(data.NextShardIterator), 1000); // 1000 = every second
        }
    });
}

//calling main function
receiveKinesisData();