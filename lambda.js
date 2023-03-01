const https = require('https');

exports.handler = (request, context, callback) => {
    callback(null, update(request.state, request.secrets, request.bucket, request.file));
};

function update(state, secrets, bucket, key) {
    // Fetch records using api calls
    let [insertTransactions, deleteTransactions, newTransactionsCursor] = apiResponse(state, secrets);

    // Populate insert and delete in records
    const records = JSON.stringify({
        insert: {
            transactions: insertTransactions
        },
        delete: {
            transactions: deleteTransactions
        }
    })

    // Store records in s3 bucket
    putObjectToS3(bucket, key, records)

    // Return response
    return ({
        state: {
            transactionsCursor: newTransactionsCursor
        },
        schema: {
            transactions: {
                primary_key: ['Event ID']
            }
        },
        hasMore: false
    });
}

function apiResponse(state, secrets) {
    const insertTransactions = [
        {
            "Organization": "Robin",
            "Building": "87 Summer Street",
            "Floor": "",
            "Space": "Conference Room",
            "Space ID": 23253,
            "Event ID": "207480119_20230101T043000Z",
            "Event Status": "CONFIRMED",
            "Created At (UTC)": "2022-09-18 04:08:41",
            "Updated At (UTC)": "2022-09-18 04:08:41",
            "All Day Event": "FALSE",
            "Title": "Test Recurring",
            "Is Recurring": "TRUE",
            "Capacity": 8,
            "Invited People": 1,
            "Attendees": 1,
            "Location": "Nicholas' Cage",
            "Started At (UTC)": "2023-01-01 04:30:00",
            "Ended At (UTC)": "2023-01-01 04:35:00",
            "Minute Duration": 5,
            "Checked In At (UTC)": "",
            "Minutes Delayed": "",
            "Automatically Unbooked At (UTC)": "",
            "Creator Robin ID": "",
            "Creator Robin Name": "",
            "Creator Department": "",
            "Creator Groups": "",
            "Creator Robin Email": "",
            "Created By Email": "conference-room@robinpowered.onmicrosoft.com",
            "Host Robin ID": "",
            "Host Robin Name": "",
            "Host Department": "",
            "Host Groups": "",
            "Host Robin Email": "conference-room@robinpowered.onmicrosoft.com",
            "Hosting Calendar Email": "conference-room@robinpowered.onmicrosoft.com",
            "Local Creator ID": "",
            "Local Creator Email": "",
            "Cancellation Type": ""
        },
        {
            "Organization": "Robin",
            "Building": "87 Summer Street",
            "Floor": "",
            "Space": "Conference Room",
            "Space ID": 23253,
            "Event ID": "207480121_20230101T180000Z",
            "Event Status": "CONFIRMED",
            "Created At (UTC)": "2022-09-18 04:08:41",
            "Updated At (UTC)": "2022-09-18 04:08:41",
            "All Day Event": "FALSE",
            "Title": "Zach Dunn in Nicholas' Cage",
            "Is Recurring": "TRUE",
            "Capacity": 8,
            "Invited People": 1,
            "Attendees": 1,
            "Location": "Nicholas' Cage",
            "Started At (UTC)": "2023-01-01 18:00:00",
            "Ended At (UTC)": "2023-01-01 18:30:00",
            "Minute Duration": 30,
            "Checked In At (UTC)": "",
            "Minutes Delayed": "",
            "Automatically Unbooked At (UTC)": "",
            "Creator Robin ID": "",
            "Creator Robin Name": "",
            "Creator Department": "",
            "Creator Groups": "",
            "Creator Robin Email": "",
            "Created By Email": "conference-room@robinpowered.onmicrosoft.com",
            "Host Robin ID": "",
            "Host Robin Name": "",
            "Host Department": "",
            "Host Groups": "",
            "Host Robin Email": "conference-room@robinpowered.onmicrosoft.com",
            "Hosting Calendar Email": "conference-room@robinpowered.onmicrosoft.com",
            "Local Creator ID": "",
            "Local Creator Email": "",
            "Cancellation Type": ""
        }
    ];
    const deleteTransactions = [];
    return [insertTransactions, deleteTransactions, '2018-01-01T00:00:00Z'];
}

// Function to store data in s3 bucket
const AWS = require('aws-sdk');

function putObjectToS3(bucket, key, data) {
    const s3 = new AWS.S3();
    const params = {
        Bucket: bucket,
        Key: key,
        Body: data
    };
    s3.putObject(params, function (err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data);           // successful response
    });
}
