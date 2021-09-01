const cassandra = require('cassandra-driver');
const stringify = require('csv-stringify');
const AWS = require('aws-sdk');
const uuidV4 = require('uuid/v4');
const moment = require('moment');

const client = new cassandra.Client({ keyspace: 'lusha' });
const contacts = [];
const s3 = new AWS.S3();

async function getData() {
    client.stream('SELECT firstName, lastName FROM contacts WHERE accountId=', [ '123456' ])
        .on('readable', function () {
            // 'readable' is emitted as soon a row is received and parsed
            var row;
            while (row = this.read()) {
                contacts.push({ firstName: row.firstName, lastName: row.lastName });
            }
        })
        .on('end', function () {
            // Stream ended, there aren't any more rows
        })
        .on('error', function (err) {
            // Something went wrong: err is a response error from Cassandra
        });
}

async function createCsv(contacts) {
    return new Promise((resolve, reject) => {
        const csvOptions = {
            header: true,
            columns: [
                { key: 'firstName', header: 'First_Name' },
                { key: 'lastName', header: 'Last_Name' },
            ],
        };

        stringify(contacts, csvOptions, async (err, output) => {
            if (err) {
                reject(err);

                return;
            }

            resolve(output);
        });
    });
}

async function uploadToS3(key, value) {
    return new Promise((resolve, reject) => {
        s3.putObject({
            Bucket: 'my-bucket',
            Key: key,
            Body: value,
            ContentType: 'text/csv',
            ACL: 'public-read',
        }, (err, data) => {
            if (err)
                return reject(err);

            return resolve();
        });
    });
}

const filename = `${uuidV4()}/lusha-exports-contacts-${moment().format('YYYY-MM-DD')}.csv`;
// add all results from Cassandra to contacts array
await getData();
// create csv from contacts object
const output = await createCsv(contacts);
// upload the csv output to S3
await uploadToS3(filename, output);

