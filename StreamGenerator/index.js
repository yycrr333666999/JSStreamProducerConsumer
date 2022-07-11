const { BlobServiceClient } = require("@azure/storage-blob");
const { Readable } = require("stream");
const { stringer: jsonStringer } = require('stream-json/jsonl/Stringer');
const axios = require("axios");
const { faker } = require("@faker-js/faker");

const blobServiceClient = BlobServiceClient.fromConnectionString("DefaultEndpointsProtocol=https;AccountName=filestreamandprocessing;AccountKey=a9vE2tna4c1q9BLMTn49DaxHGfVsFPVi8FShGCOvM2VxGSMGA7kble32BrfCRZA5txSYIk5H2St0+AStzokHzQ==;EndpointSuffix=core.windows.net");

module.exports = async function (context, req) {
    context.log('Stream generator begin');

    context.res = { status: 200 };

    let name = "blob3";
    let total = 1;
    let interval = 1;
    if (req.query.name) name = req.query.name;
    if (req.query.total) total = req.query.total;
    if (req.query.interval) interval = req.query.interval;

    const blockBlobClient = blobServiceClient.getContainerClient("container1").getBlockBlobClient(name);

    const dummyDataGeneratorStream = new Readable();

    let line = 0;
    dummyDataGeneratorStream._read = function () {
        line++;
        dummyDataGeneratorStream.push(randomObj());
        if (line % interval == 0) {
            context.log("current line" + line + "current obj" + obj.index + obj.payload);
        }
        if (line >= total) {
            dummyDataGeneratorStream.push(null);
            context.log("Stream ends");
        }
    };

    await blockBlobClient.uploadStream(dummyDataGeneratorStream.pipe(jsonStringer)); // convert to JSONL (AKA NDJSON) format, basically stringified json object separated by \n
    // triggerStreamProcessor(name);

    return;
}

function random(min, max) {
    return Math.floor(Math.random() * (max - min) + min);
}

function randomObj() {
    return obj = {
        "index": random(0, 10).toString(),
        "payload": {
            "userId": faker.datatype.uuid(),
            "username": faker.internet.userName(),
            "email": faker.internet.email(),
            "avatar": faker.image.avatar(),
            "birthdate": faker.date.birthdate(),
            "registeredAt": faker.date.past(),
        }
    };
}

async function triggerStreamProcessor(blobname) {

    const body = {
        "blob": blobname
    };

    await axios
        .post("https://liamfunctiontest0x01.azurewebsites.net/api/StreamProcessor", body)
        .then(res => { context.log(res); })
        .catch(err => { context.log(err); });
}