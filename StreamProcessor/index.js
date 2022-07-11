const { BlobServiceClient } = require("@azure/storage-blob");
const { Readable } = require("stream");
const { parser: jsonlParser } = require('stream-json/jsonl/Parser');
const { stringer: jsonStringer } = require('stream-json/jsonl/Stringer');

const blobServiceClient = BlobServiceClient.fromConnectionString("DefaultEndpointsProtocol=https;AccountName=filestreamandprocessing;AccountKey=a9vE2tna4c1q9BLMTn49DaxHGfVsFPVi8FShGCOvM2VxGSMGA7kble32BrfCRZA5txSYIk5H2St0+AStzokHzQ==;EndpointSuffix=core.windows.net");

module.exports = async function (context, req) {
    context.log('JS HTTP trigger begin');

    // if (!req.body.blob) return context.res = { status: 404 }
    context.res = { status: 200 };

    const blockBlobClient = blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob3");

    const downloadBlobStream = await blockBlobClient.download(0);

    const objStream = downloadBlobStream.readableStreamBody.pipe(jsonlParser);

    let splitedFileStreamsMap = new Map();

    objStream.on("data", (obj) => {
        let index = obj.index;
        if (!splitedFileStreamsMap.has(index)) {    // if the index is new, initiate a new stream and start the first upload
            const newStream = new Readable({ objectMode: true });
            newStream._read = function () {
                return;
            }
            newStream.push(obj);
            blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob3" + "-" + index).uploadStream(newStream.pipe(jsonStringer));   // we can later seperate these housekeeping parts into its own functions
            splitedFileStreamsMap.set(index, newStream);
        } else {    // if index exists, keep pushing to the same stream
            splitedFileStreamsMap.get(index).push(obj);
        }
    });

    dataStream.on("end", () => {
        for (const stream of splitedFileStreamsMap.values()) {
            stream.push(null);
        }
    });

    dataStream.on("error", (err) => {
        context.log(err);
    })
}