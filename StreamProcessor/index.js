const { BlobServiceClient } = require("@azure/storage-blob");
const { Readable } = require("stream");
const { parser } = require('stream-json/jsonl/Parser');
const { stringer } = require('stream-json/jsonl/Stringer');

const blobServiceClient = BlobServiceClient.fromConnectionString("DefaultEndpointsProtocol=https;AccountName=filestreamandprocessing;AccountKey=a9vE2tna4c1q9BLMTn49DaxHGfVsFPVi8FShGCOvM2VxGSMGA7kble32BrfCRZA5txSYIk5H2St0+AStzokHzQ==;EndpointSuffix=core.windows.net");

module.exports = async function (context, req) {
    context.log('JS HTTP trigger begin');

    // if (!req.body.blob) return context.res = { status: 404 }
    context.res = { status: 200 };

    const blockBlobClient = blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob1");

    const downloadBlobStream = await blockBlobClient.download(0);

    const objStream = downloadBlobStream.readableStreamBody.pipe(parser());

    let splitedFileStreamsMap = new Map();
    let allAwaitablePromises = [];

    objStream.on("data", (obj) => {
        let index = obj.value.index;
        if (!splitedFileStreamsMap.has(index)) {    // if the index is new, initiate a new stream and start the first upload
            const newStream = new Readable({ objectMode: true, highWaterMark: 16 });
            newStream._read = function () {
                return;
            }
            newStream.push(obj);
            // context.log("forking a new stream, index: " + index);
            let promise = blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob1" + "-" + index).uploadStream(newStream.pipe(stringer()));
            allAwaitablePromises.push(promise);
            // we can later seperate these housekeeping parts into its own functions
            // note we do not await with the current syntax,indstead, we push the promise into a holder array and use Promise.allSettled()
            splitedFileStreamsMap.set(index, newStream);
            context.log("forked stream initiation completed and registered, current map depth: " + splitedFileStreamsMap.size);
        } else {    // if index exists, keep pushing to the same stream
            splitedFileStreamsMap.get(index).push(obj);
        }
    });

    objStream.on("end", () => {
        context.log("processing completed, ending all streams");
        for (const stream of splitedFileStreamsMap.values()) {
            stream.push(null);
        }
    });

    objStream.on("error", (err) => {
        context.log(err);
    })

    await Promise.allSettled(allAwaitablePromises);
}