const { BlobServiceClient } = require("@azure/storage-blob");
const { Readable } = require("stream");
const { parser } = require('stream-json/jsonl/Parser');
const { stringer } = require('stream-json/jsonl/Stringer');

const blobServiceClient = BlobServiceClient.fromConnectionString("DefaultEndpointsProtocol=https;AccountName=liamteststorage0x01;AccountKey=g2dQ2iCUpoL9S5JfwRlvoj/MFCmrlhUbT6TYHiF5qRdei7N2ZFrlNMEvOgttybXLMn5k/RTeY/Ye+ASt+YVW+g==;EndpointSuffix=core.windows.net");

module.exports = async function (context, req) {
    context.log('JS HTTP trigger begin');

    // if (!req.body.blob) return context.res = { status: 404 }
    context.res = { status: 200 };

    const blockBlobClient = blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob2");

    const downloadBlobResponse = await blockBlobClient.download();
    const objStream = downloadBlobResponse.readableStreamBody.pipe(parser());

    const splitedFileStreamsMap = new Map();
    const uploadStreamsMap = new Map();

    objStream.on("data", (obj) => {
        objStream.pause();
        let index = obj.value.index;
        if (!splitedFileStreamsMap.has(index)) {    // if the index is new, initiate a new stream and start the first upload
            const newStream = new Readable({ objectMode: true });
            newStream._read = function () {
                return;
            }
            splitedFileStreamsMap.set(index, newStream);
            uploadStreamsMap.set(index, blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob1" + "-" + index).uploadStream(newStream.pipe(stringer())));
            // we can later seperate these housekeeping parts into its own functions
            // note it is not possible to await here,indstead, we push the promise into a holder array and use Promise.allSettled()
        }
        splitedFileStreamsMap.get(index).push(obj); // if index exists, keep pushing to the same stream
        objStream.resume()
    });

    objStream.on("error", (err) => {
        context.log(err);
    });

    objStream.on("end", async () => {
        context.log("processing completed, ending all streams");
        for (const stream of splitedFileStreamsMap.values()) {
            stream.push(null);
        }
    });
}