const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const { parser } = require('stream-json/jsonl/Parser');

const blobServiceClient = BlobServiceClient.fromConnectionString("DefaultEndpointsProtocol=https;AccountName=liamteststorage0x01;AccountKey=g2dQ2iCUpoL9S5JfwRlvoj/MFCmrlhUbT6TYHiF5qRdei7N2ZFrlNMEvOgttybXLMn5k/RTeY/Ye+ASt+YVW+g==;EndpointSuffix=core.windows.net");
const queueServiceClient = QueueServiceClient.fromConnectionString("DefaultEndpointsProtocol=https;AccountName=liamteststorage0x01;AccountKey=g2dQ2iCUpoL9S5JfwRlvoj/MFCmrlhUbT6TYHiF5qRdei7N2ZFrlNMEvOgttybXLMn5k/RTeY/Ye+ASt+YVW+g==;EndpointSuffix=core.windows.net");

module.exports = async function (context) {
    context.log('Queue Sender begin');

    // if (!req.body.blob) return context.res = { status: 404 }
    context.res = { status: 200 };

    const blockBlobClient = blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob2");
    let queueName = "queue1"
    const queueClient = queueServiceClient.getQueueClient(queueName);

    const downloadBlobResponse = await blockBlobClient.download();
    const objStream = downloadBlobResponse.readableStreamBody.pipe(parser());

    const splitedFileIndexesArray = new Array();

    objStream.on("data", async (obj) => {
        let index = obj.value.index;
        if (!splitedFileIndexesArray.has(index)) {    // if the index is new, initiate a new stream and start the first upload
            context.log("dispatching message to processor pool/queue, index: " + index);
            await queueClient.sendMessage(index);
            splitedFileIndexesArray.push(index);
            // we can later seperate these housekeeping parts into its own functions
            // note it is not possible to await here,indstead, we push the promise into a holder array and use Promise.allSettled()
        }
    });
}