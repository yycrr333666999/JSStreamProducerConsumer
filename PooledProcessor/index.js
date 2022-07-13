const { BlobServiceClient } = require("@azure/storage-blob");
const { Readable } = require('stream');
const { chain } = require('stream-chain');
const { IndexFilter } = require('./filter');
const { parser } = require('stream-json/jsonl/Parser');
const { stringer } = require('stream-json/jsonl/Stringer');

const blobServiceClient = BlobServiceClient.fromConnectionString("DefaultEndpointsProtocol=https;AccountName=liamteststorage0x01;AccountKey=g2dQ2iCUpoL9S5JfwRlvoj/MFCmrlhUbT6TYHiF5qRdei7N2ZFrlNMEvOgttybXLMn5k/RTeY/Ye+ASt+YVW+g==;EndpointSuffix=core.windows.net");

module.exports = async function (context, myQueueItem) {
    index = parseInt(myQueueItem);

    context.log('queued processor pool processed work item number: ', index);

    const blockBlobClient = blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob2");

    const inputStream = (await blockBlobClient.download()).readableStreamBody;

    const indexFilter = new IndexFilter(index);

    const outputStream = new Readable({ objectMode: false });

    chain([
        inputStream,
        parser(),
        indexFilter,
        stringer(),
        outputStream
    ]);

    await blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob1" + "-" + index).uploadStream(outputStream);

};

