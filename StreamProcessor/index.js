const { BlobServiceClient } = require("@azure/storage-blob");
const { Readable } = require("stream");

const blobServiceClient = BlobServiceClient.fromConnectionString("DefaultEndpointsProtocol=https;AccountName=filestreamandprocessing;AccountKey=a9vE2tna4c1q9BLMTn49DaxHGfVsFPVi8FShGCOvM2VxGSMGA7kble32BrfCRZA5txSYIk5H2St0+AStzokHzQ==;EndpointSuffix=core.windows.net");

module.exports = async function (context, req) {
    context.log('JS HTTP trigger begin');

    // if (!req.body.blob) return context.res = { status: 404 }
    context.res = { status: 200 };

    const blockBlobClient = blobServiceClient.getContainerClient("container1").getBlockBlobClient("blob2");

    const downloadBlobStream = await blockBlobClient.download(0);

    const dataStream = downloadBlobStream.readableStreamBody;

    let splitedFileStreamsMap = new Map();

    dataStream.on("data", (value) => {
        let obj = JSON.parse(value);
        let index = obj.index;
        if (!splitedFileStreamsMap.has(index)) {
            const 
            splitedFileStreamsMap.set(index, {

            })
        }
    });
}