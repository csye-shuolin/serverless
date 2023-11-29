const axios = require('axios');
const { Storage } = require('@google-cloud/storage');
const stream = require('stream');
const mailgun = require('mailgun-js');
const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');

exports.handler = async (event) => {
    const dynamoDb = new AWS.DynamoDB.DocumentClient();
    let submissionId = uuidv4(); // Generate a unique submission ID

    try {
        // SNS message is in event.Records[0].Sns.Message
        const snsMessage = JSON.parse(event.Records[0].Sns.Message);
        const submissionUrl = snsMessage.submission_url;
        const recipientEmail = snsMessage.email;

        if (!submissionUrl || !recipientEmail) {
            throw new Error('Submission URL or recipient Email not provided in the SNS message');
        }

        // Environment variables for GCS and Mailgun
        const gcsBucketName = process.env.GCS_BUCKET_NAME;
        const gcpServiceAccountKeyJson = process.env.GCP_SERVICE_ACCOUNT_KEY_JSON;
        const mailgunApiKey = process.env.MAILGUN_API_KEY;
        const mailgunDomain = process.env.MAILGUN_DOMAIN;
        const emailDeliveryTableName = process.env.EMAIL_DELIVERY_TABLE_NAME;

        // Initialize Google Cloud Storage client
        const storage = new Storage({
            credentials: JSON.parse(gcpServiceAccountKeyJson)
        });
        const bucket = storage.bucket(gcsBucketName);

        // Initialize Mailgun client
        const mg = mailgun({ apiKey: mailgunApiKey, domain: mailgunDomain });

        // Function to send email
        const sendEmail = (subject, text) => {
            const data = {
                from: 'Your Name <mailgun@demo.shuolin.me>',
                to: recipientEmail,
                subject: subject,
                text: text,
            };
            return mg.messages().send(data);
        };

        // Function to record email in dynanoDB
        async function recordEmailStatus(submissionId, email, status, errorMessage = '') {
            const params = {
                TableName: emailDeliveryTableName,
                Item: {
                    submissionId: submissionId,
                    email: email,
                    status: status,
                    errorMessage: errorMessage,
                    timestamp: new Date().toISOString()
                }
            };
            try {
                await dynamoDb.put(params).promise();
                console.log(`Recorded email status for submissionId: ${submissionId}`);
            } catch (error) {
                console.error(`Error recording email status for submissionId: ${submissionId}`, error);

            }
        }        

        // Download the file from the URL and upload to GCS
        const response = await axios.get(submissionUrl, { responseType: 'stream' });
        const fileName = submissionUrl.split('/').pop();
        const file = bucket.file(fileName);

        await new Promise((resolve, reject) => {
            const passthroughStream = new stream.PassThrough();
            stream.pipeline(
                response.data,
                passthroughStream,
                (error) => {
                    if (error) {
                        console.error('Error in stream pipeline', error);
                        reject(error);
                    }
                }
            );
            passthroughStream.pipe(file.createWriteStream())
                .on('error', reject)
                .on('finish', resolve);
        });

        // Send success email
        await sendEmail('CSYE6225 Assignment Submission Successful', `Assignment ${fileName} uploaded to ${gcsBucketName}`);
        console.log(`Uploaded ${fileName} to ${gcsBucketName}`);

        // Record sucess email
        await recordEmailStatus(submissionId, recipientEmail, 'Success');
        console.log(`Success Email ${recipientEmail} recored to DynamoDB`);

        return { statusCode: 200, body: `Uploaded ${fileName} to ${gcsBucketName}` };
    } catch (error) {
        console.error('Error:', error);

        // Send failure email
        await sendEmail('CSYE6225 Assignment Submission Failed', `Failed to upload file. Error: ${error.message}`);

        // Record failure email
        await recordEmailStatus(submissionId, recipientEmail, 'Failed', error.message);
        console.log(`Failed Email ${recipientEmail} recored to DynamoDB`);

        return { statusCode: 400, body: `Error: ${error.message}` };
    }
};
