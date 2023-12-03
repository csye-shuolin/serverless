const axios = require('axios');
const { Storage } = require('@google-cloud/storage');
const stream = require('stream');
const mailgun = require('mailgun-js');
const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');

exports.handler = async (event) => {
    const dynamoDb = new AWS.DynamoDB.DocumentClient();
    let submissionId = uuidv4(); // Generate a unique submission ID

    // SNS message is in event.Records[0].Sns.Message
    const snsMessage = JSON.parse(event.Records[0].Sns.Message);
    console.log("SNS Messages: ", snsMessage)
    const submissionUrl = snsMessage.submission_url;
    const recipientEmail = snsMessage.email;

    // Environment variables for GCS and Mailgun
    const gcsBucketName = process.env.GCS_BUCKET_NAME;
    const gcpServiceAccountKeyJson = process.env.GCP_SERVICE_ACCOUNT_KEY_JSON;
    const mailgunApiKey = process.env.MAILGUN_API_KEY;
    const mailgunDomain = process.env.MAILGUN_DOMAIN;
    const emailDeliveryTableName = process.env.EMAIL_DELIVERY_TABLE_NAME;
    const snsStatus = snsMessage.status;
    const logMessage = snsMessage.log_message;
    const assignmentId = snsMessage.assignment_id;

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
            from: 'Shuolin <mailgun@demo.shuolin.me>',
            to: recipientEmail,
            subject: subject,
            text: text,
        };
        return mg.messages().send(data);
    };

    // Function to record email in dynamoDB
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


    // Send Failure Emails
    const failSubject = 'CSYE6225 Assignment Submission Failed';
    // Case 1: Submission deadline has passed
    // Case 2: Exceeded number of attempts
    if (snsStatus == 'Failed') {
        // Send email
        await sendEmail(failSubject, `You failed to submit your assignmnet ${assignmentId}, ${logMessage}`);
        // Record email
        await recordEmailStatus(submissionId, recipientEmail, snsStatus, logMessage);
        return;
    }

    // Case 3: Invalid submission URL
    if (!submissionUrl.endsWith('.zip')) {
        // Send email
        await sendEmail(failSubject, `You failed to submit your assignment ${assignmentId}, the submission URL is invalid. A valid URL should point to a zip file`);
        // Record email
        await recordEmailStatus(submissionId, recipientEmail, 'Failed', "Invalid submission URL");
        return;
    }

    // Download the file from the URL
    const response = await axios.get(submissionUrl, { responseType: 'stream' });
    const fileName = submissionUrl.split('/').pop();
 
    // Define the path for the file in GCS
    const gcsFilePath = `${recipientEmail}/${assignmentId}/${submissionId}/${fileName}`;
    const file = bucket.file(gcsFilePath);

    // Upload the downloaded file to GCS
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

    //  Send success email and record to dynamoDB
    const successSubject = 'CSYE6225 Assignment Submission Successful'
    if (snsStatus == 'Successful') {
        await sendEmail(successSubject, `You successfully submitted the assignment "${assignmentId}", it is uploaded to the google cloud storage path ${gcsBucketName}/${gcsFilePath}`);
        await recordEmailStatus(submissionId, recipientEmail, snsStatus);
        return;
    } 

};