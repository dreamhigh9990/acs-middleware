const dotenv = require('dotenv')

dotenv.config()
module.exports = {
  endpoint: process.env.API_URL,
  port: process.env.PORT,
  iotConsumerGroup: process.env.IOT_HUB_CONSUMER_GROUP,
  eventHubConsumerGroup: process.env.EVENTHUB_CONSUMER_GROUP,
  iotHubConnectionString: process.env.IOTHUB_CONNECTION_STRING,
  iotHubName: process.env.IOTHUB_NAME,
  eventHubSenderConnectionString: process.env.EVENTHUB_SENDER_CONNECTION_STRING,
  db_host: process.env.DB_HOST,
  db_user: process.env.DB_USER,
  db_database: process.env.DB_DATABASE,
  db_password: process.env.DB_PASSWORD,
  db_port: process.env.DB_PORT,

  sendGridApiKey: process.env.SENDGRID_API_KEY,
  sendGridFromEmail: process.env.SENDGRID_FROM_EMAIL,

  twilioAuthToken: process.env.TWILIO_AUTH_TOKEN,
  twilioAccountSID: process.env.TWILIO_ACCOUNT_SID,
  twilioFromNumber: process.env.TWILIO_FROM_NUMBER
}