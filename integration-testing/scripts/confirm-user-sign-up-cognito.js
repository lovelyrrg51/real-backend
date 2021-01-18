#!/usr/bin/env node

const AWS = require('aws-sdk')
const dotenv = require('dotenv')
const moment = require('moment')
const prmt = require('prompt')
const util = require('util')

dotenv.config()

const awsRegion = process.env.AWS_REGION
if (awsRegion === undefined) throw new Error('Env var AWS_REGION must be defined')

const frontendCognitoClientId = process.env.COGNITO_FRONTEND_CLIENT_ID
if (frontendCognitoClientId === undefined) throw new Error('Env var COGNITO_FRONTEND_CLIENT_ID must be defined')

const identityPoolId = process.env.COGNITO_IDENTITY_POOL_ID
if (identityPoolId === undefined) throw new Error('Env var COGNITO_IDENTITY_POOL_ID must be defined')

const pinpointAppId = process.env.PINPOINT_APPLICATION_ID

prmt.message = ''
prmt.start()

const prmtSchema = {
  properties: {
    userId: {
      description: "User id (aka cognito user pool 'username') of user to confirm?",
    },
    confirmationCode: {
      description: 'Confirmation code from email/sms?',
      pattern: /^[0-9]{6}$/,
    },
    pinpointEndpointId: {
      description: 'Pinpoint endpoint id to send analytics to? Leave blank to skip',
    },
  },
}

// Prompt and get user input then display those data in console.
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }
  await confirmUser(result.userId, result.confirmationCode, result.pinpointEndpointId)
  if (result.pinpointEndpointId) await trackWithPinpoint(result.pinpointEndpointId, result.userId)
})

const confirmUser = async (userId, confirmationCode) => {
  await new AWS.CognitoIdentityServiceProvider({params: {ClientId: frontendCognitoClientId, Region: awsRegion}})
    .confirmSignUp({ConfirmationCode: confirmationCode, Username: userId})
    .promise()
  console.log('User confirmed.')
}

const trackWithPinpoint = async (endpointId, userId) => {
  if (pinpointAppId === undefined) throw new Error('Env var PINPOINT_APPLICATION_ID must be defined')

  const credentials = await new AWS.CognitoIdentity({params: {IdentityPoolId: identityPoolId}})
    .getCredentialsForIdentity({IdentityId: userId})
    .promise()
    .then(({Credentials: creds}) => new AWS.Credentials(creds.AccessKeyId, creds.SecretKey, creds.SessionToken))
  const pinpoint = new AWS.Pinpoint({credentials, params: {ApplicationId: pinpointAppId}})

  // https://docs.aws.amazon.com/pinpoint/latest/developerguide/event-streams-data-app.html
  const eventType = '_userauth.sign_up'
  const resp = await pinpoint
    .putEvents({
      EventsRequest: {
        BatchItem: {
          [endpointId]: {
            Endpoint: {},
            Events: {
              [eventType]: {
                EventType: eventType,
                Timestamp: moment().toISOString(),
              },
            },
          },
        },
      },
    })
    .promise()
  if (resp.EventsResponse.Results[endpointId].EventsItemResponse[eventType].StatusCode == 202) {
    console.log(`Pinpoint event '${eventType}' recorded on for endpoint '${endpointId}'`)
  } else {
    console.log(`Error recording pinpoint event '${eventType}' recorded on for endpoint '${endpointId}'`)
    console.log(util.inspect(resp, {showHidden: false, depth: null}))
  }
}
