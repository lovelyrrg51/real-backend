#!/usr/bin/env node

const AWS = require('aws-sdk')
const dotenv = require('dotenv')
const fs = require('fs')
const jwtDecode = require('jwt-decode')
const moment = require('moment')
const prmt = require('prompt')
const util = require('util')

dotenv.config()
AWS.config = new AWS.Config()

const cognitoClientId = process.env.COGNITO_TESTING_CLIENT_ID
if (cognitoClientId === undefined) throw new Error('Env var COGNITO_TESTING_CLIENT_ID must be defined')

const identityPoolId = process.env.COGNITO_IDENTITY_POOL_ID
if (identityPoolId === undefined) throw new Error('Env var COGNITO_IDENTITY_POOL_ID must be defined')

const userPoolId = process.env.COGNITO_USER_POOL_ID
if (userPoolId === undefined) throw new Error('Env var COGNITO_USER_POOL_ID must be defined')

const pinpointAppId = process.env.PINPOINT_APPLICATION_ID

const cognitoIdentityPoolClient = new AWS.CognitoIdentity({params: {IdentityPoolId: identityPoolId}})
const cognitoUserPoolClient = new AWS.CognitoIdentityServiceProvider({params: {ClientId: cognitoClientId}})

prmt.message = ''
prmt.start()

const prmtSchema = {
  properties: {
    username: {
      description: "User's email, phone or human-readable username?",
      required: true,
    },
    password: {
      description: "User's password?",
      required: true,
      hidden: true,
    },
    destination: {
      description: 'Filename to write the results to? leave blank for stdout',
    },
    pinpointEndpointId: {
      description: 'Pinpoint endpoint for analytics tracking? Leave blank to skip',
    },
  },
}

// Prompt and get user input then display those data in console.
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }
  const tokens = await generateTokens(result.username, result.password)
  const creds = await generateCredentials(tokens)
  if (result.pinpointEndpointId) trackWithPinpoint(result.pinpointEndpointId, tokens, creds)
  if (tokens) {
    const output = JSON.stringify(
      {
        authProvider: 'COGNITO',
        tokens: tokens,
        credentials: creds,
      },
      null,
      2,
    )
    if (result.destination) fs.writeFileSync(result.destination, output + '\n')
    else console.log(output)
  }
})

const generateTokens = async (username, password) => {
  // sign them in
  try {
    return await cognitoUserPoolClient
      .initiateAuth({
        AuthFlow: 'USER_PASSWORD_AUTH',
        AuthParameters: {USERNAME: username, PASSWORD: password},
      })
      .promise()
      .then((resp) => resp.AuthenticationResult)
  } catch (err) {
    console.log(err)
    return null
  }
}

const generateCredentials = async (tokens) => {
  let resp
  if (tokens) {
    // generate authenticated credentials
    const idToken = tokens.IdToken
    const userId = jwtDecode(idToken)['cognito:username']
    const Logins = {[`cognito-idp.${AWS.config.region}.amazonaws.com/${userPoolId}`]: idToken}
    resp = await cognitoIdentityPoolClient.getCredentialsForIdentity({IdentityId: userId, Logins}).promise()
  } else {
    // generate unauthenticated credentials
    // get a throwaway userId to use to generate credentials to record this event to pinpoint
    // note the frontend should make every effort to not generate a throwawy identity like this
    // if there is any userId cached on the device, it should be ok to use to get unauthenticated credentials
    resp = await cognitoIdentityPoolClient.getId().promise()
    resp = await cognitoIdentityPoolClient.getCredentialsForIdentity(resp).promise()
  }
  return resp.Credentials
}

const trackWithPinpoint = async (endpointId, tokens, creds) => {
  if (pinpointAppId === undefined) throw new Error('Env var PINPOINT_APPLICATION_ID must be defined')

  const credentials = new AWS.Credentials(creds.AccessKeyId, creds.SecretKey, creds.SessionToken)
  const pinpoint = new AWS.Pinpoint({credentials, params: {ApplicationId: pinpointAppId}})

  // https://docs.aws.amazon.com/pinpoint/latest/developerguide/event-streams-data-app.html
  const eventType = tokens ? '_userauth.sign_in' : '_userauth.auth_fail'
  let resp = await pinpoint
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
