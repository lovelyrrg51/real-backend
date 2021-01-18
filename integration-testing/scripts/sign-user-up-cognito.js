#!/usr/bin/env node

const AWS = require('aws-sdk')
const dotenv = require('dotenv')
const moment = require('moment')
const prmt = require('prompt')
const pwdGenerator = require('generate-password')
const util = require('util')
const uuidv4 = require('uuid/v4')

dotenv.config()

const frontendCognitoClientId = process.env.COGNITO_FRONTEND_CLIENT_ID
if (frontendCognitoClientId === undefined) throw new Error('Env var COGNITO_FRONTEND_CLIENT_ID must be defined')

const testingCognitoClientId = process.env.COGNITO_TESTING_CLIENT_ID
if (testingCognitoClientId === undefined) throw new Error('Env var COGNITO_TESTING_CLIENT_ID must be defined')

const identityPoolId = process.env.COGNITO_IDENTITY_POOL_ID
if (identityPoolId === undefined) throw new Error('Env var COGNITO_IDENTITY_POOL_ID must be defined')

const pinpointAppId = process.env.PINPOINT_APPLICATION_ID

// All users the test client creates must have this family name (or the sign up
// will be rejected). This is to make it easier to clean them out later.
const familyName = 'TESTER'

const identityPoolClient = new AWS.CognitoIdentity({params: {IdentityPoolId: identityPoolId}})

prmt.message = ''
prmt.start()

const prmtSchema = {
  properties: {
    email: {
      description: 'email?',
      pattern: /^\S+@\S+$/,
    },
    phone: {
      description: 'phone number? (ex: +14151231234)',
      pattern: /^\+[0-9]{5,15}$/,
    },
    password: {
      description: 'Password? leave blank to auto-generate',
      hidden: true,
    },
    autoconfirm: {
      description: 'Automatically confirm the user? Else a confirmation code will be sent to the provided email',
      default: 'true',
      message: 'Please enter "t" or "f"',
      type: 'boolean',
      required: true,
    },
    pinpoint: {
      description: 'Track analytics with Pinpoint? Creates a new endpoint',
      default: 'false',
      message: 'Please enter "t" or "f"',
      type: 'boolean',
      required: true,
    },
  },
}

// Prompt and get user input then display those data in console.
console.log('Please enter email or phone, or both.')
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }
  if (!result.email && !result.phone) throw 'At least one of email or phone is required'
  const userId = await signUserUp(result.email, result.phone, result.password, result.autoconfirm)
  if (result.pinpoint) await trackWithPinpoint(userId, result.autoconfirm)
})

const signUserUp = async (email, phone, password, autoconfirm) => {
  if (!password) {
    password = pwdGenerator.generate({length: 8})
    console.log(`Auto-generated password: ${password}`)
  }
  const userAttrs = []
  const clientMetadata = {}
  if (autoconfirm) {
    userAttrs.push({
      Name: 'family_name',
      Value: familyName,
    })
    clientMetadata.autoConfirmUser = 'true'
  }
  if (email) {
    userAttrs.push({
      Name: 'email',
      Value: email,
    })
  }
  if (phone) {
    userAttrs.push({
      Name: 'phone_number',
      Value: phone,
    })
  }

  // get a new un-authenticated ID from the identity pool
  const idResp = await identityPoolClient.getId().promise()
  const userId = idResp.IdentityId

  // create an entry in the user pool with matching 'cognito username'<->'identity id'
  const userPoolClientId = autoconfirm ? testingCognitoClientId : frontendCognitoClientId
  const userPoolClient = new AWS.CognitoIdentityServiceProvider({params: {ClientId: userPoolClientId}})
  await userPoolClient
    .signUp({
      Username: userId,
      Password: password,
      UserAttributes: userAttrs,
      ClientMetadata: clientMetadata,
    })
    .promise()
  console.log(`User signed up with auto-generated userId: '${userId}'`)
  return userId
}

const trackWithPinpoint = async (userId, autoconfirm) => {
  if (pinpointAppId === undefined) throw new Error('Env var PINPOINT_APPLICATION_ID must be defined')

  let resp = await identityPoolClient.getCredentialsForIdentity({IdentityId: userId}).promise()
  const credentials = new AWS.Credentials(
    resp.Credentials.AccessKeyId,
    resp.Credentials.SecretKey,
    resp.Credentials.SessionToken,
  )
  const pinpoint = new AWS.Pinpoint({credentials, params: {ApplicationId: pinpointAppId}})

  const endpointId = uuidv4()
  await pinpoint
    .updateEndpoint({
      EndpointId: endpointId,
      EndpointRequest: {User: {UserId: userId}},
    })
    .promise()
  console.log(`Pinpoint endpoint '${endpointId}' generated for userId '${userId}'`)

  // if we're autoconfirming, then signup is done here
  // else, it is done and recorded when the confirmation code is submitted
  if (autoconfirm) {
    // https://docs.aws.amazon.com/pinpoint/latest/developerguide/event-streams-data-app.html
    const eventType = '_userauth.sign_up'
    resp = await pinpoint
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
}
