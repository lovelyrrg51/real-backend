#!/usr/bin/env node

const AWS = require('aws-sdk')
const dotenv = require('dotenv')
const prmt = require('prompt')

dotenv.config()

const frontendCognitoClientId = process.env.COGNITO_FRONTEND_CLIENT_ID
if (frontendCognitoClientId === undefined) throw new Error('Env var COGNITO_FRONTEND_CLIENT_ID must be defined')

const testingCognitoClientId = process.env.COGNITO_TESTING_CLIENT_ID
if (testingCognitoClientId === undefined) throw new Error('Env var COGNITO_TESTING_CLIENT_ID must be defined')

const identityPoolId = process.env.COGNITO_IDENTITY_POOL_ID
if (identityPoolId === undefined) throw new Error('Env var COGNITO_IDENTITY_POOL_ID must be defined')

prmt.message = ''
prmt.start()

const prmtSchema = {
  properties: {
    usernameLike: {
      description: 'Email, phone, username or user id?',
    },
  },
}

// Prompt and get user input then display those data in console.
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }
  await sendResetPassword(result.usernameLike)
})

const sendResetPassword = async (usernameLike) => {
  const userPoolClient = new AWS.CognitoIdentityServiceProvider({params: {ClientId: testingCognitoClientId}})
  const resp = await userPoolClient.forgotPassword({Username: usernameLike}).promise()
  console.log(resp)
}
