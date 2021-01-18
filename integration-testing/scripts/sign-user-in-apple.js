#!/usr/bin/env node

const AWS = require('aws-sdk')
const dotenv = require('dotenv')
const fs = require('fs')
const prmt = require('prompt')

dotenv.config()

const identityPoolId = process.env.COGNITO_IDENTITY_POOL_ID
if (identityPoolId === undefined) throw new Error('Env var COGNITO_IDENTITY_POOL_ID must be defined')

const cognitoIndentityPoolClient = new AWS.CognitoIdentity({params: {IdentityPoolId: identityPoolId}})

prmt.message = ''
prmt.start()

const prmtSchema = {
  properties: {
    idToken: {
      description: 'Apple id token for REAL app with email scope',
      required: true,
    },
    destination: {
      description: 'Filename to write the results to? leave blank for stdout',
    },
  },
}

console.log('To generate an Apple ID token for our app... ask the frontend team for one?')

// Prompt and get user input then display those data in console.
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }
  const gqlCreds = await generateGQLCredentials(result.idToken)
  const output = JSON.stringify(
    {
      authProvider: 'APPLE',
      tokens: {IdToken: result.idToken},
      credentials: gqlCreds,
    },
    null,
    2,
  )
  if (result.destination) fs.writeFileSync(result.destination, output + '\n')
  else console.log(output)
})

const generateGQLCredentials = async (idToken) => {
  const Logins = {}
  Logins['appleid.apple.com'] = idToken

  // add the user to the identity pool
  const idResp = await cognitoIndentityPoolClient.getId({Logins}).promise()
  const userId = idResp.IdentityId

  // get credentials for appsync from the identity pool
  const resp = await cognitoIndentityPoolClient.getCredentialsForIdentity({IdentityId: userId, Logins}).promise()
  return resp.Credentials
}
