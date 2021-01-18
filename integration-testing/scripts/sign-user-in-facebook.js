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
    accessToken: {
      description: 'Facebook access token from REAL app with email scope',
      required: true,
    },
    destination: {
      description: 'Filename to write the results to? leave blank for stdout',
    },
  },
}

console.log('To generate a Facebook access token for testing:')
console.log('  - you need developer access to the Facebook REAL app')
console.log('  - go to `https://developers.facebook.com/tools/explorer/`')
console.log('  - select "REAL" as the application in the upper right')
console.log('  - copy the "Access Token" displayed')
console.log('By default the `email` scope should be included, but you can check by clicking "Get Token".')

// Prompt and get user input then display those data in console.
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }
  const gqlCreds = await generateGQLCredentials(result.accessToken)
  const output = JSON.stringify(
    {
      authProvider: 'FACEBOOK',
      tokens: {AccessToken: result.accessToken},
      credentials: gqlCreds,
    },
    null,
    2,
  )
  if (result.destination) fs.writeFileSync(result.destination, output + '\n')
  else console.log(output)
})

const generateGQLCredentials = async (accessToken) => {
  const Logins = {}
  Logins['graph.facebook.com'] = accessToken

  // add the user to the identity pool
  const idResp = await cognitoIndentityPoolClient.getId({Logins}).promise()
  const userId = idResp.IdentityId

  // get credentials for appsync from the identity pool
  const resp = await cognitoIndentityPoolClient.getCredentialsForIdentity({IdentityId: userId, Logins}).promise()
  return resp.Credentials
}
