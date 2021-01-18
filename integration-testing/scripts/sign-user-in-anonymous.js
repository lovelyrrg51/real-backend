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
    destination: {
      description: 'Filename to write the results to? leave blank for stdout',
    },
  },
}

// Prompt and get user input then display those data in console.
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }
  const gqlCreds = await generateGQLCredentials()
  const output = JSON.stringify(
    {
      authProvider: 'ANONYMOUS',
      credentials: gqlCreds,
    },
    null,
    2,
  )
  if (result.destination) fs.writeFileSync(result.destination, output + '\n')
  else console.log(output)
})

const generateGQLCredentials = async () => {
  const {IdentityId} = await cognitoIndentityPoolClient.getId().promise()
  const {Credentials} = await cognitoIndentityPoolClient.getCredentialsForIdentity({IdentityId}).promise()
  return Credentials
}
