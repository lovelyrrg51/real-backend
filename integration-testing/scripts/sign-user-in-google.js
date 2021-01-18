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

const playgroundUrl = 'https://developers.google.com/oauthplayground/'
const prmtSchema = {
  properties: {
    idToken: {
      description: `Google id token with email scope (ie from ${playgroundUrl}, with GoogleOAuth2v2 API)`,
      required: true,
    },
    destination: {
      description: 'Filename to write the results to? leave blank for stdout',
    },
  },
}

console.log('To generate a Google id token for testing:')
console.log('  - you need developer access to the Google REAL app')
console.log('  - go to `https://developers.google.com/oauthplayground/`')
console.log('  - click the settings gear in the top-right corner')
console.log('  - select "Use your own OAuth credentials"')
console.log('  - enter our OAuth Client ID & secret from the web application listed here:')
console.log('    `https://console.developers.google.com/apis/credentials?project=selfly---dev-1566405434462`')
console.log('  - in the box on the bottom left, where it says "Input your own scopes", enter "email"')
console.log('  - click "Authorize APIs"')
console.log('  - go through the authentication flow until you are back to the playground')
console.log('  - click "Exchange authorization code for tokens"')
console.log('  - in the response json on the right the id token will be displayed')

// Prompt and get user input then display those data in console.
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }
  const gqlCreds = await generateGQLCredentials(result.idToken)
  const output = JSON.stringify(
    {
      authProvider: 'GOOGLE',
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
  Logins['accounts.google.com'] = idToken

  // add the user to the identity pool
  const idResp = await cognitoIndentityPoolClient.getId({Logins}).promise()
  const userId = idResp.IdentityId

  // get credentials for appsync from the identity pool
  const resp = await cognitoIndentityPoolClient.getCredentialsForIdentity({IdentityId: userId, Logins}).promise()
  return resp.Credentials
}
