#!/usr/bin/env node

const AWS = require('aws-sdk')
const AWSAppSyncClient = require('aws-appsync').default
const dotenv = require('dotenv')
const gql = require('graphql-tag')
const prmt = require('prompt')
global.fetch = require('cross-fetch')

dotenv.config()
AWS.config = new AWS.Config()

const cognitoClientId = process.env.COGNITO_TESTING_CLIENT_ID
if (cognitoClientId === undefined) throw new Error('Env var COGNITO_TESTING_CLIENT_ID must be defined')

const identityPoolId = process.env.COGNITO_IDENTITY_POOL_ID
if (identityPoolId === undefined) throw new Error('Env var COGNITO_IDENTITY_POOL_ID must be defined')

const userPoolId = process.env.COGNITO_USER_POOL_ID
if (userPoolId === undefined) throw new Error('Env var COGNITO_USER_POOL_ID must be defined')

const appsyncApiUrl = process.env.APPSYNC_GRAPHQL_URL
if (appsyncApiUrl === undefined) throw new Error('Env var APPSYNC_GRAPHQL_URL must be defined')

const cognitoIdentityPoolClient = new AWS.CognitoIdentity({params: {IdentityPoolId: identityPoolId}})
const cognitoUserPoolClient = new AWS.CognitoIdentityServiceProvider({params: {ClientId: cognitoClientId}})

prmt.message = ''
prmt.start()

const facebookHelp = `To generate:
  - create a facebook developer account if needed, get it associated with our facebook app
  - navigate to https://developers.facebook.com/tools/explorer/
  - select our app in the top-right corner
  - copy-paste the access token
`

const googleHelp = `To generate:
  - navigate to https://developers.google.com/oauthplayground/
  - click the settings gear in the top-right corner
  - select 'Use your own OAuth credentials'
  - enter our OAuth Client ID & secret from the web application listed here:
    https://console.developers.google.com/apis/credentials?project=selfly---dev-1566405434462
  - in the box on the bottom left, where it says 'Input your own scopes', enter 'email'
  - click 'Authorize APIs'
  - go through the authentication flow until you're back to the playground
  - click 'Exchange authorization code for tokens'
  - in the response json on the right, copy-paste the **id** token
`

const prmtSchema = {
  properties: {
    authSource: {
      description: 'Where is the user from? Enter `c` for Cognito, `f` for Facebook, or `g` for Google.',
      required: true,
      pattern: /^[cfg]?$/,
    },
    username: {
      description: "User's email, phone or human-readable username?",
      required: true,
      ask: () => prmt.history('authSource').value === 'c',
    },
    password: {
      description: "User's password?",
      required: true,
      hidden: true,
      ask: () => prmt.history('authSource').value === 'c',
    },
    facebookAccessToken: {
      description: `A facebook access token for our app for the User? ${facebookHelp}?`,
      required: true,
      ask: () => prmt.history('authSource').value === 'f',
    },
    googleIdToken: {
      description: `A google **id** (not access) token for the User? ${googleHelp}?`,
      required: true,
      ask: () => prmt.history('authSource').value === 'g',
    },
    newUsername: {
      description: 'New username to for this user?',
      required: true,
    },
  },
}

// Effectively the main() function
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }

  const token = await (async () => {
    if (result.authSource === 'c') {
      process.stdout.write('Signing cognito user in...')
      const tokens = await generateCognitoTokens(result.username, result.password)
      process.stdout.write(' done.\n')
      return tokens.IdToken
    }
    if (result.authSource === 'f') return result.facebookAccessToken
    if (result.authSource === 'g') return result.googleIdToken
    throw `Unrecognized auth source '${result.authSource}'`
  })()

  process.stdout.write('Exchanging auth token for graphql-authorized JWT token...')
  const creds = await generateGQLCredentials(result.authSource, token)
  const awsCredentials = new AWS.Credentials(creds.AccessKeyId, creds.SecretKey, creds.SessionToken)
  const appsyncClient = new AWSAppSyncClient(
    {
      url: appsyncApiUrl,
      region: AWS.config.region,
      auth: {
        type: 'AWS_IAM',
        credentials: awsCredentials,
      },
      disableOffline: true,
    },
    {
      defaultOptions: {
        query: {
          fetchPolicy: 'no-cache',
        },
      },
    },
  )
  process.stdout.write(' done.\n')

  process.stdout.write('Retrieving current username...')
  let resp = await appsyncClient.query({query: querySelf})
  let curUsername = resp.data.self.username
  process.stdout.write(` '${curUsername}'.\n`)

  process.stdout.write(`Changing username to '${result.newUsername}'...`)
  const variables = {username: result.newUsername}
  await appsyncClient.mutate({mutation: setUsername, variables})
  process.stdout.write(' done.\n')

  process.stdout.write('Retrieving current username again...')
  resp = await appsyncClient.query({query: querySelf})
  curUsername = resp.data.self.username
  process.stdout.write(` '${curUsername}'.\n`)
})

const setUsername = gql`
  mutation SetUserDetails($username: String!) {
    setUserDetails(username: $username) {
      userId
      username
    }
  }
`

const querySelf = gql`
  query Self {
    self {
      userId
      username
    }
  }
`

const generateCognitoTokens = async (username, password) => {
  // sign them in
  const resp = await cognitoUserPoolClient
    .initiateAuth({
      AuthFlow: 'USER_PASSWORD_AUTH',
      AuthParameters: {USERNAME: username, PASSWORD: password},
    })
    .promise()
  return resp.AuthenticationResult
}

const generateGQLCredentials = async (authSource, token) => {
  const loginsKey = (() => {
    if (authSource === 'c') return `cognito-idp.${AWS.config.region}.amazonaws.com/${userPoolId}`
    if (authSource === 'f') return 'graph.facebook.com'
    if (authSource === 'g') return 'accounts.google.com'
    throw `Unrecognized auth source '${authSource}'`
  })()
  const Logins = {[loginsKey]: token}

  // add the user to the identity pool
  const idResp = await cognitoIdentityPoolClient.getId({Logins}).promise()
  const userId = idResp.IdentityId

  // get credentials for appsync from the identity pool
  const resp = await cognitoIdentityPoolClient.getCredentialsForIdentity({IdentityId: userId, Logins}).promise()
  return resp.Credentials
}
