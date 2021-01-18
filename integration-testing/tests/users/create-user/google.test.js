const got = require('got')

const cognito = require('../../../utils/cognito.js')
const {mutations} = require('../../../schema')

jest.retryTimes(1)

/* Run me as a one-off, as you'll have to get a valid google id token
 * for our app. Can be generated from https://developers.google.com/oauthplayground/
 *
 * The email the oauth token is generated for must be one which this amazon account
 * is authorized to send to.
 */
describe.skip('google user', () => {
  const googleIdToken = process.env.GOOGLE_ID_TOKEN
  let client

  afterEach(async () => {
    if (client) await client.mutate({mutation: mutations.deleteUser})
    client = null
  })

  test('Mutation.createGoogleUser success', async () => {
    if (googleIdToken === undefined) throw new Error('Env var GOOGLE_ID_TOKEN must be defined')

    // get the email associated with the token from google
    const email = await got
      .get('https://oauth2.googleapis.com/tokeninfo', {searchParams: {id_token: googleIdToken}})
      .json()
      .then(({email, email_verified}) => {
        expect(email_verified).toBe('true') // it's a string... ?
        return email
      })

    // get and id and credentials from the identity pool
    const logins = {[cognito.googleLoginsKey]: googleIdToken}
    const {IdentityId: userId} = await cognito.identityPoolClient.getId({Logins: logins}).promise()
    const {Credentials} = await cognito.identityPoolClient
      .getCredentialsForIdentity({IdentityId: userId, Logins: logins})
      .promise()

    // get appsync client with those creds
    client = await cognito.getAppSyncClient(Credentials)

    // pick a random username, register it, check all is good!
    const username = cognito.generateUsername()
    const fullName = 'a full name'
    await client
      .mutate({mutation: mutations.createGoogleUser, variables: {username, googleIdToken, fullName}})
      .then(({data: {createGoogleUser: user}}) => {
        expect(user.userId).toBe(userId)
        expect(user.username).toBe(username)
        expect(user.email).toBe(email)
        expect(user.fullName).toBe(fullName)
      })
  })
})
