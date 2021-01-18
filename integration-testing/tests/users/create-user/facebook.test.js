const got = require('got')

const cognito = require('../../../utils/cognito.js')
const {mutations} = require('../../../schema')

jest.retryTimes(1)

/* Run me as a one-off, as you'll have to get a valid facebook access token
 * for our app. Can be generated from https://developers.facebook.com/tools/explorer/
 *
 * The email the oauth token is generated for must be one which this amazon account
 * is authorized to send to.
 */
describe.skip('facebook user', () => {
  const facebookAccessToken = process.env.FACEBOOK_ACCESS_TOKEN
  let client

  afterEach(async () => {
    if (client) await client.mutate({mutation: mutations.deleteUser})
    client = null
  })

  test('Mutation.createFacebookUser success', async () => {
    if (facebookAccessToken === undefined) throw new Error('Env var FACEBOOK_ACCESS_TOKEN must be defined')

    // facebook only returns verified emails
    // https://stackoverflow.com/questions/14280535/is-it-possible-to-check-if-an-email-is-confirmed-on-facebook
    const {email} = await got
      .get('https://graph.facebook.com/me', {searchParams: {fields: 'email', access_token: facebookAccessToken}})
      .json()

    // get and id and credentials from the identity pool
    const logins = {[cognito.facebookLoginsKey]: facebookAccessToken}
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
      .mutate({mutation: mutations.createFacebookUser, variables: {username, facebookAccessToken, fullName}})
      .then(({data: {createFacebookUser: user}}) => {
        expect(user.userId).toBe(userId)
        expect(user.username).toBe(username)
        expect(user.email).toBe(email)
        expect(user.fullName).toBe(fullName)
      })
  })
})
