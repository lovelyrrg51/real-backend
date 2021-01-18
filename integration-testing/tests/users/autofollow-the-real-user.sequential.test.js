/**
 * This test suite cannot run in parrallel with others because it
 * depends on global state - namely the 'real' user.
 */

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const realUser = require('../../utils/real-user')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
let realLogin
jest.retryTimes(1)

beforeAll(async () => {
  realLogin = await realUser.getLogin()
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => {
  await realUser.cleanLogin()
  await loginCache.clean()
})
afterAll(async () => {
  await realUser.resetLogin()
  await loginCache.reset()
})

test('new users auto-follow a user with username `real`, if they exist', async () => {
  const {userId: realUserId} = realLogin

  // create a new user. Should auto-follow the real user
  const {client, username} = await loginCache.getCleanLogin()
  await client.query({query: queries.ourFollowedUsers}).then(({data: {self: user}}) => {
    expect(user.followedUsers.items).toHaveLength(1)
    expect(user.followedUsers.items[0].userId).toBe(realUserId)
  })

  // clear out the real user, causes its username to be dropped
  await realUser.resetLogin()
  await misc.sleep(2000)
  realLogin = null

  // reset that user as a new user. Now should not auto-follow the real user this time
  await client.mutate({mutation: mutations.resetUser, variables: {newUsername: username}})
  await client
    .query({query: queries.ourFollowedUsers})
    .then(({data: {self: user}}) => expect(user.followedUsers.items).toHaveLength(0))
})
