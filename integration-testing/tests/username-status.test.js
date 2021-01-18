const cognito = require('../utils/cognito')
const misc = require('../utils/misc')
const {queries, mutations} = require('../schema')

let anonClient, anonUsername
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())
afterEach(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

test('Check INVALID usernames', async () => {
  const {client} = await loginCache.getCleanLogin()
  const invalidUsernames = ['aa', 'a'.repeat(31), 'a!a', 'b-b', 'c?c']
  for (const username of invalidUsernames) {
    await client
      .query({query: queries.usernameStatus, variables: {username}})
      .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('INVALID'))
  }
})

test('Check username AVAILABLE/NOT_AVAILABLE', async () => {
  const {client: ourClient, username: ourUsername} = await loginCache.getCleanLogin()
  const newUsername = cognito.generateUsername()

  // check username availability
  await ourClient
    .query({query: queries.usernameStatus, variables: {username: ourUsername}})
    .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('NOT_AVAILABLE'))
  await ourClient
    .query({query: queries.usernameStatus, variables: {username: newUsername}})
    .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('AVAILABLE'))

  // change our username, check again
  await ourClient
    .mutate({mutation: mutations.setUsername, variables: {username: newUsername}})
    .then(({data: {setUserDetails: user}}) => expect(user.username).toBe(newUsername))
  await misc.sleep(2000)
  await ourClient
    .query({query: queries.usernameStatus, variables: {username: ourUsername}})
    .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('AVAILABLE'))
  await ourClient
    .query({query: queries.usernameStatus, variables: {username: newUsername}})
    .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('NOT_AVAILABLE'))
})

test('Check anonymous user can check username status', async () => {
  const {username: ourUsername} = await loginCache.getCleanLogin()
  ;({client: anonClient, username: anonUsername} = await cognito.getAnonymousAppSyncLogin())
  const newUsername = cognito.generateUsername()

  await anonClient
    .query({query: queries.usernameStatus, variables: {username: 'dashes-are-not-allowed'}})
    .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('INVALID'))
  await anonClient
    .query({query: queries.usernameStatus, variables: {username: newUsername}})
    .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('AVAILABLE'))
  await anonClient
    .query({query: queries.usernameStatus, variables: {username: ourUsername}})
    .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('NOT_AVAILABLE'))
  await anonClient
    .query({query: queries.usernameStatus, variables: {username: anonUsername}})
    .then(({data: {usernameStatus}}) => expect(usernameStatus).toBe('NOT_AVAILABLE'))
})
