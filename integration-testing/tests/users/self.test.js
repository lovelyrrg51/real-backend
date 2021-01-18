const cognito = require('../../utils/cognito')
const {mutations, queries} = require('../../schema')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Query.self for user that exists, matches Query.user', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  const selfItem = await ourClient.query({query: queries.self}).then(({data}) => {
    expect(data.self.userId).toBe(ourUserId)
    return data.self
  })

  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data}) => {
    expect(data.user.userId).toBe(ourUserId)
    expect(data.user).toEqual(selfItem)
  })
})

test('Query.self for user that does not exist', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // reset user to remove from dynamo
  await ourClient
    .mutate({mutation: mutations.resetUser})
    .then(({data}) => expect(data.resetUser.userId).toBe(ourUserId))

  // verify system see us as not registered yet
  await ourClient.query({query: queries.self, errorPolicy: 'all'}).then(({data, errors}) => {
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toEqual('ClientError: User does not exist')
    expect(data).toBeNull()
  })
})

test('Query.user matches Query.self', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  const selfItem = await ourClient.query({query: queries.self}).then(({data}) => {
    expect(data.self.userId).toBe(ourUserId)
    return data.self
  })

  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data}) => {
    expect(data.user.userId).toBe(ourUserId)
    expect(data.user).toEqual(selfItem)
  })
})
