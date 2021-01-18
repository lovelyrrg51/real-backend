const moment = require('moment')

const cognito = require('../../utils/cognito')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Grant a user a free diamond subscription', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // check we see our own basic subscription, which never expires
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.subscriptionLevel).toBe('BASIC')
    expect(user.subscriptionExpiresAt).toBeNull()
  })

  // privacy: check they can see our subscription level as well, but not the expiresAt
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.subscriptionLevel).toBe('BASIC')
    expect(user.subscriptionExpiresAt).toBeNull()
  })

  // we give grant ourselves our subscription bonus
  const subscriptionDuration = moment.duration(1, 'months')
  const before = moment().add(subscriptionDuration).toISOString()
  const expiresAt = await ourClient
    .mutate({mutation: mutations.grantUserSubscriptionBonus})
    .then(({data: {grantUserSubscriptionBonus: user}}) => {
      const after = moment().add(subscriptionDuration).toISOString()
      expect(user.userId).toBe(ourUserId)
      expect(user.subscriptionLevel).toBe('DIAMOND')
      expect(user.subscriptionExpiresAt > before).toBe(true)
      expect(user.subscriptionExpiresAt < after).toBe(true)
      return user.subscriptionExpiresAt
    })

  // privacy: check that they see that subscription as well, but the expiresAt date is hidden
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.subscriptionLevel).toBe('DIAMOND')
    expect(user.subscriptionExpiresAt).toBeNull()
  })

  // check we can't re-grant ourselves another subscription bonus
  await expect(ourClient.mutate({mutation: mutations.grantUserSubscriptionBonus})).rejects.toThrow(
    /ClientError: User `.*` has already granted themselves a subscription bonus/,
  )

  // check our subscription is as expected
  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.subscriptionLevel).toBe('DIAMOND')
    expect(user.subscriptionExpiresAt).toBe(expiresAt)
  })

  // check their subscription has not been affected by any of this
  await theirClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(theirUserId)
    expect(user.subscriptionLevel).toBe('BASIC')
    expect(user.subscriptionExpiresAt).toBeNull()
  })
})
