const cognito = require('../../utils/cognito')
const {mutations, queries} = require('../../schema')
const misc = require('../../utils/misc')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('hideFollowCounts hides follow counts and followe[r|d]Users lists', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // verify defaults
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.followCountsHidden).toBe(false)
    expect(self.followerCount).toBe(0)
    expect(self.followersCount).toBe(0)
    expect(self.followedCount).toBe(0)
    expect(self.followedsCount).toBe(0)
  })
  await ourClient
    .query({query: queries.ourFollowerUsers})
    .then(({data: {self}}) => expect(self.followerUsers.items).toHaveLength(0))
  await ourClient
    .query({query: queries.ourFollowedUsers})
    .then(({data: {self}}) => expect(self.followedUsers.items).toHaveLength(0))

  // they follow us, we follow them
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('FOLLOWING'))
  await theirClient
    .mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('FOLLOWING'))

  // check our followCountsHidden state, and our follow counts, other user can't see our setting
  await misc.sleep(1000)
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.followCountsHidden).toBeNull()
    expect(user.followerCount).toBe(1)
    expect(user.followersCount).toBe(1)
    expect(user.followedCount).toBe(1)
    expect(user.followedsCount).toBe(1)
  })
  await theirClient
    .query({query: queries.followerUsers, variables: {userId: ourUserId}})
    .then(({data: {user}}) => {
      expect(user.followerUsers.items).toHaveLength(1)
      expect(user.followerUsers.items[0].userId).toBe(theirUserId)
    })
  await theirClient
    .query({query: queries.followedUsers, variables: {userId: ourUserId}})
    .then(({data: {user}}) => {
      expect(user.followedUsers.items).toHaveLength(1)
      expect(user.followedUsers.items[0].userId).toBe(theirUserId)
    })

  // hide our follow counts
  await ourClient
    .mutate({mutation: mutations.setUserFollowCountsHidden, variables: {value: true}})
    .then(({data: {setUserDetails: user}}) => expect(user.followCountsHidden).toBe(true))

  // verify those counts are no longer visible by the other user
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.followCountsHidden).toBeNull()
    expect(user.followerCount).toBeNull()
    expect(user.followersCount).toBeNull()
    expect(user.followedCount).toBeNull()
    expect(user.followedsCount).toBeNull()
  })
  await theirClient
    .query({query: queries.followerUsers, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.followerUsers).toBeNull())
  await theirClient
    .query({query: queries.followedUsers, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.followedUsers).toBeNull())

  // verify we can still see our own counts
  // TODO: should we be able to see this? Or is this a hide-it-from-yourself setting?
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.followCountsHidden).toBe(true)
    expect(self.followerCount).toBe(1)
    expect(self.followersCount).toBe(1)
    expect(self.followedCount).toBe(1)
    expect(self.followedsCount).toBe(1)
  })
  await ourClient.query({query: queries.ourFollowerUsers}).then(({data: {self}}) => {
    expect(self.followerUsers.items).toHaveLength(1)
    expect(self.followerUsers.items[0].userId).toBe(theirUserId)
  })
  await ourClient.query({query: queries.ourFollowedUsers}).then(({data: {self}}) => {
    expect(self.followedUsers.items).toHaveLength(1)
    expect(self.followedUsers.items[0].userId).toBe(theirUserId)
  })

  // reveal our follow counts
  await ourClient
    .mutate({mutation: mutations.setUserFollowCountsHidden, variables: {value: false}})
    .then(({data: {setUserDetails: user}}) => expect(user.followCountsHidden).toBe(false))

  // verify the other user can again see those counts
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.followCountsHidden).toBeNull()
    expect(user.followerCount).toBe(1)
    expect(user.followersCount).toBe(1)
    expect(user.followedCount).toBe(1)
    expect(user.followedsCount).toBe(1)
  })
  await theirClient
    .query({query: queries.followerUsers, variables: {userId: ourUserId}})
    .then(({data: {user}}) => {
      expect(user.followerUsers.items).toHaveLength(1)
      expect(user.followerUsers.items[0].userId).toBe(theirUserId)
    })
  await theirClient
    .query({query: queries.followedUsers, variables: {userId: ourUserId}})
    .then(({data: {user}}) => {
      expect(user.followedUsers.items).toHaveLength(1)
      expect(user.followedUsers.items[0].userId).toBe(theirUserId)
    })
})
