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

test('Follow counts public user', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // check they have no followers or followeds
  let resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followedCount).toBe(0)
  expect(resp.data.user.followedsCount).toBe(0)
  expect(resp.data.user.followerCount).toBe(0)
  expect(resp.data.user.followersCount).toBe(0)
  expect(resp.data.user.followersRequestedCount).toBeNull()

  // we follow them, their follower count increments
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
  await misc.sleep(1000) // dynamo
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followerCount).toBe(1)
  expect(resp.data.user.followersCount).toBe(1)
  expect(resp.data.user.followersRequestedCount).toBeNull()
  expect(resp.data.user.followedCount).toBe(0)
  expect(resp.data.user.followedsCount).toBe(0)

  // verify their requested followers count didn't change
  resp = await theirClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followerCount).toBe(1)
  expect(resp.data.user.followersCount).toBe(1)
  expect(resp.data.user.followersRequestedCount).toBe(0)
  expect(resp.data.user.followedCount).toBe(0)
  expect(resp.data.user.followedsCount).toBe(0)

  // they follow us, their followed count increments
  resp = await theirClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
  await misc.sleep(1000) // dynamo
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followerCount).toBe(1)
  expect(resp.data.user.followersCount).toBe(1)
  expect(resp.data.user.followedCount).toBe(1)
  expect(resp.data.user.followedsCount).toBe(1)

  // unfollow, counts drop back down
  resp = await ourClient.mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}})
  expect(resp.data.unfollowUser.followedStatus).toBe('NOT_FOLLOWING')
  await misc.sleep(1000) // dynamo
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followedCount).toBe(1)
  expect(resp.data.user.followedsCount).toBe(1)
  expect(resp.data.user.followerCount).toBe(0)
  expect(resp.data.user.followersCount).toBe(0)

  resp = await theirClient.mutate({mutation: mutations.unfollowUser, variables: {userId: ourUserId}})
  expect(resp.data.unfollowUser.followedStatus).toBe('NOT_FOLLOWING')
  await misc.sleep(1000) // dynamo
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followerCount).toBe(0)
  expect(resp.data.user.followersCount).toBe(0)
  expect(resp.data.user.followedCount).toBe(0)
  expect(resp.data.user.followedsCount).toBe(0)
})

test('Follow counts private user', async () => {
  // create two new users, both private
  const {client: u1Client, userId: u1UserId} = await loginCache.getCleanLogin()
  let resp = await u1Client.mutate({
    mutation: mutations.setUserPrivacyStatus,
    variables: {privacyStatus: 'PRIVATE'},
  })
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')
  expect(resp.data.setUserDetails.followedCount).toBe(0)
  expect(resp.data.setUserDetails.followerCount).toBe(0)

  const {client: u2Client, userId: u2UserId} = await loginCache.getCleanLogin()
  resp = await u2Client.mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')
  expect(resp.data.setUserDetails.followedCount).toBe(0)
  expect(resp.data.setUserDetails.followerCount).toBe(0)

  // u1 requests to follow u2
  resp = await u1Client.mutate({mutation: mutations.followUser, variables: {userId: u2UserId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')
  await misc.sleep(1000) // dynamo
  resp = await u2Client.query({query: queries.self})
  expect(resp.data.self.followerCount).toBe(0)
  expect(resp.data.self.followersCount).toBe(0)
  expect(resp.data.self.followersRequestedCount).toBe(1)
  resp = await u1Client.query({query: queries.self})
  expect(resp.data.self.followedCount).toBe(0)
  expect(resp.data.self.followedsCount).toBe(0)

  // verify u1 cannot see u2's counts, lists
  resp = await u1Client.query({query: queries.user, variables: {userId: u2UserId}})
  expect(resp.data.user.followedCount).toBeNull()
  expect(resp.data.user.followedsCount).toBeNull()
  expect(resp.data.user.followedUsers).toBeNull()
  expect(resp.data.user.followerCount).toBeNull()
  expect(resp.data.user.followersCount).toBeNull()
  expect(resp.data.user.followersRequestedCount).toBeNull()
  expect(resp.data.user.followerUsers).toBeNull()

  // u2 accepts the follow request
  resp = await u2Client.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: u1UserId}})
  expect(resp.data.acceptFollowerUser.followerStatus).toBe('FOLLOWING')
  await misc.sleep(1000) // dynamo
  resp = await u2Client.query({query: queries.self})
  expect(resp.data.self.followerCount).toBe(1)
  expect(resp.data.self.followersCount).toBe(1)
  expect(resp.data.self.followersRequestedCount).toBe(0)
  resp = await u1Client.query({query: queries.self})
  expect(resp.data.self.followedCount).toBe(1)
  expect(resp.data.self.followedsCount).toBe(1)

  // verify now u1 can see u2's counts, lists
  resp = await u1Client.query({query: queries.user, variables: {userId: u2UserId}})
  expect(resp.data.user.followedCount).toBe(0)
  expect(resp.data.user.followedsCount).toBe(0)
  expect(resp.data.user.followedUsers.items).toHaveLength(0)
  expect(resp.data.user.followerCount).toBe(1)
  expect(resp.data.user.followersCount).toBe(1)
  expect(resp.data.user.followersRequestedCount).toBeNull()
  expect(resp.data.user.followerUsers.items).toHaveLength(1)

  // u2 now denies the follow request, counts go down
  resp = await u2Client.mutate({mutation: mutations.denyFollowerUser, variables: {userId: u1UserId}})
  expect(resp.data.denyFollowerUser.followerStatus).toBe('DENIED')
  await misc.sleep(1000) // dynamo
  resp = await u2Client.query({query: queries.self})
  expect(resp.data.self.followerCount).toBe(0)
  expect(resp.data.self.followersCount).toBe(0)
  expect(resp.data.self.followersRequestedCount).toBe(0)
  resp = await u1Client.query({query: queries.self})
  expect(resp.data.self.followedCount).toBe(0)
  expect(resp.data.self.followedsCount).toBe(0)

  // verify u1 cannot see u2's counts, lists
  resp = await u1Client.query({query: queries.user, variables: {userId: u2UserId}})
  expect(resp.data.user.followedCount).toBeNull()
  expect(resp.data.user.followedsCount).toBeNull()
  expect(resp.data.user.followedUsers).toBeNull()
  expect(resp.data.user.followerCount).toBeNull()
  expect(resp.data.user.followersCount).toBeNull()
  expect(resp.data.user.followersRequestedCount).toBeNull()
  expect(resp.data.user.followerUsers).toBeNull()

  // u2 re-accepts the follow request, counts go up
  resp = await u2Client.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: u1UserId}})
  expect(resp.data.acceptFollowerUser.followerStatus).toBe('FOLLOWING')
  await misc.sleep(1000) // dynamo
  resp = await u2Client.query({query: queries.self})
  expect(resp.data.self.followerCount).toBe(1)
  expect(resp.data.self.followersCount).toBe(1)
  expect(resp.data.self.followersRequestedCount).toBe(0)
  resp = await u1Client.query({query: queries.self})
  expect(resp.data.self.followedCount).toBe(1)
  expect(resp.data.self.followedsCount).toBe(1)

  // verify now u1 can see u2's counts, lists
  resp = await u1Client.query({query: queries.user, variables: {userId: u2UserId}})
  expect(resp.data.user.followedCount).toBe(0)
  expect(resp.data.user.followedsCount).toBe(0)
  expect(resp.data.user.followedUsers.items).toHaveLength(0)
  expect(resp.data.user.followerCount).toBe(1)
  expect(resp.data.user.followersCount).toBe(1)
  expect(resp.data.user.followersRequestedCount).toBeNull()
  expect(resp.data.user.followerUsers.items).toHaveLength(1)

  // unfollow, counts go back to zero
  resp = await u1Client.mutate({mutation: mutations.unfollowUser, variables: {userId: u2UserId}})
  expect(resp.data.unfollowUser.followedStatus).toBe('NOT_FOLLOWING')
  await misc.sleep(1000) // dynamo
  resp = await u2Client.query({query: queries.self})
  expect(resp.data.self.followerCount).toBe(0)
  expect(resp.data.self.followersCount).toBe(0)
  expect(resp.data.self.followersRequestedCount).toBe(0)
  expect(resp.data.self.followedCount).toBe(0)
  expect(resp.data.self.followedsCount).toBe(0)

  // verify u1 cannot see u2's counts, lists
  resp = await u1Client.query({query: queries.user, variables: {userId: u2UserId}})
  expect(resp.data.user.followedCount).toBeNull()
  expect(resp.data.user.followedsCount).toBeNull()
  expect(resp.data.user.followedUsers).toBeNull()
  expect(resp.data.user.followerCount).toBeNull()
  expect(resp.data.user.followersCount).toBeNull()
  expect(resp.data.user.followersRequestedCount).toBeNull()
  expect(resp.data.user.followerUsers).toBeNull()

  // request to follow then immediately deny, counts stay at zero
  resp = await u1Client.mutate({mutation: mutations.followUser, variables: {userId: u2UserId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')
  resp = await u2Client.mutate({mutation: mutations.denyFollowerUser, variables: {userId: u1UserId}})
  expect(resp.data.denyFollowerUser.followerStatus).toBe('DENIED')
  await misc.sleep(1000) // dynamo
  resp = await u2Client.query({query: queries.self})
  expect(resp.data.self.followerCount).toBe(0)
  expect(resp.data.self.followersCount).toBe(0)
  expect(resp.data.self.followersRequestedCount).toBe(0)
  expect(resp.data.self.followedCount).toBe(0)
  expect(resp.data.self.followedsCount).toBe(0)
})
