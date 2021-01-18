const cognito = require('../../utils/cognito')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('getFollowe[d|r]Users cant request NOT_FOLLOWING', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  let variables = {userId: ourUserId, followStatus: 'NOT_FOLLOWING'}
  await ourClient.query({query: queries.followedUsers, variables, errorPolicy: 'all'}).then(({data, errors}) => {
    expect(errors.length).toBeTruthy()
    expect(data.user.followedUsers).toBeNull()
  })
  await ourClient.query({query: queries.followerUsers, variables, errorPolicy: 'all'}).then(({data, errors}) => {
    expect(errors.length).toBeTruthy()
    expect(data.user.followerUsers).toBeNull()
  })
})

test('getFollowe[d|r]Users queries respond correctly for each followStatus', async () => {
  // two new private users
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await ourClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  variables = {privacyStatus: 'PRIVATE'}
  resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // there should be no followe[d|r] users
  resp = await ourClient.query({query: queries.ourFollowedUsers})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'FOLLOWING'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'FOLLOWING'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  // we follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})

  // should now be REQUESTED state
  resp = await ourClient.query({query: queries.ourFollowedUsers})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'FOLLOWING'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(1)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(theirUserId)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'FOLLOWING'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(1)
  expect(resp.data.self.followerUsers.items[0].userId).toBe(ourUserId)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  // they accept the follow request
  resp = await theirClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: ourUserId}})
  expect(resp.data.acceptFollowerUser.followerStatus).toBe('FOLLOWING')

  // should now be FOLLOWING state
  resp = await ourClient.query({query: queries.ourFollowedUsers})
  expect(resp.data.self.followedUsers.items).toHaveLength(1)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(theirUserId)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'FOLLOWING'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(1)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(theirUserId)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers})
  expect(resp.data.self.followerUsers.items).toHaveLength(1)
  expect(resp.data.self.followerUsers.items[0].userId).toBe(ourUserId)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'FOLLOWING'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(1)
  expect(resp.data.self.followerUsers.items[0].userId).toBe(ourUserId)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  // they change their mind and now deny the follow request
  resp = await theirClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}})
  expect(resp.data.denyFollowerUser.followerStatus).toBe('DENIED')

  // should now be DENIED state
  resp = await ourClient.query({query: queries.ourFollowedUsers})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'FOLLOWING'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(1)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(theirUserId)

  resp = await theirClient.query({query: queries.ourFollowerUsers})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'FOLLOWING'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(1)
  expect(resp.data.self.followerUsers.items[0].userId).toBe(ourUserId)
})

test('Get Followe[d|r] Users order', async () => {
  // us and three others
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId} = await loginCache.getCleanLogin()
  const {client: other3Client, userId: other3UserId} = await loginCache.getCleanLogin()

  // we follow all of them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: other1UserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: other2UserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: other3UserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they all follow us
  resp = await other1Client.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
  resp = await other2Client.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
  resp = await other3Client.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // verify our followed users is in the right order (most recent first)
  resp = await ourClient.query({query: queries.ourFollowedUsers})
  expect(resp.data.self.followedUsers.items).toHaveLength(3)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(other3UserId)
  expect(resp.data.self.followedUsers.items[1].userId).toBe(other2UserId)
  expect(resp.data.self.followedUsers.items[2].userId).toBe(other1UserId)

  // verify our follower users is in the right order (most recent first)
  resp = await ourClient.query({query: queries.ourFollowerUsers})
  expect(resp.data.self.followerUsers.items).toHaveLength(3)
  expect(resp.data.self.followerUsers.items[0].userId).toBe(other3UserId)
  expect(resp.data.self.followerUsers.items[1].userId).toBe(other2UserId)
  expect(resp.data.self.followerUsers.items[2].userId).toBe(other1UserId)
})

test('getFollowe[d|r]Users queries only allow followStatus FOLLOWING when querying about other users', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {userId} = await loginCache.getCleanLogin()

  // we can see their FOLLOWING relationships
  let resp = await ourClient.query({query: queries.followedUsers, variables: {userId}})
  expect(resp.data.user.followedUsers.items).toHaveLength(0)
  resp = await ourClient.query({query: queries.followedUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followedUsers.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.followerUsers, variables: {userId: userId}})
  expect(resp.data.user.followerUsers.items).toHaveLength(0)
  resp = await ourClient.query({query: queries.followerUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followerUsers.items).toHaveLength(0)

  // we can *not* see their REQUESTED relationships
  await ourClient
    .query({query: queries.followedUsers, variables: {followStatus: 'REQUESTED', userId}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data.user.followedUsers).toBeNull()
    })
  await ourClient
    .query({query: queries.followerUsers, variables: {followStatus: 'REQUESTED', userId}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data.user.followerUsers).toBeNull()
    })

  // we can *not* see their DENIED relationships
  await ourClient
    .query({query: queries.followedUsers, variables: {followStatus: 'DENIED', userId}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data.user.followedUsers).toBeNull()
    })
  await ourClient
    .query({query: queries.followerUsers, variables: {followStatus: 'DENIED', userId}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data.user.followerUsers).toBeNull()
    })
})

test('getFollowe[d|r]Users queries correctly hide responses when querying about other private users', async () => {
  // another private user, don't follow them yet
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId} = await loginCache.getCleanLogin()
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // we can *not* see their FOLLOWING relationships
  resp = await ourClient.query({query: queries.followedUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followedUsers).toBeNull()
  resp = await ourClient.query({query: queries.followerUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followerUsers).toBeNull()

  // request to follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: userId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')

  // still can't see FOLLOWING relationships
  resp = await ourClient.query({query: queries.followedUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followedUsers).toBeNull()
  resp = await ourClient.query({query: queries.followerUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followerUsers).toBeNull()

  // they deny the follow request
  resp = await theirClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}})
  expect(resp.data.denyFollowerUser.followerStatus).toBe('DENIED')

  // still can't see FOLLOWING relationships
  resp = await ourClient.query({query: queries.followedUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followedUsers).toBeNull()
  resp = await ourClient.query({query: queries.followerUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followerUsers).toBeNull()

  // they accept the follow request
  resp = await theirClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: ourUserId}})
  expect(resp.data.acceptFollowerUser.followerStatus).toBe('FOLLOWING')

  // now we *can* see FOLLOWING relationships
  resp = await ourClient.query({query: queries.followedUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followedUsers.items).toHaveLength(0)
  resp = await ourClient.query({query: queries.followerUsers, variables: {followStatus: 'FOLLOWING', userId}})
  expect(resp.data.user.followerUsers.items).toHaveLength(1)

  // we still *cannot* see their REQUESTED relationships
  await ourClient
    .query({query: queries.followedUsers, variables: {followStatus: 'REQUESTED', userId}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors).toHaveLength(1)
      expect(data.user.followedUsers).toBeNull()
    })
  await ourClient
    .query({query: queries.followerUsers, variables: {followStatus: 'REQUESTED', userId}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors).toHaveLength(1)
      expect(data.user.followerUsers).toBeNull()
    })

  // we still *cannot* see their DENIED relationships
  await ourClient
    .query({query: queries.followedUsers, variables: {followStatus: 'DENIED', userId}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors).toHaveLength(1)
      expect(data.user.followedUsers).toBeNull()
    })
  await ourClient
    .query({query: queries.followerUsers, variables: {followStatus: 'DENIED', userId}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors).toHaveLength(1)
      expect(data.user.followerUsers).toBeNull()
    })
})
