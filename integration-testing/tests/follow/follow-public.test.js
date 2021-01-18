const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

let anonClient, anonUserId
const imageBytes = misc.generateRandomJpeg(8, 8)
const imageData = new Buffer.from(imageBytes).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())
afterEach(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

test('Follow & unfollow a public user', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // check we start in a NOT_FOLLOWING state
  let resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followedStatus).toBe('NOT_FOLLOWING')
  resp = await theirClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.followerStatus).toBe('NOT_FOLLOWING')

  // we follow them, goes through immediately
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // check we have moved to a FOLLOWING state
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followedStatus).toBe('FOLLOWING')
  resp = await theirClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.followerStatus).toBe('FOLLOWING')

  // we unfollow them, goes through immediately
  resp = await ourClient.mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}})
  expect(resp.data.unfollowUser.followedStatus).toBe('NOT_FOLLOWING')

  // check we have moved to a NOT_FOLLOWING state
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.followedStatus).toBe('NOT_FOLLOWING')
  resp = await theirClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.followerStatus).toBe('NOT_FOLLOWING')
})

test('Cant follow someone if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // disable ourselves
  let resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we can't follow them
  await expect(
    ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
})

test('Anonymous users cant follow or be followed', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  ;({client: anonClient, userId: anonUserId} = await cognito.getAnonymousAppSyncLogin())

  // verify anon user can't follow us
  await expect(
    anonClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)

  // verify we can't follow them
  await expect(
    ourClient.mutate({mutation: mutations.followUser, variables: {userId: anonUserId}}),
  ).rejects.toThrow(/ClientError: Cannot follow user with status `ANONYMOUS`/)
})

test('Cant unfollow someone if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // disable ourselves
  resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we can't unfollow them
  await expect(
    ourClient.mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
})

test('Try to double follow a user', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them, goes through immediately
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // we cannot follow them again
  await expect(
    ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: .* already /)

  // verify we're still in following them
  resp = await theirClient.query({query: queries.ourFollowerUsers})
  expect(resp.data.self.followerUsers.items).toHaveLength(1)
  expect(resp.data.self.followerUsers.items[0].userId).toBe(ourUserId)

  // unfollow ther user
  resp = await ourClient.mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}})
  expect(resp.data.unfollowUser.followedStatus).toBe('NOT_FOLLOWING')

  // change the other user to private
  resp = await theirClient.mutate({
    mutation: mutations.setUserPrivacyStatus,
    variables: {privacyStatus: 'PRIVATE'},
  })
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // we follow them, goes to REQUESTED
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')

  // we cannot follow them again
  await expect(
    ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: .* already /)

  // verify we're still in REQUESTED state
  resp = await theirClient.query({query: queries.ourFollowerUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followerUsers.items).toHaveLength(1)
  expect(resp.data.self.followerUsers.items[0].userId).toBe(ourUserId)
})

test('Try to unfollow a user we are not following', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // try to unfollow them
  await expect(
    ourClient.mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: .* is not following /)
})

test('When we stop following a public user, any likes of ours on their posts are unchanged', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they add two posts
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  let variables = {postId: postId1, imageData}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  variables = {postId: postId2, imageData}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})

  // we like the first post onymously
  resp = await ourClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId: postId1}})
  expect(resp.data.onymouslyLikePost.postId).toBe(postId1)
  expect(resp.data.onymouslyLikePost.likeStatus).toBe('ONYMOUSLY_LIKED')

  // we like the second post anonymously
  resp = await ourClient.mutate({mutation: mutations.anonymouslyLikePost, variables: {postId: postId2}})
  expect(resp.data.anonymouslyLikePost.postId).toBe(postId2)
  expect(resp.data.anonymouslyLikePost.likeStatus).toBe('ANONYMOUSLY_LIKED')

  // check those likes show up in the lists
  resp = await theirClient.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(1)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(ourUserId)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.onymouslyLikedPosts.items[0].postId).toBe(postId1)
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId2)

  // we stop following the user
  resp = await ourClient.mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}})
  expect(resp.data.unfollowUser.followedStatus).toBe('NOT_FOLLOWING')

  // check nothing changed in those lists
  resp = await theirClient.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(1)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(ourUserId)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.onymouslyLikedPosts.items[0].postId).toBe(postId1)
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId2)
})
