const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

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

test('When we stop following a private user, any likes of ours on their posts disappear', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they go private
  resp = await theirClient.mutate({
    mutation: mutations.setUserPrivacyStatus,
    variables: {privacyStatus: 'PRIVATE'},
  })
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

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
  await misc.sleep(2000)
  resp = await theirClient.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post.onymousLikeCount).toBe(1)
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(1)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(ourUserId)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.onymouslyLikedPosts.items[0].postId).toBe(postId1)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId2)

  // we stop following the user
  resp = await ourClient.mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}})
  expect(resp.data.unfollowUser.followedStatus).toBe('NOT_FOLLOWING')

  // check those likes disappeared from the lists
  await misc.sleep(2000)
  resp = await theirClient.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post.onymousLikeCount).toBe(0)
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post).toBeNull() // access denied

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(0)
})

test('When a private user decides to deny our following, any likes of ours on their posts disappear', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they go private
  resp = await theirClient.mutate({
    mutation: mutations.setUserPrivacyStatus,
    variables: {privacyStatus: 'PRIVATE'},
  })
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

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
  await misc.sleep(2000)
  resp = await theirClient.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post.onymousLikeCount).toBe(1)
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(1)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(ourUserId)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.onymouslyLikedPosts.items[0].postId).toBe(postId1)
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId2)

  // now they deny our following
  resp = await theirClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}})
  expect(resp.data.denyFollowerUser.followerStatus).toBe('DENIED')

  // check we can no longer see lists of likes
  await misc.sleep(2000)
  resp = await theirClient.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post.onymousLikeCount).toBe(0)
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(0)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(0)
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(0)
})
