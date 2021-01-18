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

test('Blocking a user causes their onymous likes on our posts to dissapear', async () => {
  // us and them
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  await misc.sleep(1000) // dynamo

  // they like the post
  resp = await theirClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId}})

  // verify we can see the like
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.onymousLikeCount).toBe(1)
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(1)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(theirUserId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)

  // verify we can see the like has disappeared
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.onymousLikeCount).toBe(0)
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(0)
})

test('Blocking a user causes their anonymous likes on our posts to dissapear', async () => {
  // us and them
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // they anonmously like the post
  resp = await theirClient.mutate({mutation: mutations.anonymouslyLikePost, variables: {postId}})

  // verify they see the like
  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)

  // verify the like has disappeared
  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(0)
})

test('Blocking a user that has requested to follow us causes their follow request to dissapear', async () => {
  // us and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we go private
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await ourClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})

  // they request to follow us
  resp = await theirClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})

  // verify they can see that follow request
  resp = await theirClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(1)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(ourUserId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})

  // the follow request has disappeared
  resp = await theirClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)
})

test('Blocking a user that we have denied following to causes their follow request to dissapear', async () => {
  // us and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we go private
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await ourClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})

  // they request to follow us
  resp = await theirClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})

  // we deny that follow request
  resp = await ourClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: theirUserId}})

  // verify they can see that follow request
  resp = await theirClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(1)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(ourUserId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})

  // the follow request has disappeared
  resp = await theirClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)
})

test('Blocking a follower causes unfollowing, our posts in their feed and first story to disapear', async () => {
  // us and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they follow us
  let resp = await theirClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})

  // we add a story
  const postId = uuidv4()
  let variables = {postId, imageData, lifetime: 'PT1H'}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // verify that post shows up in their feed
  resp = await theirClient.query({query: queries.selfFeed})
  expect(resp.data.self.feed.items).toHaveLength(1)
  expect(resp.data.self.feed.items[0].postId).toBe(postId)

  // verify we show up in their followed users with stories
  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(1)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(ourUserId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})

  // verify that post does not show up in their feed
  resp = await theirClient.query({query: queries.selfFeed})
  expect(resp.data.self.feed.items).toHaveLength(0)

  // verify we do not show up in their followed users with stories
  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(0)

  // verify they are no longer following us
  resp = await ourClient.query({query: queries.ourFollowerUsers})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)
})

test('Blocking a user that we have requested to follow us causes our follow request to dissapear', async () => {
  // us and them
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they go private
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})

  // we request to follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})

  // verify we can see that follow request
  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(1)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(theirUserId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})

  // the follow request has disappeared
  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)
})

test('Blocking a user that has denied our following to causes our follow request to dissapear', async () => {
  // us and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they go private
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})

  // we request to follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})

  // they deny that follow request
  resp = await theirClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}})

  // verify we can see that follow request
  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(1)
  expect(resp.data.self.followedUsers.items[0].userId).toBe(theirUserId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})

  // the follow request has disappeared
  resp = await ourClient.query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
  expect(resp.data.self.followedUsers.items).toHaveLength(0)
})

test('Blocking a user we follow causes unfollowing, their posts in feed and first story to disapear', async () => {
  // us and them
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})

  // they post a story
  const postId = uuidv4()
  let variables = {postId, imageData, lifetime: 'PT1H'}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  await misc.sleep(1000) // dynamo

  // verify that post shows up in our feed
  resp = await ourClient.query({query: queries.selfFeed})
  expect(resp.data.self.feed.items).toHaveLength(1)
  expect(resp.data.self.feed.items[0].postId).toBe(postId)

  // verify they show up in our followed users with stories
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(1)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(theirUserId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})

  // verify that post does not show up in our feed
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.selfFeed})
  expect(resp.data.self.feed.items).toHaveLength(0)

  // verify they do not show up in our followed users with stories
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(0)

  // verify we are no longer following them
  resp = await theirClient.query({query: queries.ourFollowerUsers})
  expect(resp.data.self.followerUsers.items).toHaveLength(0)
})
