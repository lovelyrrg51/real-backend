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

test('Delete a post that was our next story to expire', async () => {
  // us, them, they follow us
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()
  let resp = await theirClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // we create a post
  const postId = uuidv4()
  let variables = {postId, imageData, lifetime: 'PT1H'}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // verify we see that post
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toHaveLength(1)
  expect(resp.data.user.posts.items[0].postId).toBe(postId)

  // verify we see it as a story
  resp = await ourClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories.items).toHaveLength(1)
  expect(resp.data.user.stories.items[0].postId).toBe(postId)

  // verify our post count reacted
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.postCount).toBe(1)

  // verify it showed up in their feed
  resp = await theirClient.query({query: queries.selfFeed})
  expect(resp.data.self.feed.items).toHaveLength(1)
  expect(resp.data.self.feed.items[0].postId).toBe(postId)

  // verify we show up in the first followed users list
  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(1)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(ourUserId)

  // delete the post
  resp = await ourClient.mutate({mutation: mutations.deletePost, variables: {postId}})
  expect(resp.data.deletePost.postStatus).toBe('DELETING')

  // verify we cannot see that post
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toHaveLength(0)
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId, postStatus: 'DELETING'}})
  expect(resp.data.user.posts.items).toHaveLength(0)

  // verify we cannot see it as a story
  resp = await ourClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories.items).toHaveLength(0)

  // verify our post count reacted
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.postCount).toBe(0)

  // verify it disappeared from their feed
  resp = await theirClient.query({query: queries.selfFeed})
  expect(resp.data.self.feed.items).toHaveLength(0)

  // verify we do not show up in the first followed users list
  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(0)
})

test('Deleting image post', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we create an image post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('PENDING')

  // verify we can see the post
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId, postStatus: 'PENDING'}})
  expect(resp.data.user.posts.items).toHaveLength(1)
  expect(resp.data.user.posts.items[0].postId).toBe(postId)

  // delete the post
  resp = await ourClient.mutate({mutation: mutations.deletePost, variables: {postId}})
  expect(resp.data.deletePost.postStatus).toBe('DELETING')

  // verify we can no longer see the post
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId, postStatus: 'PENDING'}})
  expect(resp.data.user.posts.items).toHaveLength(0)
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId, postStatus: 'DELETING'}})
  expect(resp.data.user.posts.items).toHaveLength(0)
})

test('Invalid attempts to delete posts', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const postId = uuidv4()

  // verify can't delete post that doens't exist
  await expect(ourClient.mutate({mutation: mutations.deletePost, variables: {postId}})).rejects.toThrow(
    'not exist',
  )

  // create a post
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId}})
  expect(resp.data.addPost.postId).toBe(postId)

  // verify another user can't delete our post
  const {client: theirClient} = await loginCache.getCleanLogin()
  await expect(
    theirClient.mutate({
      mutation: mutations.deletePost,
      variables: {postId},
    }),
  ).rejects.toThrow("another User's post")

  // verify we can actually delete that post
  resp = await ourClient.mutate({mutation: mutations.deletePost, variables: {postId}})
  expect(resp.data.deletePost.postStatus).toBe('DELETING')
})

test('Cant delete a post if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we create a post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // we disable ourselves
  resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we can't delete that post
  await expect(ourClient.mutate({mutation: mutations.deletePost, variables: {postId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('When a post is deleted, any likes of it disappear', async () => {
  // us and them, they add a post
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables})

  // we onymously like it
  resp = await ourClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId}})

  // they anonymously like it
  resp = await theirClient.mutate({mutation: mutations.anonymouslyLikePost, variables: {postId}})

  // verify the post is now in the like lists
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(1)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(ourUserId)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.onymouslyLikedPosts.items[0].postId).toBe(postId)

  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId)

  // delete the post
  resp = await theirClient.mutate({mutation: mutations.deletePost, variables: {postId}})
  expect(resp.data.deletePost.postStatus).toBe('DELETING')

  // verify the post has disappeared from the like lists
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(0)
})
