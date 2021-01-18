const got = require('got')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const imageBytes = misc.generateRandomJpeg(8, 8)
const imageData = new Buffer.from(imageBytes).toString('base64')
const imageHeaders = {'Content-Type': 'image/jpeg'}
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Cant request over 100 of any of the like lists', async () => {
  // we add a post
  const {client: ourClient} = await loginCache.getCleanLogin()
  const postId = uuidv4()
  let variables = {postId, imageData}
  await ourClient.mutate({mutation: mutations.addPost, variables})

  // verify these queries go through without error with just under the limit
  await ourClient.query({query: queries.post, variables: {postId, onymouslyLikedByLimit: 100}})
  await ourClient.query({query: queries.self, variables: {onymouslyLikedPostsLimit: 100}})
  await ourClient.query({query: queries.self, variables: {anonymouslyLikedPostsLimit: 100}})

  // verify they fail when asking for just over the limit
  await ourClient
    .query({query: queries.post, variables: {postId, onymouslyLikedByLimit: 101}, errorPolicy: 'all'})
    .then(({errors}) => expect(errors.length).toBeTruthy())
  await ourClient
    .query({query: queries.self, variables: {onymouslyLikedPostsLimit: 101}, errorPolicy: 'all'})
    .then(({errors}) => expect(errors.length).toBeTruthy())
  await ourClient
    .query({query: queries.self, variables: {anonymouslyLikedPostsLimit: 101}, errorPolicy: 'all'})
    .then(({errors}) => expect(errors.length).toBeTruthy())
})

test('Order of users that have onymously liked a post', async () => {
  // us and two other private users
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  await misc.sleep(1000) // dynamo

  // all three of us onymously like it
  resp = await other2Client.mutate({mutation: mutations.onymouslyLikePost, variables: {postId}})
  resp = await ourClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId}})
  resp = await other1Client.mutate({mutation: mutations.onymouslyLikePost, variables: {postId}})

  // check details on the post
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  let post = resp.data.post
  expect(post.likeStatus).toBe('ONYMOUSLY_LIKED')
  expect(post.onymousLikeCount).toBe(3)
  expect(post.onymouslyLikedBy.items).toHaveLength(3)
  expect(post.onymouslyLikedBy.items[0].userId).toBe(other2UserId)
  expect(post.onymouslyLikedBy.items[1].userId).toBe(ourUserId)
  expect(post.onymouslyLikedBy.items[2].userId).toBe(other1UserId)

  // check order of list of users that onymously liked the post
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(3)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(other2UserId)
  expect(resp.data.post.onymouslyLikedBy.items[1].userId).toBe(ourUserId)
  expect(resp.data.post.onymouslyLikedBy.items[2].userId).toBe(other1UserId)

  // we dislike it
  resp = await ourClient.mutate({mutation: mutations.dislikePost, variables: {postId}})
  post = resp.data.dislikePost
  expect(post.likeStatus).toBe('NOT_LIKED')
  expect(post.onymousLikeCount).toBe(2)
  expect(post.onymouslyLikedBy.items).toHaveLength(2)
  expect(post.onymouslyLikedBy.items[0].userId).toBe(other2UserId)
  expect(post.onymouslyLikedBy.items[1].userId).toBe(other1UserId)

  // check order of list of users that onymously liked the post
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(2)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(other2UserId)
  expect(resp.data.post.onymouslyLikedBy.items[1].userId).toBe(other1UserId)

  // other2 dislikes it
  resp = await other2Client.mutate({mutation: mutations.dislikePost, variables: {postId}})
  post = resp.data.dislikePost
  expect(post.likeStatus).toBe('NOT_LIKED')
  expect(post.onymousLikeCount).toBeNull()
  expect(post.onymouslyLikedBy).toBeNull()

  // double check the post
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  post = resp.data.post
  expect(post.likeStatus).toBe('NOT_LIKED')
  expect(post.onymousLikeCount).toBe(1)
  expect(post.onymouslyLikedBy.items).toHaveLength(1)
  expect(post.onymouslyLikedBy.items[0].userId).toBe(other1UserId)
})

test('Order of onymously liked posts', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we add two posts
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  let variables = {postId: postId1, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  variables = {postId: postId2, imageData}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})

  // we onymously like both in reverse order
  resp = await ourClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId: postId2}})
  resp = await ourClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId: postId1}})

  // check list order
  resp = await ourClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.onymouslyLikedPosts.items).toHaveLength(2)
  expect(resp.data.user.onymouslyLikedPosts.items[0].postId).toBe(postId1)
  expect(resp.data.user.onymouslyLikedPosts.items[1].postId).toBe(postId2)

  // dislike the older one and re-like it
  resp = await ourClient.mutate({mutation: mutations.dislikePost, variables: {postId: postId2}})
  resp = await ourClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId: postId2}})

  // check list order has reversed
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(2)
  expect(resp.data.self.onymouslyLikedPosts.items[0].postId).toBe(postId2)
  expect(resp.data.self.onymouslyLikedPosts.items[1].postId).toBe(postId1)
})

test('Order of anonymously liked posts', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we add two posts
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  let variables = {postId: postId1, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  variables = {postId: postId2, imageData}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})

  // we anonymously like both in reverse order
  resp = await ourClient.mutate({mutation: mutations.anonymouslyLikePost, variables: {postId: postId2}})
  resp = await ourClient.mutate({mutation: mutations.anonymouslyLikePost, variables: {postId: postId1}})

  // check list order
  resp = await ourClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.anonymouslyLikedPosts.items).toHaveLength(2)
  expect(resp.data.user.anonymouslyLikedPosts.items[0].postId).toBe(postId1)
  expect(resp.data.user.anonymouslyLikedPosts.items[1].postId).toBe(postId2)

  // dislike the older one and re-like it
  resp = await ourClient.mutate({mutation: mutations.dislikePost, variables: {postId: postId2}})
  resp = await ourClient.mutate({mutation: mutations.anonymouslyLikePost, variables: {postId: postId2}})

  // check list order has reversed
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(2)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId2)
  expect(resp.data.self.anonymouslyLikedPosts.items[1].postId).toBe(postId1)
})

test('Media objects show up correctly in lists of liked posts', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // add an image post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId}})
  const uploadUrl = resp.data.addPost.imageUploadUrl
  await got.put(uploadUrl, {headers: imageHeaders, body: imageBytes})
  await misc.sleepUntilPostProcessed(ourClient, postId)

  // we anonymously like the post, they onymously like it
  resp = await ourClient.mutate({mutation: mutations.anonymouslyLikePost, variables: {postId}})
  resp = await theirClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId}})

  // we check our list of posts we anonymously liked
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].image.url).toBeTruthy()

  // we check their list of posts they onymously liked
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.onymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.user.onymouslyLikedPosts.items[0].postId).toBe(postId)
  expect(resp.data.user.onymouslyLikedPosts.items[0].image.url).toBeTruthy()
})

test('Like lists and counts are private to the owner of the post', async () => {
  // https://github.com/real-social-media/backend/issues/16
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, postType: 'TEXT_ONLY', text: 'lore ipsum'}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // verify we can see like counts on the post
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.anonymousLikeCount).toBe(0)
  expect(resp.data.post.onymousLikeCount).toBe(0)
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(0)

  // verify they cannot see like counts on the post
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.anonymousLikeCount).toBeNull()
  expect(resp.data.post.onymousLikeCount).toBeNull()
  expect(resp.data.post.onymouslyLikedBy).toBeNull()

  // they like the post
  resp = await theirClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId}})
  expect(resp.data.onymouslyLikePost.postId).toBe(postId)
  expect(resp.data.onymouslyLikePost.likeStatus).toBe('ONYMOUSLY_LIKED')

  // verify we can see that like reflected in the totals
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.anonymousLikeCount).toBe(0)
  expect(resp.data.post.onymousLikeCount).toBe(1)
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(1)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(theirUserId)

  // verify they cannot see that like reflected in the totals
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.anonymousLikeCount).toBeNull()
  expect(resp.data.post.onymousLikeCount).toBeNull()
  expect(resp.data.post.onymouslyLikedBy).toBeNull()
})
