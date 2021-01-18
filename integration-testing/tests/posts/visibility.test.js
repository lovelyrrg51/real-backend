const fs = require('fs')
const got = require('got')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const imageBytes = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
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

test('Visiblity of post() and user.posts() for a public user', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // a user that follows us
  const {client: followerClient} = await loginCache.getCleanLogin()
  let resp = await followerClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // some rando off the internet
  const {client: randoClient} = await loginCache.getCleanLogin()

  // we add a image post, give s3 trigger a second to fire
  const postId = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId}})
  expect(resp.data.addPost.postId).toBe(postId)
  const uploadUrl = resp.data.addPost.imageUploadUrl
  expect(uploadUrl).toBeTruthy()

  // upload the image, give S3 trigger a second to fire
  await got.put(uploadUrl, {headers: imageHeaders, body: imageBytes})
  await misc.sleepUntilPostProcessed(ourClient, postId)

  // we should see the post
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toEqual([expect.objectContaining({postId})])
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toEqual([expect.objectContaining({postId})])
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toMatchObject({postId})

  // our follower should be able to see the post
  resp = await followerClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toEqual([expect.objectContaining({postId})])
  resp = await followerClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toMatchObject({postId})

  // the rando off the internet should be able to see the post
  resp = await randoClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toEqual([expect.objectContaining({postId})])
  resp = await randoClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toMatchObject({postId})
})

test('Visiblity of post() and user.posts() for a private user', async () => {
  // our user, set to private
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await ourClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // some rando off the internet
  const {client: randoClient} = await loginCache.getCleanLogin()

  // we add a image post, give s3 trigger a second to fire
  const postId = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.image).toBeTruthy()

  // we should see the post
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toEqual([expect.objectContaining({postId})])
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toEqual([expect.objectContaining({postId})])
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toMatchObject({postId})

  // the rando off the internet should *not* be able to see the post
  resp = await randoClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts).toBeNull()
  resp = await randoClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toBeNull()
})

test('Visiblity of post() and user.posts() for the follow stages user', async () => {
  // our user, set to private
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await ourClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // a user will follows us
  const {client: followerClient, userId: followerUserId} = await loginCache.getCleanLogin()

  // we add a image post, give s3 trigger a second to fire
  const postId = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.image).toBeTruthy()

  // request to follow, should *not* be able to see the post
  resp = await followerClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  resp = await followerClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toBeNull()
  resp = await followerClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.posts).toBeNull()

  // deny the follow request, should *not* be able to see the post
  resp = await ourClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: followerUserId}})
  resp = await followerClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toBeNull()
  resp = await followerClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.posts).toBeNull()

  // accept the follow request, should be able to see the post
  resp = await ourClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: followerUserId}})
  resp = await followerClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toMatchObject({postId})
  resp = await followerClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toEqual([expect.objectContaining({postId})])
})

test('Post that does not exist', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  const postId = uuidv4()
  const resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toBeNull()
})

test('Post that is not complete', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a image post, we don't complete it
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('PENDING')

  // check we can see the post
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.postStatus).toBe('PENDING')

  // check they cannot see the post
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toBeNull()
})

test('Post.viewedBy only visible to post owner', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // verify we can see the viewedBy list (and it's empty)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.viewedBy.items).toHaveLength(0)

  // verify they cannot see the viewedBy list
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.viewedBy).toBeNull()

  // they follow us
  resp = await theirClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // verify they cannot see the viewedBy list
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.viewedBy).toBeNull()
})
