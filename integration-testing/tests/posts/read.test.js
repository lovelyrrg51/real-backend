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

test('Filter User.posts by variour criteria', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const completedImagePostId = uuidv4()
  const pendingImagePostId = uuidv4()
  const completedTextOnlyPostId = uuidv4()
  const archivedTextOnlyPostId = uuidv4()

  // add a completed image post
  let variables = {postId: completedImagePostId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(completedImagePostId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.postType).toBe('IMAGE')

  // add pending image post
  variables = {postId: pendingImagePostId}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(pendingImagePostId)
  expect(resp.data.addPost.postStatus).toBe('PENDING')
  expect(resp.data.addPost.postType).toBe('IMAGE')

  // add completed text-only post
  variables = {postId: completedTextOnlyPostId, postType: 'TEXT_ONLY', text: 't'}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(completedTextOnlyPostId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.postType).toBe('TEXT_ONLY')

  // add archived text-only post
  variables = {postId: archivedTextOnlyPostId, postType: 'TEXT_ONLY', text: 't'}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(archivedTextOnlyPostId)
  expect(resp.data.addPost.postType).toBe('TEXT_ONLY')
  resp = await ourClient.mutate({mutation: mutations.archivePost, variables})
  expect(resp.data.archivePost.postId).toBe(archivedTextOnlyPostId)
  expect(resp.data.archivePost.postStatus).toBe('ARCHIVED')

  // check user's completed posts
  variables = {userId: ourUserId}
  resp = await ourClient.query({query: queries.userPosts, variables})
  expect(resp.data.user.posts.items).toHaveLength(2)
  expect(resp.data.user.posts.items[0].postId).toBe(completedTextOnlyPostId)
  expect(resp.data.user.posts.items[1].postId).toBe(completedImagePostId)

  // check user's pending text-only posts
  variables = {userId: ourUserId, postStatus: 'PENDING', postType: 'TEXT_ONLY'}
  resp = await ourClient.query({query: queries.userPosts, variables})
  expect(resp.data.user.posts.items).toHaveLength(0)

  // check user's archived text-only posts
  variables = {userId: ourUserId, postStatus: 'ARCHIVED', postType: 'TEXT_ONLY'}
  resp = await ourClient.query({query: queries.userPosts, variables})
  expect(resp.data.user.posts.items).toHaveLength(1)
  expect(resp.data.user.posts.items[0].postId).toBe(archivedTextOnlyPostId)

  // check user's pending posts
  variables = {userId: ourUserId, postStatus: 'PENDING'}
  resp = await ourClient.query({query: queries.userPosts, variables})
  expect(resp.data.user.posts.items).toHaveLength(1)
  expect(resp.data.user.posts.items[0].postId).toBe(pendingImagePostId)

  // check user's completed image posts
  variables = {userId: ourUserId, postStatus: 'COMPLETED', postType: 'IMAGE'}
  resp = await ourClient.query({query: queries.userPosts, variables})
  expect(resp.data.user.posts.items).toHaveLength(1)
  expect(resp.data.user.posts.items[0].postId).toBe(completedImagePostId)
})
