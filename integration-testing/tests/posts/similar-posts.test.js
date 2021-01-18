const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

let anonClient
const imageBytes = misc.generateRandomJpeg(300, 200)
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

test('Add post with keywords attribute', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  const [postId1, postId2, postId3, postId4] = [uuidv4(), uuidv4(), uuidv4(), uuidv4()]
  let keywords = ['mine', 'bird']

  // Add three posts
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, imageData, keywords}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.keywords.sort()).toEqual(keywords.sort())
    })

  keywords = ['tea', 'here']
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, imageData, keywords}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.keywords.sort()).toEqual(keywords.sort())
    })

  keywords = ['shirt', 'bug', 'bird', 'here']
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId3, imageData, keywords}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId3)
      expect(post.keywords.sort()).toEqual(keywords.sort())
    })

  keywords = []
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId4, imageData, keywords}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId4)
      expect(post.keywords).toEqual(keywords)
    })

  await misc.sleep(2000) // dynamo
  await ourClient
    .query({query: queries.similarPosts, variables: {postId: postId1}})
    .then(({data: {similarPosts: posts}}) => {
      expect(posts.items).toHaveLength(2)
      expect(posts.items.map((post) => post.postId).sort()).toEqual([postId1, postId3].sort())
      expect(posts.items.map((post) => post.postedBy.userId).sort()).toEqual([ourUserId, theirUserId].sort())
    })

  await ourClient
    .query({query: queries.similarPosts, variables: {postId: postId2}})
    .then(({data: {similarPosts: posts}}) => {
      expect(posts.items).toHaveLength(2)
      expect(posts.items.map((post) => post.postId).sort()).toEqual([postId2, postId3].sort())
      expect(posts.items.map((post) => post.postedBy.userId).sort()).toEqual([theirUserId, theirUserId])
    })

  await expect(ourClient.query({query: queries.similarPosts, variables: {postId: postId4}})).rejects.toThrow(
    /ClientError: Empty keywords are not allowed/,
  )
})
