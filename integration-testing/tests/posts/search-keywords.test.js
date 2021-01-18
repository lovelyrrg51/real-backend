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

test('Add post with keywords attribute - serach keywords', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  const [postId1, postId2] = [uuidv4(), uuidv4()]
  let keywords = ['mine', 'bird', 'tea', 'hera']

  // Add two posts
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, imageData, keywords}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.keywords.sort()).toEqual(keywords.sort())
    })

  keywords = ['tea', 'bird', 'here']
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, imageData, keywords}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.keywords.sort()).toEqual(keywords.sort())
    })
  await misc.sleep(2000) // dynamo

  let searchKeyword = 'shir'
  await ourClient
    .query({query: queries.searchKeywords, variables: {keyword: searchKeyword}})
    .then(({data: {searchKeywords: keywords}}) => {
      expect(keywords).toEqual([])
    })

  searchKeyword = 'her'
  await ourClient
    .query({query: queries.searchKeywords, variables: {keyword: searchKeyword}})
    .then(({data: {searchKeywords: keywords}}) => {
      expect(keywords.sort()).toEqual(['here', 'hera'].sort())
    })

  searchKeyword = 'min'
  await ourClient
    .query({query: queries.searchKeywords, variables: {keyword: searchKeyword}})
    .then(({data: {searchKeywords: keywords}}) => {
      expect(keywords).toEqual(['mine'])
    })
})

test('Remove post - serach keywords', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  const [postId1, postId2] = [uuidv4(), uuidv4()]
  let keywords = ['mine', 'bird', 'tea', 'hera']

  // Add two posts
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, imageData, keywords}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.keywords.sort()).toEqual(keywords.sort())
    })

  keywords = ['tea', 'bird', 'here']
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, imageData, keywords}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.keywords.sort()).toEqual(keywords.sort())
    })

  let searchKeyword = 'min'
  await ourClient
    .query({query: queries.searchKeywords, variables: {keyword: searchKeyword}})
    .then(({data: {searchKeywords: keywords}}) => {
      expect(keywords).toEqual(['mine'])
    })

  // Remove our post
  await ourClient
    .mutate({mutation: mutations.deletePost, variables: {postId: postId1}})
    .then(({data: {deletePost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('DELETING')
    })
  await misc.sleep(2000) // dynamo

  searchKeyword = 'min'
  await ourClient
    .query({query: queries.searchKeywords, variables: {keyword: searchKeyword}})
    .then(({data: {searchKeywords: keywords}}) => {
      expect(keywords).toEqual([])
    })

  searchKeyword = 'her'
  await ourClient
    .query({query: queries.searchKeywords, variables: {keyword: searchKeyword}})
    .then(({data: {searchKeywords: keywords}}) => {
      expect(keywords).toEqual(['here'])
    })
})
