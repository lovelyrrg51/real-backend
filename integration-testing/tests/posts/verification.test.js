const fs = require('fs')
const got = require('got')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const smallGrantData = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
const smallGrantDataB64 = new Buffer.from(smallGrantData).toString('base64')
const bigGrantData = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'big-grant.jpg'))
const imageHeaders = {'Content-Type': 'image/jpeg'}
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Add image post passes verification', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we add a image post, check in PENDING
  const postId = uuidv4()
  let variables = {postId, takenInReal: true, originalFormat: 'HEIC', originalMetadata: '{}'}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  let post = resp.data.addPost
  expect(post.postId).toBe(postId)
  expect(post.postStatus).toBe('PENDING')
  expect(post.isVerified).toBeNull()

  // upload the image
  let uploadUrl = post.imageUploadUrl
  expect(uploadUrl).toBeTruthy()
  await got.put(uploadUrl, {headers: imageHeaders, body: bigGrantData})
  await misc.sleepUntilPostProcessed(ourClient, postId)

  // check the post is now verified
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  post = resp.data.post
  expect(post.postId).toBe(postId)
  expect(post.postStatus).toBe('COMPLETED')
  expect(post.isVerified).toBe(true)
})

test('Add image post fails verification', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we add a image post, give s3 trigger a second to fire
  const postId = uuidv4()
  let variables = {postId, imageData: smallGrantDataB64}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  let post = resp.data.addPost
  expect(post.postId).toBe(postId)
  expect(post.postStatus).toBe('COMPLETED')
  expect(post.isVerified).toBe(false)

  // check those values stuck in DB
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  post = resp.data.post
  expect(post.postId).toBe(postId)
  expect(post.postStatus).toBe('COMPLETED')
  expect(post.isVerified).toBe(false)
})

test('Add image post verification hidden hides verification state', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we add a image post with verification hidden, give s3 trigger a second to fire
  const postId = uuidv4()
  let variables = {postId, verificationHidden: true, imageData: smallGrantDataB64}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  let post = resp.data.addPost
  expect(post.postId).toBe(postId)
  expect(post.postStatus).toBe('COMPLETED')
  expect(post.verificationHidden).toBe(true)
  expect(post.isVerified).toBe(true) // even though in reality it failed verification

  // check those values stuck in DB
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  post = resp.data.post
  expect(post.postId).toBe(postId)
  expect(post.postStatus).toBe('COMPLETED')
  expect(post.isVerified).toBe(true) // even though in reality it failed verification

  // change the verification hidden setting of the post
  resp = await ourClient.mutate({mutation: mutations.editPost, variables: {postId, verificationHidden: false}})
  expect(resp.data.editPost.postId).toBe(postId)
  expect(resp.data.editPost.verificationHidden).toBe(false)

  // now the real verification status should show up
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.isVerified).toBe(false)

  // change the verification hidden setting of the post again
  resp = await ourClient.mutate({mutation: mutations.editPost, variables: {postId, verificationHidden: true}})
  expect(resp.data.editPost.postId).toBe(postId)
  expect(resp.data.editPost.verificationHidden).toBe(true)

  // now the real verification status should *not* show up
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.isVerified).toBe(true)
})

test('Post verification hidden setting is private to post owner', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post without setting verification hidden
  const postId = uuidv4()
  let variables = {postId, imageData: smallGrantDataB64}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.verificationHidden).toBe(false)

  // verify when we look at our post we see the verification hidden setting
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.verificationHidden).toBe(false)

  // verify when someone else looks at our post they do *not* see the verification hidden setting
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.verificationHidden).toBeNull()

  // we set the verification hidden setting
  resp = await ourClient.mutate({mutation: mutations.editPost, variables: {postId, verificationHidden: true}})
  expect(resp.data.editPost.postId).toBe(postId)
  expect(resp.data.editPost.verificationHidden).toBe(true)

  // verify when we look at our post we see the verification hidden setting
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.verificationHidden).toBe(true)

  // verify when someone else looks at our post they do *not* see the verification hidden setting
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.verificationHidden).toBeNull()
})
