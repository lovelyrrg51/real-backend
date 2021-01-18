const fs = require('fs')
const got = require('got')
const path = require('path')
const requestImageSize = require('request-image-size')
const uuidv4 = require('uuid/v4')

const cognito = require('../utils/cognito')
const misc = require('../utils/misc')
const {mutations, queries} = require('../schema')

const jpgHeaders = {'Content-Type': 'image/jpeg'}
const pngHeaders = {'Content-Type': 'image/png'}
const heicHeaders = {'Content-Type': 'image/heic'}

const imageData = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'grant.jpg'))
const imageHeight = 320
const imageWidth = 240

const bigImageData = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'big-blank.jpg'))
const bigImageHeight = 2000
const bigImageWidth = 4000

const heicImageData = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'IMG_0265.HEIC'))
const heicImageHeight = 3024
const heicImageWidth = 4032

const pngData = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'grant.png'))
const pngHeight = 320
const pngWidth = 240
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Uploading image sets width, height and colors', async () => {
  const {client} = await loginCache.getCleanLogin()

  // upload an image post
  const postId = uuidv4()
  const uploadUrl = await client
    .mutate({mutation: mutations.addPost, variables: {postId}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.image).toBeNull()
      return post.imageUploadUrl
    })

  // double check the image post
  await client.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.image).toBeNull()
  })

  // upload the first of those images, give the s3 trigger a second to fire
  await got.put(uploadUrl, {headers: jpgHeaders, body: imageData})
  await misc.sleepUntilPostProcessed(client, postId)

  // check width, height and colors are now set
  await client.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.postStatus).toBe('COMPLETED')
    expect(post.image.height).toBe(imageHeight)
    expect(post.image.width).toBe(imageWidth)
    expect(post.image.colors).toHaveLength(5)
    expect(post.image.colors[0].r).toBeTruthy()
    expect(post.image.colors[0].g).toBeTruthy()
    expect(post.image.colors[0].b).toBeTruthy()
  })
})

test('Uploading png image', async () => {
  const {client} = await loginCache.getCleanLogin()

  // create a pending image post
  const postId = uuidv4()
  const uploadUrl = await client
    .mutate({mutation: mutations.addPost, variables: {postId}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postStatus).toBe('PENDING')
      return post.imageUploadUrl
    })

  // upload a png, give the s3 trigger a second to fire
  await got.put(uploadUrl, {headers: pngHeaders, body: pngData})
  await misc.sleep(5000)

  // check that post ended up in an ERROR state
  await client.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.postStatus).toBe('COMPLETED')
    expect(post.image.height).toBe(pngHeight)
    expect(post.image.width).toBe(pngWidth)
    expect(post.image.colors).toHaveLength(5)
    expect(post.image.colors[0].r).toBeTruthy()
    expect(post.image.colors[0].g).toBeTruthy()
    expect(post.image.colors[0].b).toBeTruthy()
  })
})

test('Upload heic image', async () => {
  const {client} = await loginCache.getCleanLogin()

  // create a pending image post
  const postId = uuidv4()
  const uploadUrl = await client
    .mutate({mutation: mutations.addPost, variables: {postId, imageFormat: 'HEIC'}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postStatus).toBe('PENDING')
      expect(post.imageUploadUrl).toContain('native.heic')
      return post.imageUploadUrl
    })

  // upload a heic, give the s3 trigger a second to fire
  await got.put(uploadUrl, {headers: heicHeaders, body: heicImageData})
  await misc.sleepUntilPostProcessed(client, postId, {maxWaitMs: 20 * 1000})

  // check that post completed and generated all thumbnails ok
  const image = await client.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.postStatus).toBe('COMPLETED')
    expect(post.isVerified).toBe(true)
    expect(post.image).toBeTruthy()
    return post.image
  })

  // check the native image size dims
  await requestImageSize(image.url).then(({width, height}) => {
    expect(width).toBe(heicImageWidth)
    expect(height).toBe(heicImageHeight)
  })

  // check the 64p image size dims
  await requestImageSize(image.url64p).then(({width, height}) => {
    expect(width).toBeLessThan(114)
    expect(height).toBe(64)
  })

  // check the 480p image size dims
  await requestImageSize(image.url480p).then(({width, height}) => {
    expect(width).toBeLessThan(854)
    expect(height).toBe(480)
  })

  // check the 1080p image size dims
  await requestImageSize(image.url1080p).then(({width, height}) => {
    expect(width).toBeLessThan(1920)
    expect(height).toBe(1080)
  })

  // check the 4k image size dims
  await requestImageSize(image.url4k).then(({width, height}) => {
    expect(width).toBeLessThan(3840)
    expect(height).toBe(2160)
  })
})

test('Thumbnails built on successful upload', async () => {
  const {client} = await loginCache.getCleanLogin()

  // create a pending image post
  const postId = uuidv4()
  const uploadUrl = await client
    .mutate({mutation: mutations.addPost, variables: {postId}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postStatus).toBe('PENDING')
      return post.imageUploadUrl
    })

  // upload a big jpeg, give the s3 trigger a second to fire
  await got.put(uploadUrl, {headers: jpgHeaders, body: bigImageData})
  await misc.sleep(5000) // big jpeg, so takes at least a few seconds to process
  await misc.sleepUntilPostProcessed(client, postId)

  const image = await client.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.image).toBeTruthy()
    return post.image
  })

  // check the native image size dims
  await requestImageSize(image.url).then(({width, height}) => {
    expect(width).toBe(bigImageWidth)
    expect(height).toBe(bigImageHeight)
  })

  // check the 64p image size dims
  await requestImageSize(image.url64p).then(({width, height}) => {
    expect(width).toBe(114)
    expect(height).toBeLessThan(64)
  })

  // check the 480p image size dims
  await requestImageSize(image.url480p).then(({width, height}) => {
    expect(width).toBe(854)
    expect(height).toBeLessThan(480)
  })

  // check the 1080p image size dims
  await requestImageSize(image.url1080p).then(({width, height}) => {
    expect(width).toBe(1920)
    expect(height).toBeLessThan(1080)
  })

  // check the 4k image size dims
  await requestImageSize(image.url4k).then(({width, height}) => {
    expect(width).toBe(3840)
    expect(height).toBeLessThan(2160)
  })
})
