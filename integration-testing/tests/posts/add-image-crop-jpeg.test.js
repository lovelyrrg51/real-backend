const exifReader = require('exif-reader')
const fs = require('fs')
const got = require('got')
const path = require('path')
const requestImageSize = require('request-image-size')
const sharp = require('sharp')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const jpegHeight = 32
const jpegWidth = 64
const jpegBytes = misc.generateRandomJpeg(jpegWidth, jpegHeight)
const jpegData = new Buffer.from(jpegBytes).toString('base64')
const jpegHeaders = {'Content-Type': 'image/jpeg'}
const grantBytes = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
const grantData = new Buffer.from(grantBytes).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Invalid jpeg crops, direct gql data upload', async () => {
  const {client} = await loginCache.getCleanLogin()

  // can't crop negative
  const postId1 = uuidv4()
  await expect(
    client.mutate({
      mutation: mutations.addPost,
      variables: {
        postId: postId1,
        imageData: jpegData,
        crop: {upperLeft: {x: 1, y: -1}, lowerRight: {x: jpegWidth - 1, y: jpegHeight - 1}},
      },
    }),
  ).rejects.toThrow(/ClientError: .* cannot be negative/)

  // can't down to zero area
  const postId2 = uuidv4()
  await expect(
    client.mutate({
      mutation: mutations.addPost,
      variables: {
        postId: postId2,
        imageData: jpegData,
        crop: {upperLeft: {x: 100, y: 1}, lowerRight: {x: 100, y: jpegHeight - 1}},
      },
    }),
  ).rejects.toThrow(/ClientError: .* must be strictly greater than /)

  // can't crop wider than post is. Post gets created and left in ERROR state in backend
  const postId3 = uuidv4()
  await client
    .mutate({
      mutation: mutations.addPost,
      variables: {
        postId: postId3,
        imageData: jpegData,
        crop: {upperLeft: {x: 1, y: 1}, lowerRight: {x: jpegWidth + 1, y: jpegHeight - 1}},
      },
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId3)
      expect(post.postStatus).toBe('ERROR')
    })

  // double check DB state on all those posts
  await client
    .query({query: queries.postsThree, variables: {postId1, postId2, postId3}})
    .then(({data: {post1, post2, post3}}) => {
      expect(post1).toBeNull()
      expect(post2).toBeNull()
      expect(post3.postId).toBe(postId3)
      expect(post3.postStatus).toBe('ERROR')
    })
})

test('Invalid jpeg crops, upload via cloudfront', async () => {
  const {client} = await loginCache.getCleanLogin()

  // add a post that with a crop that's too wide
  const postId1 = uuidv4()
  const uploadUrl = await client
    .mutate({
      mutation: mutations.addPost,
      variables: {
        postId: postId1,
        crop: {upperLeft: {x: 1, y: 1}, lowerRight: {x: jpegWidth + 1, y: jpegHeight - 1}},
      },
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('PENDING')
      expect(post.imageUploadUrl).toBeTruthy()
      return post.imageUploadUrl
    })
  await got.put(uploadUrl, {headers: jpegHeaders, body: jpegBytes})
  await misc.sleepUntilPostProcessed(client, postId1)
  // we check the post ended up in error state at the end of the test

  // can't crop negative
  const postId2 = uuidv4()
  await expect(
    client.mutate({
      mutation: mutations.addPost,
      variables: {
        postId: postId2,
        crop: {upperLeft: {x: 1, y: -1}, lowerRight: {x: jpegWidth - 1, y: jpegHeight - 1}},
      },
    }),
  ).rejects.toThrow(/ClientError: .* cannot be negative/)

  // can't down to zero area
  const postId3 = uuidv4()
  await expect(
    client.mutate({
      mutation: mutations.addPost,
      variables: {postId: postId3, crop: {upperLeft: {x: 100, y: 1}, lowerRight: {x: 100, y: jpegHeight - 1}}},
    }),
  ).rejects.toThrow(/ClientError: .* must be strictly greater than /)

  // check DB state on all those posts
  await client
    .query({query: queries.postsThree, variables: {postId1, postId2, postId3}})
    .then(({data: {post1, post2, post3}}) => {
      expect(post1.postStatus).toBe('ERROR')
      expect(post2).toBeNull()
      expect(post3).toBeNull()
    })
})

test('Valid jpeg crop, direct upload via gql', async () => {
  const {client} = await loginCache.getCleanLogin()

  // add the post
  const postId = uuidv4()
  const postImage = await client
    .mutate({
      mutation: mutations.addPost,
      variables: {postId, imageData: jpegData, crop: {upperLeft: {x: 1, y: 2}, lowerRight: {x: 3, y: 5}}},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.image.url).toBeTruthy()
      return post.image
    })

  // check size of the native image
  await requestImageSize(postImage.url).then(({width, height}) => {
    expect(width).toBe(2)
    expect(height).toBe(3)
  })

  // check size of the 4K thumbnail
  await requestImageSize(postImage.url4k).then(({width, height}) => {
    expect(width).toBe(2)
    expect(height).toBe(3)
  })
})

test('Valid jpeg crop, upload via cloudfront', async () => {
  const {client} = await loginCache.getCleanLogin()

  // add the post
  const postId = uuidv4()
  const uploadUrl = await client
    .mutate({
      mutation: mutations.addPost,
      variables: {
        postId,
        crop: {
          upperLeft: {x: jpegWidth / 4, y: jpegHeight / 4},
          lowerRight: {x: (jpegWidth * 3) / 4, y: (jpegHeight * 3) / 4},
        },
      },
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postStatus).toBe('PENDING')
      expect(post.imageUploadUrl).toBeTruthy()
      return post.imageUploadUrl
    })
  await got.put(uploadUrl, {headers: jpegHeaders, body: jpegBytes})
  await misc.sleepUntilPostProcessed(client, postId)

  // retrieve the post object, check some image sizes
  const postImage = await client.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.postStatus).toBe('COMPLETED')
    expect(post.image.url).toBeTruthy()
    return post.image
  })
  await requestImageSize(postImage.url).then(({width, height}) => {
    expect(width).toBe(jpegWidth / 2)
    expect(height).toBe(jpegHeight / 2)
  })
  await requestImageSize(postImage.url4k).then(({width, height}) => {
    expect(width).toBe(jpegWidth / 2)
    expect(height).toBe(jpegHeight / 2)
  })
})

test('Valid jpeg crop, metadata preserved', async () => {
  const {client} = await loginCache.getCleanLogin()

  // pull exif data (only exif) from the original image
  const orgExif = await sharp(grantBytes)
    .metadata()
    .then(({exif}) => exifReader(exif))
  expect(orgExif).toBeTruthy()

  // add the post with a crop
  const postId = uuidv4()
  const postImage = await client
    .mutate({
      mutation: mutations.addPost,
      variables: {postId, imageData: grantData, crop: {upperLeft: {x: 10, y: 20}, lowerRight: {x: 30, y: 40}}},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.image.url).toBeTruthy()
      return post.image
    })

  // get the exif data of the cropped image
  const croppedBytes = await got.get(postImage.url).buffer()
  const newExif = await sharp(croppedBytes)
    .metadata()
    .then(({exif}) => exifReader(exif))

  // make sure exif data hasn't changed
  expect(newExif).toEqual(orgExif)

  // Want to write the cropped image out to a file? here's how to do it
  // fs.writeFile('./cropped.jpeg', croppedBytes)
})
