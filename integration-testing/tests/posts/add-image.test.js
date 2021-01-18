const got = require('got')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const imageBytes = misc.generateRandomJpeg(300, 200)
const imageData = new Buffer.from(imageBytes).toString('base64')
const imageBytes2 = misc.generateRandomJpeg(300, 200)
const imageHeaders = {'Content-Type': 'image/jpeg'}
const heicHeaders = {'Content-Type': 'image/heic'}
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Cant use jpeg data for an HEIC image', async () => {
  const {client} = await loginCache.getCleanLogin()

  // add a post as HEIC, but actually send up jpeg data
  const postId1 = uuidv4()
  let variables = {postId: postId1, imageData, imageFormat: 'HEIC'}
  let resp = await client.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId1)
  expect(resp.data.addPost.postStatus).toBe('ERROR')
  expect(resp.data.addPost.image).toBeNull()
  expect(resp.data.addPost.imageUploadUrl).toBeNull()

  // check the post, make sure it error'd out
  resp = await client.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post.postId).toBe(postId1)
  expect(resp.data.post.postStatus).toBe('ERROR')
  expect(resp.data.post.isVerified).toBeNull()
  expect(resp.data.post.image).toBeNull()

  // add a post as HEIC, but actually send up jpeg data
  const postId2 = uuidv4()
  resp = await client.mutate({mutation: mutations.addPost, variables: {postId: postId2, imageFormat: 'HEIC'}})
  expect(resp.data.addPost.postId).toBe(postId2)
  let uploadUrl = resp.data.addPost.imageUploadUrl
  expect(uploadUrl).toBeTruthy()

  // upload some jpeg data pretending to be heic, let the s3 trigger fire
  await got.put(uploadUrl, {headers: heicHeaders, body: imageData})
  await misc.sleep(3000)

  // check the post, make sure it error'd out
  resp = await client.query({query: queries.post, variables: {postId: postId2}})
  expect(resp.data.post.postId).toBe(postId2)
  expect(resp.data.post.postStatus).toBe('ERROR')
  expect(resp.data.post.isVerified).toBeNull()
  expect(resp.data.post.image).toBeNull()
})

test('Add image post with image data directly included', async () => {
  const {client} = await loginCache.getCleanLogin()

  // add the post with image data included in the gql call
  const postId = uuidv4()
  let resp = await client.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postType).toBe('IMAGE')
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.imageUploadUrl).toBeNull()
  const image = resp.data.addPost.image
  expect(image.url).toBeTruthy()

  // verify we can access all of the urls
  await got.head(image.url)
  await got.head(image.url4k)
  await got.head(image.url1080p)
  await got.head(image.url480p)
  await got.head(image.url64p)

  // check the data in the native image is correct
  const s3ImageData = await got.get(image.url).buffer()
  expect(s3ImageData).toEqual(imageBytes)

  // double check everything saved to db correctly
  resp = await client.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.postStatus).toBe('COMPLETED')
  expect(resp.data.post.imageUploadUrl).toBeNull()
  const imageCheck = resp.data.post.image

  expect(imageCheck.url.split('?')[0]).toBe(image.url.split('?')[0])
  expect(imageCheck.url4k.split('?')[0]).toBe(image.url4k.split('?')[0])
  expect(imageCheck.url1080p.split('?')[0]).toBe(image.url1080p.split('?')[0])
  expect(imageCheck.url480p.split('?')[0]).toBe(image.url480p.split('?')[0])
  expect(imageCheck.url64p.split('?')[0]).toBe(image.url64p.split('?')[0])
})

test('Add image post (with postType specified), check non-duplicates are not marked as such', async () => {
  const {client} = await loginCache.getCleanLogin()

  // we add a image post, give s3 trigger a second to fire
  const postId = uuidv4()
  let variables = {postId, postType: 'IMAGE'}
  let resp = await client.mutate({mutation: mutations.addPost, variables})
  let post = resp.data.addPost
  expect(post.postId).toBe(postId)
  expect(post.postStatus).toBe('PENDING')
  expect(post.imageUploadUrl).toBeTruthy()
  expect(post.image).toBeNull()
  let uploadUrl = post.imageUploadUrl

  // upload the image, give S3 trigger a second to fire
  await got.put(uploadUrl, {headers: imageHeaders, body: imageBytes})
  await misc.sleepUntilPostProcessed(client, postId)

  // add another image post with a different image
  const postId2 = uuidv4()
  variables = {postId: postId2}
  resp = await client.mutate({mutation: mutations.addPost, variables})
  uploadUrl = resp.data.addPost.imageUploadUrl
  await got.put(uploadUrl, {headers: imageHeaders, body: imageBytes2})
  await misc.sleepUntilPostProcessed(client, postId2)

  // check the post has changed status and looks good
  resp = await client.query({query: queries.post, variables: {postId}})
  post = resp.data.post
  expect(post.postId).toBe(postId)
  expect(post.postStatus).toBe('COMPLETED')
  expect(post.imageUploadUrl).toBeNull()
  expect(post.image.url).toBeTruthy()
  expect(post.originalPost.postId).toBe(postId)

  // check the originalPost properties don't point at each other
  resp = await client.query({query: queries.post, variables: {postId: postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.postStatus).toBe('COMPLETED')
  expect(resp.data.post.originalPost.postId).toBe(postId)
  resp = await client.query({query: queries.post, variables: {postId: postId2}})
  expect(resp.data.post.postId).toBe(postId2)
  expect(resp.data.post.postStatus).toBe('COMPLETED')
  expect(resp.data.post.originalPost.postId).toBe(postId2)
})

test('Post.originalPost - duplicates caught on creation, privacy', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  const ourPostId = uuidv4()
  const theirPostId = uuidv4()

  // we add a image post, complete it, check it's original
  let variables = {postId: ourPostId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(ourPostId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.originalPost.postId).toBe(ourPostId)
  await misc.sleep(1000) // dynamo

  // they add another image post with the same image, original should point back to first post
  variables = {postId: theirPostId, imageData}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(theirPostId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.originalPost.postId).toBe(ourPostId)
  await misc.sleep(1000) // dynamo

  // check each others post objects directly
  resp = await theirClient.query({query: queries.post, variables: {postId: ourPostId}})
  expect(resp.data.post.postId).toBe(ourPostId)
  expect(resp.data.post.postStatus).toBe('COMPLETED')
  expect(resp.data.post.originalPost.postId).toBe(ourPostId)
  resp = await ourClient.query({query: queries.post, variables: {postId: theirPostId}})
  expect(resp.data.post.postId).toBe(theirPostId)
  expect(resp.data.post.postStatus).toBe('COMPLETED')
  expect(resp.data.post.originalPost.postId).toBe(ourPostId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)
  expect(resp.data.blockUser.blockedStatus).toBe('BLOCKING')

  // verify they can't see their post's originalPost
  resp = await theirClient.query({query: queries.post, variables: {postId: theirPostId}})
  expect(resp.data.post.postId).toBe(theirPostId)
  expect(resp.data.post.originalPost).toBeNull()

  // we unblock them
  resp = await ourClient.mutate({mutation: mutations.unblockUser, variables: {userId: theirUserId}})
  expect(resp.data.unblockUser.userId).toBe(theirUserId)
  expect(resp.data.unblockUser.blockedStatus).toBe('NOT_BLOCKING')

  // we go private
  resp = await ourClient.mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // verify they can't see their post's originalPost
  resp = await theirClient.query({query: queries.post, variables: {postId: theirPostId}})
  expect(resp.data.post.postId).toBe(theirPostId)
  expect(resp.data.post.originalPost).toBeNull()

  // they request to follow us, we accept
  resp = await theirClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')
  resp = await ourClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: theirUserId}})
  expect(resp.data.acceptFollowerUser.followerStatus).toBe('FOLLOWING')

  // verify they *can* see their post's originalPost
  resp = await theirClient.query({query: queries.post, variables: {postId: theirPostId}})
  expect(resp.data.post.postId).toBe(theirPostId)
  expect(resp.data.post.originalPost.postId).toBe(ourPostId)
})

test('Add post setAsUserPhoto failures', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // verify doesn't set user photo if uploaded image can't be processed (image data included with upload)
  const postId1 = uuidv4()
  let variables = {postId: postId1, postType: 'IMAGE', setAsUserPhoto: true, imageData: 'notimagedata'}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId1)
  expect(resp.data.addPost.postStatus).toBe('ERROR')

  // add a pending post with setAsUserPhoto
  const postId2 = uuidv4()
  variables = {postId: postId2, postType: 'IMAGE', setAsUserPhoto: true}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId2)
  expect(resp.data.addPost.postStatus).toBe('PENDING')
  let uploadUrl = resp.data.addPost.imageUploadUrl
  expect(uploadUrl).toBeTruthy()

  // upload the image data that isn't a valid image, give it a long time to process
  await got.put(uploadUrl, {headers: imageHeaders, body: 'not-a-valid-image'})
  await misc.sleep(10 * 1000)

  // check that our profile photo has not changed
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.userId).toBe(ourUserId)
  expect(resp.data.self.photo).toBeNull()
})

test('Add post setAsUserPhoto success', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // check we have no profile photo
  let resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.userId).toBe(ourUserId)
  expect(resp.data.self.photo).toBeNull()

  // add a post and use setAsUserPhoto with image data directly uploaded
  const postId1 = uuidv4()
  let variables = {postId: postId1, postType: 'IMAGE', setAsUserPhoto: true, imageData, takenInReal: true}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId1)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // make sure dynamo converges
  await misc.sleep(2000)

  // check that our profile photo has changed
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.userId).toBe(ourUserId)
  expect(resp.data.self.photo.url).toContain(postId1)

  // add a post and use setAsUserPhoto, don't include image data directly
  const postId2 = uuidv4()
  variables = {postId: postId2, postType: 'IMAGE', setAsUserPhoto: true, takenInReal: true}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId2)
  expect(resp.data.addPost.postStatus).toBe('PENDING')
  let uploadUrl = resp.data.addPost.imageUploadUrl
  expect(uploadUrl).toBeTruthy()

  // upload the image data
  await got.put(uploadUrl, {headers: imageHeaders, body: imageBytes})
  await misc.sleepUntilPostProcessed(ourClient, postId2)

  // make sure dynamo converges
  await misc.sleep(2000)

  // check that our profile photo has changed
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.userId).toBe(ourUserId)
  expect(resp.data.self.photo.url).toContain(postId2)
})
