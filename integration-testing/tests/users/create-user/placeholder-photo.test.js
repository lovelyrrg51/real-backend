const fs = require('fs')
const got = require('got')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../../utils/cognito.js')
const {mutations, queries} = require('../../../schema')

const grantData = fs.readFileSync(path.join(__dirname, '..', '..', '..', 'fixtures', 'grant.jpg'))
const grantDataB64 = new Buffer.from(grantData).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

// expects the placeholder photos directory in the REAL-Themes bucket *not* to be set up
test('Mutation.createCognitoOnlyUser with no placeholder photos in bucket fails softly', async () => {
  const {client, userId, username} = await loginCache.getCleanLogin()

  // reset the user to clear & re-initialize their presence from dynamo
  let resp = await client.mutate({mutation: mutations.resetUser, variables: {newUsername: username}})
  expect(resp['errors']).toBeUndefined()

  resp = await client.query({query: queries.self})
  expect(resp['errors']).toBeUndefined()
  expect(resp['data']['self']['userId']).toBe(userId)
  expect(resp['data']['self']['photo']).toBeNull()
})

/* This test expects the placeholder photos directory in the REAL-Themes bucket
 * to be set up with exactly one placeholder photo */
test.skip('Mutation.createCognitoOnlyUser with placeholder photo in bucket works', async () => {
  // These variables must be filed in correctly
  const placeholderPhotosDomain = ''
  const placeholderPhotosDirectory = ''
  const placeholderPhotoCode = ''

  const {client, userId, username} = await loginCache.getCleanLogin()

  // reset the user to clear & re-initialize their presence from dynamo
  let resp = await client.mutate({mutation: mutations.resetUser, variables: {newUsername: username}})
  expect(resp['errors']).toBeUndefined()

  resp = await client.query({query: queries.self})
  expect(resp['errors']).toBeUndefined()
  const urlRoot = `https://${placeholderPhotosDomain}/${placeholderPhotosDirectory}/${placeholderPhotoCode}/`
  const urlRootRE = new RegExp(`^${urlRoot}.*$`)
  expect(resp['data']['self']['userId']).toBe(userId)

  expect(resp['data']['self']['photo']['url']).toMatch(urlRootRE)
  expect(resp['data']['self']['photo']['url64p']).toMatch(urlRootRE)
  expect(resp['data']['self']['photo']['url480p']).toMatch(urlRootRE)
  expect(resp['data']['self']['photo']['url1080p']).toMatch(urlRootRE)
  expect(resp['data']['self']['photo']['url4k']).toMatch(urlRootRE)

  expect(resp['data']['self']['photo']['url']).toMatch(/.*\/native\.jpg$/)
  expect(resp['data']['self']['photo']['url64p']).toMatch(/.*\/64p\.jpg$/)
  expect(resp['data']['self']['photo']['url480p']).toMatch(/.*\/480p\.jpg$/)
  expect(resp['data']['self']['photo']['url1080p']).toMatch(/.*\/1080p\.jpg$/)
  expect(resp['data']['self']['photo']['url4k']).toMatch(/.*\/4K\.jpg$/)

  // verify we can access the urls
  await got.head(resp['data']['self']['photo']['url'])
  await got.head(resp['data']['self']['photo']['url4k'])
  await got.head(resp['data']['self']['photo']['url1080p'])
  await got.head(resp['data']['self']['photo']['url480p'])
  await got.head(resp['data']['self']['photo']['url64p'])

  // If you want to manually verify these urls, here they are
  //console.log(resp['data']['self']['photo']['url'])
  //console.log(resp['data']['self']['photo']['url64p'])
  //console.log(resp['data']['self']['photo']['url480p'])
  //console.log(resp['data']['self']['photo']['url1080p'])
  //console.log(resp['data']['self']['photo']['url4k'])

  // now set a custom profile photo, and make sure the placeholder urls go away

  // create a post with an image
  const [postId, mediaId] = [uuidv4(), uuidv4()]
  resp = await client.mutate({mutation: mutations.addPost, variables: {postId, mediaId, imageData: grantDataB64}})
  expect(resp['errors']).toBeUndefined()
  expect(resp['data']['addPost']['postId']).toBe(postId)
  expect(resp['data']['addPost']['postStatus']).toBe('COMPLETED')

  // set our photo
  resp = await client.mutate({mutation: mutations.setUserDetails, variables: {photoPostId: postId}})
  expect(resp['errors']).toBeUndefined()
  expect(resp['data']['setUserDetails']['photo']).toBeTruthy()

  // check that it is really set already set
  resp = await client.query({query: queries.self})
  expect(resp['errors']).toBeUndefined()
  expect(resp['data']['self']['photo']).toBeTruthy()

  // check that the urls are no longer coming from the placeholder photos bucket
  expect(resp['data']['self']['photo']['url']).not.toMatch(urlRootRE)
  expect(resp['data']['self']['photo']['url64p']).not.toMatch(urlRootRE)
  expect(resp['data']['self']['photo']['url480p']).not.toMatch(urlRootRE)
  expect(resp['data']['self']['photo']['url1080p']).not.toMatch(urlRootRE)
  expect(resp['data']['self']['photo']['url4k']).not.toMatch(urlRootRE)
})
