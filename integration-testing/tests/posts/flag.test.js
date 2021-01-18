const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

let anonClient
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
afterEach(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

test('Cant flag our own post', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.flagStatus).toBe('NOT_FLAGGED')

  // verify we cant flag that post
  await expect(ourClient.mutate({mutation: mutations.flagPost, variables: {postId}})).rejects.toThrow(
    /ClientError: .* their own post /,
  )

  // check we did not flag the post is not flagged
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.flagStatus).toBe('NOT_FLAGGED')
})

test('Anybody can flag post of public user', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // they flag that post
  resp = await theirClient.mutate({mutation: mutations.flagPost, variables: {postId}})
  expect(resp.data.flagPost.postId).toBe(postId)
})

test('Disabled and anonymous users cannot flag posts', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()
  ;({client: anonClient} = await cognito.getAnonymousAppSyncLogin())

  // they add a post
  const postId = uuidv4()
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)

  // we disable ourselves
  resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we can't flag their post
  await expect(ourClient.mutate({mutation: mutations.flagPost, variables: {postId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )

  // verify anonymous user can't flag their post
  await expect(anonClient.mutate({mutation: mutations.flagPost, variables: {postId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Follower can flag post of private user, non-follower cannot', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // we go private
  await ourClient
    .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
    .then(({data}) => expect(data.setUserDetails.privacyStatus).toBe('PRIVATE'))

  // verify non-follower cannot flag the post
  await expect(theirClient.mutate({mutation: mutations.flagPost, variables: {postId}})).rejects.toThrow(
    /ClientError: .* does not have access to post/,
  )

  // they request to follow us
  await theirClient
    .mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
    .then(({data}) => expect(data.followUser.followedStatus).toBe('REQUESTED'))

  // we accept their follow requqest
  await ourClient
    .mutate({mutation: mutations.acceptFollowerUser, variables: {userId: theirUserId}})
    .then(({data}) => expect(data.acceptFollowerUser.followerStatus).toBe('FOLLOWING'))

  // verify follower can flag the post
  await theirClient
    .mutate({mutation: mutations.flagPost, variables: {postId}})
    .then(({data}) => expect(data.flagPost.flagStatus).toBe('FLAGGED'))
})

test('Cannot flag post that does not exist', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // try to flag a non-existent post
  const postId = uuidv4()
  await expect(ourClient.mutate({mutation: mutations.flagPost, variables: {postId}})).rejects.toThrow(
    /ClientError: Post .* does not exist/,
  )
})

test('Post.flagStatus changes correctly when post is flagged', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // check they have not flagged the post
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.flagStatus).toBe('NOT_FLAGGED')

  // they flag the post
  resp = await theirClient.mutate({mutation: mutations.flagPost, variables: {postId}})
  expect(resp.data.flagPost.postId).toBe(postId)
  expect(resp.data.flagPost.flagStatus).toBe('FLAGGED')

  // double check that was saved
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.flagStatus).toBe('FLAGGED')
})

test('Cannot double-flag a post', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // they flag the post
  resp = await theirClient.mutate({mutation: mutations.flagPost, variables: {postId}})

  // try to flag it a second time
  await expect(theirClient.mutate({mutation: mutations.flagPost, variables: {postId}})).rejects.toThrow(
    /ClientError: .* has already been flagged /,
  )
})

test('Cannot flag post of user that has blocked us', async () => {
  // us and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // they add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables})

  // they block us
  resp = await theirClient.mutate({mutation: mutations.blockUser, variables: {userId: ourUserId}})

  // verify we cannot flag their post
  await expect(ourClient.mutate({mutation: mutations.flagPost, variables: {postId}})).rejects.toThrow(
    /ClientError: .* has been blocked by owner /,
  )

  // they unblock us
  resp = await theirClient.mutate({mutation: mutations.unblockUser, variables: {userId: ourUserId}})

  // verify we can flag their post
  resp = await ourClient.mutate({mutation: mutations.flagPost, variables: {postId}})
  expect(resp.data.flagPost.flagStatus).toBe('FLAGGED')
})

test('Cannot flag post of user we have blocked', async () => {
  // us and them
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables})

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})

  // verify we cannot flag their post
  await expect(ourClient.mutate({mutation: mutations.flagPost, variables: {postId}})).rejects.toThrow(
    /ClientError: .* has blocked owner /,
  )

  // we unblock them
  resp = await ourClient.mutate({mutation: mutations.unblockUser, variables: {userId: theirUserId}})

  // verify we can flag their post
  resp = await ourClient.mutate({mutation: mutations.flagPost, variables: {postId}})
  expect(resp.data.flagPost.flagStatus).toBe('FLAGGED')
})
