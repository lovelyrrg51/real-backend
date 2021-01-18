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
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())
afterEach(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

test('Cant flag our own comment', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // they add a post
  const postId = uuidv4()
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)

  // we add a comment to that post
  const commentId = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addComment, variables: {commentId, postId, text: 'lore'}})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // verify we cant flag that comment
  await expect(ourClient.mutate({mutation: mutations.flagComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: .* their own comment /,
  )

  // check the comment flagStatus shows we did not flag it
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.comments.items[0].commentId).toBe(commentId)
  expect(resp.data.post.comments.items[0].flagStatus).toBe('NOT_FLAGGED')
})

test('Anybody can flag a comment of private user on post of public user', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const {client: randoClient} = await loginCache.getCleanLogin()

  // they go private
  const privacyStatus = 'PRIVATE'
  let resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus}})
  expect(resp.data.setUserDetails.userId).toBe(theirUserId)
  expect(resp.data.setUserDetails.privacyStatus).toBe(privacyStatus)

  // we add a post
  const postId = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)

  // they comment on the post
  const commentId = uuidv4()
  resp = await theirClient.mutate({mutation: mutations.addComment, variables: {commentId, postId, text: 'lore'}})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // verify rando can flag that comment
  resp = await randoClient.mutate({mutation: mutations.flagComment, variables: {commentId}})
  expect(resp.data.flagComment.commentId).toBe(commentId)
  expect(resp.data.flagComment.flagStatus).toBe('FLAGGED')

  // double check the flag status
  await misc.sleep(2000)
  resp = await randoClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.comments.items[0].commentId).toBe(commentId)
  expect(resp.data.post.comments.items[0].flagStatus).toBe('FLAGGED')

  // verify can't double-flag
  await expect(randoClient.mutate({mutation: mutations.flagComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: .* has already been flagged /,
  )
})

test('Cant flag a comment if we are anonymous or disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()
  ;({client: anonClient} = await cognito.getAnonymousAppSyncLogin())

  // they add a post
  const postId = uuidv4()
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)

  // they add a comment to their post
  const commentId = uuidv4()
  resp = await theirClient.mutate({mutation: mutations.addComment, variables: {commentId, postId, text: 'lore'}})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // we disable ourselves
  resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we can't flag their comment
  await expect(ourClient.mutate({mutation: mutations.flagComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )

  // verify anonymous user can't flat their comment
  await expect(anonClient.mutate({mutation: mutations.flagComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Follower can flag comment on post of private user, non-follower cannot', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // we add a comment to our post
  const commentId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addComment, variables: {commentId, postId, text: 'lore'}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId))

  // we go private
  await ourClient
    .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
    .then(({data}) => expect(data.setUserDetails.privacyStatus).toBe('PRIVATE'))

  // verify they can't flag their comment
  await expect(theirClient.mutate({mutation: mutations.flagComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: User does not have access /,
  )

  // they request to follow us
  await theirClient
    .mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
    .then(({data}) => expect(data.followUser.followedStatus).toBe('REQUESTED'))

  // we accept their follow requqest
  await ourClient
    .mutate({mutation: mutations.acceptFollowerUser, variables: {userId: theirUserId}})
    .then(({data}) => expect(data.acceptFollowerUser.followerStatus).toBe('FOLLOWING'))

  // verify they have not flagged the comment
  await theirClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.postId).toBe(postId)
    expect(data.post.comments.items[0].commentId).toBe(commentId)
    expect(data.post.comments.items[0].flagStatus).toBe('NOT_FLAGGED')
  })

  // verify they can now flag the comment
  await theirClient.mutate({mutation: mutations.flagComment, variables: {commentId}}).then(({data}) => {
    expect(data.flagComment.commentId).toBe(commentId)
    expect(data.flagComment.flagStatus).toBe('FLAGGED')
  })

  // verify the comment flag stuck
  await theirClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.postId).toBe(postId)
    expect(data.post.comments.items[0].commentId).toBe(commentId)
    expect(data.post.comments.items[0].flagStatus).toBe('FLAGGED')
  })
})

test('Comments flagged by post owner are force-deleted', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // they add two comments to our post
  const [commentId1, commentId2] = [uuidv4(), uuidv4()]
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId: commentId1, postId, text: 'lore'}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId1))
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId: commentId2, postId, text: 'lore'}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId2))

  // verify we can flag the first comment (as public post owner user)
  await ourClient.mutate({mutation: mutations.flagComment, variables: {commentId: commentId1}}).then(({data}) => {
    expect(data.flagComment.commentId).toBe(commentId1)
    expect(data.flagComment.flagStatus).toBe('FLAGGED')
  })

  // verify the flagged comment has been deleted
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.commentsCount).toBe(1)
    expect(data.post.comments.items).toHaveLength(1)
    expect(data.post.comments.items[0].commentId).toBe(commentId2)
  })

  // we go private
  await ourClient
    .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
    .then(({data}) => expect(data.setUserDetails.privacyStatus).toBe('PRIVATE'))

  // verify we can flag the second comment (as private post owner user)
  await misc.sleep(2000)
  await ourClient.mutate({mutation: mutations.flagComment, variables: {commentId: commentId2}}).then(({data}) => {
    expect(data.flagComment.commentId).toBe(commentId2)
    expect(data.flagComment.flagStatus).toBe('FLAGGED')
  })

  // verify the flagged comment has been deleted
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.commentsCount).toBe(0)
    expect(data.post.comments.items).toHaveLength(0)
  })
})

test('Cannot flag comment that does not exist', async () => {
  const {client} = await loginCache.getCleanLogin()

  // try to flag a non-existent post
  const commentId = uuidv4()
  await expect(client.mutate({mutation: mutations.flagComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: Comment .* does not exist/,
  )
})

test('Cannot flag comment of user that has blocked us', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // they add a post
  const postId = uuidv4()
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)

  // they add a comment to their post
  const commentId = uuidv4()
  resp = await theirClient.mutate({mutation: mutations.addComment, variables: {commentId, postId, text: 'lore'}})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // they block us
  resp = await theirClient.mutate({mutation: mutations.blockUser, variables: {userId: ourUserId}})
  expect(resp.data.blockUser.userId).toBe(ourUserId)
  expect(resp.data.blockUser.blockedStatus).toBe('BLOCKING')

  // verify we cannot flag their comment
  await expect(ourClient.mutate({mutation: mutations.flagComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: .* has been blocked by owner /,
  )

  // they unblock us
  resp = await theirClient.mutate({mutation: mutations.unblockUser, variables: {userId: ourUserId}})
  expect(resp.data.unblockUser.userId).toBe(ourUserId)
  expect(resp.data.unblockUser.blockedStatus).toBe('NOT_BLOCKING')

  // verify we can flag their comment
  resp = await ourClient.mutate({mutation: mutations.flagComment, variables: {commentId}})
  expect(resp.data.flagComment.flagStatus).toBe('FLAGGED')
})

test('Cannot flag comment of user we have blocked', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they add a post
  const postId = uuidv4()
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)

  // they add a comment to their post
  const commentId = uuidv4()
  resp = await theirClient.mutate({mutation: mutations.addComment, variables: {commentId, postId, text: 'lore'}})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)
  expect(resp.data.blockUser.blockedStatus).toBe('BLOCKING')

  // verify we cannot flag their comment
  await expect(ourClient.mutate({mutation: mutations.flagComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: .* has blocked owner /,
  )

  // we unblock them
  resp = await ourClient.mutate({mutation: mutations.unblockUser, variables: {userId: theirUserId}})
  expect(resp.data.unblockUser.userId).toBe(theirUserId)
  expect(resp.data.unblockUser.blockedStatus).toBe('NOT_BLOCKING')

  // verify we can flag their comment
  resp = await ourClient.mutate({mutation: mutations.flagComment, variables: {commentId}})
  expect(resp.data.flagComment.flagStatus).toBe('FLAGGED')
})
