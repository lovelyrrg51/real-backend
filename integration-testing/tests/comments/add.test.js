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

test('Add a comments', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.commentCount).toBe(0)
  expect(resp.data.addPost.commentsCount).toBe(0)
  expect(resp.data.addPost.comments.items).toHaveLength(0)

  // we comment on the post
  const ourCommentId = uuidv4()
  const ourText = 'nice post'
  variables = {commentId: ourCommentId, postId, text: ourText}
  resp = await ourClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(ourCommentId)

  // check we can see that comment
  await misc.sleep(1000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(1)
  expect(resp.data.post.commentsCount).toBe(1)
  expect(resp.data.post.comments.items).toHaveLength(1)
  expect(resp.data.post.comments.items[0].commentId).toBe(ourCommentId)
  expect(resp.data.post.comments.items[0].commentedBy.userId).toBe(ourUserId)
  expect(resp.data.post.comments.items[0].text).toBe(ourText)

  // they comment on the post
  const theirCommentId = uuidv4()
  const theirText = 'lore ipsum'
  variables = {commentId: theirCommentId, postId, text: theirText}
  resp = await theirClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(theirCommentId)

  // check we see both comments, in order, on the post
  await misc.sleep(1000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(2)
  expect(resp.data.post.commentsCount).toBe(2)
  expect(resp.data.post.comments.items).toHaveLength(2)
  expect(resp.data.post.comments.items[0].commentId).toBe(ourCommentId)
  expect(resp.data.post.comments.items[1].commentId).toBe(theirCommentId)
  expect(resp.data.post.comments.items[1].commentedBy.userId).toBe(theirUserId)
  expect(resp.data.post.comments.items[1].text).toBe(theirText)
})

test('Verify commentIds cannot be re-used ', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // they comment on the post
  const commentId = uuidv4()
  variables = {commentId, postId, text: 'nice lore'}
  resp = await theirClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // check we cannot add another comment re-using that commentId
  await expect(
    ourClient.mutate({mutation: mutations.addComment, variables: {commentId, postId, text: 'i agree'}}),
  ).rejects.toThrow(/ClientError: Comment .* already exists/)
})

test('Cant add comments to post that doesnt exist', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const commentId = uuidv4()
  await expect(
    ourClient.mutate({mutation: mutations.addComment, variables: {commentId, postId: 'pid', text: 't'}}),
  ).rejects.toThrow(/ClientError: Post .* does not exist$/)
})

test('Cant add comments if user is disabled or anonymous', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  ;({client: anonClient} = await cognito.getAnonymousAppSyncLogin())

  // we add a post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)

  // we disable ourselves
  resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // check we cannot comment
  let variables = {commentId: uuidv4(), postId, text: 'no way'}
  await expect(ourClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )

  // check anonymous user can't comment
  await expect(anonClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Cant add comments to post with comments disabled', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post with comments disabled
  const postId = uuidv4()
  let variables = {postId, imageData, commentsDisabled: true}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.commentsDisabled).toBe(true)

  // check they cannot comment on the post
  variables = {commentId: uuidv4(), postId, text: 'no way'}
  await expect(theirClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: Comments are disabled on post/,
  )
})

test('Cant add comments to a post of a user that has blocked us, or a user we have blocked', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // they block us
  let variables = {userId: ourUserId}
  let resp = await theirClient.mutate({mutation: mutations.blockUser, variables})
  expect(resp.data.blockUser.userId).toBe(ourUserId)
  expect(resp.data.blockUser.blockedStatus).toBe('BLOCKING')

  // they add a post
  const theirPostId = uuidv4()
  variables = {postId: theirPostId, imageData}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(theirPostId)

  // we add a post
  const ourPostId = uuidv4()
  variables = {postId: ourPostId, imageData}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(ourPostId)

  // check we cannot comment on their post
  variables = {commentId: uuidv4(), postId: theirPostId, text: 'no way'}
  await expect(ourClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: .* has blocked user /,
  )

  // check they cannot comment on our post
  variables = {commentId: uuidv4(), postId: ourPostId, text: 'no way'}
  await expect(theirClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: .* has blocked post owner /,
  )
})

test('Cant add comments to a post of a private user unless were following them', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they go private
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})

  // they add a post
  const postId = uuidv4()
  variables = {postId, imageData}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // check they can comment on their own post
  let commentId = uuidv4()
  variables = {commentId: commentId, postId, text: 'no way'}
  resp = await theirClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // check we cannot comment on the post
  variables = {commentId: uuidv4(), postId, text: 'no way'}
  await expect(ourClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: .* is private /,
  )

  // we request to follow them
  variables = {userId: theirUserId}
  resp = await ourClient.mutate({mutation: mutations.followUser, variables})

  // check we cannot comment on the post
  variables = {commentId: uuidv4(), postId, text: 'no way'}
  await expect(ourClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: .* is private /,
  )

  // they accept our follow request
  variables = {userId: ourUserId}
  resp = await theirClient.mutate({mutation: mutations.acceptFollowerUser, variables})

  // check we _can_ comment on the post
  commentId = uuidv4()
  variables = {commentId, postId, text: 'nice lore'}
  resp = await ourClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // they change their mind and now deny our following
  variables = {userId: ourUserId}
  resp = await theirClient.mutate({mutation: mutations.denyFollowerUser, variables})

  // check we cannot comment on the post
  variables = {commentId: uuidv4(), postId, text: 'no way'}
  await expect(ourClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: .* is private /,
  )
})
