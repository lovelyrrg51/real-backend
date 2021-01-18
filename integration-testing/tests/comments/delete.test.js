const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

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

test('Delete comments', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.commentCount).toBe(0)
  expect(resp.data.addPost.commentsCount).toBe(0)
  expect(resp.data.addPost.comments.items).toHaveLength(0)

  // they comment on the post
  const theirCommentId = uuidv4()
  variables = {commentId: theirCommentId, postId, text: 'lore'}
  resp = await theirClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(theirCommentId)

  // we comment on the post
  const ourCommentId = uuidv4()
  variables = {commentId: ourCommentId, postId, text: 'ipsum'}
  resp = await ourClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(ourCommentId)

  // check we see both comments, in order, on the post
  await misc.sleep(1000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(2)
  expect(resp.data.post.commentsCount).toBe(2)
  expect(resp.data.post.comments.items).toHaveLength(2)
  expect(resp.data.post.comments.items[0].commentId).toBe(theirCommentId)
  expect(resp.data.post.comments.items[1].commentId).toBe(ourCommentId)

  // they delete their comment
  variables = {commentId: theirCommentId}
  resp = await theirClient.mutate({mutation: mutations.deleteComment, variables})
  expect(resp.data.deleteComment.commentId).toBe(theirCommentId)

  // check we only see one comment on the post now
  await misc.sleep(1000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(1)
  expect(resp.data.post.commentsCount).toBe(1)
  expect(resp.data.post.comments.items).toHaveLength(1)
  expect(resp.data.post.comments.items[0].commentId).toBe(ourCommentId)

  // we delete our comment
  variables = {commentId: ourCommentId}
  resp = await ourClient.mutate({mutation: mutations.deleteComment, variables})
  expect(resp.data.deleteComment.commentId).toBe(ourCommentId)

  // check no comments appear on the post now
  await misc.sleep(1000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(0)
  expect(resp.data.post.commentsCount).toBe(0)
  expect(resp.data.post.comments.items).toHaveLength(0)
})

test('Delete someone elses comment on our post', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.commentCount).toBe(0)
  expect(resp.data.addPost.commentsCount).toBe(0)
  expect(resp.data.addPost.comments.items).toHaveLength(0)

  // they comment on the post
  const theirCommentId = uuidv4()
  variables = {commentId: theirCommentId, postId, text: 'lore'}
  resp = await theirClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(theirCommentId)

  // check we can see that comment on the post
  await misc.sleep(1000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(1)
  expect(resp.data.post.commentsCount).toBe(1)
  expect(resp.data.post.comments.items).toHaveLength(1)
  expect(resp.data.post.comments.items[0].commentId).toBe(theirCommentId)

  // we delete their comment
  variables = {commentId: theirCommentId}
  resp = await ourClient.mutate({mutation: mutations.deleteComment, variables})
  expect(resp.data.deleteComment.commentId).toBe(theirCommentId)

  // check no comments appear on the post now
  await misc.sleep(1000)
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(0)
  expect(resp.data.post.commentsCount).toBe(0)
  expect(resp.data.post.comments.items).toHaveLength(0)
})

test('Cant delete a comment that doesnt exist', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  await expect(
    ourClient.mutate({mutation: mutations.deleteComment, variables: {commentId: uuidv4()}}),
  ).rejects.toThrow(/ClientError: No comment/)
})

test('Cant delete comments if our user is disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)

  // we add a comment on our post
  const commentId = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addComment, variables: {commentId, postId, text: 't'}})
  expect(resp.data.addComment.commentId).toBe(commentId)

  // we disable ourselves
  resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // check we cannot delete that comment
  await expect(ourClient.mutate({mutation: mutations.deleteComment, variables: {commentId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Cant delete someone elses comment on someone elses post', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // they add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.commentCount).toBe(0)
  expect(resp.data.addPost.commentsCount).toBe(0)
  expect(resp.data.addPost.comments.items).toHaveLength(0)

  // they comment on the post
  const theirCommentId = uuidv4()
  variables = {commentId: theirCommentId, postId, text: 'lore'}
  resp = await theirClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(theirCommentId)

  // verify we can't delete their comment
  await expect(
    ourClient.mutate({mutation: mutations.deleteComment, variables: {commentId: theirCommentId}}),
  ).rejects.toThrow(/ClientError: .* not authorized to delete/)

  // check they can see that comment on the post
  await misc.sleep(1000)
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(1)
  expect(resp.data.post.commentsCount).toBe(1)
  expect(resp.data.post.comments.items).toHaveLength(1)
  expect(resp.data.post.comments.items[0].commentId).toBe(theirCommentId)
})

test('Can delete comments even if we have comments disabled and the post has comments disabled', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

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
  variables = {commentId: ourCommentId, postId, text: 'lore'}
  resp = await ourClient.mutate({mutation: mutations.addComment, variables})
  expect(resp.data.addComment.commentId).toBe(ourCommentId)

  // we disable comments for our user
  variables = {commentsDisabled: true}
  resp = await ourClient.mutate({mutation: mutations.setUserMentalHealthSettings, variables})

  // verify we can see that comment on the post
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(1)
  expect(resp.data.post.commentsCount).toBe(1)
  expect(resp.data.post.comments.items).toHaveLength(1)
  expect(resp.data.post.comments.items[0].commentId).toBe(ourCommentId)

  // we disable comments on the post
  variables = {postId, commentsDisabled: true}
  resp = await ourClient.mutate({mutation: mutations.editPost, variables})

  // verify we can't see comments on post
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBeNull()
  expect(resp.data.post.commentsCount).toBeNull()
  expect(resp.data.post.comments).toBeNull()

  // but we can still delete the comment
  variables = {commentId: ourCommentId}
  resp = await ourClient.mutate({mutation: mutations.deleteComment, variables})
  expect(resp.data.deleteComment.commentId).toBe(ourCommentId)

  // we enable comments on the post
  variables = {postId, commentsDisabled: false}
  resp = await ourClient.mutate({mutation: mutations.editPost, variables})

  // verify the comment has disappeared from the post
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.commentCount).toBe(0)
  expect(resp.data.post.commentsCount).toBe(0)
  expect(resp.data.post.comments.items).toHaveLength(0)
})

test('Deleting comments in the presence of post views adjusts comment counts on post correctly', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // they comment on the post
  const commentId1 = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId: commentId1, postId, text: 'lore'}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId1))

  // we view the post
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})

  // we comment on the post
  const commentId2 = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addComment, variables: {commentId: commentId2, postId, text: 'lore'}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId2))

  // they comment on the post
  const commentId3 = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId: commentId3, postId, text: 'lore'}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId3))

  // they comment on the post again
  const commentId4 = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId: commentId4, postId, text: 'lore'}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId4))

  // check the post has the correct counts
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.postId).toBe(postId)
    expect(data.post.commentCount).toBe(4)
    expect(data.post.commentsCount).toBe(4)
    expect(data.post.commentsViewedCount).toBe(2)
    expect(data.post.commentsUnviewedCount).toBe(2)
    expect(data.post.comments.items).toHaveLength(4)
  })

  // check the post appears in the postsWithUnviewedComments
  await ourClient.query({query: queries.self}).then(({data, errors}) => {
    expect(errors).toBeUndefined()
    expect(data.self.userId).toBe(ourUserId)
    expect(data.self.postsWithUnviewedComments.items).toHaveLength(1)
    expect(data.self.postsWithUnviewedComments.items[0].postId).toBe(postId)
  })

  // they delete their last unviewed comment
  await theirClient
    .mutate({mutation: mutations.deleteComment, variables: {commentId: commentId4}})
    .then(({data}) => expect(data.deleteComment.commentId).toBe(commentId4))

  // check the post has the correct counts
  await misc.sleep(1000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.postId).toBe(postId)
    expect(data.post.commentCount).toBe(3)
    expect(data.post.commentsCount).toBe(3)
    expect(data.post.commentsViewedCount).toBe(2)
    expect(data.post.commentsUnviewedCount).toBe(1)
    expect(data.post.comments.items).toHaveLength(3)
  })

  // we delete our comment
  await ourClient
    .mutate({mutation: mutations.deleteComment, variables: {commentId: commentId2}})
    .then(({data}) => expect(data.deleteComment.commentId).toBe(commentId2))

  // check the post has the correct counts
  await misc.sleep(1000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.postId).toBe(postId)
    expect(data.post.commentCount).toBe(2)
    expect(data.post.commentsCount).toBe(2)
    expect(data.post.commentsViewedCount).toBe(1)
    expect(data.post.commentsUnviewedCount).toBe(1)
    expect(data.post.comments.items).toHaveLength(2)
  })

  // check the post still appears in the postsWithUnviewedComments
  await ourClient.query({query: queries.self}).then(({data, errors}) => {
    expect(errors).toBeUndefined()
    expect(data.self.userId).toBe(ourUserId)
    expect(data.self.postsWithUnviewedComments.items).toHaveLength(1)
    expect(data.self.postsWithUnviewedComments.items[0].postId).toBe(postId)
  })

  // they delete their last unviewed comment
  await theirClient
    .mutate({mutation: mutations.deleteComment, variables: {commentId: commentId3}})
    .then(({data}) => expect(data.deleteComment.commentId).toBe(commentId3))

  // check the post has the correct counts
  await misc.sleep(1000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.postId).toBe(postId)
    expect(data.post.commentCount).toBe(1)
    expect(data.post.commentsCount).toBe(1)
    expect(data.post.commentsViewedCount).toBe(1)
    expect(data.post.commentsUnviewedCount).toBe(0)
    expect(data.post.comments.items).toHaveLength(1)
  })

  // check the post does not appears in the postsWithUnviewedComments
  await ourClient.query({query: queries.self}).then(({data, errors}) => {
    expect(errors).toBeUndefined()
    expect(data.self.userId).toBe(ourUserId)
    expect(data.self.postsWithUnviewedComments.items).toHaveLength(0)
  })

  // they delete their viewed comment
  await theirClient
    .mutate({mutation: mutations.deleteComment, variables: {commentId: commentId1}})
    .then(({data}) => expect(data.deleteComment.commentId).toBe(commentId1))

  // check the post has the correct counts
  await misc.sleep(1000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data}) => {
    expect(data.post.postId).toBe(postId)
    expect(data.post.commentCount).toBe(0)
    expect(data.post.commentsCount).toBe(0)
    expect(data.post.commentsViewedCount).toBe(0)
    expect(data.post.commentsUnviewedCount).toBe(0)
    expect(data.post.comments.items).toHaveLength(0)
  })
})
