const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

let anonClient
const imageBytes = misc.generateRandomJpeg(8, 8)
const imageData = new Buffer.from(imageBytes).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

// https://github.com/real-social-media/bad_words/blob/master/bucket/bad_words.json
const badWord = 'uoiFZP8bjS'

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

test('Add comments with bad word - force banned', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, email: theirEmail, userId: theirUserId} = await loginCache.getCleanLogin()
  const {client: otherClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  await ourClient.mutate({mutation: mutations.addPost, variables}).then(({data: {addPost: post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.commentCount).toBe(0)
    expect(post.commentsCount).toBe(0)
    expect(post.comments.items).toHaveLength(0)
  })

  // we comment on the post
  const ourCommentId = uuidv4()
  const ourText = 'nice post'
  variables = {commentId: ourCommentId, postId, text: ourText}
  await ourClient.mutate({mutation: mutations.addComment, variables}).then(({data: {addComment: comment}}) => {
    expect(comment.commentId).toBe(ourCommentId)
  })

  // check we can see that comment
  await misc.sleep(1000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.commentCount).toBe(1)
    expect(post.commentsCount).toBe(1)
    expect(post.comments.items).toHaveLength(1)
    expect(post.comments.items[0].commentId).toBe(ourCommentId)
    expect(post.comments.items[0].commentedBy.userId).toBe(ourUserId)
    expect(post.comments.items[0].text).toBe(ourText)
  })

  // they comment on the post 5 times
  for (const i of Array(5).keys()) {
    variables = {commentId: uuidv4(), postId, text: `lore ipsum ${i}`}
    await theirClient.mutate({mutation: mutations.addComment, variables})
  }

  // they comment on the post with bad word, verify they are force disabled
  const theirCommentId = uuidv4()
  const theirText = `lore ipsum ${badWord}`
  variables = {commentId: theirCommentId, postId, text: theirText}
  await theirClient.mutate({mutation: mutations.addComment, variables}).then(({data: {addComment: comment}}) => {
    expect(comment.commentId).toBe(theirCommentId)
  })

  // verify they are force disabled
  await theirClient.query({query: queries.self}).then(({data}) => expect(data.self.userStatus).toBe('DISABLED'))

  // other tries to change email with banned email and it's also disabled
  await theirClient.mutate({mutation: mutations.deleteUser}).then(({data: {deleteUser: user}}) => {
    expect(user.userId).toBe(theirUserId)
    expect(user.userStatus).toBe('DELETING')
  })
  await misc.sleep(2000)

  await expect(
    otherClient.mutate({mutation: mutations.startChangeUserEmail, variables: {email: theirEmail}}),
  ).rejects.toThrow(/ClientError: User email is already banned and disabled/)

  await otherClient.query({query: queries.self}).then(({data}) => expect(data.self.userStatus).toBe('DISABLED'))
})
