/**
 * This test suite cannot run in parrallel with others because it
 * depends on global state - namely the 'real' user.
 */

const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const realUser = require('../../utils/real-user')
const {mutations, queries} = require('../../schema')
const loginCache = new cognito.AppSyncLoginCache()
let realLogin
jest.retryTimes(1)

beforeAll(async () => {
  realLogin = await realUser.getLogin()
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => {
  await realUser.cleanLogin()
  await loginCache.clean()
})
afterAll(async () => {
  await realUser.resetLogin()
  await loginCache.reset()
})

test('If the `real` or `ian` users flag a post, it should be immediately archived', async () => {
  const {client: realClient} = realLogin
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: randoClient} = await loginCache.getCleanLogin()

  // set the ian user's username to 'ian'
  // note as of this writing this is the only test that uses the 'ian' user
  // as such doesn't make sense yet to break it out as a special user like the 'real' user
  const {client: ianClient} = await loginCache.getCleanLogin()
  await ianClient.mutate({mutation: mutations.setUsername, variables: {username: 'ian'}})

  // We add three posts
  const [postId1, postId2, postId3] = [uuidv4(), uuidv4(), uuidv4()]
  const postVariables = {postType: 'TEXT_ONLY', text: 'this is really ofensive'}
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {...postVariables, postId: postId1}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {...postVariables, postId: postId2}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {...postVariables, postId: postId3}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))

  // the real user flags the first post
  await realClient
    .mutate({mutation: mutations.flagPost, variables: {postId: postId1}})
    .then(({data: {flagPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('COMPLETED') // archiving happens asyncronously
      expect(post.flagStatus).toBe('FLAGGED')
    })

  // the ian user flags the second post
  await ianClient
    .mutate({mutation: mutations.flagPost, variables: {postId: postId2}})
    .then(({data: {flagPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('COMPLETED') // archiving happens asyncronously
      expect(post.flagStatus).toBe('FLAGGED')
    })

  // a rando user flags the third post
  await randoClient
    .mutate({mutation: mutations.flagPost, variables: {postId: postId3}})
    .then(({data: {flagPost: post}}) => {
      expect(post.postId).toBe(postId3)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.flagStatus).toBe('FLAGGED')
    })

  // check the first post is really archived
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId: postId1}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId1)
    expect(post.postStatus).toBe('ARCHIVED')
  })

  // check the second post is really archived
  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId2)
    expect(post.postStatus).toBe('ARCHIVED')
  })

  // check the third post is not archived
  await ourClient.query({query: queries.post, variables: {postId: postId3}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId3)
    expect(post.postStatus).toBe('COMPLETED')
  })
})
