const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const imageData = misc.generateRandomJpeg(8, 8)
const imageDataB64 = new Buffer.from(imageData).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

// generic dating criteria that matches itself
const datingVariables = {
  fullName: 'Hunter S',
  displayName: 'Hunter S',
  gender: 'FEMALE',
  location: {latitude: 30, longitude: 50}, // different from that used in other test suites
  dateOfBirth: '2000-01-01',
  height: 90,
  matchAgeRange: {min: 20, max: 30},
  matchGenders: ['FEMALE'],
  matchLocationRadius: 50,
  matchHeightRange: {min: 0, max: 110},
}

test('Cannot comment to image post if the match_status is not confirmed', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we both set details that would make us match each other, and enable dating
  const [pid1, pid2] = [uuidv4(), uuidv4()]
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: pid1, imageData: imageDataB64, takenInReal: true}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(pid1))
  await ourClient
    .mutate({mutation: mutations.setUserDetails, variables: {...datingVariables, photoPostId: pid1}})
    .then(({data: {setUserDetails: user}}) => expect(user.userId).toBe(ourUserId))
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: pid2, imageData: imageDataB64, takenInReal: true}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(pid2))
  await theirClient
    .mutate({mutation: mutations.setUserDetails, variables: {...datingVariables, photoPostId: pid2}})
    .then(({data: {setUserDetails: user}}) => expect(user.userId).toBe(theirUserId))
  await misc.sleep(2000)
  await ourClient
    .mutate({mutation: mutations.setUserDatingStatus, variables: {status: 'ENABLED'}})
    .then(({data: {setUserDatingStatus: user}}) => expect(user.datingStatus).toBe('ENABLED'))
  await theirClient
    .mutate({mutation: mutations.setUserDatingStatus, variables: {status: 'ENABLED'}})
    .then(({data: {setUserDatingStatus: user}}) => expect(user.datingStatus).toBe('ENABLED'))
  await misc.sleep(2000)
  await ourClient
    .query({query: queries.user, variables: {userId: theirUserId}})
    .then(({data: {user}}) => expect(user.matchStatus).toBe('POTENTIAL'))
  await theirClient
    .query({query: queries.user, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.matchStatus).toBe('POTENTIAL'))

  // we try to add comment to their post
  const ourCommentId = uuidv4()
  const ourText = 'nice post'
  let variables = {commentId: ourCommentId, postId: pid2, text: ourText}
  await expect(ourClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: Cannot add comment unless it is a confirmed match on dating/,
  )

  // they try to add comment to our post
  const theirCommentId = uuidv4()
  variables = {commentId: theirCommentId, postId: pid1, text: ourText}
  await expect(theirClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: Cannot add comment unless it is a confirmed match on dating/,
  )

  // we approve them
  await ourClient.mutate({mutation: mutations.approveMatch, variables: {userId: theirUserId}})
  await misc.sleep(2000)

  // we try to add comment to their post
  variables = {commentId: ourCommentId, postId: pid2, text: ourText}
  await expect(ourClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: Cannot add comment unless it is a confirmed match on dating/,
  )
  // they try to add comment to our post
  variables = {commentId: theirCommentId, postId: pid1, text: ourText}
  await expect(theirClient.mutate({mutation: mutations.addComment, variables})).rejects.toThrow(
    /ClientError: Cannot add comment unless it is a confirmed match on dating/,
  )

  // they approve us
  await theirClient.mutate({mutation: mutations.approveMatch, variables: {userId: ourUserId}})
  await misc.sleep(2000)

  // we try to add comment to their post
  variables = {commentId: ourCommentId, postId: pid2, text: ourText}
  await ourClient.mutate({mutation: mutations.addComment, variables}).then(({data: {addComment: comment}}) => {
    expect(comment.commentId).toBe(ourCommentId)
  })
})
