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

test('Cannot create direct/group chat if the match_status is not confirmed', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const {userId: otherUserId} = await loginCache.getCleanLogin()

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

  // try to create direct chat
  let [chatId, messageId] = [uuidv4(), uuidv4()]
  const messageText = 'lore ipsum'
  let variables = {userId: theirUserId, chatId, messageId, messageText}
  await expect(ourClient.mutate({mutation: mutations.createDirectChat, variables})).rejects.toThrow(
    /ClientError: Cannot chat user viewed on dating unless it is a match/,
  )

  // we approve them, check statues
  await ourClient.mutate({mutation: mutations.approveMatch, variables: {userId: theirUserId}})
  await misc.sleep(2000)
  await ourClient
    .query({query: queries.user, variables: {userId: theirUserId}})
    .then(({data: {user}}) => expect(user.matchStatus).toBe('APPROVED'))
  await theirClient
    .query({query: queries.user, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.matchStatus).toBe('POTENTIAL'))

  // try to create direct chat
  await expect(ourClient.mutate({mutation: mutations.createDirectChat, variables})).rejects.toThrow(
    /ClientError: Cannot chat user viewed on dating unless it is a match/,
  )

  variables = {
    chatId,
    name: 'x',
    userIds: [theirUserId, otherUserId],
    messageId: messageId,
    messageText: 'm',
  }
  // theirUser should be skipped in the group chat
  await ourClient
    .mutate({mutation: mutations.createGroupChat, variables})
    .then(({data: {createGroupChat: chat}}) => {
      expect(chat.userCount).toBe(2)
      expect(chat.usersCount).toBe(2)
      expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, otherUserId].sort())
    })
})
