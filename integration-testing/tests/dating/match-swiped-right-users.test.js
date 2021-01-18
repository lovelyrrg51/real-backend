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

test('POTENTIAL -> CONFIRMED', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const {client: otherClient, userId: otherUserId} = await loginCache.getCleanLogin()

  // we both set details that would make us match each other, and enable dating
  const [pid1, pid2, pid3] = [uuidv4(), uuidv4(), uuidv4()]
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
  await otherClient
    .mutate({mutation: mutations.addPost, variables: {postId: pid3, imageData: imageDataB64, takenInReal: true}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(pid3))
  await otherClient
    .mutate({mutation: mutations.setUserDetails, variables: {...datingVariables, photoPostId: pid3}})
    .then(({data: {setUserDetails: user}}) => expect(user.userId).toBe(otherUserId))

  await misc.sleep(2000)
  await ourClient
    .mutate({mutation: mutations.setUserDatingStatus, variables: {status: 'ENABLED'}})
    .then(({data: {setUserDatingStatus: user}}) => expect(user.datingStatus).toBe('ENABLED'))
  await theirClient
    .mutate({mutation: mutations.setUserDatingStatus, variables: {status: 'ENABLED'}})
    .then(({data: {setUserDatingStatus: user}}) => expect(user.datingStatus).toBe('ENABLED'))
  await otherClient
    .mutate({mutation: mutations.setUserDatingStatus, variables: {status: 'ENABLED'}})
    .then(({data: {setUserDatingStatus: user}}) => expect(user.datingStatus).toBe('ENABLED'))

  await misc.sleep(2000)
  await ourClient
    .query({query: queries.user, variables: {userId: theirUserId}})
    .then(({data: {user}}) => expect(user.matchStatus).toBe('POTENTIAL'))
  await ourClient
    .query({query: queries.user, variables: {userId: otherUserId}})
    .then(({data: {user}}) => expect(user.matchStatus).toBe('POTENTIAL'))

  // they and other approve us, check statues
  await theirClient.mutate({mutation: mutations.approveMatch, variables: {userId: ourUserId}})
  await misc.sleep(2000)
  await theirClient
    .query({query: queries.user, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.matchStatus).toBe('APPROVED'))

  await otherClient.mutate({mutation: mutations.approveMatch, variables: {userId: ourUserId}})
  await otherClient
    .query({query: queries.user, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.matchStatus).toBe('APPROVED'))

  // try to fetch swiped right users, should be diamond subscription level
  await expect(ourClient.query({query: queries.swipedRightUsers})).rejects.toThrow(
    /ClientError: User subscription level is not diamond/,
  )

  // we give ourselves some free diamond
  await ourClient
    .mutate({mutation: mutations.grantUserSubscriptionBonus})
    .then(({data: {grantUserSubscriptionBonus: user}}) => {
      expect(user.userId).toBe(ourUserId)
      expect(user.subscriptionLevel).toBe('DIAMOND')
    })

  await ourClient.query({query: queries.swipedRightUsers}).then(({data: {swipedRightUsers: users}}) => {
    expect(users.map((u) => u.userId).sort()).toEqual([theirUserId, otherUserId].sort())
  })
})
