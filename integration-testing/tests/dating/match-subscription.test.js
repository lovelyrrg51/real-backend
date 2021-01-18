const uuidv4 = require('uuid/v4')
// the aws-appsync-subscription-link pacakge expects WebSocket to be globaly defined, like in the browser
global.WebSocket = require('ws')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, subscriptions} = require('../../schema')

const imageData = new Buffer.from(misc.generateRandomJpeg(8, 8)).toString('base64')
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
  location: {latitude: 10, longitude: 10}, // different from that used in other test suites
  height: 90,
  dateOfBirth: '2000-01-01',
  matchAgeRange: {min: 20, max: 30},
  matchLocationRadius: 50,
  matchHeightRange: {min: 0, max: 110},
}

test(
  'USER_DATING_MATCH_CHANGED notification triggers correctly',
  async () => {
    const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
    const {client: o1Client, userId: o1UserId} = await loginCache.getCleanLogin()
    const {client: o2Client, userId: o2UserId} = await loginCache.getCleanLogin()

    // set dating parameters for so that we match both of them, but they don't match each other
    const [pid1, pid2, pid3] = [uuidv4(), uuidv4(), uuidv4()]
    await ourClient
      .mutate({mutation: mutations.addPost, variables: {postId: pid1, imageData, takenInReal: true}})
      .then(({data: {addPost: post}}) => expect(post.postId).toBe(pid1))
    await o1Client
      .mutate({mutation: mutations.addPost, variables: {postId: pid2, imageData, takenInReal: true}})
      .then(({data: {addPost: post}}) => expect(post.postId).toBe(pid2))
    await o2Client
      .mutate({mutation: mutations.addPost, variables: {postId: pid3, imageData, takenInReal: true}})
      .then(({data: {addPost: post}}) => expect(post.postId).toBe(pid3))
    await ourClient
      .mutate({
        mutation: mutations.setUserDetails,
        variables: {...datingVariables, photoPostId: pid1, gender: 'FEMALE', matchGenders: ['MALE']},
      })
      .then(({data: {setUserDetails: user}}) => expect(user.userId).toBe(ourUserId))
    await o1Client
      .mutate({
        mutation: mutations.setUserDetails,
        variables: {...datingVariables, photoPostId: pid2, gender: 'MALE', matchGenders: ['FEMALE']},
      })
      .then(({data: {setUserDetails: user}}) => expect(user.userId).toBe(o1UserId))
    await o2Client
      .mutate({
        mutation: mutations.setUserDetails,
        variables: {...datingVariables, photoPostId: pid3, gender: 'MALE', matchGenders: ['FEMALE']},
      })
      .then(({data: {setUserDetails: user}}) => expect(user.userId).toBe(o2UserId))

    // enable subscriptions for all users
    const ourHandlers = []
    const o1Handlers = []
    const o2Handlers = []
    const ourSub = await ourClient
      .subscribe({query: subscriptions.onNotification, variables: {userId: ourUserId}})
      .subscribe({
        next: ({data: {onNotification: notification}}) => {
          if (notification.type.startsWith('USER_DATING_')) {
            const handler = ourHandlers.shift()
            expect(handler).toBeDefined()
            handler(notification)
          }
        },
        error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
      })
    const o1Sub = await o1Client
      .subscribe({query: subscriptions.onNotification, variables: {userId: o1UserId}})
      .subscribe({
        next: ({data: {onNotification: notification}}) => {
          if (notification.type.startsWith('USER_DATING_')) {
            const handler = o1Handlers.shift()
            expect(handler).toBeDefined()
            handler(notification)
          }
        },
        error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
      })
    const o2Sub = await o2Client
      .subscribe({query: subscriptions.onNotification, variables: {userId: o2UserId}})
      .subscribe({
        next: ({data: {onNotification: notification}}) => {
          if (notification.type.startsWith('USER_DATING_')) {
            const handler = o2Handlers.shift()
            expect(handler).toBeDefined()
            handler(notification)
          }
        },
        error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
      })
    const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
    await misc.sleep(2000) // let the subscriptions initialize

    // we enable dating
    await ourClient
      .mutate({mutation: mutations.setUserDatingStatus, variables: {status: 'ENABLED'}})
      .then(({data: {setUserDatingStatus: user}}) => expect(user.datingStatus).toBe('ENABLED'))

    // o1 enables dating, verify notifications
    let ourNextNotification = new Promise((resolve) => ourHandlers.push(resolve))
    let o1NextNotification = new Promise((resolve) => o1Handlers.push(resolve))
    await o1Client
      .mutate({mutation: mutations.setUserDatingStatus, variables: {status: 'ENABLED'}})
      .then(({data: {setUserDatingStatus: user}}) => expect(user.datingStatus).toBe('ENABLED'))
    await ourNextNotification.then((notification) => {
      expect(notification.type).toBe('USER_DATING_MATCH_CHANGED')
      expect(notification.userId).toBe(ourUserId)
      expect(notification.matchUserId).toBe(o1UserId)
    })
    await o1NextNotification.then((notification) => {
      expect(notification.type).toBe('USER_DATING_MATCH_CHANGED')
      expect(notification.userId).toBe(o1UserId)
      expect(notification.matchUserId).toBe(ourUserId)
    })

    // o2 enables dating, verify notifications
    ourNextNotification = new Promise((resolve) => ourHandlers.push(resolve))
    let o2NextNotification = new Promise((resolve) => o2Handlers.push(resolve))
    await o2Client
      .mutate({mutation: mutations.setUserDatingStatus, variables: {status: 'ENABLED'}})
      .then(({data: {setUserDatingStatus: user}}) => expect(user.datingStatus).toBe('ENABLED'))
    await ourNextNotification.then((notification) => {
      expect(notification.type).toBe('USER_DATING_MATCH_CHANGED')
      expect(notification.userId).toBe(ourUserId)
      expect(notification.matchUserId).toBe(o2UserId)
    })
    await o2NextNotification.then((notification) => {
      expect(notification.type).toBe('USER_DATING_MATCH_CHANGED')
      expect(notification.userId).toBe(o2UserId)
      expect(notification.matchUserId).toBe(ourUserId)
    })

    // o1 & o2 approve us, we reject o1, no notifications
    await o1Client.mutate({mutation: mutations.approveMatch, variables: {userId: ourUserId}})
    await o2Client.mutate({mutation: mutations.approveMatch, variables: {userId: ourUserId}})
    await ourClient.mutate({mutation: mutations.rejectMatch, variables: {userId: o1UserId}})

    // set expected notifications, we approve o2, verify notifications for transition to CONFIRMED
    ourNextNotification = new Promise((resolve) => ourHandlers.push(resolve))
    o2NextNotification = new Promise((resolve) => o2Handlers.push(resolve))
    await ourClient.mutate({mutation: mutations.approveMatch, variables: {userId: o2UserId}})
    await ourNextNotification.then((notification) => {
      expect(notification.type).toBe('USER_DATING_MATCH_CHANGED')
      expect(notification.userId).toBe(ourUserId)
      expect(notification.matchUserId).toBe(o2UserId)
    })
    await o2NextNotification.then((notification) => {
      expect(notification.type).toBe('USER_DATING_MATCH_CHANGED')
      expect(notification.userId).toBe(o2UserId)
      expect(notification.matchUserId).toBe(ourUserId)
    })

    // shut down the subscriptions
    ourSub.unsubscribe()
    o1Sub.unsubscribe()
    o2Sub.unsubscribe()
    await subInitTimeout
  },
  90 * 1000,
)
