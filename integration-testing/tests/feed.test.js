const uuidv4 = require('uuid/v4')

const cognito = require('../utils/cognito')
const misc = require('../utils/misc')
const {mutations, queries, subscriptions} = require('../schema')

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

test('When followed user adds/deletes a post, our feed reacts', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('FOLLOWING'))

  // we subscribe to feed notifications
  const handlers = []
  const sub = await ourClient
    .subscribe({query: subscriptions.onNotification, variables: {userId: ourUserId}})
    .subscribe({
      next: ({data: {onNotification: notification}}) => {
        if (notification.type === 'USER_FEED_CHANGED') {
          const handler = handlers.shift()
          expect(handler).toBeDefined()
          handler(notification)
        }
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // our feed starts empty
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))

  // they add two posts, verify they shows up in our feed in order and notifications
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  let nextNotification = new Promise((resolve) => handlers.push(resolve))
  let nextNextNotification = new Promise((resolve) => handlers.push(resolve))
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, text: 'Im sorry dave', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId1))
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, text: 'I cant do that', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId2))
  await misc.sleep(2000)
  await ourClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(2)
    expect(user.feed.items[0].postId).toBe(postId2)
    expect(user.feed.items[0].text).toBe('I cant do that')
    expect(user.feed.items[0].image).toBeTruthy()
    expect(user.feed.items[1].postId).toBe(postId1)
    expect(user.feed.items[1].text).toBe('Im sorry dave')
    expect(user.feed.items[1].image).toBeTruthy()
  })
  await Promise.all([nextNotification, nextNextNotification])

  // they archive a post, verify that post is not longer in the feed and notifications
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  await theirClient
    .mutate({mutation: mutations.archivePost, variables: {postId: postId1}})
    .then(({data: {archivePost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('ARCHIVED')
    })
  await misc.sleep(2000)
  await ourClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(1)
    expect(user.feed.items[0].postId).toBe(postId2)
    expect(user.feed.items[0].text).toBe('I cant do that')
  })
  await nextNotification

  // shut down oursubscription
  sub.unsubscribe()
  await subInitTimeout
})

test('When we follow/unfollow a user with posts, our feed reacts', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we subscribe to feed notifications
  const handlers = []
  const sub = await ourClient
    .subscribe({query: subscriptions.onNotification, variables: {userId: ourUserId}})
    .subscribe({
      next: ({data: {onNotification: notification}}) => {
        if (notification.type === 'USER_FEED_CHANGED') {
          const handler = handlers.shift()
          expect(handler).toBeDefined()
          handler(notification)
        }
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // they add two posts
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, text: 'Im sorry dave', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId1))
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, text: 'I cant do that', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId2))

  // our feed starts empty
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))

  // we follow them, verify our feed and notifications
  let nextNotification = new Promise((resolve) => handlers.push(resolve))
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('FOLLOWING'))
  await misc.sleep(2000)
  await ourClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(2)
    expect(user.feed.items[0].postId).toBe(postId2)
    expect(user.feed.items[0].text).toBe('I cant do that')
    expect(user.feed.items[0].image).toBeTruthy()
    expect(user.feed.items[1].postId).toBe(postId1)
    expect(user.feed.items[1].text).toBe('Im sorry dave')
    expect(user.feed.items[1].image).toBeTruthy()
  })
  await nextNotification

  // we unfollow them, verify our feed and notifications
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  await ourClient
    .mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}})
    .then(({data: {unfollowUser: user}}) => expect(user.followedStatus).toBe('NOT_FOLLOWING'))
  await misc.sleep(2000)
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))
  await nextNotification

  // shut down oursubscription
  sub.unsubscribe()
  await subInitTimeout
})

test('When a private user accepts or denies our follow request, our feed reacts', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // set them to private
  await theirClient
    .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
    .then(({data: {setUserDetails: user}}) => expect(user.privacyStatus).toBe('PRIVATE'))

  // they add two posts
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, text: 'Im sorry dave', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId1))
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, text: 'I cant do that', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId2))

  // our feed starts empty
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))

  // we request to follow them, our feed does not react
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('REQUESTED'))
  await misc.sleep(2000)
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))

  // they accept our follow request, and those two posts show up in our feed
  await theirClient
    .mutate({mutation: mutations.acceptFollowerUser, variables: {userId: ourUserId}})
    .then(({data: {acceptFollowerUser: user}}) => expect(user.followerStatus).toBe('FOLLOWING'))
  await misc.sleep(2000)
  await ourClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(2)
    expect(user.feed.items[0].postId).toBe(postId2)
    expect(user.feed.items[1].postId).toBe(postId1)
  })

  // they change their mind and deny the request, and those two posts disapear from our feed
  await theirClient
    .mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}})
    .then(({data: {denyFollowerUser: user}}) => expect(user.followerStatus).toBe('DENIED'))
  await misc.sleep(2000)
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))
})

test('When a user changes PRIVATE to PUBLIC, and we had an REQUESTED follow request, our feed reacts', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // set them to private
  await theirClient
    .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
    .then(({data: {setUserDetails: user}}) => expect(user.privacyStatus).toBe('PRIVATE'))

  // they add two posts
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, text: 'Im sorry dave', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId1))
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, text: 'I cant do that', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId2))

  // our feed starts empty
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))

  // we request to follow them, our feed does not react
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('REQUESTED'))
  await misc.sleep(2000)
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))

  // they change from private to public
  await theirClient
    .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PUBLIC'}})
    .then(({data: {setUserDetails: user}}) => expect(user.privacyStatus).toBe('PUBLIC'))

  // our follow request should have gone though, so their two posts should now be in our feed
  await misc.sleep(2000)
  await ourClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(2)
    expect(user.feed.items[0].postId).toBe(postId2)
    expect(user.feed.items[1].postId).toBe(postId1)
  })
})

// waiting on a way to externally trigger the 'archive expired posts' cron job
test.skip('Post that expires is removed from feed', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('FOLLOWING'))

  // they add a post that expires in a millisecond
  const postId = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId, lifetime: 'PT0.001S', imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId))

  // since cron job hasn't yet run, that post should be in our feed
  await ourClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(1)
    expect(user.feed.items[0].postId).toBe(postId)
  })

  // TODO trigger the cron job

  // that post should now have disappeared from our feed
  //resp = await ourClient.query({query: queries.selfFeed})
  //expect(resp.data.self.feed.items).toHaveLength(0)
})

test('Feed Post.postedBy.blockerStatus and followedStatus are filled in correctly', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('FOLLOWING'))

  // they add a post
  const postId = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId))

  // see how that looks in our feed
  await ourClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(1)
    expect(user.feed.items[0].postId).toBe(postId)
    expect(user.feed.items[0].postedBy.userId).toBe(theirUserId)
    expect(user.feed.items[0].postedBy.blockerStatus).toBe('NOT_BLOCKING')
    expect(user.feed.items[0].postedBy.followedStatus).toBe('FOLLOWING')
  })

  // see how that looks in their feed
  await theirClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(1)
    expect(user.feed.items[0].postId).toBe(postId)
    expect(user.feed.items[0].postedBy.userId).toBe(theirUserId)
    expect(user.feed.items[0].postedBy.blockerStatus).toBe('SELF')
    expect(user.feed.items[0].postedBy.followedStatus).toBe('SELF')
  })
})

test('Feed privacy', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // verify we can see our feed, via self and user queries
  await ourClient
    .query({query: queries.self})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))
  await ourClient
    .query({query: queries.user, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.feed.items).toHaveLength(0))

  // verify they can *not* see our feed
  await theirClient
    .query({query: queries.user, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.feed).toBeNull())
})
