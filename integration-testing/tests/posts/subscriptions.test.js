const got = require('got')
const uuidv4 = require('uuid/v4')
// the aws-appsync-subscription-link pacakge expects WebSocket to be globaly defined, like in the browser
global.WebSocket = require('ws')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, subscriptions} = require('../../schema')

const imageHeaders = {'Content-Type': 'image/jpeg'}
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

test('POST_COMPLETED notification triggers correctly posts', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // we subscribe to notifications
  const handlers = []
  const sub = await client.subscribe({query: subscriptions.onNotification, variables: {userId}}).subscribe({
    next: ({data: {onNotification: notification}}) => {
      if (notification.type.startsWith('POST_')) {
        const handler = handlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      }
    },
    error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
  })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // create a text-only post, verify it completes automatically and we are notified
  let nextNotification = new Promise((resolve) => handlers.push(resolve))
  const postId1 = uuidv4()
  await client
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, postType: 'TEXT_ONLY', text: 'lore'}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('COMPLETED')
    })
  await nextNotification.then((notification) => {
    expect(notification.type).toBe('POST_COMPLETED')
    expect(notification.postId).toBe(postId1)
  })

  // create an image post, upload the image data along with post, verify
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  const postId2 = uuidv4()
  await client
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, imageData}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('COMPLETED')
    })
  await nextNotification.then((notification) => {
    expect(notification.type).toBe('POST_COMPLETED')
    expect(notification.postId).toBe(postId2)
  })

  // archive a post, then restore it (verify no spurious notification)
  await client
    .mutate({mutation: mutations.archivePost, variables: {postId: postId1}})
    .then(({data: {archivePost: post}}) => expect(post.postStatus).toBe('ARCHIVED'))
  await client
    .mutate({mutation: mutations.restoreArchivedPost, variables: {postId: postId1}})
    .then(({data: {restoreArchivedPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))

  // create another image post, don't upload the image data yet
  const postId3 = uuidv4()
  const uploadUrl = await client
    .mutate({mutation: mutations.addPost, variables: {postId: postId3}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId3)
      expect(post.postStatus).toBe('PENDING')
      return post.imageUploadUrl
    })

  // upload the image data to cloudfront, verify notification received
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  await got.put(uploadUrl, {headers: imageHeaders, body: imageBytes})
  await nextNotification.then((notification) => {
    expect(notification.type).toBe('POST_COMPLETED')
    expect(notification.postId).toBe(postId3)
  })

  // shut down the subscription
  sub.unsubscribe()
  await subInitTimeout
})

test('POST_ERROR notification triggers correctly posts', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // we subscribe to notifications
  const handlers = []
  const sub = await client.subscribe({query: subscriptions.onNotification, variables: {userId}}).subscribe({
    next: ({data: {onNotification: notification}}) => {
      if (notification.type.startsWith('POST_')) {
        const handler = handlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      }
    },
    error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
  })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // create an image post, upload invalid image data along with post, verify
  let nextNotification = new Promise((resolve) => handlers.push(resolve))
  const postId1 = uuidv4()
  await client
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, imageData: 'invalid-image-data'}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('ERROR')
    })
  await nextNotification.then((notification) => {
    expect(notification.type).toBe('POST_ERROR')
    expect(notification.postId).toBe(postId1)
  })

  // create another image post, don't upload the image data yet
  const postId2 = uuidv4()
  const uploadUrl = await client
    .mutate({mutation: mutations.addPost, variables: {postId: postId2}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('PENDING')
      return post.imageUploadUrl
    })

  // upload some invalid image data to cloudfront, verify
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  await got.put(uploadUrl, {headers: imageHeaders, body: 'other-invalid-image-data'})
  await nextNotification.then((notification) => {
    expect(notification.type).toBe('POST_ERROR')
    expect(notification.postId).toBe(postId2)
  })

  // shut down the subscription
  sub.unsubscribe()
  await subInitTimeout
})

test('Post message triggers cannot be called from external graphql client', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // create an image post in pending state
  const postId = uuidv4()
  await client
    .mutate({mutation: mutations.addPost, variables: {postId, postType: 'IMAGE'}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postType).toBe('IMAGE')
      expect(post.postStatus).toBe('PENDING')
    })

  // verify we can't call the trigger method, even with well-formed input
  await expect(
    client.mutate({
      mutation: mutations.triggerPostNotification,
      variables: {input: {userId, type: 'COMPLETED', postId, postStatus: 'COMPLETED', isVerified: false}},
    }),
  ).rejects.toThrow(/ClientError: Access denied/)
})

test('Cannot subscribe to other users notifications', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // verify we cannot subscribe to their messages
  // Note: there doesn't seem to be any error thrown at the time of subscription, it's just that
  // the subscription next() method is never triggered
  await ourClient
    .subscribe({query: subscriptions.onPostNotification, variables: {userId: theirUserId}})
    .subscribe({
      next: (resp) => expect(`Should not be called: ${resp}`).toBeNull(),
      error: (resp) => expect(`Not expected to be called: ${resp}`).toBeNull(),
    })

  // they create an image post, complete it
  const postId = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData, takenInReal: true}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postStatus).toBe('COMPLETED')
    })

  // wait for some messages to show up, ensure none did
  await misc.sleep(5000)

  // we don't unsubscribe from the subscription because
  //  - it's not actually active, although I have yet to find a way to expect() that
  //  - unsubcribing results in the AWS SDK throwing errors
})

test('Format for COMPLETED message notifications', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // we subscribe to post notifications
  const handlers = []
  const sub = await client.subscribe({query: subscriptions.onPostNotification, variables: {userId}}).subscribe({
    next: ({data: {onPostNotification: notification}}) => {
      const handler = handlers.shift()
      expect(handler).toBeDefined()
      handler(notification)
    },
    error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
  })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541

  // we two pending posts: one that will fail verification and another that will pass
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  const uploadUrl1 = await client
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, postType: 'IMAGE'}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('PENDING')
      expect(post.imageUploadUrl).toBeTruthy()
      return post.imageUploadUrl
    })
  const uploadUrl2 = await client
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, postType: 'IMAGE', takenInReal: true}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('PENDING')
      expect(post.imageUploadUrl).toBeTruthy()
      return post.imageUploadUrl
    })

  // upload one image, verify notified correctly
  let nextNotification = new Promise((resolve) => handlers.push(resolve))
  await got.put(uploadUrl1, {headers: imageHeaders, body: imageBytes})
  await nextNotification.then((notification) => {
    expect(notification).toEqual({
      __typename: 'PostNotification',
      userId,
      type: 'COMPLETED',
      post: {
        __typename: 'Post',
        postId: postId1,
        postStatus: 'COMPLETED',
        isVerified: false,
      },
    })
  })

  // upload the other image, verify notified correctly
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  await got.put(uploadUrl2, {headers: imageHeaders, body: imageBytes})
  await nextNotification.then((notification) => {
    expect(notification).toEqual({
      __typename: 'PostNotification',
      userId,
      type: 'COMPLETED',
      post: {
        __typename: 'Post',
        postId: postId2,
        postStatus: 'COMPLETED',
        isVerified: true,
      },
    })
  })

  // shut down the subscription
  sub.unsubscribe()
  await subInitTimeout
})
