const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

let anonClient, anonUserId, anonUsername
const imageData1 = misc.generateRandomJpeg(8, 8)
const imageData2 = misc.generateRandomJpeg(8, 8)
const imageData1B64 = new Buffer.from(imageData1).toString('base64')
const imageData2B64 = new Buffer.from(imageData2).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

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

test('Report post views', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId} = await loginCache.getCleanLogin()

  // we add two posts
  const postId1 = uuidv4()
  const postId2 = uuidv4()
  let variables = {postId: postId1, imageData: imageData1B64}

  await ourClient.mutate({mutation: mutations.addPost, variables}).then(({data: {addPost}}) => {
    expect(addPost.postId).toBe(postId1)
  })

  variables = {postId: postId2, imageData: imageData2B64}
  await ourClient.mutate({mutation: mutations.addPost, variables})

  // verify we have no post views
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.postViewedByCount).toBe(0)
  })

  // verify niether of the posts have views
  await ourClient.query({query: queries.post, variables: {postId: postId1}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(0)
  })

  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(0)
  })

  // other1 reports to have viewed both posts
  await other1Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {postIds: [postId1, postId2]},
  })

  // other2 reports to have viewed one post
  await other2Client.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId2]}})

  // we report to have viewed both posts (should not be recorded on our own posts)
  await other1Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {postIds: [postId1, postId2]},
  })

  // verify our view counts are correct
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.postViewedByCount).toBe(3)
  })

  // verify the two posts have the right viewed by counts
  await ourClient.query({query: queries.post, variables: {postId: postId1}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(1)
  })

  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(2)
  })

  // verify the two posts have the right viewedBy lists
  await ourClient.query({query: queries.post, variables: {postId: postId1}}).then(({data: {post}}) => {
    expect(post.viewedBy.items).toHaveLength(1)
    expect(post.viewedBy.items[0].userId).toBe(other1UserId)
  })

  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.viewedBy.items).toHaveLength(2)
    expect(post.viewedBy.items[0].userId).toBe(other1UserId)
    expect(post.viewedBy.items[1].userId).toBe(other2UserId)
  })
})

test('Cannot report post views if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: imageData1B64}})
    .then(({data: {addPost}}) => {
      expect(addPost.postId).toBe(postId)
      expect(addPost.postStatus).toBe('COMPLETED')
    })

  // we disable ourselves
  await ourClient.mutate({mutation: mutations.disableUser}).then(({data: {disableUser}}) => {
    expect(disableUser.userId).toBe(ourUserId)
    expect(disableUser.userStatus).toBe('DISABLED')
  })

  // verify we cannot report post views
  await expect(
    ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
})

test('Anonymous user can report post views', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  ;({client: anonClient, userId: anonUserId, username: anonUsername} = await cognito.getAnonymousAppSyncLogin())

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: imageData1B64}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.postStatus).toBe('COMPLETED')
    })

  // anonymous user reports a view of the post
  await anonClient
    .mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})
    .then(({data}) => expect(data.reportPostViews).toBe(true))

  // check the anonymous user shows up in the post views
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.viewedByCount).toBe(1)
    expect(post.viewedBy.items).toHaveLength(1)
    expect(post.viewedBy.items[0].userId).toBe(anonUserId)
    expect(post.viewedBy.items[0].username).toBe(anonUsername)
  })
})

test('Post.viewedStatus', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we a posts
  const postId = uuidv4()
  let variables = {postId, imageData: imageData1B64}

  await ourClient.mutate({mutation: mutations.addPost, variables}).then(({data: {addPost}}) => {
    expect(addPost.postId).toBe(postId)
    expect(addPost.viewedStatus).toBe('VIEWED')
  })

  // verify they haven't viewed the post
  await theirClient.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.viewedStatus).toBe('NOT_VIEWED')
  })

  // they report to have viewed the post
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})

  // verify that's reflected in the viewedStatus
  await theirClient.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.viewedStatus).toBe('VIEWED')
  })
})

test('Report post views on non-completed posts are ignored', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client} = await loginCache.getCleanLogin()
  const {client: other2Client} = await loginCache.getCleanLogin()

  // add a pending post
  const postId1 = uuidv4()
  let variables = {postId: postId1}

  await ourClient.mutate({mutation: mutations.addPost, variables}).then(({data: {addPost}}) => {
    expect(addPost.postId).toBe(postId1)
    expect(addPost.postStatus).toBe('PENDING')
  })

  // add an archived post
  const postId2 = uuidv4()
  variables = {postId: postId2, imageData: imageData2B64}

  await ourClient.mutate({mutation: mutations.addPost, variables}).then(({data: {addPost}}) => {
    expect(addPost.postId).toBe(postId2)
  })

  await ourClient
    .mutate({mutation: mutations.archivePost, variables: {postId: postId2}})
    .then(({data: {archivePost}}) => {
      expect(archivePost.postId).toBe(postId2)
      expect(archivePost.postStatus).toBe('ARCHIVED')
    })

  // other1 reports to have viewed both posts
  await other1Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {postIds: [postId1, postId2]},
  })

  // other2 reports to have viewed one post
  await other2Client.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId2]}})

  // we report to have viewed both posts (should not be recorded on our own posts)
  await other1Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {postIds: [postId1, postId2]},
  })

  // verify the two posts have no viewed by counts
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId: postId1}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(0)
  })
  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(0)
  })
})

test('Post views are de-duplicated by user', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client} = await loginCache.getCleanLogin()
  const {client: other2Client} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  let variables = {postId, imageData: imageData1B64}

  await ourClient.mutate({mutation: mutations.addPost, variables}).then(({data: {addPost}}) => {
    expect(addPost.postId).toBe(postId)
  })

  // other1 reports to have viewed that post twice
  await other1Client.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId, postId]}})

  // check counts de-duplicated
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.postViewedByCount).toBe(1)
  })
  await ourClient.query({query: queries.post, variables: {postId: postId}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(1)
  })

  // other2 report to have viewed that post once
  await other2Client.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})

  // check counts
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.postViewedByCount).toBe(2)
  })
  await ourClient.query({query: queries.post, variables: {postId: postId}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(2)
  })

  // other1 report to have viewed that post yet again
  await other1Client.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId, postId]}})

  // check counts have not changed
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.postViewedByCount).toBe(2)
  })
  await ourClient.query({query: queries.post, variables: {postId: postId}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(2)
  })
})

test('Report post views error conditions', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // must report at least one view
  let variables = {postIds: []}
  await expect(ourClient.mutate({mutation: mutations.reportPostViews, variables})).rejects.toThrow(
    /ClientError: A minimum of 1 post id /,
  )

  // can't report more than 100 views
  variables = {
    postIds: Array(101)
      .fill()
      .map(() => uuidv4()),
  }
  await expect(ourClient.mutate({mutation: mutations.reportPostViews, variables})).rejects.toThrow(
    /ClientError: A max of 100 post ids /,
  )
})

test('Post views on duplicate posts are recorded on viewed post and original post', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const {client: otherClient, userId: otherUserId} = await loginCache.getCleanLogin()

  // we add an image post, they add a post that's a duplicate of ours
  const [ourPostId, theirPostId] = [uuidv4(), uuidv4()]
  await ourClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: ourPostId, imageData: imageData1B64, takenInReal: true},
    })
    .then(({data: {addPost}}) => {
      expect(addPost.postId).toBe(ourPostId)
      expect(addPost.postStatus).toBe('COMPLETED')
      expect(addPost.originalPost.postId).toBe(ourPostId)
    })
  await misc.sleep(2000)
  await theirClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: theirPostId, imageData: imageData1B64, takenInReal: true},
    })
    .then(({data: {addPost}}) => {
      expect(addPost.postId).toBe(theirPostId)
      expect(addPost.postStatus).toBe('COMPLETED')
      expect(addPost.originalPost.postId).toBe(ourPostId)
    })

  // verify the original post is our post on both posts, and there are no views on either post
  await ourClient.query({query: queries.post, variables: {postId: ourPostId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(ourPostId)
    expect(post.viewedByCount).toBe(0)
    expect(post.originalPost.postId).toBe(ourPostId)
  })
  await theirClient.query({query: queries.post, variables: {postId: theirPostId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(theirPostId)
    expect(post.viewedByCount).toBe(0)
    expect(post.originalPost.postId).toBe(ourPostId)
  })

  // other records one post view on their post
  // verify that shows up as a view on both posts
  await otherClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [theirPostId]}})
  await misc.sleep(2000)
  await theirClient.query({query: queries.post, variables: {postId: theirPostId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(theirPostId)
    expect(post.viewedByCount).toBe(1)
    expect(post.viewedBy.items).toHaveLength(1)
    expect(post.viewedBy.items[0].userId).toBe(otherUserId)
  })
  await ourClient.query({query: queries.post, variables: {postId: ourPostId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(ourPostId)
    expect(post.viewedByCount).toBe(1)
    expect(post.viewedBy.items).toHaveLength(1)
    expect(post.viewedBy.items[0].userId).toBe(otherUserId)
  })

  // verify both of our users also recored a view
  await ourClient.query({query: queries.self}).then(({data: {self}}) => expect(self.postViewedByCount).toBe(1))
  await theirClient.query({query: queries.self}).then(({data: {self}}) => expect(self.postViewedByCount).toBe(1))

  // they record a view on their own post
  // verify it is not recorded as a view on their post, but does get recorded on the original post
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [theirPostId]}})
  await misc.sleep(2000)
  await theirClient.query({query: queries.post, variables: {postId: theirPostId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(theirPostId)
    expect(post.viewedByCount).toBe(1)
    expect(post.viewedBy.items).toHaveLength(1)
    expect(post.viewedBy.items[0].userId).toBe(otherUserId)
  })
  await ourClient.query({query: queries.post, variables: {postId: ourPostId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(ourPostId)
    expect(post.viewedByCount).toBe(2)
    expect(post.viewedBy.items).toHaveLength(2)
    expect(post.viewedBy.items[0].userId).toBe(otherUserId)
    expect(post.viewedBy.items[1].userId).toBe(theirUserId)
  })

  // we record a post view on their post
  // verify it is not recorded as a view on their post, but does not get recorded on our post
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [theirPostId]}})
  await misc.sleep(2000)
  await theirClient.query({query: queries.post, variables: {postId: theirPostId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(theirPostId)
    expect(post.viewedByCount).toBe(2)
    expect(post.viewedBy.items).toHaveLength(2)
    expect(post.viewedBy.items[0].userId).toBe(otherUserId)
    expect(post.viewedBy.items[1].userId).toBe(ourUserId)
  })
  await ourClient.query({query: queries.post, variables: {postId: ourPostId}}).then(({data: {post}}) => {
    expect(post.postId).toBe(ourPostId)
    expect(post.viewedByCount).toBe(2)
    expect(post.viewedBy.items).toHaveLength(2)
    expect(post.viewedBy.items[0].userId).toBe(otherUserId)
    expect(post.viewedBy.items[1].userId).toBe(theirUserId)
  })
})

test('Post views and deleted on user delete/reset', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // they add a post
  const postId = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: imageData1B64}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId))

  // we view the post
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})

  // verify they can see our post view
  await misc.sleep(2000)
  await theirClient.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(1)
    expect(post.viewedBy.items).toHaveLength(1)
    expect(post.viewedBy.items[0].userId).toBe(ourUserId)
  })

  // we reset our user
  await ourClient
    .mutate({mutation: mutations.resetUser})
    .then(({data: {resetUser: user}}) => expect(user.userStatus).toBe('RESETTING'))

  // verify the view has disappeared
  await misc.sleep(2000)
  await theirClient.query({query: queries.post, variables: {postId}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(0)
    expect(post.viewedBy.items).toHaveLength(0)
  })
})

test('Report post views with FOCUS view type', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId} = await loginCache.getCleanLogin()

  // we add two posts
  const postId1 = uuidv4()
  const postId2 = uuidv4()
  let variables = {postId: postId1, imageData: imageData1B64}

  await ourClient.mutate({mutation: mutations.addPost, variables}).then(({data: {addPost}}) => {
    expect(addPost.postId).toBe(postId1)
  })

  variables = {postId: postId2, imageData: imageData2B64}
  await ourClient.mutate({mutation: mutations.addPost, variables})

  // verify we have no post views
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.postViewedByCount).toBe(0)
  })

  // verify niether of the posts have views
  await ourClient.query({query: queries.post, variables: {postId: postId1}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(0)
  })
  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(0)
  })

  // other1 reports to have viewed both posts with FOCUS view type
  await other1Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {
      postIds: [postId1, postId2],
      viewType: 'FOCUS',
    },
  })

  // other2 reports to have viewed one post
  await other2Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {
      postIds: [postId2],
      viewType: 'THUMBNAIL',
    },
  })

  // we report to have viewed both posts (should not be recorded on our own posts)
  await other1Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {postIds: [postId1, postId2]},
  })

  // verify our view counts are correct
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self}}) => {
    expect(self.postViewedByCount).toBe(3)
  })

  // verify the two posts have the right viewed by counts
  await ourClient.query({query: queries.post, variables: {postId: postId1}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(1)
    expect(post.viewedStatus).toBe('VIEWED')
  })

  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.viewedByCount).toBe(2)
    expect(post.viewedStatus).toBe('VIEWED')
  })

  // verify the two posts have the right viewedBy lists
  await ourClient.query({query: queries.post, variables: {postId: postId1}}).then(({data: {post}}) => {
    expect(post.viewedBy.items).toHaveLength(1)
    expect(post.viewedBy.items[0].userId).toBe(other1UserId)
  })

  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.viewedBy.items).toHaveLength(2)
    expect(post.viewedBy.items[0].userId).toBe(other1UserId)
    expect(post.viewedBy.items[1].userId).toBe(other2UserId)
  })
})
