const fs = require('fs')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const grantData = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
const grantDataB64 = new Buffer.from(grantData).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Blocked user only see absolutely minimal profile of blocker via direct access', async () => {
  // us and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we block them
  let resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)

  // we add an image post
  const postId = uuidv4()
  let variables = {postId, imageData: grantDataB64, takenInReal: true}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  await misc.sleepUntilPostProcessed(ourClient, postId)

  // we set some details on our profile
  resp = await ourClient.mutate({
    mutation: mutations.setUserDetails,
    variables: {
      photoPostId: postId,
      bio: 'testing',
      fullName: 'test test',
      dateOfBirth: '2020-01-09',
      gender: 'FEMALE',
    },
  })
  variables = {version: 'v2020-01-01.1'}
  resp = await ourClient.mutate({mutation: mutations.setUserAcceptedEULAVersion, variables})

  // retrieve our user object
  resp = await ourClient.query({query: queries.self})
  const ourUserFull = resp.data.self
  expect(ourUserFull.userId).toBe(ourUserId)
  expect(ourUserFull.username).toBeTruthy()
  expect(ourUserFull.acceptedEULAVersion).toBeTruthy()
  expect(ourUserFull.albumCount).toBe(0)
  expect(ourUserFull.albums.items).toHaveLength(0)
  expect(ourUserFull.anonymouslyLikedPosts.items).toHaveLength(0)
  expect(ourUserFull.bio).toBeTruthy()
  expect(ourUserFull.blockedStatus).toBe('SELF')
  expect(ourUserFull.blockerStatus).toBe('SELF')
  expect(ourUserFull.blockedUsers.items).toHaveLength(1)
  expect(ourUserFull.cardCount).toBe(0)
  expect(ourUserFull.cards.items).toHaveLength(0)
  expect(ourUserFull.chatCount).toBe(0)
  expect(ourUserFull.chats.items).toHaveLength(0)
  expect(ourUserFull.chatsWithUnviewedMessagesCount).toBe(0)
  expect(ourUserFull.commentsDisabled).toBe(false)
  expect(ourUserFull.dateOfBirth).toBe('2020-01-09')
  expect(ourUserFull.datingStatus).toBe('DISABLED')
  expect(ourUserFull.directChat).toBeNull()
  expect(ourUserFull.email).toBeTruthy()
  expect(ourUserFull.feed.items).toHaveLength(1)
  expect(ourUserFull.followCountsHidden).toBe(false)
  expect(ourUserFull.followerCount).toBe(0)
  expect(ourUserFull.followersCount).toBe(0)
  expect(ourUserFull.followersRequestedCount).toBe(0)
  expect(ourUserFull.followedCount).toBe(0)
  expect(ourUserFull.followedsCount).toBe(0)
  expect(ourUserFull.followerStatus).toBe('SELF')
  expect(ourUserFull.followedStatus).toBe('SELF')
  expect(ourUserFull.followerUsers.items).toHaveLength(0)
  expect(ourUserFull.followedUsers.items).toHaveLength(0)
  expect(ourUserFull.followedUsersWithStories.items).toHaveLength(0)
  expect(ourUserFull.fullName).toBeTruthy()
  expect(ourUserFull.gender).toBe('FEMALE')
  expect(ourUserFull.languageCode).toBeTruthy()
  expect(ourUserFull.likesDisabled).toBe(false)
  expect(ourUserFull.onymouslyLikedPosts.items).toHaveLength(0)
  // skip phone number as that is null for anyone other than SELF, and that's tested elsewhere
  // expect(ourUserFull.phoneNumber).toBeTruthy()
  expect(ourUserFull.photo).toBeTruthy()
  expect(ourUserFull.postCount).toBe(1)
  expect(ourUserFull.posts.items).toHaveLength(1)
  expect(ourUserFull.postsWithUnviewedComments.items).toHaveLength(0)
  expect(ourUserFull.postsByNewCommentActivity.items).toHaveLength(0)
  expect(ourUserFull.postViewedByCount).toBe(0)
  expect(ourUserFull.privacyStatus).toBe('PUBLIC')
  expect(ourUserFull.sharingDisabled).toBe(false)
  expect(ourUserFull.signedUpAt).toBeTruthy()
  expect(ourUserFull.subscriptionLevel).toBe('BASIC')
  expect(ourUserFull.subscriptionExpiresAt).toBeNull()
  expect(ourUserFull.themeCode).toBeTruthy()
  expect(ourUserFull.userStatus).toBe('ACTIVE')
  expect(ourUserFull.verificationHidden).toBe(false)
  expect(ourUserFull.viewCountsHidden).toBe(false)

  // verify they see only a absolutely minimal profile of us
  resp = await theirClient.query({query: queries.user, variables: {userId: ourUserId}})
  const ourUserLimited = resp.data.user
  expect(ourUserLimited.userId).toBe(ourUserFull.userId)
  expect(ourUserLimited.username).toBe(ourUserFull.username)
  expect(ourUserLimited.blockerStatus).toBe('BLOCKING')

  // adjust everything nulled out or changed, then compare
  ourUserFull.acceptedEULAVersion = null
  ourUserFull.albumCount = null
  ourUserFull.albums = null
  ourUserFull.anonymouslyLikedPosts = null
  ourUserFull.bio = null
  ourUserFull.blockerStatus = 'BLOCKING'
  ourUserFull.blockedStatus = 'NOT_BLOCKING'
  ourUserFull.blockedUsers = null
  ourUserFull.cardCount = null
  ourUserFull.cards = null
  ourUserFull.chatCount = null
  ourUserFull.chats = null
  ourUserFull.chatsWithUnviewedMessagesCount = null
  ourUserFull.commentsDisabled = null
  ourUserFull.dateOfBirth = null
  ourUserFull.datingStatus = null
  ourUserFull.email = null
  ourUserFull.feed = null
  ourUserFull.followCountsHidden = null
  ourUserFull.followedCount = null
  ourUserFull.followedsCount = null
  ourUserFull.followerCount = null
  ourUserFull.followersCount = null
  ourUserFull.followersRequestedCount = null
  ourUserFull.followedStatus = 'NOT_FOLLOWING'
  ourUserFull.followerStatus = 'NOT_FOLLOWING'
  ourUserFull.followedUsers = null
  ourUserFull.followerUsers = null
  ourUserFull.followedUsersWithStories = null
  ourUserFull.fullName = null
  ourUserFull.gender = null
  ourUserFull.languageCode = null
  ourUserFull.likesDisabled = null
  ourUserFull.onymouslyLikedPosts = null
  // ourUserFull.phoneNumber is already null
  ourUserFull.photo = null
  ourUserFull.postCount = null
  ourUserFull.posts = null
  ourUserFull.postsWithUnviewedComments = null
  ourUserFull.postsByNewCommentActivity = null
  ourUserFull.postViewedByCount = null
  ourUserFull.privacyStatus = null
  ourUserFull.sharingDisabled = null
  ourUserFull.signedUpAt = null
  ourUserFull.stories = null
  ourUserFull.subscriptionLevel = null
  ourUserFull.themeCode = null
  ourUserFull.userStatus = null
  ourUserFull.verificationHidden = null
  ourUserFull.viewCountsHidden = null
  expect(ourUserFull).toEqual(ourUserLimited)
})

test('Blocked cannot see blocker in search results, blocker can see blocked in search results', async () => {
  // use and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // change our username to something without a dash https://github.com/Imcloug/Selfly-BackEnd/issues/48
  const ourUsername = 'TESTER' + misc.shortRandomString()
  await ourClient.mutate({mutation: mutations.setUsername, variables: {username: ourUsername}})

  // change their username to something without a dash https://github.com/Imcloug/Selfly-BackEnd/issues/48
  const theirUsername = 'TESTER' + misc.shortRandomString()
  await theirClient.mutate({mutation: mutations.setUsername, variables: {username: theirUsername}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // verify they show up in our search results
  let resp = await ourClient.query({query: queries.searchUsers, variables: {searchToken: theirUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(theirUserId)

  // verify we show up in their search results
  resp = await theirClient.query({query: queries.searchUsers, variables: {searchToken: ourUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(ourUserId)

  // we block them
  resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)

  // verify they still show up in our search results
  resp = await ourClient.query({query: queries.searchUsers, variables: {searchToken: theirUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(theirUserId)

  // verify we do not show up in their search results
  resp = await theirClient.query({query: queries.searchUsers, variables: {searchToken: ourUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(0)
})

test('Blocked cannot see blockers follower or followed users lists', async () => {
  // use and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we block them
  let resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)

  // verify they cannot see our list of followers or followed
  resp = await theirClient.query({query: queries.followedUsers, variables: {userId: ourUserId}})
  expect(resp.data.user.followedUsers).toBeNull()
  resp = await theirClient.query({query: queries.followerUsers, variables: {userId: ourUserId}})
  expect(resp.data.user.followerUsers).toBeNull()

  // verify we can still see their list of followers or followed
  resp = await ourClient.query({query: queries.followedUsers, variables: {userId: theirUserId}})
  resp = await ourClient.query({query: queries.followerUsers, variables: {userId: theirUserId}})
})

test('Blocked cannot see blockers posts or stories', async () => {
  // use and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we block them
  let resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)

  // verify they cannot see our posts or stories
  resp = await theirClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories).toBeNull()
  resp = await theirClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts).toBeNull()

  // verify we can see their posts or stories
  resp = await theirClient.query({query: queries.userStories, variables: {userId: theirUserId}})
  expect(resp.data.user.stories.items).toHaveLength(0)
  resp = await theirClient.query({query: queries.userPosts, variables: {userId: theirUserId}})
  expect(resp.data.user.posts.items).toHaveLength(0)
})

test('Blocked cannot see blockers lists of likes', async () => {
  // use and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we block them
  let resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)

  // verify they cannot see our lists of likes
  resp = await theirClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.onymouslyLikedPosts).toBeNull()
  expect(resp.data.user.anonymouslyLikedPosts).toBeNull()

  // verify we can see their list of onymous likes
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.onymouslyLikedPosts.items).toHaveLength(0)
  expect(resp.data.user.anonymouslyLikedPosts).toBeNull()
})

test('Blocked cannot see directly see blockers posts', async () => {
  // use and them
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we block them
  let resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
  expect(resp.data.blockUser.userId).toBe(theirUserId)

  // we add an image post, complete it
  const postId1 = uuidv4()
  let variables = {postId: postId1, imageData: grantDataB64}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId1)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // they add an image post, complete it
  const postId2 = uuidv4()
  variables = {postId: postId2, imageData: grantDataB64}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId2)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // verify they cannot see our post or likers of the post
  resp = await theirClient.query({query: queries.post, variables: {postId: postId1}})
  expect(resp.data.post).toBeNull()

  // verify we can see their post and likers of the post
  resp = await ourClient.query({query: queries.post, variables: {postId: postId2}})
  expect(resp.data.post.postId).toBe(postId2)
})
