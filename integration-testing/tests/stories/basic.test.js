const fs = require('fs')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const imageBytes = misc.generateRandomJpeg(8, 8)
const imageData = new Buffer.from(imageBytes).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Posts that are not within a day of expiring do not show up as a stories', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they add two posts that are not close to expiring
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  let variables = {postId: postId1, imageData, text: 'never expires'}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId1)

  variables = {postId: postId2, imageData, text: 'in a week', lifetime: 'P7D'}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId2)

  // verify they still have no stories
  resp = await theirClient.query({query: queries.userStories, variables: {userId: theirUserId}})
  expect(resp.data.user.stories.items).toHaveLength(0)
  resp = await ourClient.query({query: queries.userStories, variables: {userId: theirUserId}})
  expect(resp.data.user.stories.items).toHaveLength(0)

  // verify we don't see them as having stories
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(0)
})

test('Add a post that shows up as story', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they add a post that expires in a day
  const postId = uuidv4()
  let variables = {postId, imageData, text: 'immediate story', lifetime: 'P1D'}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // that post should show up as a story for them
  resp = await ourClient.query({query: queries.userStories, variables: {userId: theirUserId}})
  expect(resp.data.user.stories.items).toHaveLength(1)
  expect(resp.data.user.stories.items[0].postId).toBe(postId)

  // they should show up as having a story to us
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(1)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(theirUserId)
  expect(resp.data.self.followedUsersWithStories.items[0].blockerStatus).toBe('NOT_BLOCKING')
  expect(resp.data.self.followedUsersWithStories.items[0].followedStatus).toBe('FOLLOWING')

  // verify they cannot see our followedUsersWithStories
  resp = await theirClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.followedUsersWithStories).toBeNull()
})

test('Add posts with images show up in stories', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const imageBytes = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
  const imageData = new Buffer.from(imageBytes).toString('base64')

  // we add a image post, give s3 trigger a second to fire
  const postId1 = uuidv4()
  let variables = {postId: postId1, lifetime: 'PT1M', imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId1)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.image).toBeTruthy()

  // we add a image post, give s3 trigger a second to fire
  const postId2 = uuidv4()
  variables = {postId: postId2, lifetime: 'PT2H', imageData}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId2)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.image).toBeTruthy()

  // verify we see those stories
  resp = await ourClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories.items).toHaveLength(2)
  expect(resp.data.user.stories.items[0].postId).toBe(postId1)
  expect(resp.data.user.stories.items[0].image.url).toBeTruthy()
  expect(resp.data.user.stories.items[1].postId).toBe(postId2)
  expect(resp.data.user.stories.items[1].image.url).toBeTruthy()
})

test('Stories are ordered by first-to-expire-first', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we add three stories with various lifetimes
  const [postId1, postId2, postId3] = [uuidv4(), uuidv4(), uuidv4()]
  let variables = {postId: postId1, imageData, text: '6 hrs', lifetime: 'PT6H'}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId1)

  variables = {postId: postId2, imageData, text: '1 hr', lifetime: 'PT1H'}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId2)

  variables = {postId: postId3, imageData, text: '12 hrs', lifetime: 'PT12H'}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId3)

  // verify those show up in the right order
  resp = await ourClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories.items).toHaveLength(3)
  expect(resp.data.user.stories.items[0].postId).toBe(postId2)
  expect(resp.data.user.stories.items[1].postId).toBe(postId1)
  expect(resp.data.user.stories.items[2].postId).toBe(postId3)
})

test('Followed users with stories are ordered by first-to-expire-first', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId} = await loginCache.getCleanLogin()

  // we follow the two other users
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: other1UserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: other2UserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they each add a story
  let variables = {postId: uuidv4(), imageData, text: '12 hrs', lifetime: 'PT12H'}
  resp = await other1Client.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  variables = {postId: uuidv4(), imageData, text: '6 hrs', lifetime: 'PT6H'}
  resp = await other2Client.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // verify those show up in the right order
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(2)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(other2UserId)
  expect(resp.data.self.followedUsersWithStories.items[1].userId).toBe(other1UserId)

  // another story is added that's about to expire
  variables = {postId: uuidv4(), imageData, text: '1 hr', lifetime: 'PT1H'}
  resp = await other1Client.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // verify that reversed the order
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(2)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(other1UserId)
  expect(resp.data.self.followedUsersWithStories.items[1].userId).toBe(other2UserId)

  // another story is added that doesn't change the order
  variables = {postId: uuidv4(), imageData, text: '13 hrs', lifetime: 'PT13H'}
  resp = await other2Client.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // verify order has not changed
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(2)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(other1UserId)
  expect(resp.data.self.followedUsersWithStories.items[1].userId).toBe(other2UserId)
})

test('Stories of private user are visible to themselves and followers only', async () => {
  // us, a private user with a story
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const postId = uuidv4()
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await ourClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')
  variables = {postId, imageData, text: 'expires in an hour', lifetime: 'PT1H'}
  resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // verify we can see our story
  resp = await ourClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories.items).toHaveLength(1)
  expect(resp.data.user.stories.items[0].postId).toBe(postId)
  resp = await ourClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories.items).toHaveLength(1)
  expect(resp.data.user.stories.items[0].postId).toBe(postId)

  // verify new user, not yet following us, cannot see our stories
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  resp = await theirClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories).toBeNull()

  // they request to follow us, verify still cannot see our stories
  resp = await theirClient.mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')
  resp = await theirClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories).toBeNull()

  // we deny their request, verify they cannot see our stories
  resp = await ourClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: theirUserId}})
  expect(resp.data.denyFollowerUser.followerStatus).toBe('DENIED')
  resp = await theirClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories).toBeNull()

  // approve their request, verify they can now see our stories
  resp = await ourClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: theirUserId}})
  expect(resp.data.acceptFollowerUser.followerStatus).toBe('FOLLOWING')
  resp = await theirClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories.items).toHaveLength(1)
  expect(resp.data.user.stories.items[0].postId).toBe(postId)

  // they unfollow us, verify they cannot see our stories
  resp = await theirClient.mutate({mutation: mutations.unfollowUser, variables: {userId: ourUserId}})
  expect(resp.data.unfollowUser.followedStatus).toBe('NOT_FOLLOWING')
  resp = await theirClient.query({query: queries.userStories, variables: {userId: ourUserId}})
  expect(resp.data.user.stories).toBeNull()
})

// waiting on a way to externally trigger the 'archive expired posts' cron job
test.skip('Post that expires is removed from stories', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they add a post that expires in a millisecond
  const postId = uuidv4()
  let variables = {postId, imageData, text: 'expires 1ms', lifetime: 'PT0.001S'}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // cron job hasn't yet run, so that post should be a story
  resp = await theirClient.query({query: queries.userStories, variables: {userId: theirUserId}})
  expect(resp.data.user.stories.items).toHaveLength(1)
  expect(resp.data.user.stories.items[0].postId).toBe(postId)
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(1)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(theirUserId)

  // TODO trigger the cron job

  // that post should now have disappeared from stories
  resp = await theirClient.query({query: queries.userStories, variables: {userId: theirUserId}})
  expect(resp.data.user.stories.items).toHaveLength(0)
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(0)
})

test('Post that is archived is removed from stories', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we follow them
  let resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')

  // they add a post that expires in an hour
  const postId = uuidv4()
  let variables = {postId, imageData, text: 'expires in an hour', lifetime: 'PT1H'}
  resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // that post should be a story
  resp = await theirClient.query({query: queries.userStories, variables: {userId: theirUserId}})
  expect(resp.data.user.stories.items).toHaveLength(1)
  expect(resp.data.user.stories.items[0].postId).toBe(postId)
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(1)
  expect(resp.data.self.followedUsersWithStories.items[0].userId).toBe(theirUserId)

  // they archive that post
  resp = await theirClient.mutate({mutation: mutations.archivePost, variables: {postId}})
  expect(resp.data.archivePost.postStatus).toBe('ARCHIVED')

  // that post should now have disappeared from stories
  resp = await theirClient.query({query: queries.userStories, variables: {userId: theirUserId}})
  expect(resp.data.user.stories.items).toHaveLength(0)
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.followedUsersWithStories.items).toHaveLength(0)
})
