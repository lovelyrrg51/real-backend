const cognito = require('../../utils/cognito')
const {mutations} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Cant accept or deny a follow request if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they go private
  const privacyStatus = 'PRIVATE'
  let resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus}})
  expect(resp.data.setUserDetails.userId).toBe(theirUserId)
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // we request to follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')

  // they disable themselves
  resp = await theirClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(theirUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify they can't deny or accept the following
  await expect(
    theirClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: ourUserId}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
  await expect(
    theirClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
})

test('Try to double-accept a follow request', async () => {
  // us and a private user
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // we request follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')

  // they accept the follow request
  resp = await theirClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: ourUserId}})
  expect(resp.data.acceptFollowerUser.followerStatus).toBe('FOLLOWING')

  // they try to accept the follow request again
  await expect(
    theirClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: ourUserId}}),
  ).rejects.toThrow(/ClientError: .* already follows user .* with status /)
})

test('Try to double-deny a follow request', async () => {
  // us and a private user
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  let variables = {privacyStatus: 'PRIVATE'}
  let resp = await theirClient.mutate({mutation: mutations.setUserPrivacyStatus, variables})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // we request follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('REQUESTED')

  // they accept the follow request
  resp = await theirClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}})
  expect(resp.data.denyFollowerUser.followerStatus).toBe('DENIED')

  // they try to accept the follow request again
  await expect(
    theirClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}}),
  ).rejects.toThrow(/ClientError: .* already follows user .* with status /)
})

test('Cant accept/deny non-existent follow requests', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()
  await expect(
    ourClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: .* has not requested /)
  await expect(
    ourClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: .* has not requested /)
})

test('Cant request to follow a user that has blocked us', async () => {
  // us and them
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they block us
  let resp = await theirClient.mutate({mutation: mutations.blockUser, variables: {userId: ourUserId}})

  // verify we cannot request to follow them
  await expect(
    ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: .* has been blocked by /)

  // they unblock us
  resp = await theirClient.mutate({mutation: mutations.unblockUser, variables: {userId: ourUserId}})

  // verify we can request to follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
})

test('Cant request to follow a user that we have blocked', async () => {
  // us and them
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // we block them
  let resp = await ourClient.mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})

  // verify we cannot request to follow them
  await expect(
    ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}}),
  ).rejects.toThrow(/ClientError: .* has blocked /)

  // we unblock them
  resp = await ourClient.mutate({mutation: mutations.unblockUser, variables: {userId: theirUserId}})

  // verify we can request to follow them
  resp = await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
  expect(resp.data.followUser.followedStatus).toBe('FOLLOWING')
})
