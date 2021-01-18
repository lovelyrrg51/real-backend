const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations} = require('../../schema')

const AuthFlow = cognito.AuthFlow
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('setting invalid username fails', async () => {
  const {client} = await loginCache.getCleanLogin()
  const usernameTooShort = 'aa'
  const usernameTooLong = 'a'.repeat(31)
  const usernameBadChar = 'a!a'

  const mutation = mutations.setUsername
  await expect(client.mutate({mutation, variables: {username: usernameTooShort}})).rejects.toThrow(
    /ClientError: Username .* does not validate/,
  )
  await expect(client.mutate({mutation, variables: {username: usernameTooLong}})).rejects.toThrow(
    /ClientError: Username .* does not validate/,
  )
  await expect(client.mutate({mutation, variables: {username: usernameBadChar}})).rejects.toThrow(
    /ClientError: Username .* does not validate/,
  )
})

test('changing username succeeds, then can use it to login in lowercase', async () => {
  const {client, password} = await loginCache.getCleanLogin()
  const username = 'TESTERYESnoMAYBEso' + misc.shortRandomString()
  await client.mutate({mutation: mutations.setUsername, variables: {username}})

  // try to login as the user in cognito with that new username, lowered
  const AuthParameters = {USERNAME: username.toLowerCase(), PASSWORD: password}
  const resp = await cognito.userPoolClient.initiateAuth({AuthFlow, AuthParameters}).promise()
  expect(resp).toHaveProperty('AuthenticationResult.AccessToken')
  expect(resp).toHaveProperty('AuthenticationResult.ExpiresIn')
  expect(resp).toHaveProperty('AuthenticationResult.RefreshToken')
  expect(resp).toHaveProperty('AuthenticationResult.IdToken')
})

test('collision on changing username fails, login username is not changed', async () => {
  const {client: ourClient, password: ourPassword} = await loginCache.getCleanLogin()
  const {client: theirClient, password: theirPassword} = await loginCache.getCleanLogin()

  const ourUsername = 'TESTERgotSOMEcase' + misc.shortRandomString()
  await ourClient.mutate({mutation: mutations.setUsername, variables: {username: ourUsername}})

  const theirUsername = 'TESTERYESnoMAYBEso' + misc.shortRandomString()
  await theirClient.mutate({mutation: mutations.setUsername, variables: {username: theirUsername}})

  // try and fail setting user1's username to user2's
  await expect(
    ourClient.mutate({mutation: mutations.setUsername, variables: {username: theirUsername}}),
  ).rejects.toThrow(/ClientError: Username .* already taken /)

  // verify user1 can still login with their original username
  let AuthParameters = {USERNAME: ourUsername.toLowerCase(), PASSWORD: ourPassword}
  let resp = await cognito.userPoolClient.initiateAuth({AuthFlow, AuthParameters}).promise()
  expect(resp).toHaveProperty('AuthenticationResult.AccessToken')
  expect(resp).toHaveProperty('AuthenticationResult.ExpiresIn')
  expect(resp).toHaveProperty('AuthenticationResult.RefreshToken')
  expect(resp).toHaveProperty('AuthenticationResult.IdToken')

  // verify user2 can still login with their original username
  AuthParameters = {USERNAME: theirUsername.toLowerCase(), PASSWORD: theirPassword}
  resp = await cognito.userPoolClient.initiateAuth({AuthFlow, AuthParameters}).promise()
  expect(resp).toHaveProperty('AuthenticationResult.AccessToken')
  expect(resp).toHaveProperty('AuthenticationResult.ExpiresIn')
  expect(resp).toHaveProperty('AuthenticationResult.RefreshToken')
  expect(resp).toHaveProperty('AuthenticationResult.IdToken')
})
