const crypto = require('crypto')
const pwdGenerator = require('generate-password')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations} = require('../../schema')

let anonClient, anonUserId
const loginCache = new cognito.AppSyncLoginCache()
const AuthFlow = cognito.AuthFlow
jest.retryTimes(1)

const realPublicKeyPem = process.env.REAL_PUBLIC_KEY_PEM
if (realPublicKeyPem === undefined) throw new Error('Env var REAL_PUBLIC_KEY_PEM must be defined')

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())
afterEach(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

const generateValidPassword = () => pwdGenerator.generate({length: 8})
const generateInvalidPassword = () => pwdGenerator.generate({length: 4})

/**
 * How passwords should be encrypted before passing them as an argument in Mutation.setUserPassword().
 * Note that `realPublicKeyPem` is a string that starts with `-----BEGIN PUBLIC KEY-----\n....` and ends
 * with `...\n-----END PUBLIC KEY-----`. The newlines are important - that's the PEM format, not our choice.
 */
const encrypt = (str) => crypto.publicEncrypt(realPublicKeyPem, Buffer.from(str)).toString('base64')

test('Anonymous user cannot setPassword', async () => {
  ;({client: anonClient, userId: anonUserId} = await cognito.getAnonymousAppSyncLogin())
  const password = generateValidPassword()
  await expect(
    anonClient.mutate({mutation: mutations.setPassword, variables: {encryptedPassword: encrypt(password)}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)

  // verify new password does not work
  await misc.sleep(2000)
  await expect(
    cognito.userPoolClient
      .initiateAuth({AuthFlow, AuthParameters: {USERNAME: anonUserId, PASSWORD: password}})
      .promise(),
  ).rejects.toThrow('Incorrect username or password.')
})

test('Cant set password without encrypting it for transit', async () => {
  const {client, password: oldPassword, userId} = await loginCache.getCleanLogin()
  const password = generateValidPassword()
  await expect(
    client.mutate({mutation: mutations.setPassword, variables: {encryptedPassword: password}}),
  ).rejects.toThrow(/ClientError: Unable to decrypt /)

  // verify new password does not work, old password does
  await misc.sleep(2000)
  await expect(
    cognito.userPoolClient
      .initiateAuth({AuthFlow, AuthParameters: {USERNAME: userId, PASSWORD: password}})
      .promise(),
  ).rejects.toThrow('Incorrect username or password.')
  await cognito.userPoolClient
    .initiateAuth({AuthFlow, AuthParameters: {USERNAME: userId, PASSWORD: oldPassword}})
    .promise()
    .then(({AuthenticationResult}) => expect(AuthenticationResult).toBeTruthy())
})

test('Cant set password to invalid password', async () => {
  const {client, password: oldPassword, userId} = await loginCache.getCleanLogin()
  const password = generateInvalidPassword()
  await expect(
    client.mutate({mutation: mutations.setPassword, variables: {encryptedPassword: encrypt(password)}}),
  ).rejects.toThrow(/ClientError: Invalid password/)

  // verify new password does not work, old password does
  await misc.sleep(2000)
  await expect(
    cognito.userPoolClient
      .initiateAuth({AuthFlow, AuthParameters: {USERNAME: userId, PASSWORD: password}})
      .promise(),
  ).rejects.toThrow('Incorrect username or password.')
  await cognito.userPoolClient
    .initiateAuth({AuthFlow, AuthParameters: {USERNAME: userId, PASSWORD: oldPassword}})
    .promise()
    .then(({AuthenticationResult}) => expect(AuthenticationResult).toBeTruthy())
})

test('Set password success', async () => {
  const {client, userId, password: oldPassword} = await loginCache.getCleanLogin()
  const password = generateValidPassword()
  await client
    .mutate({mutation: mutations.setPassword, variables: {encryptedPassword: encrypt(password)}})
    .then(({data: {setUserPassword: user}}) => expect(user.userId).toBe(userId))

  // verify old password does not work, new password does
  await misc.sleep(2000)
  await expect(
    cognito.userPoolClient
      .initiateAuth({AuthFlow, AuthParameters: {USERNAME: userId, PASSWORD: oldPassword}})
      .promise(),
  ).rejects.toThrow('Incorrect username or password.')
  await cognito.userPoolClient
    .initiateAuth({AuthFlow, AuthParameters: {USERNAME: userId, PASSWORD: password}})
    .promise()
    .then(({AuthenticationResult}) => expect(AuthenticationResult).toBeTruthy())
})
