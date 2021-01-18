const dotenv = require('dotenv')
const got = require('got')

const cognito = require('../../utils/cognito')
jest.retryTimes(1)

dotenv.config()

const api_key = process.env.REAL_AUTH_API_KEY
if (api_key === undefined) throw new Error('Env var REAL_AUTH_API_KEY must be defined')

const api_root = process.env.REAL_AUTH_API_ROOT
if (api_root === undefined) throw new Error('Env var REAL_AUTH_API_ROOT must be defined')

const loginCache = new cognito.AppSyncLoginCache()

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

const uri = api_root + '/username/status'
const headers = {'x-api-key': api_key}

test('Malformed requests fail', async () => {
  // No api key
  await got.get(uri, {throwHttpErrors: false}).then(({statusCode, body}) => {
    expect(statusCode).toBe(403)
    expect(JSON.parse(body)).toEqual({message: 'Forbidden'})
  })

  // No username query param
  await got.get(uri, {throwHttpErrors: false, headers}).then(({statusCode, body}) => {
    expect(statusCode).toBe(400)
    expect(JSON.parse(body)).toEqual({message: 'Query parameter `username` is required'})
  })
})

test('Invalid usernames', async () => {
  // too short
  await got
    .get(uri, {searchParams: {username: 'ab'}, headers})
    .json()
    .then((data) => expect(data).toEqual({status: 'INVALID'}))

  // bad char
  await got
    .get(uri, {searchParams: {username: 'aaa!aaa'}, headers})
    .json()
    .then((data) => expect(data).toEqual({status: 'INVALID'}))

  // bad char
  await got
    .get(uri, {searchParams: {username: 'aaa-aaa'}, headers})
    .json()
    .then((data) => expect(data).toEqual({status: 'INVALID'}))
})

test('Username availability', async () => {
  const {username: takenUsername} = await loginCache.getCleanLogin()

  // not available
  await got
    .get(uri, {searchParams: {username: takenUsername}, headers})
    .json()
    .then((data) => expect(data).toEqual({status: 'NOT_AVAILABLE'}))

  // available
  await got
    .get(uri, {searchParams: {username: takenUsername + 'aa_cc'}, headers})
    .json()
    .then((data) => expect(data).toEqual({status: 'AVAILABLE'}))
})
