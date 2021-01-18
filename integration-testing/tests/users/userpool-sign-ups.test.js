const AWS = require('aws-sdk')
const dotenv = require('dotenv')
const pwdGenerator = require('generate-password')
const uuidv4 = require('uuid/v4')

jest.retryTimes(1)
dotenv.config()

const cognitoClientId = process.env.COGNITO_TESTING_CLIENT_ID
if (cognitoClientId === undefined) throw new Error('Env var COGNITO_TESTING_CLIENT_ID must be defined')

const cognitoClient = new AWS.CognitoIdentityServiceProvider({params: {ClientId: cognitoClientId}})

// All users the test client creates must have this family name (or the sign up
// will be rejected). This is to make it easier to clean them out later.
const familyName = 'TESTER'

function generatePassword() {
  return pwdGenerator.generate({length: 8})
}

// TODO: make an effort to clean up after ourselves and not leave testing users in cognito

describe('Test sign-ups to cognito user pool', () => {
  test('cant sign up with invalid username (not a uuid v4)', async () => {
    const username = 'not-a-uuid'
    const password = generatePassword()

    await expect(
      cognitoClient
        .signUp({
          Username: username,
          Password: password,
          UserAttributes: [
            {
              Name: 'family_name',
              Value: familyName,
            },
          ],
        })
        .promise(),
    ).rejects.toThrow(/Invalid username format/)
  })

  test('cant sign up with uppercase characters in email', async () => {
    const username = 'us-east-1:' + uuidv4()
    const password = generatePassword()
    const email = 'success+NOTOK@simulator.amazonses.com'

    await expect(
      cognitoClient
        .signUp({
          Username: username,
          Password: password,
          UserAttributes: [
            {
              Name: 'family_name',
              Value: familyName,
            },
            {
              Name: 'email',
              Value: email,
            },
          ],
        })
        .promise(),
    ).rejects.toThrow(/User's email .* has upper case characters/)
  })

  test('cant sign up with testing client without setting family name to correct value', async () => {
    const username = 'us-east-1:' + uuidv4()
    const password = generatePassword()

    await expect(
      cognitoClient
        .signUp({
          Username: username,
          Password: password,
        })
        .promise(),
    ).rejects.toThrow(/Invalid family_name/)

    await expect(
      cognitoClient
        .signUp({
          Username: username,
          Password: password,
          UserAttributes: [
            {
              Name: 'family_name',
              Value: 'thefailers',
            },
          ],
        })
        .promise(),
    ).rejects.toThrow(/Invalid family_name/)
  })

  test('users signed up with testing client can sign in and have expected properties', async () => {
    const username = 'us-east-1:' + uuidv4()
    const password = generatePassword()

    const respSignUp = await cognitoClient
      .signUp({
        Username: username,
        Password: password,
        UserAttributes: [
          {
            Name: 'family_name',
            Value: familyName,
          },
        ],
        ClientMetadata: {autoConfirmUser: 'true'},
      })
      .promise()
    expect(respSignUp.UserConfirmed).toBe(true)

    // sign in to get an access token
    const respSignIn = await cognitoClient
      .initiateAuth({
        AuthFlow: 'USER_PASSWORD_AUTH',
        AuthParameters: {USERNAME: username, PASSWORD: password},
      })
      .promise()
    const accessToken = respSignIn.AuthenticationResult.AccessToken

    // get the user details, check the email and phone were auto-verified
    const user = await cognitoClient.getUser({AccessToken: accessToken}).promise()
    expect(user.Username).toBe(username)
    const userAttrs = new Map(user.UserAttributes.map((ua) => [ua.Name, ua.Value]))
    expect(userAttrs.get('sub')).toBeTruthy()
    expect(userAttrs.get('family_name')).toBe(familyName)
  })

  test('users signed up with testing client have email & phone auto-verified', async () => {
    const username = 'us-east-1:' + uuidv4()
    const password = generatePassword()
    const email = 'success+any@simulator.amazonses.com'
    const phone = '+14155551313'

    await cognitoClient
      .signUp({
        Username: username,
        Password: password,
        UserAttributes: [
          {
            Name: 'family_name',
            Value: familyName,
          },
          {
            Name: 'email',
            Value: email,
          },
          {
            Name: 'phone_number',
            Value: phone,
          },
        ],
        ClientMetadata: {autoConfirmUser: 'true'},
      })
      .promise()

    // sign in to get an access token
    const respSignIn = await cognitoClient
      .initiateAuth({
        AuthFlow: 'USER_PASSWORD_AUTH',
        AuthParameters: {USERNAME: username, PASSWORD: password},
      })
      .promise()
    const accessToken = respSignIn.AuthenticationResult.AccessToken

    // get the user details, check the email and phone were auto-verified
    const user = await cognitoClient.getUser({AccessToken: accessToken}).promise()
    expect(user.Username).toBe(username)
    const userAttrs = new Map(user.UserAttributes.map((ua) => [ua.Name, ua.Value]))
    expect(userAttrs.get('email')).toBe(email)
    expect(userAttrs.get('email_verified')).toBe('true')
    expect(userAttrs.get('phone_number')).toBe(phone)
    expect(userAttrs.get('phone_number_verified')).toBe('true')
  })

  test('users signed up with testing client auto imm. sign in with username, email or phone', async () => {
    const username = 'us-east-1:' + uuidv4()
    const password = generatePassword()
    const email = 'success+any@simulator.amazonses.com'
    const phone = '+14155551314'

    const respSignUp = await cognitoClient
      .signUp({
        Username: username,
        Password: password,
        UserAttributes: [
          {
            Name: 'family_name',
            Value: familyName,
          },
          {
            Name: 'email',
            Value: email,
          },
          {
            Name: 'phone_number',
            Value: phone,
          },
        ],
        ClientMetadata: {autoConfirmUser: 'true'},
      })
      .promise()
    expect(respSignUp.UserConfirmed).toBe(true)

    // test signing in with userId (aka cognito 'username') (not really a normal use case, but useful when testing)
    const respUsername = await cognitoClient
      .initiateAuth({
        AuthFlow: 'USER_PASSWORD_AUTH',
        AuthParameters: {USERNAME: username, PASSWORD: password},
      })
      .promise()
    expect(respUsername).toHaveProperty('AuthenticationResult.AccessToken')
    expect(respUsername).toHaveProperty('AuthenticationResult.ExpiresIn')
    expect(respUsername).toHaveProperty('AuthenticationResult.RefreshToken')
    expect(respUsername).toHaveProperty('AuthenticationResult.IdToken')

    // test signing in with email
    const respEmail = await cognitoClient
      .initiateAuth({
        AuthFlow: 'USER_PASSWORD_AUTH',
        AuthParameters: {USERNAME: email, PASSWORD: password},
      })
      .promise()
    expect(respEmail).toHaveProperty('AuthenticationResult.AccessToken')
    expect(respEmail).toHaveProperty('AuthenticationResult.ExpiresIn')
    expect(respEmail).toHaveProperty('AuthenticationResult.RefreshToken')
    expect(respEmail).toHaveProperty('AuthenticationResult.IdToken')

    // test signing in with phone
    const respPhone = await cognitoClient
      .initiateAuth({
        AuthFlow: 'USER_PASSWORD_AUTH',
        AuthParameters: {USERNAME: phone, PASSWORD: password},
      })
      .promise()
    expect(respPhone).toHaveProperty('AuthenticationResult.AccessToken')
    expect(respPhone).toHaveProperty('AuthenticationResult.ExpiresIn')
    expect(respPhone).toHaveProperty('AuthenticationResult.RefreshToken')
    expect(respPhone).toHaveProperty('AuthenticationResult.IdToken')
  })

  test('Cant use CUSTOM_AUTH AuthFlow', async () => {
    const username = 'us-east-1:' + uuidv4()
    const password = generatePassword()
    const email = 'success+any@simulator.amazonses.com'

    let resp = await cognitoClient
      .signUp({
        Username: username,
        Password: password,
        UserAttributes: [
          {
            Name: 'family_name',
            Value: familyName,
          },
          {
            Name: 'email',
            Value: email,
          },
        ],
        ClientMetadata: {autoConfirmUser: 'true'},
      })
      .promise()
    expect(resp.UserConfirmed).toBe(true)

    // verify we can't use the custom auth flow
    await expect(
      cognitoClient
        .initiateAuth({
          AuthFlow: 'CUSTOM_AUTH',
          AuthParameters: {USERNAME: username, PASSWORD: password},
        })
        .promise(),
    ).rejects.toThrow(/CUSTOM_AUTH is not enabled/)
    await expect(
      cognitoClient
        .initiateAuth({
          AuthFlow: 'CUSTOM_AUTH',
          AuthParameters: {USERNAME: email, PASSWORD: password},
        })
        .promise(),
    ).rejects.toThrow(/CUSTOM_AUTH is not enabled/)

    // verify we can't use the custom auth flow
    resp = await cognitoClient
      .initiateAuth({
        AuthFlow: 'USER_PASSWORD_AUTH',
        AuthParameters: {USERNAME: email, PASSWORD: password},
      })
      .promise()
    expect(resp).toHaveProperty('AuthenticationResult.AccessToken')
    expect(resp).toHaveProperty('AuthenticationResult.ExpiresIn')
    expect(resp).toHaveProperty('AuthenticationResult.RefreshToken')
    expect(resp).toHaveProperty('AuthenticationResult.IdToken')
  })
})
