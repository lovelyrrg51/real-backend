#!/usr/bin/env node

const AWS = require('aws-sdk')
const AWSAppSyncClient = require('aws-appsync').default
const dotenv = require('dotenv')
const fs = require('fs')
const gql = require('graphql-tag')
const got = require('got')
const http = require('http')
const moment = require('moment')
const path = require('path')
const prmt = require('prompt')
const tough = require('tough-cookie')
const uuidv4 = require('uuid/v4')
global.fetch = require('cross-fetch')

dotenv.config()
AWS.config = new AWS.Config()

const cognitoClientId = process.env.COGNITO_TESTING_CLIENT_ID
if (cognitoClientId === undefined) throw new Error('Env var COGNITO_TESTING_CLIENT_ID must be defined')

const identityPoolId = process.env.COGNITO_IDENTITY_POOL_ID
if (identityPoolId === undefined) throw new Error('Env var COGNITO_IDENTITY_POOL_ID must be defined')

const userPoolId = process.env.COGNITO_USER_POOL_ID
if (userPoolId === undefined) throw new Error('Env var COGNITO_USER_POOL_ID must be defined')

const appsyncApiUrl = process.env.APPSYNC_GRAPHQL_URL
if (appsyncApiUrl === undefined) throw new Error('Env var APPSYNC_GRAPHQL_URL must be defined')

const cognitoIdentityPoolClient = new AWS.CognitoIdentity({params: {IdentityPoolId: identityPoolId}})
const cognitoUserPoolClient = new AWS.CognitoIdentityServiceProvider({params: {ClientId: cognitoClientId}})

prmt.message = ''
prmt.start()

const facebookHelp = `To generate:
  - create a facebook developer account if needed, get it associated with our facebook app
  - navigate to https://developers.facebook.com/tools/explorer/
  - select our app in the top-right corner
  - copy-paste the access token
`

const googleHelp = `To generate:
  - navigate to https://developers.google.com/oauthplayground/
  - click the settings gear in the top-right corner
  - select 'Use your own OAuth credentials'
  - enter our OAuth Client ID & secret from the web application listed here:
    https://console.developers.google.com/apis/credentials?project=selfly---dev-1566405434462
  - in the box on the bottom left, where it says 'Input your own scopes', enter 'email'
  - click 'Authorize APIs'
  - go through the authentication flow until you're back to the playground
  - click 'Exchange authorization code for tokens'
  - in the response json on the right, copy-paste the **id** token
`

const prmtSchema = {
  properties: {
    authSource: {
      description: 'Where is the user from? Enter `c` for Cognito, `f` for Facebook, or `g` for Google.',
      required: true,
      pattern: /^[cfg]?$/,
    },
    username: {
      description: "User's email, phone or human-readable username?",
      required: true,
      ask: () => prmt.history('authSource').value === 'c',
    },
    password: {
      description: "User's password?",
      required: true,
      hidden: true,
      ask: () => prmt.history('authSource').value === 'c',
    },
    facebookAccessToken: {
      description: `A facebook access token for our app for the User? ${facebookHelp}?`,
      required: true,
      ask: () => prmt.history('authSource').value === 'f',
    },
    googleIdToken: {
      description: `A google **id** (not access) token for the User? ${googleHelp}?`,
      required: true,
      ask: () => prmt.history('authSource').value === 'g',
    },
    postType: {
      description: 'Type of post to add? Enter `t` for TEXT_ONLY, `i` for IMAGE, or `v` for VIDEO',
      required: true,
    },
    imageFormat: {
      description: 'Format of image to add? Enter `j` for JPEG or `h` for HEIC',
      required: true,
      ask: () => prmt.history('postType').value === 'i',
    },
    path: {
      description: 'Path to image or video file to upload? Ex: `./image.jpeg` ',
      required: true,
      ask: () => prmt.history('postType').value !== 't',
    },
    text: {
      description: 'Text for the post?',
      required: false,
    },
  },
}

// Effectively the main() function
prmt.get(prmtSchema, async (err, result) => {
  if (err) {
    console.log(err)
    return 1
  }

  const token = await (async () => {
    if (result.authSource === 'c') {
      process.stdout.write('Signing cognito user in...')
      const tokens = await generateCognitoTokens(result.username, result.password)
      process.stdout.write(' done.\n')
      return tokens.IdToken
    }
    if (result.authSource === 'f') return result.facebookAccessToken
    if (result.authSource === 'g') return result.googleIdToken
    throw `Unrecognized auth source '${result.authSource}'`
  })()

  process.stdout.write('Exchanging auth token for graphql-authorized JWT token...')
  const creds = await generateGQLCredentials(result.authSource, token)
  const awsCredentials = new AWS.Credentials(creds.AccessKeyId, creds.SecretKey, creds.SessionToken)
  const appsyncClient = new AWSAppSyncClient(
    {
      url: appsyncApiUrl,
      region: AWS.config.region,
      auth: {
        type: 'AWS_IAM',
        credentials: awsCredentials,
      },
      disableOffline: true,
    },
    {
      defaultOptions: {
        query: {
          fetchPolicy: 'no-cache',
        },
      },
    },
  )
  process.stdout.write(' done.\n')

  const postId = uuidv4()
  let resp
  if (result.postType === 't') {
    if (!result.text) {
      throw 'TEXT_ONLY posts must have text'
    }

    process.stdout.write('Adding post...')
    const variables = {postId, text: result.text}
    await appsyncClient.mutate({mutation: addTextOnlyPost, variables})
    process.stdout.write(' done.\n')
    resp = await appsyncClient.query({query: getPost, variables: {postId}})
  } else {
    process.stdout.write('Reading image or video from disk...')
    const obj = fs.readFileSync(result.path)
    process.stdout.write(' done.\n')

    let uploadUrl
    process.stdout.write('Adding pending post...')
    if (result.postType === 'i') {
      const imageFormat = result.imageFormat === 'h' ? 'HEIC' : 'JPEG'
      const variables = {postId, text: result.text, imageFormat}
      resp = await appsyncClient.mutate({mutation: addImagePost, variables})
      uploadUrl = resp.data.addPost.imageUploadUrl
    }
    if (result.postType === 'v') {
      const variables = {postId, text: result.text}
      resp = await appsyncClient.mutate({mutation: addVideoPost, variables})
      uploadUrl = resp.data.addPost.videoUploadUrl
    }
    process.stdout.write(' done.\n')

    process.stdout.write('Uploading media...')
    await uploadMedia(obj, uploadUrl)
    process.stdout.write(' done.\n')

    process.stdout.write('Waiting for upload to be processed...')
    do {
      await new Promise((resolve) => setTimeout(resolve, 1000)) // sleep one second
      process.stdout.write('.')
      resp = await appsyncClient.query({query: getPost, variables: {postId}})
    } while (['PENDING', 'PROCESSING'].includes(resp.data.post.postStatus))
    process.stdout.write(' done.\n')
  }

  if (resp.data.post.postStatus === 'ERROR') {
    process.stdout.write('Error processing upload. Invalid upload?\n')
  } else {
    const post = resp.data.post

    // set up cookie jar if this is a video post
    const cookieJar = new tough.CookieJar()
    if (post.postType === 'VIDEO') {
      const url = post.video.urlMasterM3U8
      const cookies = post.video.accessCookies
      const expires = moment(cookies.expiresAt).toDate().toUTCString()
      const cookieProps = `Secure; Domain=${cookies.domain}; Path=${cookies.path}; Expires=${expires}`
      cookieJar.setCookie(`CloudFront-Policy=${cookies.policy}; ${cookieProps}`, url)
      cookieJar.setCookie(`CloudFront-Signature=${cookies.signature}; ${cookieProps}`, url)
      cookieJar.setCookie(`CloudFront-Key-Pair-Id=${cookies.keyPairId}; ${cookieProps}`, url)
    }

    process.stdout.write('Post successfully added.\n')
    process.stdout.write('Opening http server to display the new post.\n')

    const port = 1337
    http
      .createServer((req, res) => {
        if (req.url === '/') {
          process.stdout.write(`Serving url '${req.url}'\n`)
          res.write('<html><head></head><body>')
          res.write(`<p>Post: <i>${post.postId}</i> by user <i>${post.postedBy.username}</i></p>`)
          res.write(`<p>At: ${post.postedAt}</p>`)
          res.write(`<p>Type: ${post.postType}</p>`)
          res.write(`<p>Text: ${post.text}</p>`)
          if (post.postType === 'VIDEO') {
            // https://github.com/video-dev/hls.js/blob/v0.13.2/README.md
            const videoMasterM3U8 = '/video-hls/video.m3u8'
            const hlsType = 'application/vnd.apple.mpegurl'
            const videoId = 'video'
            res.write(`<p>HLS: <video id="${videoId}" controls></video></p>`)
            res.write('<script src="https://cdn.jsdelivr.net/npm/hls.js@v0.13.2"></script>')
            res.write('<script>')
            res.write(`var video = document.getElementById("${videoId}");`)
            res.write(`if (video.canPlayType("${hlsType}")) {video.src = "${videoMasterM3U8}"}`)
            res.write(`else {var hls = new Hls(); hls.loadSource("${videoMasterM3U8}"); hls.attachMedia(video)}`)
            res.write('</script>')
          }
          if (post.postType === 'IMAGE' || post.postType === 'VIDEO') {
            res.write('<p>64p: <img src="/image/url64p"></p>')
            res.write('<p>480p: <img src="/image/url480p"></p>')
            res.write('<p>1080p: <img src="/image/url1080p"></p>')
            res.write('<p>4k: <img src="/image/url4k"></p>')
            res.write('<p>Native: <img src="/image/url"></p>')
          }
          res.end('</body></html>')
        }

        // Proxying images. Not necessary because with signed urls, no same-origin policies to worry about
        if (req.url.startsWith('/image/')) {
          process.stdout.write(`Proxing url '${req.url}'\n`)
          const filename = path.basename(req.url)
          const cfUrl = post.image[filename]
          got.stream(cfUrl).pipe(res)
        }

        // Proxying video files. Allows us to get around browser's same-origin policies as they apply to cookies
        if (req.url.startsWith('/video-hls/')) {
          process.stdout.write(`Proxing url '${req.url}'\n`)
          const videoDir = path.dirname(post.video.urlMasterM3U8)
          const filename = path.basename(req.url)
          const cfUrl = `${videoDir}/${filename}`
          got.stream(cfUrl, {cookieJar}).pipe(res)
        }
      })
      .listen(port)
    process.stdout.write(`Http server open at http://localhost:${port} (ctrl-c to close)\n`)
  }
})

const addImagePost = gql`
  mutation AddImagePost($postId: ID!, $text: String, $imageFormat: ImageFormat) {
    addPost(
      postId: $postId
      text: $text
      imageInput: {takenInReal: true, originalFormat: "HEIC", imageFormat: $imageFormat}
    ) {
      postId
      imageUploadUrl
    }
  }
`

const addVideoPost = gql`
  mutation AddVideoPost($postId: ID!, $text: String) {
    addPost(postId: $postId, text: $text, postType: VIDEO) {
      postId
      videoUploadUrl
    }
  }
`

const addTextOnlyPost = gql`
  mutation AddTextOnlyPost($postId: ID!, $text: String!) {
    addPost(postId: $postId, text: $text, postType: TEXT_ONLY) {
      postId
    }
  }
`

const getPost = gql`
  query GetPost($postId: ID!) {
    post(postId: $postId) {
      postId
      postType
      postStatus
      postedBy {
        userId
        username
      }
      postedAt
      text
      image {
        url
        url4k
        url1080p
        url480p
        url64p
      }
      video {
        urlMasterM3U8
        accessCookies {
          domain
          path
          expiresAt
          policy
          signature
          keyPairId
        }
      }
    }
  }
`

const uploadMedia = async (obj, url) => {
  return got.put(url, {body: obj, headers: {'Content-Type': 'image/jpeg'}})
}

const generateCognitoTokens = async (username, password) => {
  // sign them in
  const resp = await cognitoUserPoolClient
    .initiateAuth({
      AuthFlow: 'USER_PASSWORD_AUTH',
      AuthParameters: {USERNAME: username, PASSWORD: password},
    })
    .promise()
  return resp.AuthenticationResult
}

const generateGQLCredentials = async (authSource, token) => {
  const loginsKey = (() => {
    if (authSource === 'c') return `cognito-idp.${AWS.config.region}.amazonaws.com/${userPoolId}`
    if (authSource === 'f') return 'graph.facebook.com'
    if (authSource === 'g') return 'accounts.google.com'
    throw `Unrecognized auth source '${authSource}'`
  })()
  const Logins = {[loginsKey]: token}

  // add the user to the identity pool
  const idResp = await cognitoIdentityPoolClient.getId({Logins}).promise()
  const userId = idResp.IdentityId

  // get credentials for appsync from the identity pool
  const resp = await cognitoIdentityPoolClient.getCredentialsForIdentity({IdentityId: userId, Logins}).promise()
  return resp.Credentials
}
