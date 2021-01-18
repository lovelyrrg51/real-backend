/* Misc utils functions for use in tests */

const jpeg = require('jpeg-js')
const gql = require('graphql-tag')

const shortRandomString = () => Math.random().toString(36).substring(7)

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const generateRandomJpeg = (width, height) => {
  const buf = Buffer.alloc(width * height * 4)
  let i = 0
  while (i < buf.length) {
    buf[i++] = Math.floor(Math.random() * 256)
  }
  const imgData = {
    data: buf,
    width: width,
    height: height,
  }
  const quality = 50
  return jpeg.encode(imgData, quality).data
}

const sleepUntilPostProcessed = async (
  gqlClient,
  postId,
  {maxWaitMs = 10 * 1000, pollingIntervalMs = 1000} = {},
) => {
  const queryPost = gql`
    query Post($postId: ID!) {
      post(postId: $postId) {
        postStatus
      }
    }
  `
  const notProcessedStatuses = ['PENDING', 'PROCESSING']
  let waitedMs = 0
  while (waitedMs < maxWaitMs) {
    let postStatus = await gqlClient
      .query({query: queryPost, variables: {postId}})
      .then(({data}) => data.post.postStatus)
    if (!notProcessedStatuses.includes(postStatus)) return
    await sleep(pollingIntervalMs)
    waitedMs += pollingIntervalMs
  }
  throw Error(`Post ${postId} never left statuses ${notProcessedStatuses}`)
}

module.exports = {
  generateRandomJpeg,
  shortRandomString,
  sleep,
  sleepUntilPostProcessed,
}
