const gql = require('graphql-tag')

const fragments = require('./fragments.js')

module.exports.onNotification = gql`
  subscription OnNotification($userId: ID!) {
    onNotification(userId: $userId) {
      userId
      type
      followedUserId
      matchUserId
      postId
      userChatsWithUnviewedMessagesCount
    }
  }
`

module.exports.onCardNotification = gql`
  subscription OnCardNotification($userId: ID!) {
    onCardNotification(userId: $userId) {
      userId
      type
      card {
        cardId
        title
        subTitle
        action
      }
    }
  }
`

module.exports.onChatMessageNotification = gql`
  subscription OnChatMessageNotification($userId: ID!) {
    onChatMessageNotification(userId: $userId) {
      userId
      type
      message {
        ...ChatMessageFragment
      }
    }
  }
  ${fragments.chatMessage}
`

module.exports.onPostNotification = gql`
  subscription OnPostNotification($userId: ID!) {
    onPostNotification(userId: $userId) {
      userId
      type
      post {
        postId
        postStatus
        isVerified
      }
    }
  }
`
