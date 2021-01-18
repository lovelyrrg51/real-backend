#!/usr/bin/env node

const AWS = require('aws-sdk')
const dotenv = require('dotenv')
const elasticsearch = require('elasticsearch')
const httpAwsEs = require('http-aws-es')

dotenv.config()
AWS.config = new AWS.Config()

const endpoint = process.env.ELASTICSEARCH_ENDPOINT
if (endpoint === undefined) throw new Error('Env var ELASTICSEARCH_ENDPOINT must be defined')

if (process.argv.length != 3) {
  console.log(`Usage: ${__filename} <query string>`)
  process.exit(1)
}

const main = async (queryStr) => {
  const esClient = elasticsearch.Client({
    hosts: ['https://' + endpoint],
    connectionClass: httpAwsEs,
  })

  let resp = await esClient.search({
    index: 'users',
    size: 20,
    body: {
      query: {
        // This should be kept in sync with the Query.searchUsers.request.vtl mapping template
        multi_match: {
          query: queryStr,
          fields: ['username^2', 'fullName'],
          slop: 2,
          type: 'phrase_prefix',
        },
      },
    },
  })
  console.log(JSON.stringify(resp, null, 2))
}

main(process.argv[2])
